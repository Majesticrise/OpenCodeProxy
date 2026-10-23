mod config;
mod handlers;
mod logger;
mod metrics;
mod middleware;
mod opencode_identity;
mod proxy;
mod settings;
mod state;
use dashmap::DashMap;

use crate::{
    config::RoutingConfig,
    logger::{LogBuffer, LogLayer},
    metrics::MetricsStore,
    proxy::ProxyPool,
    settings::RuntimeSettings,
    state::AppState,
};
use axum::{
    middleware as axum_middleware,
    routing::{get, post},
    Router,
};
use std::{
    collections::HashMap,
    env,
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[tokio::main]
async fn main() {
    let log_buffer = Arc::new(LogBuffer::new());
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| config::LOG_LEVEL.to_string());

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new(filter))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout())),
        )
        .with(LogLayer::new(log_buffer.clone()));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("============================================================");
    info!("Zen Proxy (Rust version v1.2.6 - WebUI Integrated)");
    info!("============================================================");

    // ── 代理池 ──
    let proxy_pool = ProxyPool::new(&config::PROXY_FILE);
    let pool_clone = proxy_pool.clone();
    tokio::spawn(async move {
        pool_clone.load_proxies().await;
    });

    let pool_refresh = proxy_pool.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            info!("Periodic proxy pool refresh triggered (1-hour interval)...");
            if !Path::new(&*config::PROXY_FILE).exists() {
                pool_refresh.fetch_free_proxies().await;
            } else {
                info!("Local proxies file is active. Skipping auto public refresh.");
            }
        }
    });

    // ── 模型路由 ──
    let routing = RoutingConfig::from_file_or_env();
    if routing.mapping.is_empty() {
        info!("Routing: no rules configured, models will be passed through as-is.");
    } else {
        let keys: Vec<_> = routing.mapping.keys().cloned().collect();
        info!(
            "Routing: {} rule(s) loaded (fallback_chain={}), keys={:?}",
            routing.mapping.len(),
            routing.enable_fallback_chain,
            keys,
        );
    }

    // ── 运行时设置（匿名/实名模式 + API Key） ──
    let settings = Arc::new(RwLock::new(RuntimeSettings::load()));

    // ── Metrics ──
    let metrics_path = if *config::PERSIST_METRICS {
        Some(std::path::PathBuf::from(&*config::METRICS_FILE))
    } else {
        None
    };
    let metrics = Arc::new(MetricsStore::with_persistence(metrics_path));

    let metrics_for_flush = metrics.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            metrics_for_flush.flush_if_dirty().await;
        }
    });

    // ── AppState ──
    let state = AppState {
        pool: proxy_pool,
        log_buffer: log_buffer.clone(),
        metrics,
        client_cache: Arc::new(DashMap::new()),
        routing: Arc::new(RwLock::new(routing)),
        upstream_models: Arc::new(RwLock::new(Default::default())),
        session_proxy_binding: Arc::new(RwLock::new(HashMap::new())),
        settings,
    };

    let app = Router::new()
        .route("/", get(handlers::root))
        .route("/static/*path", get(handlers::static_handler))
        .route("/health", get(handlers::health))
        .route("/api/status", get(handlers::api_status))
        .route("/api/logs", get(handlers::api_logs))
        .route("/api/metrics", get(handlers::api_metrics))
        .route("/api/toggle", post(handlers::api_toggle))
        .route(
            "/api/routing",
            get(handlers::api_get_routing).post(handlers::api_set_routing),
        )
        .route("/api/upstream/models", get(handlers::api_upstream_models))
        .route("/api/upstream/refresh", post(handlers::api_upstream_refresh))
        // 运行时设置
        .route("/api/settings", get(handlers::api_get_settings))
        .route("/api/settings/mode", post(handlers::api_set_mode))
        .route("/api/settings/apikey", post(handlers::api_set_api_key))
        // OpenAI 兼容端点
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/chat/completions", post(handlers::chat_completions))
        .layer(axum_middleware::from_fn(middleware::strip_auth_middleware))
        .with_state(state);

    let addr = format!("{}:{}", *config::LISTEN_HOST, *config::LISTEN_PORT);
    info!("Listen:    http://{}", addr);
    info!("WebUI:     http://{}/", addr);
    info!("Upstream:  {}", *config::ZEN_UPSTREAM);
    info!("============================================================");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    // 优雅退出：把最近 5s 窗口内还没落盘的累计指标写掉
    info!("Shutdown: flushing metrics totals...");
    metrics_for_flush.flush_blocking();
}

/// Ctrl+C（以及 Unix 下的 SIGTERM）触发优雅停机。
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}