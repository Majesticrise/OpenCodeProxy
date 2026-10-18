mod config;
mod handlers;
mod logger;
mod metrics;
mod middleware;
mod opencode_identity;
mod proxy;
mod state;
use dashmap::DashMap;

use crate::{
    config::RoutingConfig,
    logger::{LogBuffer, LogLayer},
    metrics::MetricsStore,
    proxy::ProxyPool,
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

    // 终端输出走 fmt::layer，Web 缓冲走自定义 LogLayer
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

    // ── Metrics（累计数字持久化到磁盘） ──
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

    // ── 组装 AppState ──
    let state = AppState {
        pool: proxy_pool,
        log_buffer: log_buffer.clone(),
        metrics,
        client_cache: Arc::new(DashMap::new()),
        routing: Arc::new(RwLock::new(routing)),
        upstream_models: Arc::new(RwLock::new(Default::default())),
        session_proxy_binding: Arc::new(RwLock::new(HashMap::new())),
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
    axum::serve(listener, app).await.unwrap();
}