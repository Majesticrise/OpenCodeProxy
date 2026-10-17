mod config;
mod handlers;
mod logger;
mod metrics;
mod middleware;
mod proxy;
mod state;
use dashmap::DashMap;

use crate::{
    logger::{LogBuffer, LogLayer},
    metrics::MetricsStore,
    proxy::ProxyPool,
    state::AppState,
};
use axum::{
    // 重命名导入，避免与 crate 内的 `mod middleware` 冲突
    middleware as axum_middleware,
    routing::{get, post},
    Router,
};
use std::{env, path::Path, sync::Arc, time::Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[tokio::main]
async fn main() {
    let log_buffer = Arc::new(LogBuffer::new());
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| config::LOG_LEVEL.to_string());

    // 终端输出走 fmt::layer，Web 缓冲走自定义 LogLayer
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new(filter))
        .with(tracing_subscriber::fmt::layer().with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout())))
        .with(LogLayer::new(log_buffer.clone()));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("============================================================");
    info!("Zen Proxy (Rust version v1.2.6 - WebUI Integrated)");
    info!("============================================================");

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

    let state = AppState {
        pool: proxy_pool,
        log_buffer: log_buffer.clone(),
        metrics: Arc::new(MetricsStore::new()),
        client_cache: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/", get(handlers::root))
        .route("/static/*path", get(handlers::static_handler))
        .route("/health", get(handlers::health))
        .route("/api/status", get(handlers::api_status))
        .route("/api/logs", get(handlers::api_logs))
        .route("/api/metrics", get(handlers::api_metrics))
        .route("/api/toggle", post(handlers::api_toggle))
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/chat/completions", post(handlers::chat_completions))
        // 关键：用 crate 内部的 middleware 模块；axum 的 from_fn 通过别名调用
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