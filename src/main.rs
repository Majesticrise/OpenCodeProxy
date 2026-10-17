mod config;
mod handlers;
mod logger;
mod middleware;
mod proxy;
mod state;

use crate::{logger::LogBuffer, logger::WebLogWriter, proxy::ProxyPool, state::AppState};
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::{env, path::Path, sync::Arc, time::Duration};
use tracing::info;

#[tokio::main]
async fn main() {
    let log_buffer = Arc::new(LogBuffer::new());
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| config::LOG_LEVEL.to_string());

    // 初始化 tracing，将日志同时输出到终端和 Web 端
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(WebLogWriter { buffer: log_buffer.clone() })
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("============================================================");
    info!("Zen Proxy (Rust version v1.2.6 - WebUI Integrated)");
    info!("============================================================");

    let proxy_pool = ProxyPool::new(&config::PROXY_FILE);
    let pool_clone = proxy_pool.clone();
    tokio::spawn(async move { pool_clone.load_proxies().await; });

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
    };

    let app = Router::new()
        .route("/", get(handlers::root))
        .route("/static/*path", get(handlers::static_handler))
        .route("/health", get(handlers::health))
        .route("/api/status", get(handlers::api_status))
        .route("/api/logs", get(handlers::api_logs))
        .route("/api/toggle", post(handlers::api_toggle))
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/chat/completions", post(handlers::chat_completions))
        .layer(middleware::from_fn(middleware::strip_auth_middleware))
        .with_state(state);

    let addr = format!("{}:{}", *config::LISTEN_HOST, *config::LISTEN_PORT);
    info!("Listen:    http://{}", addr);
    info!("WebUI:     http://{}/", addr);
    info!("Upstream:  {}", *config::ZEN_UPSTREAM);
    info!("============================================================");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
