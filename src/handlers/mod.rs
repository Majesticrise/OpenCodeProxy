pub mod forward;
pub mod models;
pub mod routing;
pub mod upstream;

pub use forward::chat_completions;
pub use models::list_models;
pub use routing::{api_get_routing, api_set_routing};
pub use upstream::{api_upstream_models, api_upstream_refresh};

// 保留原有的非拆分函数
use axum::{
    extract::State,
    http::{StatusCode, Uri},
    response::{Html, IntoResponse},
    Json,
};
use serde_json::json;

use crate::state::AppState;

pub async fn api_metrics(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::to_value(state.metrics.snapshot()).unwrap_or_else(|_| json!({})))
}

pub async fn root() -> impl IntoResponse {
    Html(include_str!("../../web/index.html"))
}

pub async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/static/");
    match path {
        "style.css" => (
            [(axum::http::header::CONTENT_TYPE, "text/css")],
            include_str!("../../web/style.css"),
        ).into_response(),
        "app.js" => (
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            include_str!("../../web/app.js"),
        ).into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let inner = state.pool.inner.read().await;
    let pool_size = inner.proxies.len();
    let running = inner.is_running.load(Ordering::Relaxed);
    let avg_latency = if pool_size > 0 {
        let now = std::time::Instant::now();
        let avail: Vec<_> = inner
            .proxies
            .iter()
            .filter(|p| p.available(now) && p.latency > 0.0)
            .collect();
        if avail.is_empty() {
            0.0
        } else {
            avail.iter().map(|p| p.latency).sum::<f64>() / avail.len() as f64
        }
    } else {
        0.0
    };
    let total_requests = inner.total_requests.load(Ordering::Relaxed);
    drop(inner);
    let fetching = state.pool.is_fetching.load(Ordering::Relaxed);
    Json(json!({
        "running": running, "pool_size": pool_size, "fetching": fetching,
        "total_requests": total_requests, "avg_latency": avg_latency
    }))
}

pub async fn api_logs(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!(state.log_buffer.get_all()))
}

pub async fn api_toggle(State(state): State<AppState>) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let inner = state.pool.inner.read().await;
    let current = inner.is_running.load(Ordering::Relaxed);
    inner.is_running.store(!current, Ordering::Relaxed);
    tracing::info!("Proxy service toggled: {}", if !current { "RUNNING" } else { "STOPPED" });
    Json(json!({ "running": !current }))
}

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let upstream = crate::config::ZEN_UPSTREAM.clone();
    let snapshot = state.metrics.snapshot();
    let (total, available, proven, fetching) = {
        let inner = state.pool.inner.read().await;
        let now = std::time::Instant::now();
        (
            inner.proxies.len(),
            inner.proxies.iter().filter(|p| p.available(now)).count(),
            inner
                .proxies
                .iter()
                .filter(|p| p.available(now) && p.success_count > 0)
                .count(),
            state.pool.is_fetching.load(Ordering::Relaxed),
        )
    };
    Json(json!({
        "status": "ok",
        "proxies": { "total": total, "available": available, "proven": proven, "fetching": fetching },
        "upstream": upstream,
        "metrics": { "success_rate_1h": snapshot.success_rate, "p95_latency_1h": snapshot.p95_latency },
    }))
}