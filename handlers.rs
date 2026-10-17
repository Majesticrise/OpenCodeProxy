use crate::{config::*, state::AppState};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde_json::json;
use std::time::Duration;
use tracing::{error, info, warn};

pub async fn root() -> impl IntoResponse {
    Html(include_str!("../web/index.html"))
}

pub async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/static/");
    match path {
        "style.css" => ([(axum::http::header::CONTENT_TYPE, "text/css")], include_str!("../web/style.css")).into_response(),
        "app.js" => ([(axum::http::header::CONTENT_TYPE, "application/javascript")], include_str!("../web/app.js")).into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let pool_size = inner.proxies.len();
    let running = inner.is_running.load(std::sync::atomic::Ordering::Relaxed);
    let avg_latency = if pool_size > 0 {
        inner.proxies.iter().map(|p| p.1).sum::<f64>() / pool_size as f64
    } else { 0.0 };
    Json(json!({
        "running": running,
        "pool_size": pool_size,
        "fetching": inner.is_fetching,
        "total_requests": inner.total_requests.load(std::sync::atomic::Ordering::Relaxed),
        "avg_latency": avg_latency
    }))
}

pub async fn api_logs(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!(state.log_buffer.get_all()))
}

pub async fn api_toggle(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let current = inner.is_running.load(std::sync::atomic::Ordering::Relaxed);
    inner.is_running.store(!current, std::sync::atomic::Ordering::Relaxed);
    info!("Proxy service toggled: {}", if !current { "RUNNING" } else { "STOPPED" });
    Json(json!({ "running": !current }))
}

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let size = state.pool.inner.read().await.proxies.len();
    Json(json!({ "status": "ok", "proxies": size }))
}

pub async fn list_models() -> impl IntoResponse {
    let models = vec![
        json!({"id": "deepseek-v4-flash-free", "object": "model", "owned_by": "deepseek"}),
        json!({"id": "big-pickle", "object": "model", "owned_by": "opencode"}),
        json!({"id": "mimo-v2.5-free", "object": "model", "owned_by": "xiaomi"}),
        json!({"id": "nemotron-3-super-free", "object": "model", "owned_by": "nvidia"}),
        json!({"id": "minimax-m3-free", "object": "model", "owned_by": "minimax"}),
    ];
    Json(json!({ "object": "list", "data": models }))
}

pub async fn chat_completions(
    State(state): State<AppState>,
    body_bytes: bytes::Bytes,
) -> impl IntoResponse {
    if !state.pool.inner.read().await.is_running.load(std::sync::atomic::Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Proxy service is stopped by user.").into_response();
    }

    let body_json: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(json) => json,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response(),
    };

    let model = body_json.get("model").and_then(|m| m.as_str()).unwrap_or("deepseek-v4-flash-free");
    let is_stream = body_json.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
    info!("→ {} model={} messages", if is_stream { "STREAM" } else { "SYNC  " }, model);

    state.pool.inner.read().await.total_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut forward_body = body_json.clone();
    if let Some(obj) = forward_body.as_object_mut() { obj.remove("variant"); }

    let max_retries = 5;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;

    for attempt in 0..max_retries {
        let proxy = state.pool.get_proxy().await;
        last_proxy = proxy.clone();
        info!("Chat Completion attempt {}/{} using proxy: {:?}", attempt + 1, max_retries, proxy);

        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_millis(2500));
        if let Some(ref p) = proxy {
            if let Ok(rp) = reqwest::Proxy::all(p) { client_builder = client_builder.proxy(rp); }
        }

        let client = match client_builder.build() {
            Ok(c) => c,
            Err(e) => { error!("Failed to build reqwest client: {}", e); continue; }
        };

        let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
        match client.post(&target_url).header("Content-Type", "application/json").json(&forward_body).send().await {
            Ok(resp) if resp.status().as_u16() == 429 => {
                warn!("Proxy {:?} hit 429. Retrying...", proxy);
                if let Some(ref p) = proxy { state.pool.report_failure(p).await; }
                continue;
            }
            Ok(resp) => { target_resp = Some(resp); break; }
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy { state.pool.report_failure(p).await; }
                continue;
            }
        }
    }

    let resp = match target_resp {
        Some(r) => r,
        None => {
            warn!("All proxies failed. Falling back to direct connection.");
            let client = match reqwest::Client::builder().connect_timeout(Duration::from_millis(4000)).timeout(Duration::from_secs(300)).build() {
                Ok(c) => c,
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create client: {}", e)).into_response(),
            };
            let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
            match client.post(&target_url).header("Content-Type", "application/json").json(&forward_body).send().await {
                Ok(r) => r,
                Err(e) => return (StatusCode::BAD_GATEWAY, format!("Upstream direct connection failed: {}", e)).into_response(),
            }
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut headers = HeaderMap::new();
    for (key, value) in resp.headers().iter() {
        let name = key.as_str().to_lowercase();
        if name == "content-encoding" || name == "content-length" || name == "transfer-encoding" { continue; }
        if let Ok(val) = HeaderValue::from_bytes(value.as_bytes()) {
            if let Ok(header_name) = axum::http::header::HeaderName::from_bytes(key.as_str().as_bytes()) {
                headers.insert(header_name, val);
            }
        }
    }

    info!("  ← status={} (routed via: {:?})", status, last_proxy);
    let stream = resp.bytes_stream().map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}