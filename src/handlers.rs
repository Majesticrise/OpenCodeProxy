use crate::{config::*, state::AppState};
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde_json::json;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tracing::{error, info, warn};

pub async fn api_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    Json(serde_json::to_value(snapshot).unwrap_or_else(|_| json!({})))
}

pub async fn root() -> impl IntoResponse {
    Html(include_str!("../web/index.html"))
}

pub async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/static/");
    match path {
        "style.css" => (
            [(axum::http::header::CONTENT_TYPE, "text/css")],
            include_str!("../web/style.css"),
        )
            .into_response(),
        "app.js" => (
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            include_str!("../web/app.js"),
        )
            .into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let pool_size = inner.proxies.len();
    let running = inner.is_running.load(Ordering::Relaxed);
    let avg_latency = if pool_size > 0 {
        inner.proxies.iter().map(|p| p.1).sum::<f64>() / pool_size as f64
    } else {
        0.0
    };
    let total_requests = inner.total_requests.load(Ordering::Relaxed);
    drop(inner);
    let fetching = state.pool.is_fetching.load(Ordering::Relaxed);
    Json(json!({
        "running": running,
        "pool_size": pool_size,
        "fetching": fetching,
        "total_requests": total_requests,
        "avg_latency": avg_latency
    }))
}

pub async fn api_logs(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!(state.log_buffer.get_all()))
}

pub async fn api_toggle(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let current = inner.is_running.load(Ordering::Relaxed);
    inner.is_running.store(!current, Ordering::Relaxed);
    info!(
        "Proxy service toggled: {}",
        if !current { "RUNNING" } else { "STOPPED" }
    );
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
    use crate::metrics::{try_extract_usage_from_chunk, MetricsGuard, TrackerState};
    use std::sync::Mutex;

    if !state.pool.inner.read().await.is_running.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Proxy service is stopped by user.",
        )
            .into_response();
    }

    let body_json: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(json) => json,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response()
        }
    };

    let model = body_json
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("deepseek-v4-flash-free")
        .to_string();
    let is_stream = body_json
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);
    info!(
        "→ {} model={} messages",
        if is_stream { "STREAM" } else { "SYNC  " },
        model
    );

    // 累计计数不需要回滚，放这里即可
    state
        .pool
        .inner
        .read()
        .await
        .total_requests
        .fetch_add(1, Ordering::Relaxed);

    // 注意：active_requests / active_streams 的 fetch_add 已移到真正构造
    // Response 之前（见下方）。这样任何早退分支都不会泄漏计数。

    // 直接拿走 body_json，不再 clone
    let mut forward_body = body_json;
    if let Some(obj) = forward_body.as_object_mut() {
        obj.remove("variant");
    }

    let max_retries = 5;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();

    for attempt in 0..max_retries {
        // 排除本轮已失败的代理，避免重复抽到同一只
        let proxy = loop {
            match state.pool.get_proxy().await {
                Some(p) if !tried.contains(&p) => break Some(p),
                Some(_) => continue, // 抽到重复，再抽
                None => break None,
            }
        };
        if let Some(ref p) = proxy {
            tried.insert(p.clone());
        }
        last_proxy = proxy.clone();
        info!(
            "Chat Completion attempt {}/{} using proxy: {:?}",
            attempt + 1,
            max_retries,
            proxy
        );

        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_millis(2500));
        if let Some(ref p) = proxy {
            if let Ok(rp) = reqwest::Proxy::all(p) {
                client_builder = client_builder.proxy(rp);
            }
        }

        let client = match client_builder.build() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build reqwest client: {}", e);
                continue;
            }
        };

        let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
        match client
            .post(&target_url)
            .header("Content-Type", "application/json")
            .json(&forward_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().as_u16() == 429 => {
                warn!("Proxy {:?} hit 429. Retrying...", proxy);
                if let Some(ref p) = proxy {
                    state.pool.report_failure(p).await;
                }
                continue;
            }
            Ok(resp) => {
                target_resp = Some(resp);
                break;
            }
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy {
                    state.pool.report_failure(p).await;
                }
                continue;
            }
        }
    }

    let resp = match target_resp {
        Some(r) => r,
        None => {
            warn!("All proxies failed. Falling back to direct connection.");
            let client = match reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(4000))
                .timeout(Duration::from_secs(300))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create client: {}", e),
                    )
                        .into_response()
                }
            };
            let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
            match client
                .post(&target_url)
                .header("Content-Type", "application/json")
                .json(&forward_body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!("Upstream direct connection failed: {}", e),
                    )
                        .into_response()
                }
            }
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut headers = HeaderMap::new();
    for (key, value) in resp.headers().iter() {
        let name = key.as_str().to_lowercase();
        if name == "content-encoding" || name == "content-length" || name == "transfer-encoding" {
            continue;
        }
        if let Ok(val) = HeaderValue::from_bytes(value.as_bytes()) {
            if let Ok(header_name) =
                axum::http::header::HeaderName::from_bytes(key.as_str().as_bytes())
            {
                headers.insert(header_name, val);
            }
        }
    }

    info!("  ← status={} (routed via: {:?})", status, last_proxy);

    // ── 埋点：先递增活跃计数，再构造 tracker / Response ──
    // 递增与 MetricsGuard 的构造在同一个"事务"里；
    // MetricsGuard 挂到 Response.extensions 后，无论流式还是非流式，
    // 响应被 drop 时都会 fetch_sub 对消。
    state.metrics.active_requests.fetch_add(1, Ordering::Relaxed);
    if is_stream {
        state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
    }

    let tracker = Arc::new(TrackerState {
        start: std::time::Instant::now(),
        model: model.clone(),
        is_stream,
        status: status.as_u16(),
        usage: Mutex::new(None),
    });

    // ⚠️ usage 缓冲改为 Vec<u8>，按字节累积。
    // 原实现用 String + from_utf8 会在 chunk 边界落在多字节字符中间时
    // 整个 chunk 被静默丢弃，导致 usage 或后续 SSE 数据丢失。
    let usage_buf = Arc::new(Mutex::new(Vec::<u8>::new()));

    let tracker_for_stream = tracker.clone();
    let buf_for_stream = usage_buf.clone();

    let stream = resp.bytes_stream().map(move |result| {
        if let Ok(ref chunk) = result {
            let mut buf = buf_for_stream.lock().unwrap();
            if let Some(u) = try_extract_usage_from_chunk(&chunk[..], &mut buf) {
                *tracker_for_stream.usage.lock().unwrap() = Some(u);
            }
        }
        result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });

    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;

    // ── 将 Guard 通过 Arc 塞进 response.extensions ──
    // http 1.5+ 的 Extensions::insert 要求 T: Clone + Send + Sync + 'static。
    // MetricsGuard 实现了 Drop（用于在响应 drop 时记录指标并减计数），
    // 如果给它 derive(Clone)，克隆副本析构时会重复触发 Drop，导致计数错乱。
    // 因此这里用 Arc 包装：Arc<MetricsGuard> 天然 Clone，
    // 且只有最后一个强引用被释放时才会真正 drop 内部的 MetricsGuard。
    response.extensions_mut().insert(Arc::new(MetricsGuard {
        metrics: state.metrics.clone(),
        tracker,
    }));

    response
}