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

// ───────────────────────── 通用辅助 ─────────────────────────

async fn get_or_build_client(
    state: &AppState,
    proxy: &Option<String>,
) -> Result<reqwest::Client, reqwest::Error> {
    if let Some(c) = state.client_cache.get(proxy).map(|v| v.clone()) {
        return Ok(c);
    }

    let mut b = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_millis(2500))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8);

    if let Some(p) = proxy {
        if let Ok(rp) = reqwest::Proxy::all(p) {
            b = b.proxy(rp);
        }
    }

    let c = b.build()?;
    state.client_cache.insert(proxy.clone(), c.clone());
    Ok(c)
}

async fn pick_unused_proxy(
    state: &AppState,
    tried: &std::collections::HashSet<String>,
) -> Option<String> {
    for _ in 0..16 {
        match state.pool.get_proxy().await {
            Some(p) if !tried.contains(&p) => return Some(p),
            Some(_) => continue,
            None => return None,
        }
    }
    None
}

// ───────────────────────── 静态与状态端点 ─────────────────────────

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
        let now = std::time::Instant::now();
        let avail: Vec<&crate::proxy::ProxyEntry> = inner
            .proxies
            .iter()
            .filter(|p| p.available(now))
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
    // ⚠️ 这份列表必须与上游 /v1/models 保持一致
    // 上游换模型时，这里也要同步更新
    let models = vec![
        json!({"id": "deepseek-v4-flash-free", "object": "model", "owned_by": "deepseek"}),
        json!({"id": "big-pickle", "object": "model", "owned_by": "opencode"}),
        json!({"id": "mimo-v2.5-free", "object": "model", "owned_by": "xiaomi"}),
        json!({"id": "nemotron-3-super-free", "object": "model", "owned_by": "nvidia"}),
        json!({"id": "minimax-m3-free", "object": "model", "owned_by": "minimax"}),
    ];
    Json(json!({ "object": "list", "data": models }))
}

// ───────────────────────── 核心转发 ─────────────────────────

pub async fn chat_completions(
    State(state): State<AppState>,
    body_bytes: bytes::Bytes,
) -> impl IntoResponse {
    use crate::metrics::{try_extract_usage_from_chunk, MetricsGuard, TrackerState};
    use std::sync::Mutex;

    // ── 1. 服务开关 ──
    if !state
        .pool
        .inner
        .read()
        .await
        .is_running
        .load(Ordering::Relaxed)
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Proxy service is stopped by user.",
        )
            .into_response();
    }

    // ── 2. JSON 解析 ──
    let body_json: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(j) => j,
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

    state
        .pool
        .inner
        .read()
        .await
        .total_requests
        .fetch_add(1, Ordering::Relaxed);

    // ── 3. 准备转发 body ──
    let mut forward_body = body_json;
    if let Some(obj) = forward_body.as_object_mut() {
        obj.remove("variant");
    }

    // ── 4. 重试循环 ──
    let max_retries = 5usize;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();

    // 用于去重：记录第一个由代理返回的"可能是代理侧"的 4xx/5xx 错误
    // 若下一个代理返回完全相同的 (status, body)，说明不是代理问题
    let mut first_upstream_error: Option<(u16, Vec<u8>)> = None;

    for attempt in 0..max_retries {
        let proxy = pick_unused_proxy(&state, &tried).await;

        if proxy.is_none() && attempt > 0 {
            break;
        }
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

        let client = match get_or_build_client(&state, &proxy).await {
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
            // 网络层错误
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy {
                    state.pool.report_failure(p).await;
                }
                tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                continue;
            }

            // 收到 HTTP 响应
            Ok(resp) => {
                let code = resp.status().as_u16();

                // 2xx：正常返回
                if resp.status().is_success() {
                    if let Some(ref p) = proxy {
                        state.pool.report_success(p).await;
                    }
                    target_resp = Some(resp);
                    break;
                }

                // 429：短冷却重试
                if code == 429 {
                    warn!("Proxy {:?} hit 429. Cooling down...", proxy);
                    if let Some(ref p) = proxy {
                        state.pool.report_rate_limit(p).await;
                    }
                    tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                    continue;
                }

                // 其他 4xx / 5xx：读 body
                let body_bytes = resp.bytes().await.unwrap_or_default();
                let body_str = String::from_utf8_lossy(&body_bytes).into_owned();

                let looks_proxy_related = body_str.contains("Model is unavailable")
                    || body_str.contains("rate limit")
                    || body_str.contains("too many requests")
                    || body_str.contains("forbidden")
                    || body_str.contains("blocked")
                    || code >= 500;

                if looks_proxy_related {
                    // ⭐ 关键：判断是否与上一个代理返回完全相同
                    let is_same_as_first = first_upstream_error
                        .as_ref()
                        .map(|(c, b)| *c == code && b.as_slice() == body_bytes.as_slice())
                        .unwrap_or(false);

                    if is_same_as_first {
                        warn!(
                            "Upstream returned identical {} from multiple proxies ({}). \
                             This is NOT a proxy issue — returning immediately.",
                            code,
                            body_str.chars().take(160).collect::<String>()
                        );
                        // 该代理也标记失败（它确实没帮上忙）
                        if let Some(ref p) = proxy {
                            state.pool.report_failure(p).await;
                        }
                        return Response::builder()
                            .status(code)
                            .header("content-type", "application/json")
                            .body(Body::from(body_bytes))
                            .unwrap()
                            .into_response();
                    }

                    // 第一次遇到：记下来，冷却重试
                    if first_upstream_error.is_none() {
                        first_upstream_error = Some((code, body_bytes.clone()));
                    }

                    warn!(
                        "Proxy {:?} got {} ({}). Treating as proxy-side failure, retrying…",
                        proxy,
                        code,
                        body_str.chars().take(160).collect::<String>()
                    );
                    if let Some(ref p) = proxy {
                        state.pool.report_failure(p).await;
                    }
                    tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                    continue;
                }

                // 请求本身的问题（模型名错、schema 错）：直接返回，不重试
                warn!("Upstream returned {} (non-retryable): {}", code, body_str);
                return Response::builder()
                    .status(code)
                    .header("content-type", "application/json")
                    .body(Body::from(body_bytes))
                    .unwrap()
                    .into_response();
            }
        }
    }

    // ── 5. 直连兜底 ──
    let resp = match target_resp {
        Some(r) => r,
        None => {
            warn!("All proxies failed. Falling back to direct connection.");
            // ⭐ 修正：直连时清空 last_proxy，避免日志误导
            last_proxy = None;

            let client = match get_or_build_client(&state, &None).await {
                Ok(c) => c,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create direct client: {}", e),
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

    // ── 6. 构造响应头 ──
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut headers = HeaderMap::new();
    for (key, value) in resp.headers().iter() {
        let name = key.as_str().to_lowercase();
        if name == "content-encoding"
            || name == "content-length"
            || name == "transfer-encoding"
            || name == "connection"
        {
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

    // ── 7. 非流式分支 ──
    if !is_stream {
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to read non-stream body: {}", e);
                return (StatusCode::BAD_GATEWAY, format!("read body: {e}")).into_response();
            }
        };

        state.metrics.active_requests.fetch_add(1, Ordering::Relaxed);

        let tracker = Arc::new(TrackerState {
            start: std::time::Instant::now(),
            model: model.clone(),
            is_stream: false,
            status: status.as_u16(),
            usage: Mutex::new(None),
        });

        let mut buf = Vec::new();
        if let Some(u) = try_extract_usage_from_chunk(&bytes, &mut buf) {
            *tracker.usage.lock().unwrap() = Some(u);
        }

        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        if let Ok(cl) = HeaderValue::from_str(&bytes.len().to_string()) {
            headers.insert(axum::http::header::CONTENT_LENGTH, cl);
        }

        let mut response = Response::new(Body::from(bytes));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
            .extensions_mut()
            .insert(Arc::new(MetricsGuard {
                metrics: state.metrics.clone(),
                tracker,
            }));
        return response;
    }

    // ── 8. 流式分支 ──
    state.metrics.active_requests.fetch_add(1, Ordering::Relaxed);
    state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);

    let tracker = Arc::new(TrackerState {
        start: std::time::Instant::now(),
        model: model.clone(),
        is_stream: true,
        status: status.as_u16(),
        usage: Mutex::new(None),
    });

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

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;

    response
        .extensions_mut()
        .insert(Arc::new(MetricsGuard {
            metrics: state.metrics.clone(),
            tracker,
        }));

    response
}