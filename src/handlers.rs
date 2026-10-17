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

/// 按代理 URL 取或建 Client。代理为 None 时表示直连。
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

/// 从未尝试过的代理里抽一个；最多重抽 16 次，避免池内全试过时死循环。
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
        // 只统计未处于冷却期的节点，避免被拖低
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

    // 累计计数不需要回滚，放这里即可
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

    for attempt in 0..max_retries {
        // 选一个未试过的代理
        let proxy = pick_unused_proxy(&state, &tried).await;

        // 池空时 attempt=0 允许直连一次；attempt>0 且仍无新代理则跳出
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

        // 取或建 client
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
            // 网络层错误：换代理重试
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy {
                    state.pool.report_failure(p).await;
                }
                tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                continue;
            }

            // 拿到 HTTP 响应
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

                // 其他 4xx / 5xx：读 body 判断是否"换代理就能过"
                let body_bytes = resp.bytes().await.unwrap_or_default();
                let body_str = String::from_utf8_lossy(&body_bytes).into_owned();

                let looks_proxy_related = body_str.contains("Model is unavailable")
                    || body_str.contains("rate limit")
                    || body_str.contains("too many requests")
                    || body_str.contains("forbidden")
                    || body_str.contains("blocked")
                    || code >= 500;

                if looks_proxy_related {
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

                // 真正属于请求本身的错（模型名错、schema 错等）：原样透传，不再重试
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

    // ── 6. 构造响应头（过滤 hop-by-hop 与长度相关头） ──
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

    // ── 7. 非流式分支：聚合整段 body，避免 chunked 传输 ──
    if !is_stream {
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to read non-stream body: {}", e);
                return (StatusCode::BAD_GATEWAY, format!("read body: {e}")).into_response();
            }
        };

        // 确定要返回成功响应了，才递增活跃计数（与 Guard 同事务）
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

        // 明确告知长度与类型，避免客户端拿到 chunked
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

    // usage 缓冲按字节累积，避免 UTF-8 边界截断
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

    // MetricsGuard 用 Arc 包装（Extensions::insert 要求 T: Clone）
    response
        .extensions_mut()
        .insert(Arc::new(MetricsGuard {
            metrics: state.metrics.clone(),
            tracker,
        }));

    response
}