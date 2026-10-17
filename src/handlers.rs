use crate::{config::*, state::{AppState, UpstreamCache}};
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
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

// ───────────────────────── 模型路由 ─────────────────────────

fn resolve_candidates(routing: &RoutingConfig, requested: &str) -> Vec<String> {
    if let Some(list) = routing.mapping.get(requested) {
        return list.clone();
    }
    if let Some(list) = routing.mapping.get("*") {
        return list.clone();
    }
    vec![requested.to_string()]
}

// ───────────────────────── 上游探测 ─────────────────────────

async fn probe_upstream(state: &AppState) -> Result<(Vec<String>, String), String> {
    let url = format!("{}/models", *ZEN_UPSTREAM);

    // 先试直连
    let direct = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    match direct.get(&url).send().await {
        Ok(r) if r.status().is_success() => {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(models) = parse_models(&v) {
                    return Ok((models, "direct".to_string()));
                }
            }
        }
        Ok(r) => {
            return Err(format!("direct returned {}", r.status()));
        }
        Err(e) => {
            warn!("Direct probe failed: {}", e);
        }
    }

    // 用代理再试 3 次
    for _ in 0..3 {
        let Some(proxy) = state.pool.get_proxy().await else {
            break;
        };
        let mut b = reqwest::Client::builder().timeout(Duration::from_secs(8));
        if let Ok(rp) = reqwest::Proxy::all(&proxy) {
            b = b.proxy(rp);
        }
        let Ok(c) = b.build() else { continue };
        match c.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                if let Ok(v) = r.json::<serde_json::Value>().await {
                    if let Some(models) = parse_models(&v) {
                        return Ok((models, format!("proxy:{}", proxy)));
                    }
                }
            }
            Ok(_) | Err(_) => continue,
        }
    }

    Err("all probes failed".to_string())
}

fn parse_models(v: &serde_json::Value) -> Option<Vec<String>> {
    let data = v.get("data")?.as_array()?;
    let models: Vec<String> = data
        .iter()
        .filter_map(|m| m.get("id").and_then(|i| i.as_str()).map(String::from))
        .collect();
    if models.is_empty() { None } else { Some(models) }
}

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
    Json(serde_json::to_value(state.metrics.snapshot()).unwrap_or_else(|_| json!({})))
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
        ).into_response(),
        "app.js" => (
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            include_str!("../web/app.js"),
        ).into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let pool_size = inner.proxies.len();
    let running = inner.is_running.load(Ordering::Relaxed);
    let avg_latency = if pool_size > 0 {
        let now = Instant::now();
        let avail: Vec<_> = inner.proxies.iter().filter(|p| p.available(now)).collect();
        if avail.is_empty() { 0.0 } else {
            avail.iter().map(|p| p.latency).sum::<f64>() / avail.len() as f64
        }
    } else { 0.0 };
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
    info!("Proxy service toggled: {}", if !current { "RUNNING" } else { "STOPPED" });
    Json(json!({ "running": !current }))
}

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let size = state.pool.inner.read().await.proxies.len();
    Json(json!({ "status": "ok", "proxies": size }))
}

/// 客户端可见的模型列表：
/// - 有映射表时返回映射的 key（去 "*"）
/// - 否则返回硬编码列表
pub async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    let routing = state.routing.read().unwrap().clone();
    if !routing.mapping.is_empty() {
        let mut models: Vec<_> = routing
            .mapping
            .keys()
            .filter(|k| k.as_str() != "*")
            .map(|id| {
                let candidates = routing.mapping.get(id).cloned().unwrap_or_default();
                json!({
                    "id": id,
                    "object": "model",
                    "owned_by": "zen-proxy",
                    "upstream_candidates": candidates,
                })
            })
            .collect();
        if !models.is_empty() {
            if routing.mapping.contains_key("*") {
                models.push(json!({
                    "id": "*",
                    "object": "model",
                    "owned_by": "zen-proxy",
                    "upstream_candidates": routing.mapping.get("*").cloned().unwrap_or_default(),
                }));
            }
            return Json(json!({ "object": "list", "data": models }));
        }
    }

    let models = vec![
        json!({"id": "deepseek-v4-flash-free", "object": "model", "owned_by": "deepseek"}),
        json!({"id": "big-pickle", "object": "model", "owned_by": "opencode"}),
        json!({"id": "mimo-v2.5-free", "object": "model", "owned_by": "xiaomi"}),
        json!({"id": "nemotron-3-super-free", "object": "model", "owned_by": "nvidia"}),
        json!({"id": "minimax-m3-free", "object": "model", "owned_by": "minimax"}),
    ];
    Json(json!({ "object": "list", "data": models }))
}

// ───────────────────────── 路由配置 API ─────────────────────────

pub async fn api_get_routing(State(state): State<AppState>) -> impl IntoResponse {
    let routing = state.routing.read().unwrap().clone();
    Json(json!(routing))
}

pub async fn api_set_routing(
    State(state): State<AppState>,
    Json(new_routing): Json<RoutingConfig>,
) -> impl IntoResponse {
    let mut guard = state.routing.write().unwrap();
    let before_map = guard.mapping.clone();
    let before_fb = guard.enable_fallback_chain;

    *guard = new_routing;

    info!(
        "Routing config updated: {} entries (fallback={}), was {} entries (fallback={})",
        guard.mapping.len(),
        guard.enable_fallback_chain,
        before_map.len(),
        before_fb,
    );

    Json(json!({ "ok": true, "mapping": guard.mapping }))
}

/// 返回当前缓存的上游模型列表（不触发网络）
pub async fn api_upstream_models(State(state): State<AppState>) -> impl IntoResponse {
    let cache = state.upstream_models.read().unwrap().clone();
    Json(json!({
        "models": cache.models,
        "source": cache.source,
        "last_error": cache.last_error,
        "age_secs": cache.fetched_at.map(|t| t.elapsed().as_secs()),
    }))
}

/// 主动触发一次上游模型探测，刷新缓存
pub async fn api_upstream_refresh(State(state): State<AppState>) -> impl IntoResponse {
    let result = probe_upstream(&state).await;
    let mut cache = state.upstream_models.write().unwrap();
    match result {
        Ok((models, source)) => {
            let count = models.len();
            *cache = UpstreamCache {
                models,
                fetched_at: Some(Instant::now()),
                last_error: None,
                source: Some(source.clone()),
            };
            info!("Upstream model probe ok: {} models via {}", count, source);
            Json(json!({
                "ok": true,
                "models": cache.models,
                "source": source,
            }))
        }
        Err(e) => {
            cache.last_error = Some(e.clone());
            warn!("Upstream model probe failed: {}", e);
            Json(json!({ "ok": false, "error": e }))
        }
    }
}

// ───────────────────────── 核心转发 ─────────────────────────

pub async fn chat_completions(
    State(state): State<AppState>,
    body_bytes: bytes::Bytes,
) -> impl IntoResponse {
    use crate::metrics::{try_extract_usage_from_chunk, MetricsGuard, TrackerState};
    use std::sync::Mutex;

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

    let body_json: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(j) => j,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response()
        }
    };

    let requested_model = body_json
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("deepseek-v4-flash-free")
        .to_string();
    let is_stream = body_json
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    // ── 模型路由解析 ──
    let routing_snapshot = state.routing.read().unwrap().clone();
    let candidates = resolve_candidates(&routing_snapshot, &requested_model);
    let enable_fallback = routing_snapshot.enable_fallback_chain;

    if candidates.len() == 1 && candidates[0] == requested_model {
        info!(
            "→ {} model={} messages",
            if is_stream { "STREAM" } else { "SYNC  " },
            requested_model
        );
    } else {
        info!(
            "→ {} requested={} → candidates={:?}",
            if is_stream { "STREAM" } else { "SYNC  " },
            requested_model,
            candidates
        );
    }

    state
        .pool
        .inner
        .read()
        .await
        .total_requests
        .fetch_add(1, Ordering::Relaxed);

    let mut forward_body = body_json;
    if let Some(obj) = forward_body.as_object_mut() {
        obj.remove("variant");
    }
    let model_for_metrics = requested_model.clone();

    // ── 重试循环（代理 × 模型候选） ──
    let max_retries = 8usize;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut candidate_idx = 0usize;
    let mut first_upstream_error: Option<(u16, Vec<u8>)> = None;

    for attempt in 0..max_retries {
        let current_upstream = candidates
            .get(candidate_idx)
            .cloned()
            .unwrap_or_else(|| requested_model.clone());

        // 更新 forward_body 的 model 字段
        if let Some(obj) = forward_body.as_object_mut() {
            obj.insert("model".to_string(), json!(current_upstream));
        }

        let proxy = pick_unused_proxy(&state, &tried).await;
        if proxy.is_none() && attempt > 0 && candidate_idx + 1 >= candidates.len() {
            break;
        }
        if let Some(ref p) = proxy {
            tried.insert(p.clone());
        }
        last_proxy = proxy.clone();

        info!(
            "Chat Completion attempt {}/{} model={} proxy={:?}",
            attempt + 1,
            max_retries,
            current_upstream,
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
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy {
                    state.pool.report_failure(p).await;
                }
                tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                continue;
            }
            Ok(resp) => {
                let code = resp.status().as_u16();

                if resp.status().is_success() {
                    if let Some(ref p) = proxy {
                        state.pool.report_success(p).await;
                    }
                    target_resp = Some(resp);
                    break;
                }

                if code == 429 {
                    warn!("Proxy {:?} hit 429. Cooling down...", proxy);
                    if let Some(ref p) = proxy {
                        state.pool.report_rate_limit(p).await;
                    }
                    tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                    continue;
                }

                let body_bytes = resp.bytes().await.unwrap_or_default();
                let body_str = String::from_utf8_lossy(&body_bytes).into_owned();

                let is_model_unavailable = body_str.contains("Model is unavailable");

                // ⭐ fallback chain：模型不可用且还有下一候选 → 切下一个
                if is_model_unavailable
                    && enable_fallback
                    && candidate_idx + 1 < candidates.len()
                {
                    warn!(
                        "Model {} unavailable, falling back to {}",
                        current_upstream,
                        candidates[candidate_idx + 1]
                    );
                    candidate_idx += 1;
                    // 不清代理失败（模型问题不是代理问题），但重新挑代理
                    continue;
                }

                let looks_proxy_related = is_model_unavailable
                    || body_str.contains("rate limit")
                    || body_str.contains("too many requests")
                    || body_str.contains("forbidden")
                    || body_str.contains("blocked")
                    || code >= 500;

                if looks_proxy_related {
                    let is_same_as_first = first_upstream_error
                        .as_ref()
                        .map(|(c, b)| *c == code && b.as_slice() == body_bytes.as_slice())
                        .unwrap_or(false);

                    if is_same_as_first {
                        warn!(
                            "Upstream identical {} from multiple proxies. Returning immediately.",
                            code
                        );
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

                    if first_upstream_error.is_none() {
                        first_upstream_error = Some((code, body_bytes.clone()));
                    }

                    warn!(
                        "Proxy {:?} got {} ({}). Retrying...",
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

    let resp = match target_resp {
        Some(r) => r,
        None => {
            warn!("All proxies failed. Falling back to direct connection.");
            last_proxy = None;
            let client = match get_or_build_client(&state, &None).await {
                Ok(c) => c,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create direct client: {}", e),
                    ).into_response()
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
                    ).into_response()
                }
            }
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut headers = HeaderMap::new();
    for (key, value) in resp.headers().iter() {
        let name = key.as_str().to_lowercase();
        if matches!(
            name.as_str(),
            "content-encoding" | "content-length" | "transfer-encoding" | "connection"
        ) {
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

    info!("  ← status={} requested={} via={:?}", status, requested_model, last_proxy);

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
            start: Instant::now(),
            model: model_for_metrics,
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
        response.extensions_mut().insert(Arc::new(MetricsGuard {
            metrics: state.metrics.clone(),
            tracker,
        }));
        return response;
    }

    state.metrics.active_requests.fetch_add(1, Ordering::Relaxed);
    state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
    let tracker = Arc::new(TrackerState {
        start: Instant::now(),
        model: model_for_metrics,
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
    response.extensions_mut().insert(Arc::new(MetricsGuard {
        metrics: state.metrics.clone(),
        tracker,
    }));
    response
}