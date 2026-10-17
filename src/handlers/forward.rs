use crate::{config::*, opencode_identity, state::AppState};
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

fn resolve_candidates(routing: &RoutingConfig, requested: &str) -> Vec<String> {
    if let Some(list) = routing.mapping.get(requested) {
        return list.clone();
    }
    if let Some(list) = routing.mapping.get("*") {
        return list.clone();
    }
    vec![requested.to_string()]
}

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

    let routing_snapshot = state.routing.read().unwrap().clone();
    let candidates = resolve_candidates(&routing_snapshot, &requested_model);
    let enable_fallback = routing_snapshot.enable_fallback_chain;

    if candidates.len() == 1 && candidates[0] == requested_model {
        info!(
            "→ {} model={}",
            if is_stream { "STREAM" } else { "SYNC  " },
            requested_model
        );
    } else {
        info!(
            "→ {} requested={} candidates={:?}",
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

    // 免费层工具契约注入
    if *INJECT_FREE_TIER_TOOLS {
        opencode_identity::inject_free_tier_tools(&mut forward_body);
    }

    // 流式请求强制要求上游回报 usage，供 metrics 统计
    if is_stream {
        if let Some(obj) = forward_body.as_object_mut() {
            obj.entry("stream_options")
                .or_insert_with(|| json!({ "include_usage": true }));
        }
    }

    let model_for_metrics = requested_model.clone();
    let max_retries = 8usize;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut candidate_idx = 0usize;
    // ⚠️ 用 bytes::Bytes 而非 Vec<u8>：
    //   1) 与上游返回的类型一致，避免 clone 时 O(n) 拷贝
    //   2) 避免 `body_bytes.as_slice()`（Bytes 没有该稳定方法）
    let mut first_upstream_error: Option<(u16, bytes::Bytes)> = None;

    for attempt in 0..max_retries {
        let current_upstream = candidates
            .get(candidate_idx)
            .cloned()
            .unwrap_or_else(|| requested_model.clone());

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

        let client = match get_or_build_client(&state, &proxy).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build reqwest client: {}", e);
                continue;
            }
        };

        let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);

        // 身份头注入
        let (builder, session_id, request_id) = if *INJECT_FREE_TIER_HEADERS {
            opencode_identity::apply_headers(client.post(&target_url))
        } else {
            (
                client
                    .post(&target_url)
                    .header("Content-Type", "application/json"),
                String::new(),
                String::new(),
            )
        };

        info!(
            "attempt {}/{} model={} proxy={:?} session={} request={}",
            attempt + 1,
            max_retries,
            current_upstream,
            proxy,
            &session_id[..session_id.len().min(12)],
            &request_id[..request_id.len().min(12)]
        );

        match builder.json(&forward_body).send().await {
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

                // fallback chain：模型不可用且还有下一候选 → 切下一个
                if is_model_unavailable && enable_fallback && candidate_idx + 1 < candidates.len() {
                    warn!(
                        "Model {} unavailable, falling back to {}",
                        current_upstream,
                        candidates[candidate_idx + 1]
                    );
                    candidate_idx += 1;
                    continue;
                }

                let looks_proxy_related = is_model_unavailable
                    || body_str.contains("rate limit")
                    || body_str.contains("too many requests")
                    || body_str.contains("forbidden")
                    || body_str.contains("blocked")
                    || code >= 500;

                if looks_proxy_related {
                    // ✅ b 与 body_bytes 都是 Bytes，直接字节比较即可
                    let is_same_as_first = first_upstream_error
                        .as_ref()
                        .map(|(c, b)| *c == code && b == &body_bytes)
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

    // 直连兜底
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
                    )
                        .into_response()
                }
            };
            let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
            let (builder, _, _) = if *INJECT_FREE_TIER_HEADERS {
                opencode_identity::apply_headers(client.post(&target_url))
            } else {
                (
                    client
                        .post(&target_url)
                        .header("Content-Type", "application/json"),
                    String::new(),
                    String::new(),
                )
            };
            match builder.json(&forward_body).send().await {
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

    info!(
        "  ← status={} requested={} via={:?}",
        status, requested_model, last_proxy
    );

    // 非流式
    if !is_stream {
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("read body: {e}")).into_response()
            }
        };
        state
            .metrics
            .active_requests
            .fetch_add(1, Ordering::Relaxed);
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

    // 流式
    state
        .metrics
        .active_requests
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .active_streams
        .fetch_add(1, Ordering::Relaxed);
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