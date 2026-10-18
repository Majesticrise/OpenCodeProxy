use crate::{config::*, opencode_identity, state::AppState};
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

fn impersonate() -> wreq_util::Profile {
    wreq_util::Profile::Chrome131
}

fn resolve_candidates(routing: &RoutingConfig, requested: &str) -> Vec<String> {
    if let Some(list) = routing.mapping.get(requested) {
        return list.clone();
    }
    if let Some(list) = routing.mapping.get("*") {
        return list.clone();
    }
    vec![requested.to_string()]
}

fn estimate_prompt_tokens(body: &serde_json::Value) -> u64 {
    let mut total_chars = 0u64;
    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            if let Some(content) = msg.get("content") {
                match content {
                    serde_json::Value::String(s) => {
                        total_chars += s.chars().count() as u64;
                    }
                    serde_json::Value::Array(parts) => {
                        for part in parts {
                            if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                total_chars += text.chars().count() as u64;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    (total_chars / 3).max(1)
}

async fn get_or_build_client(
    state: &AppState,
    proxy: &Option<String>,
) -> Result<wreq::Client, wreq::Error> {
    if let Some(c) = state.client_cache.get(proxy).map(|v| v.clone()) {
        return Ok(c);
    }
    let mut b = wreq::Client::builder()
        .emulation(impersonate())
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_millis(2500))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8);
    if let Some(p) = proxy {
        if let Ok(rp) = wreq::Proxy::all(p) {
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

const MAX_SESSION_BINDINGS: usize = 10_000;

fn update_session_binding(
    bindings: &mut HashMap<String, Option<String>>,
    session_id: &str,
    proxy: Option<String>,
) {
    if bindings.len() >= MAX_SESSION_BINDINGS && !bindings.contains_key(session_id) {
        warn!(
            "Session binding table reached {} entries; clearing to prevent unbounded growth",
            MAX_SESSION_BINDINGS
        );
        bindings.clear();
    }
    bindings.insert(session_id.to_string(), proxy);
}

/// 聚合上游 SSE 为非流式 JSON。
///
/// ⭐ P2 修正：除了 `content` 还累加：
///   - `reasoning_content`（deepseek-reasoner 类模型的推理内容）
///   - `tool_calls`（按 `index` 合并 delta，拼接 arguments 字符串）
///
/// 这样非流式下游才能拿到与流式下游等价的信息。
async fn aggregate_sse_to_json(
    resp: wreq::Response,
) -> Result<(Vec<u8>, Option<crate::metrics::UsageInfo>), String> {
    let mut stream = resp.bytes_stream();
    let mut buf = Vec::<u8>::new();
    let mut content = String::new();
    let mut reasoning_content = String::new();
    // tool_calls 按 index 合并（Vec 下标即 OpenAI 的 tool_call.index）
    let mut tool_calls: Vec<serde_json::Value> = Vec::new();
    let mut usage: Option<crate::metrics::UsageInfo> = None;
    let mut id = String::new();
    let mut model = String::new();
    let mut broke_midway = false;

    loop {
        match stream.next().await {
            Some(Ok(chunk)) => {
                buf.extend_from_slice(&chunk);

                let mut start = 0;
                while let Some(pos) = buf[start..].iter().position(|&b| b == b'\n') {
                    let end = start + pos;
                    let line = String::from_utf8_lossy(&buf[start..end]).to_string();
                    start = end + 1;
                    let line = line.trim();
                    let json_str = if let Some(s) = line.strip_prefix("data:") {
                        s.trim_start()
                    } else {
                        continue;
                    };
                    if json_str.is_empty() || json_str == "[DONE]" {
                        continue;
                    }
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                        if id.is_empty() {
                            if let Some(i) = v.get("id").and_then(|i| i.as_str()) {
                                id = i.to_string();
                            }
                        }
                        if model.is_empty() {
                            if let Some(m) = v.get("model").and_then(|m| m.as_str()) {
                                model = m.to_string();
                            }
                        }
                        if let Some(u) = v.get("usage") {
                            if !u.is_null() {
                                usage = Some(crate::metrics::UsageInfo::from_json(u));
                            }
                        }

                        // ⭐ 拿整个 delta 对象，分别处理 content / reasoning / tool_calls
                        if let Some(delta) = v
                            .get("choices")
                            .and_then(|c| c.as_array())
                            .and_then(|a| a.first())
                            .and_then(|c| c.get("delta"))
                        {
                            if let Some(c) =
                                delta.get("content").and_then(|c| c.as_str())
                            {
                                content.push_str(c);
                            }
                            if let Some(rc) = delta
                                .get("reasoning_content")
                                .and_then(|r| r.as_str())
                            {
                                reasoning_content.push_str(rc);
                            }
                            if let Some(tc_arr) =
                                delta.get("tool_calls").and_then(|t| t.as_array())
                            {
                                for tc in tc_arr {
                                    let idx = tc
                                        .get("index")
                                        .and_then(|i| i.as_u64())
                                        .unwrap_or(0)
                                        as usize;
                                    while tool_calls.len() <= idx {
                                        tool_calls.push(json!({
                                            "id": "",
                                            "type": "function",
                                            "function": { "name": "", "arguments": "" }
                                        }));
                                    }
                                    if let Some(cid) =
                                        tc.get("id").and_then(|i| i.as_str())
                                    {
                                        if !cid.is_empty() {
                                            tool_calls[idx]["id"] = json!(cid);
                                        }
                                    }
                                    if let Some(name) = tc
                                        .get("function")
                                        .and_then(|f| f.get("name"))
                                        .and_then(|n| n.as_str())
                                    {
                                        if !name.is_empty() {
                                            tool_calls[idx]["function"]["name"] =
                                                json!(name);
                                        }
                                    }
                                    if let Some(args) = tc
                                        .get("function")
                                        .and_then(|f| f.get("arguments"))
                                        .and_then(|a| a.as_str())
                                    {
                                        let prev = tool_calls[idx]["function"]
                                            ["arguments"]
                                            .as_str()
                                            .unwrap_or("");
                                        tool_calls[idx]["function"]["arguments"] =
                                            json!(format!("{}{}", prev, args));
                                    }
                                }
                            }
                        }
                    }
                }
                if start > 0 {
                    buf.drain(..start);
                }
            }
            Some(Err(e)) => {
                warn!("Non-stream aggregation: upstream broke mid-way: {}", e);
                broke_midway = true;
                break;
            }
            None => break,
        }
    }

    if broke_midway && content.is_empty() && id.is_empty() && tool_calls.is_empty() {
        return Err("upstream stream broke before any data".to_string());
    }

    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // ⭐ P2 修正：finish_reason 语义修正。
    //    "length" 在 OpenAI 语义里是 token 上限，上游断流用它会误导客户端。
    //    有 tool_calls → "tool_calls"；否则一律 "stop"（尽力交付已收到的部分）。
    //    上游中断的事实通过非标准字段 x_upstream_broken 暴露。
    let finish_reason = if !tool_calls.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };

    let mut message = json!({
        "role": "assistant",
        "content": content,
    });
    if !reasoning_content.is_empty() {
        message["reasoning_content"] = json!(reasoning_content);
    }
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }

    let mut response_json = json!({
        "id": if id.is_empty() { format!("chatcmpl-{}", created) } else { id },
        "object": "chat.completion",
        "created": created,
        "model": if model.is_empty() { "unknown" } else { &model },
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason
        }],
        "usage": usage
    });
    if broke_midway {
        response_json["x_upstream_broken"] = json!(true);
    }

    let bytes =
        serde_json::to_vec(&response_json).map_err(|e| format!("serialize: {}", e))?;
    Ok((bytes, usage))
}

pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body_bytes: bytes::Bytes,
) -> impl IntoResponse {
    use crate::metrics::{try_extract_usage_from_chunk, MetricsGuard, TrackerState};

    let request_start = Instant::now();

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
    let downstream_stream = body_json
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    let mut session_id = headers
        .get("x-opencode-session")
        .or_else(|| headers.get("X-Session-Id"))
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.starts_with("ses_") && s.len() == 30)
        .map(String::from)
        .unwrap_or_else(|| opencode_identity::derive_session_from_messages(&body_json));

    let messages_snapshot: serde_json::Value = body_json
        .get("messages")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));

    let bound: Option<Option<String>> = {
        let bindings = state.session_proxy_binding.read().unwrap();
        bindings.get(&session_id).cloned()
    };

    // 注意：preferred_proxy / skip_proxy_loop 只基于「进入本次请求时的绑定」计算。
    // 如果循环内因 403 触发换 session，这两个值不会重算——但此时已经进入循环，
    // preferred_proxy 只在 attempt == 0 被消费（已过去），skip_proxy_loop 只影响
    // 是否进循环（已决定进入）。所以现有行为是正确的，只是依赖「进入时机」。
    // 如果将来把换 session 逻辑移到循环开始前，需要重新计算这两个值。
    let preferred_proxy: Option<String> = match &bound {
        Some(Some(url)) => {
            if state.pool.is_available(url).await {
                Some(url.clone())
            } else {
                None
            }
        }
        _ => None,
    };

    let skip_proxy_loop = matches!(bound, Some(None));

    let routing_snapshot = state.routing.read().unwrap().clone();
    let candidates = resolve_candidates(&routing_snapshot, &requested_model);
    let enable_fallback = routing_snapshot.enable_fallback_chain;

    info!(
        "→ {} model={} session={} bind={} (downstream_stream={})",
        if downstream_stream { "STREAM" } else { "SYNC  " },
        requested_model,
        &session_id[..session_id.len().min(12)],
        match &bound {
            Some(Some(url)) => format!("proxy:{}", &url[..url.len().min(16)]),
            Some(None) => "direct".to_string(),
            None => "none".to_string(),
        },
        downstream_stream
    );

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
        obj.insert("stream".to_string(), json!(true));
        obj.entry("stream_options")
            .or_insert_with(|| json!({ "include_usage": true }));
    }

    if *INJECT_FREE_TIER_TOOLS {
        opencode_identity::inject_free_tier_tools(&mut forward_body);
    }

    let estimated_prompt = estimate_prompt_tokens(&forward_body);

    let model_for_metrics = requested_model.clone();
    let max_retries = 8usize;
    let mut target_resp: Option<wreq::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut candidate_idx = 0usize;
    let mut first_upstream_error: Option<(u16, bytes::Bytes)> = None;
    let mut freetier_403_count = 0u32;

    for attempt in 0..max_retries {
        if skip_proxy_loop {
            break;
        }

        let current_upstream = candidates
            .get(candidate_idx)
            .cloned()
            .unwrap_or_else(|| requested_model.clone());

        if let Some(obj) = forward_body.as_object_mut() {
            obj.insert("model".to_string(), json!(current_upstream));
        }

        let proxy = if attempt == 0 && preferred_proxy.is_some() {
            preferred_proxy.clone()
        } else {
            pick_unused_proxy(&state, &tried).await
        };

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
                error!("Failed to build wreq client: {}", e);
                continue;
            }
        };

        let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);

        let (builder, _, request_id) = if *INJECT_FREE_TIER_HEADERS {
            opencode_identity::apply_headers(client.post(&target_url), &session_id)
        } else {
            (
                client
                    .post(&target_url)
                    .header("Content-Type", "application/json"),
                String::new(),
                String::new(),
            )
        };

        let builder = builder.header("Accept", "text/event-stream");

        debug!(
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
                debug!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
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
                    {
                        let mut bindings = state.session_proxy_binding.write().unwrap();
                        update_session_binding(&mut bindings, &session_id, proxy.clone());
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

                if code == 403 && body_str.contains("FreeTierError") {
                    freetier_403_count += 1;
                    warn!(
                        "Proxy {:?} hit 403 FreeTierError ({}/2). session={}",
                        proxy,
                        freetier_403_count,
                        &session_id[..session_id.len().min(12)]
                    );
                    if let Some(ref p) = proxy {
                        state.pool.report_banned(p).await;
                    }
                    if freetier_403_count >= 2 {
                        {
                            let mut bindings = state.session_proxy_binding.write().unwrap();
                            bindings.remove(&session_id);
                        }
                        let seed_body = serde_json::json!({
                            "model": requested_model,
                            "messages": messages_snapshot.clone(),
                        });
                        let new_session = opencode_identity::derive_session_from_messages_with_nonce(
                            &seed_body,
                            freetier_403_count,
                        );
                        warn!(
                            "403 FreeTierError threshold reached; rotating session {} → {}",
                            &session_id[..session_id.len().min(12)],
                            &new_session[..new_session.len().min(12)]
                        );
                        session_id = new_session;
                    }
                    tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                    continue;
                }

                let is_model_unavailable = body_str.contains("Model is unavailable");

                if is_model_unavailable && enable_fallback && candidate_idx + 1 < candidates.len()
                {
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
                    || code == 408
                    || code >= 500;

                if looks_proxy_related {
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
                opencode_identity::apply_headers(client.post(&target_url), &session_id)
            } else {
                (
                    client
                        .post(&target_url)
                        .header("Content-Type", "application/json"),
                    String::new(),
                    String::new(),
                )
            };
            let builder = builder.header("Accept", "text/event-stream");
            match builder.json(&forward_body).send().await {
                Ok(r) => {
                    {
                        let mut bindings = state.session_proxy_binding.write().unwrap();
                        update_session_binding(&mut bindings, &session_id, None);
                    }
                    r
                }
                Err(e) => {
                    {
                        let mut bindings = state.session_proxy_binding.write().unwrap();
                        bindings.remove(&session_id);
                    }
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!("Upstream direct connection failed: {}", e),
                    )
                        .into_response();
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

    if !downstream_stream {
        let (bytes, usage) = match aggregate_sse_to_json(resp).await {
            Ok(x) => x,
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("aggregate failed: {e}"))
                    .into_response()
            }
        };

        state
            .metrics
            .active_requests
            .fetch_add(1, Ordering::Relaxed);

        let tracker = Arc::new(TrackerState::new(
            request_start,
            model_for_metrics,
            false,
            status.as_u16(),
            estimated_prompt,
        ));
        if let Some(u) = usage {
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

    state
        .metrics
        .active_requests
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .active_streams
        .fetch_add(1, Ordering::Relaxed);

    let tracker = Arc::new(TrackerState::new(
        request_start,
        model_for_metrics,
        true,
        status.as_u16(),
        estimated_prompt,
    ));

    let usage_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let tracker_for_stream = tracker.clone();
    let buf_for_stream = usage_buf.clone();

    let byte_stream = resp.bytes_stream();

    let stream = stream::unfold(
        (byte_stream, false, false, tracker_for_stream, buf_for_stream),
        move |(mut inner, done, received_any, tracker, buf)| async move {
            if done {
                return None;
            }

            match inner.next().await {
                Some(Ok(chunk)) => {
                    {
                        let mut b = buf.lock().unwrap();
                        if let Some(u) = try_extract_usage_from_chunk(&chunk[..], &mut b) {
                            *tracker.usage.lock().unwrap() = Some(u);
                        }
                    }

                    if let Ok(text) = std::str::from_utf8(&chunk) {
                        for line in text.lines() {
                            let line = line.trim();
                            let json_str = if let Some(s) = line.strip_prefix("data:") {
                                s.trim_start()
                            } else {
                                continue;
                            };
                            if json_str.is_empty() || json_str == "[DONE]" {
                                continue;
                            }
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                                if let Some(delta) = v
                                    .get("choices")
                                    .and_then(|c| c.as_array())
                                    .and_then(|a| a.first())
                                    .and_then(|c| c.get("delta"))
                                    .and_then(|d| d.get("content"))
                                    .and_then(|c| c.as_str())
                                {
                                    tracker
                                        .streamed_chars
                                        .fetch_add(delta.chars().count() as u64, Ordering::Relaxed);
                                }
                            }
                        }
                    }

                    Some((
                        Ok::<_, std::io::Error>(chunk),
                        (inner, false, true, tracker, buf),
                    ))
                }
                Some(Err(e)) => {
                    let body: bytes::Bytes = if !received_any {
                        let err_json = serde_json::json!({
                            "error": {
                                "type": "proxy_upstream_error",
                                "message": format!("Upstream stream broke before any data: {}", e)
                            }
                        });
                        warn!(
                            "Upstream SSE stream broke before any chunk arrived: {}. Sending error event.",
                            e
                        );
                        bytes::Bytes::from(format!(
                            "\n\ndata: {}\n\ndata: [DONE]\n\n",
                            err_json
                        ))
                    } else {
                        warn!("Upstream SSE stream broken: {}. Sending [DONE].", e);
                        bytes::Bytes::from_static(b"\n\ndata: [DONE]\n\n")
                    };
                    Some((
                        Ok::<_, std::io::Error>(body),
                        (inner, true, received_any, tracker, buf),
                    ))
                }
                None => None,
            }
        },
    );

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response.extensions_mut().insert(Arc::new(MetricsGuard {
        metrics: state.metrics.clone(),
        tracker,
    }));
    response
}