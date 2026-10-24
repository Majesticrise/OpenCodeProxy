use crate::{config::*, opencode_identity, settings::Mode, state::AppState};
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

/// 实名模式专用的 Console org id（可选）。
/// 从环境变量 `OPENCODE_ORG_ID` 读取，不落盘到 settings.json，
/// 避免把工作区标识和密钥一起明文存储。
fn authenticated_org_id() -> Option<String> {
    std::env::var("OPENCODE_ORG_ID")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
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
    if let Some(tools) = body.get("tools") {
        total_chars += tools.to_string().chars().count() as u64;
    }
    (total_chars / 3).max(1)
}

/// 客户端缓存软上限：代理池上限（proxy.rs 的 MAX_POOL_SIZE=120）+ 换血期间的临时余量。
const CLIENT_CACHE_SOFT_CAP: usize = 150;

async fn get_or_build_client(
    state: &AppState,
    proxy: &Option<String>,
) -> Result<wreq::Client, wreq::Error> {
    if let Some(c) = state.client_cache.get(proxy).map(|v| v.clone()) {
        return Ok(c);
    }
    // 池子换血后被移除代理的 Client 会一直滞留在缓存里，这里做清理：
    // 超过软上限时，淘汰「已不在代理池中」的 URL 对应条目；直连（None）保留。
    if state.client_cache.len() >= CLIENT_CACHE_SOFT_CAP {
        let keys: Vec<Option<String>> =
            state.client_cache.iter().map(|e| e.key().clone()).collect();
        let mut stale = Vec::new();
        for k in keys {
            if let Some(url) = &k {
                if !state.pool.contains(url).await {
                    stale.push(k);
                }
            }
        }
        for k in stale {
            state.client_cache.remove(&k);
        }
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

/// SSE 事件聚合的中间状态（上游强制流式，非流式客户端由网关聚合回 JSON）。
#[derive(Default)]
struct SseAgg {
    content: String,
    reasoning_content: String,
    tool_calls: Vec<serde_json::Value>,
    usage: Option<crate::metrics::UsageInfo>,
    id: String,
    model: String,
}

/// 把一条已解析的 `data: {...}` 事件合并进聚合状态。
/// 单独成函数是为了让「流结束后没有换行符的最后一行」复用同一条路径，也便于单测。
fn apply_sse_event(v: &serde_json::Value, agg: &mut SseAgg) {
    if agg.id.is_empty() {
        if let Some(i) = v.get("id").and_then(|i| i.as_str()) {
            agg.id = i.to_string();
        }
    }
    if agg.model.is_empty() {
        if let Some(m) = v.get("model").and_then(|m| m.as_str()) {
            agg.model = m.to_string();
        }
    }
    if let Some(u) = v.get("usage") {
        if !u.is_null() {
            agg.usage = Some(crate::metrics::UsageInfo::from_json(u));
        }
    }
    if let Some(delta) = v
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
        .and_then(|c| c.get("delta"))
    {
        if let Some(c) = delta.get("content").and_then(|c| c.as_str()) {
            agg.content.push_str(c);
        }
        if let Some(rc) = delta.get("reasoning_content").and_then(|r| r.as_str()) {
            agg.reasoning_content.push_str(rc);
        }
        if let Some(tc_arr) = delta.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in tc_arr {
                let idx = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;
                while agg.tool_calls.len() <= idx {
                    agg.tool_calls.push(json!({
                        "id": "",
                        "type": "function",
                        "function": { "name": "", "arguments": "" }
                    }));
                }
                if let Some(cid) = tc.get("id").and_then(|i| i.as_str()) {
                    if !cid.is_empty() {
                        agg.tool_calls[idx]["id"] = json!(cid);
                    }
                }
                if let Some(name) = tc
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                {
                    if !name.is_empty() {
                        agg.tool_calls[idx]["function"]["name"] = json!(name);
                    }
                }
                if let Some(args) = tc
                    .get("function")
                    .and_then(|f| f.get("arguments"))
                    .and_then(|a| a.as_str())
                {
                    let prev = agg.tool_calls[idx]["function"]["arguments"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    agg.tool_calls[idx]["function"]["arguments"] =
                        json!(format!("{}{}", prev, args));
                }
            }
        }
    }
}

/// 解析并合并一行 SSE 文本；`data:` 前缀之外的行（注释 / 空行 / 其他）安全忽略。
fn merge_sse_line(line: &str, agg: &mut SseAgg) {
    let line = line.trim();
    let json_str = if let Some(s) = line.strip_prefix("data:") {
        s.trim_start()
    } else {
        return;
    };
    if json_str.is_empty() || json_str == "[DONE]" {
        return;
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
        apply_sse_event(&v, agg);
    }
}

async fn aggregate_sse_to_json(
    resp: wreq::Response,
) -> Result<(Vec<u8>, Option<crate::metrics::UsageInfo>), String> {
    let mut stream = resp.bytes_stream();
    let mut buf = Vec::<u8>::new();
    let mut agg = SseAgg::default();
    let mut broke_midway = false;

    loop {
        match stream.next().await {
            Some(Ok(chunk)) => {
                buf.extend_from_slice(&chunk);
                while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                    merge_sse_line(&String::from_utf8_lossy(&line_bytes), &mut agg);
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

    // 收尾：流结束后缓冲里可能残留最后一行（上游没以换行结尾），同样要合并
    if !buf.is_empty() {
        merge_sse_line(&String::from_utf8_lossy(&buf), &mut agg);
    }

    if broke_midway && agg.content.is_empty() && agg.id.is_empty() && agg.tool_calls.is_empty() {
        return Err("upstream stream broke before any data".to_string());
    }

    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let finish_reason = if !agg.tool_calls.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };

    let mut message = json!({ "role": "assistant", "content": agg.content });
    if !agg.reasoning_content.is_empty() {
        message["reasoning_content"] = json!(agg.reasoning_content);
    }
    if !agg.tool_calls.is_empty() {
        message["tool_calls"] = json!(agg.tool_calls);
    }

    let mut response_json = json!({
        "id": if agg.id.is_empty() { format!("chatcmpl-{}", created) } else { agg.id.clone() },
        "object": "chat.completion",
        "created": created,
        "model": if agg.model.is_empty() { "unknown".to_string() } else { agg.model.clone() },
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason
        }],
        "usage": agg.usage
    });
    if broke_midway {
        response_json["x_upstream_broken"] = json!(true);
    }

    let bytes = serde_json::to_vec(&response_json).map_err(|e| format!("serialize: {}", e))?;
    Ok((bytes, agg.usage))
}

/// 脱敏密钥用于日志：只显示前 7 位和后 4 位（`oc_sk_` 前缀保留）。
fn mask_key(key: &str) -> String {
    if key.len() <= 11 {
        "***".to_string()
    } else {
        format!("{}...{}", &key[..7], &key[key.len() - 4..])
    }
}

/// 上游永远以流式请求（免费层伪装要求），并确保 `stream_options.include_usage` 存在。
/// 客户端已显式设置的字段值不覆盖（例如显式传了 `include_usage: false` 就尊重之），
/// 只在缺失时补 `true`。此前用 `entry().or_insert_with()` 整体写入，客户端传
/// `{"stream_options":{}}` 时 `include_usage` 不会被补上，导致拿不到真实 usage。
/// 探测任务（probe.rs）复用同一函数保持伪装契约一致。
pub(crate) fn force_stream_options(body: &mut serde_json::Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.remove("variant");
    obj.insert("stream".to_string(), json!(true));
    let so = obj.entry("stream_options").or_insert_with(|| json!({}));
    match so.as_object_mut() {
        Some(map) => {
            map.entry("include_usage").or_insert_with(|| json!(true));
        }
        None => {
            *so = json!({ "include_usage": true });
        }
    }
}

/// 构造「计入指标」的错误响应。
/// 请求一旦通过校验（已计入 pool.total_requests），无论最终成败都必须落一条 metric，
/// 否则 WebUI 的总请求数 / 成功率会与 /api/status 的 pool 计数口径不一致且偏乐观。
fn tracked_error_response(
    state: &AppState,
    request_start: Instant,
    status: u16,
    estimated_prompt: u64,
    content_type: &'static str,
    body: impl Into<bytes::Bytes>,
    meta: crate::metrics::TrackerMeta,
) -> Response {
    use crate::metrics::{MetricsGuard, TrackerState};

    state
        .metrics
        .active_requests
        .fetch_add(1, Ordering::Relaxed);
    let tracker = Arc::new(TrackerState::new(
        request_start,
        false,
        status,
        estimated_prompt,
        meta,
    ));
    let mut response = Response::new(Body::from(body.into()));
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    *response.status_mut() = status;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response.extensions_mut()
        .insert(Arc::new(MetricsGuard {
            metrics: state.metrics.clone(),
            tracker,
            db: Some(state.db.clone()),
        }));
    response
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

    // ⭐ 读取运行时设置：模式 + API Key
    let settings_snapshot = state.settings.read().unwrap().clone();
    let is_authenticated = matches!(settings_snapshot.mode, Mode::Authenticated);

    // 实名模式必须配置 API Key，否则直接拒绝
    if is_authenticated && settings_snapshot.api_key.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Authenticated mode requires an API key. Configure it in WebUI first.",
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
        .unwrap_or(DEFAULT_MODELS[0].0)
        .to_string();
    let downstream_stream = body_json
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    // session：客户端头优先，否则时间戳生成
    let mut session_id = headers
        .get("x-opencode-session")
        .or_else(|| headers.get("X-Session-Id"))
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.starts_with("ses_") && s.len() == 30)
        .map(String::from)
        .unwrap_or_else(opencode_identity::generate_session_id);

    // 实名模式：不查绑定，强制直连
    let bound: Option<Option<String>> = if is_authenticated {
        None
    } else {
        let bindings = state.session_proxy_binding.read().unwrap();
        bindings.get(&session_id).cloned()
    };

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

    // 实名模式强制跳过代理循环
    let skip_proxy_loop = if is_authenticated {
        true
    } else {
        matches!(bound, Some(None))
    };

    let routing_snapshot = state.routing.read().unwrap().clone();
    let candidates = resolve_candidates(&routing_snapshot, &requested_model);
    let enable_fallback = routing_snapshot.enable_fallback_chain;

    info!(
        "→ {} mode={:?} model={} session={} bind={} (downstream_stream={})",
        if downstream_stream { "STREAM" } else { "SYNC  " },
        settings_snapshot.mode,
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
    force_stream_options(&mut forward_body);

    if *INJECT_FREE_TIER_TOOLS {
        opencode_identity::inject_free_tier_tools(&mut forward_body);
    }

    let estimated_prompt = estimate_prompt_tokens(&forward_body);

    // 落库元数据：model_used / proxy_url 在拿到最终响应后回填
    let mut meta = crate::metrics::TrackerMeta {
        model: requested_model.clone(),
        model_used: requested_model.clone(),
        proxy_url: None,
        mode: if is_authenticated { "authenticated" } else { "anonymous" }.to_string(),
        session_id: session_id.clone(),
    };
    let max_retries = 8usize;
    let mut target_resp: Option<wreq::Response> = None;
    let mut last_proxy: Option<String> = None;
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut candidate_idx = 0usize;
    let mut first_upstream_error: Option<(u16, bytes::Bytes)> = None;
    let mut freetier_403_count = 0u32;

    // 实名模式 skip_proxy_loop = true，循环直接 break，走下方直连分支
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
                        let new_session = opencode_identity::generate_session_id();
                        warn!(
                            "403 FreeTierError threshold reached; rotating session {} → {}",
                            &session_id[..session_id.len().min(12)],
                            &new_session[..new_session.len().min(12)]
                        );
                        session_id = new_session;
                        meta.session_id = session_id.clone();
                    }
                    tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                    continue;
                }

                let is_model_unavailable = body_str.contains("Model is unavailable");

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
                        return tracked_error_response(
                            &state,
                            request_start,
                            code,
                            estimated_prompt,
                            "application/json",
                            body_bytes,
                            meta.clone(),
                        );
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
                return tracked_error_response(
                    &state,
                    request_start,
                    code,
                    estimated_prompt,
                    "application/json",
                    body_bytes,
                    meta.clone(),
                );
            }
        }
    }

    let resp = match target_resp {
        Some(r) => r,
        None => {
            if is_authenticated {
                info!("Authenticated mode: direct connection with API key");
            } else {
                warn!("All proxies failed. Falling back to direct connection.");
            }
            last_proxy = None;
            let client = match get_or_build_client(&state, &None).await {
                Ok(c) => c,
                Err(e) => {
                    return tracked_error_response(
                        &state,
                        request_start,
                        500,
                        estimated_prompt,
                        "text/plain; charset=utf-8",
                        format!("Failed to create direct client: {}", e),
                        meta.clone(),
                    )
                }
            };
            let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);

            let (mut builder, _, _) = if *INJECT_FREE_TIER_HEADERS {
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

            // ⭐ 实名模式：注入 Authorization + 可选 x-opencode-org-id
            if is_authenticated {
                if let Some(key) = &settings_snapshot.api_key {
                    builder = builder.header("Authorization", format!("Bearer {}", key));
                    // 脱敏诊断日志：确认密钥格式、org 头、session 都在
                    let org = authenticated_org_id();
                    debug!(
                        "authenticated request: url={} key={} org_id_set={} session={}",
                        target_url,
                        mask_key(key),
                        org.is_some(),
                        &session_id[..session_id.len().min(12)],
                    );
                    if let Some(org_id) = org {
                        builder = builder.header("x-opencode-org-id", org_id);
                    }
                }
            }

            let builder = builder.header("Accept", "text/event-stream");
            match builder.json(&forward_body).send().await {
                Ok(r) => {
                    let status_code = r.status().as_u16();

                    // ⭐ 实名模式下 401/403 时把上游 body 打出来，方便诊断
                    if is_authenticated && (status_code == 401 || status_code == 403) {
                        let body = r.bytes().await.unwrap_or_default();
                        let body_str = String::from_utf8_lossy(&body);
                        warn!(
                            "Authenticated upstream {} error body: {}",
                            status_code,
                            body_str.chars().take(500).collect::<String>()
                        );
                        return tracked_error_response(
                            &state,
                            request_start,
                            status_code,
                            estimated_prompt,
                            "application/json",
                            body,
                            meta.clone(),
                        );
                    }

                    // 只有匿名模式写回绑定；实名模式总是直连，不写绑定
                    if !is_authenticated {
                        let mut bindings = state.session_proxy_binding.write().unwrap();
                        update_session_binding(&mut bindings, &session_id, None);
                    }
                    r
                }
                Err(e) => {
                    if !is_authenticated {
                        let mut bindings = state.session_proxy_binding.write().unwrap();
                        bindings.remove(&session_id);
                    }
                    return tracked_error_response(
                        &state,
                        request_start,
                        502,
                        estimated_prompt,
                        "text/plain; charset=utf-8",
                        format!("Upstream direct connection failed: {}", e),
                        meta.clone(),
                    );
                }
            }
        }
    };

    // 回填落库元数据：实际使用的模型 / 代理 /（可能已轮换的）session
    meta.model_used = forward_body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or(&requested_model)
        .to_string();
    meta.proxy_url = last_proxy.clone();
    meta.session_id = session_id.clone();

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
                return tracked_error_response(
                    &state,
                    request_start,
                    502,
                    estimated_prompt,
                    "text/plain; charset=utf-8",
                    format!("aggregate failed: {e}"),
                    meta.clone(),
                )
            }
        };

        state
            .metrics
            .active_requests
            .fetch_add(1, Ordering::Relaxed);

        let tracker = Arc::new(TrackerState::new(
            request_start,
            false,
            status.as_u16(),
            estimated_prompt,
            meta.clone(),
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
            db: Some(state.db.clone()),
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
        true,
        status.as_u16(),
        estimated_prompt,
        meta.clone(),
    ));

    let usage_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let tracker_for_stream = tracker.clone();
    let buf_for_stream = usage_buf.clone();

    let byte_stream = resp.bytes_stream();

    let stream = stream::unfold(
        (
            byte_stream,
            false,
            false,
            tracker_for_stream,
            buf_for_stream,
            Vec::<u8>::new(),
        ),
        move |(mut inner, done, received_any, tracker, buf, mut pending)| async move {
            if done {
                return None;
            }

            match inner.next().await {
                Some(Ok(chunk)) => {
                    // 首字节延迟（TTFT）：上游第一个 chunk 的到达时刻，只记一次
                    if tracker.ttft_ms.load(Ordering::Relaxed) == 0 {
                        tracker.ttft_ms.store(
                            tracker.start.elapsed().as_millis() as u64,
                            Ordering::Relaxed,
                        );
                    }
                    {
                        let mut b = buf.lock().unwrap();
                        if let Some(u) = try_extract_usage_from_chunk(&chunk[..], &mut b) {
                            *tracker.usage.lock().unwrap() = Some(u);
                        }
                    }

                    // 按完整行统计输出字符：SSE 可能把一行 JSON 或一个多字节字符拆在
                    // 相邻 chunk 里，逐 chunk 解 UTF-8 会整块丢失，先攒行再解析。
                    pending.extend_from_slice(&chunk);
                    while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
                        let line_bytes: Vec<u8> = pending.drain(..=pos).collect();
                        let line = String::from_utf8_lossy(&line_bytes);
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

                    Some((
                        Ok::<_, std::io::Error>(chunk),
                        (inner, false, true, tracker, buf, pending),
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
                        (inner, true, received_any, tracker, buf, pending),
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
        db: Some(state.db.clone()),
    }));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── force_stream_options ──

    #[test]
    fn force_stream_injects_and_merges() {
        let mut body = json!({"variant": "x", "stream": false, "stream_options": {}});
        force_stream_options(&mut body);
        assert_eq!(body["stream"], true);
        assert!(body.get("variant").is_none());
        // 客户端传了空的 stream_options 也必须补上 include_usage
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn force_stream_respects_explicit_include_usage() {
        let mut body = json!({"stream_options": {"include_usage": false}});
        force_stream_options(&mut body);
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], false);
    }

    #[test]
    fn force_stream_on_missing_or_malformed_stream_options() {
        let mut a = json!({});
        force_stream_options(&mut a);
        assert_eq!(a["stream_options"]["include_usage"], true);

        // 非对象的异常值直接重置为合法对象
        let mut b = json!({"stream_options": "garbage"});
        force_stream_options(&mut b);
        assert_eq!(b["stream_options"]["include_usage"], true);
    }

    // ── SSE 聚合 ──

    #[test]
    fn merge_sse_line_parses_data_events() {
        let mut agg = SseAgg::default();
        let ev = json!({
            "id": "chatcmpl-1",
            "model": "m",
            "choices": [{"delta": {"content": "你好"}}]
        })
        .to_string();
        merge_sse_line(&format!("data: {}", ev), &mut agg);
        // 空行 / 注释 / [DONE] 都要被安全忽略
        merge_sse_line("", &mut agg);
        merge_sse_line(": keep-alive", &mut agg);
        merge_sse_line("data: [DONE]", &mut agg);

        assert_eq!(agg.content, "你好");
        assert_eq!(agg.id, "chatcmpl-1");
        assert_eq!(agg.model, "m");
        assert!(agg.usage.is_none());
    }

    #[test]
    fn merge_sse_line_captures_usage() {
        let mut agg = SseAgg::default();
        let ev = json!({
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        })
        .to_string();
        merge_sse_line(&format!("data: {}", ev), &mut agg);
        assert_eq!(agg.usage.unwrap().total_tokens, 3);
    }

    #[test]
    fn apply_sse_event_accumulates_tool_calls_across_events() {
        // tool_calls 的 arguments 会被上游切成多个增量片段，必须按 index 拼接
        let mut agg = SseAgg::default();
        apply_sse_event(
            &json!({"choices": [{"delta": {"tool_calls": [
                {"index": 0, "id": "call_1", "function": {"name": "read", "arguments": "{\"file"}}
            ]}}]}),
            &mut agg,
        );
        apply_sse_event(
            &json!({"choices": [{"delta": {"tool_calls": [
                {"index": 0, "function": {"arguments": "Path\":\"a\"}"}}
            ]}}]}),
            &mut agg,
        );
        assert_eq!(agg.tool_calls.len(), 1);
        assert_eq!(agg.tool_calls[0]["id"], "call_1");
        assert_eq!(agg.tool_calls[0]["function"]["name"], "read");
        assert_eq!(
            agg.tool_calls[0]["function"]["arguments"],
            json!("{\"filePath\":\"a\"}")
        );
    }

    #[test]
    fn apply_sse_event_reasoning_content() {
        let mut agg = SseAgg::default();
        apply_sse_event(
            &json!({"choices": [{"delta": {"reasoning_content": "hmm"}}]}),
            &mut agg,
        );
        assert_eq!(agg.reasoning_content, "hmm");
    }
}