//! 上游健康探测：定期直连发一个 1-token 请求，区分「上游挂了」和「代理不行」。
//!
//! 结果分三级：
//! - `ok`：HTTP 2xx，上游可用且契约有效
//! - `restricted`：拿到任何 HTTP 响应（403/429/5xx 等）——上游在线，只是拒绝了我们
//! - `down`：网络层失败（连接拒绝/超时/DNS）——上游（或本机网络）不可达

use crate::{config::*, db::Db, handlers::forward::force_stream_options, opencode_identity};
use serde_json::json;
use std::time::{Duration, Instant};
use tracing::{info, warn};

fn impersonate() -> wreq_util::Profile {
    wreq_util::Profile::Chrome131
}

/// 探测一次并落库。绝不 panic / .unwrap()，失败只记录。
pub async fn probe_once(db: &Db) {
    let start = Instant::now();
    let client = match wreq::Client::builder()
        .emulation(impersonate())
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_millis(2500))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("health probe: build client failed: {}", e);
            return;
        }
    };

    // 与转发链路完全一致的伪装契约（stream 强制 + 工具注入 + 身份头）
    let mut body = json!({
        "model": DEFAULT_MODELS[0].0,
        "messages": [{ "role": "user", "content": "say ok" }],
        "stream": false
    });
    force_stream_options(&mut body);
    if *INJECT_FREE_TIER_TOOLS {
        opencode_identity::inject_free_tier_tools(&mut body);
    }

    let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
    let session = opencode_identity::generate_session_id();
    let (builder, _, _) = if *INJECT_FREE_TIER_HEADERS {
        opencode_identity::apply_headers(client.post(&target_url), &session)
    } else {
        (
            client
                .post(&target_url)
                .header("Content-Type", "application/json"),
            String::new(),
            String::new(),
        )
    };

    let latency = start.elapsed().as_millis() as u64;
    match builder
        .header("Accept", "text/event-stream")
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => {
            let code = resp.status().as_u16();
            let result = if resp.status().is_success() { "ok" } else { "restricted" };
            info!(
                "health probe: {} (HTTP {}, {}ms)",
                result, code, latency
            );
            db.record_probe(
                crate::db::now_secs(),
                result,
                Some(code),
                latency,
            );
        }
        Err(e) => {
            warn!("health probe: down ({}, {}ms)", e, latency);
            db.record_probe(crate::db::now_secs(), "down", None, latency);
        }
    }
}
