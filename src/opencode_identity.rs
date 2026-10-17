use crate::config::OPENCODE_UA;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// 对齐 OpenCode CLI Identifier.descending("ses")
/// 格式：ses_ + 12位hex + 14位Base62，总长30
pub fn generate_session_id() -> String {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFF;
    let raw = ((ts_ms & 0xFFFF_FFFF) << 12) | counter;
    let inverted = !raw & 0xFFFF_FFFF_FFFF;
    let hex_full = format!("{:012x}", inverted);
    let mut rng = rand::thread_rng();
    let suffix: String = (0..14)
        .map(|_| BASE62[rng.gen_range(0..62)] as char)
        .collect();
    format!("ses_{}{}", hex_full, suffix)
}

/// 升序消息 ID，前缀 msg_
pub fn generate_request_id() -> String {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFF;
    let raw = ((ts_ms & 0xFFFF_FFFF) << 12) | counter;
    let hex_full = format!("{:012x}", raw);
    let mut rng = rand::thread_rng();
    let suffix: String = (0..14)
        .map(|_| BASE62[rng.gen_range(0..62)] as char)
        .collect();
    format!("msg_{}{}", hex_full, suffix)
}

/// 向请求体注入免费层工具契约
/// 规则：
///   - tools 必须包含 read，且 read.parameters.properties 非空
///   - tools 至少 2 个，否则补 bash
///   - 客户端传的 read 若 parameters 为 null / properties 为空，会被就地补全
pub fn inject_free_tier_tools(body: &mut serde_json::Value) {
    let obj = match body.as_object_mut() {
        Some(o) => o,
        None => return,
    };
    let tools = obj.entry("tools").or_insert_with(|| serde_json::json!([]));
    let arr = match tools.as_array_mut() {
        Some(a) => a,
        None => return,
    };

    let has_read = arr.iter().any(|t| {
        t.get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            == Some("read")
    });

    if !has_read {
        // 没有 read，直接补一个完整定义
        arr.push(serde_json::json!({
            "type": "function",
            "function": {
                "name": "read",
                "description": "Read a file",
                "parameters": {
                    "type": "object",
                    "properties": { "filePath": { "type": "string" } },
                    "required": ["filePath"]
                }
            }
        }));
    } else {
        // 已有 read：确保 parameters 存在，且 properties 非空
        for t in arr.iter_mut() {
            let is_read = t
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str())
                == Some("read");
            if !is_read {
                continue;
            }
            if let Some(func) = t.get_mut("function").and_then(|f| f.as_object_mut()) {
                let params = func
                    .entry("parameters")
                    .or_insert_with(|| serde_json::json!({}));
                let props_ok = params
                    .get("properties")
                    .and_then(|v| v.as_object())
                    .map(|o| !o.is_empty())
                    .unwrap_or(false);
                if !props_ok {
                    *params = serde_json::json!({
                        "type": "object",
                        "properties": { "filePath": { "type": "string" } },
                        "required": ["filePath"]
                    });
                }
            }
        }
    }

    // 保证至少 2 个工具，若没有 bash 则补一个
    if arr.len() < 2 {
        let has_bash = arr.iter().any(|t| {
            t.get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str())
                == Some("bash")
        });
        if !has_bash {
            arr.push(serde_json::json!({
                "type": "function",
                "function": {
                    "name": "bash",
                    "description": "Run a bash command",
                    "parameters": {
                        "type": "object",
                        "properties": { "command": { "type": "string" } },
                        "required": ["command"]
                    }
                }
            }));
        }
    }
}

/// 构造请求头，返回 (builder, session_id, request_id) 供日志使用
/// UA 来自 config::OPENCODE_UA（可通过环境变量 OPENCODE_UA 覆盖）
pub fn apply_headers(
    builder: reqwest::RequestBuilder,
) -> (reqwest::RequestBuilder, String, String) {
    let session_id = generate_session_id();
    let request_id = generate_request_id();
    let builder = builder
        .header("Content-Type", "application/json")
        .header("User-Agent", OPENCODE_UA.as_str())
        .header("x-opencode-client", "cli")
        .header("x-opencode-session", session_id.as_str())
        .header("x-opencode-request", request_id.as_str())
        .header("x-opencode-project", "global")
        .header("x-session-affinity", session_id.as_str())
        .header("X-Session-Id", session_id.as_str());
    (builder, session_id, request_id)
}