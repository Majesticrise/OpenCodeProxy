use crate::config::OPENCODE_UA;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub fn generate_session_id() -> String {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    // ⭐ 关键：counter 必须限制在 0..=63
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0x3F;
    let raw = ((ts_ms & 0xFFFF_FFFF) << 12) | counter;
    let inverted = !raw & 0xFFFF_FFFF_FFFF;
    let hex_full = format!("{:012x}", inverted);
    // 此时 hex_full 的末尾应自然落在 fc0..fff 范围内
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

/// 免费层要求的四个占位工具及其最小非空 schema。
/// 来自 PR #16826 的实测：
/// - read 必须有非空 properties（否则 403）
/// - bash 的 parameters 不能是 {}（否则 403）
/// - glob / grep 同理，但优先级低于 read / bash
fn placeholder_schema(name: &str) -> Option<serde_json::Value> {
    match name {
        "bash" => Some(serde_json::json!({
            "type": "object",
            "properties": {
                "command": { "type": "string" }
            },
            "required": ["command"]
        })),
        "read" => Some(serde_json::json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string" }
            },
            "required": ["filePath"]
        })),
        "glob" => Some(serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string" }
            },
            "required": ["pattern"]
        })),
        "grep" => Some(serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string" }
            },
            "required": ["pattern"]
        })),
        _ => None,
    }
}

/// 确保指定名称的工具存在，并且其 parameters 是非空的合法 schema。
fn ensure_tool_with_schema(arr: &mut Vec<serde_json::Value>, name: &str, description: &str) {
    // 1) 查找是否已存在
    let existing_idx = arr.iter().position(|t| {
        t.get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            == Some(name)
    });

    if let Some(idx) = existing_idx {
        // 已存在：强制修复 parameters，防止空对象或 properties 为空
        let Some(func) = arr[idx].get_mut("function").and_then(|f| f.as_object_mut()) else {
            return;
        };
        let params = func
            .entry("parameters")
            .or_insert_with(|| serde_json::json!({}));

        let props_ok = params
            .get("properties")
            .and_then(|v| v.as_object())
            .map(|o| !o.is_empty())
            .unwrap_or(false);

        if !props_ok {
            if let Some(schema) = placeholder_schema(name) {
                *params = schema;
            }
        }
        return;
    }

    // 2) 不存在：插入完整定义
    let Some(schema) = placeholder_schema(name) else {
        return;
    };
    arr.push(serde_json::json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": schema
        }
    }));
}

/// 向请求体注入免费层工具契约。
///
/// 规则（2026-10-14 之后）：
/// - tools 必须包含 read，且 read.parameters.properties 非空
/// - bash 的 parameters 不能是空对象 {}
/// - tools 数量至少 2
/// - 客户端已传入的工具会被保留，但若 schema 为空会被就地修复
pub fn inject_free_tier_tools(body: &mut serde_json::Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let tools = obj.entry("tools").or_insert_with(|| serde_json::json!([]));
    let Some(arr) = tools.as_array_mut() else {
        return;
    };

    // 先修复客户端可能传入的「空壳」read / bash / glob / grep
    for name in ["read", "bash", "glob", "grep"] {
        ensure_tool_with_schema(arr, name, tool_description(name));
    }

    // 保证 read 一定存在（ensure_tool_with_schema 已处理，这里再兜一层）
    let has_read = arr.iter().any(|t| {
        t.get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            == Some("read")
    });
    if !has_read {
        ensure_tool_with_schema(arr, "read", "Read a file");
    }

    // 保证至少 2 个工具
    if arr.len() < 2 {
        // 如果只有 read，补 bash
        let has_bash = arr.iter().any(|t| {
            t.get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str())
                == Some("bash")
        });
        if !has_bash {
            ensure_tool_with_schema(arr, "bash", "Run a bash command");
        }
    }

    // 最终检查：遍历所有工具，把仍为空的 parameters 填成最小 schema
    for t in arr.iter_mut() {
        let Some(func) = t.get_mut("function").and_then(|f| f.as_object_mut()) else {
            continue;
        };
        let Some(name) = func.get("name").and_then(|n| n.as_str()).map(String::from) else {
            continue;
        };
        let params = func
            .entry("parameters")
            .or_insert_with(|| serde_json::json!({}));
        let is_empty = params
            .as_object()
            .map(|o| o.is_empty())
            .unwrap_or(true);
        if is_empty {
            if let Some(schema) = placeholder_schema(&name) {
                *params = schema;
            } else {
                // 非 quartet 工具，给一个通用的非空 schema，避免某些严格后端拒绝
                *params = serde_json::json!({
                    "type": "object",
                    "properties": {
                        "input": { "type": "string" }
                    },
                    "required": ["input"]
                });
            }
        }
    }
}

fn tool_description(name: &str) -> &'static str {
    match name {
        "read" => "Read a file",
        "bash" => "Run a bash command",
        "glob" => "Match files by pattern",
        "grep" => "Search file contents by pattern",
        _ => "",
    }
}

/// 构造请求头，返回 (builder, session_id, request_id) 供日志使用。
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