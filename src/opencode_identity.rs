use crate::config::OPENCODE_UA;
use rand::Rng;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// 官方 6 大核心工具
const OFFICIAL_TOOLS: [&str; 6] = ["bash", "edit", "glob", "grep", "read", "write"];

/// 每个工具的最小非空 schema
fn official_tool_schema(name: &str) -> serde_json::Value {
    match name {
        "bash" => serde_json::json!({
            "type": "object",
            "properties": { "command": { "type": "string" } },
            "required": ["command"]
        }),
        "edit" => serde_json::json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string" },
                "oldString": { "type": "string" },
                "newString": { "type": "string" }
            },
            "required": ["filePath", "oldString", "newString"]
        }),
        "glob" => serde_json::json!({
            "type": "object",
            "properties": { "pattern": { "type": "string" } },
            "required": ["pattern"]
        }),
        "grep" => serde_json::json!({
            "type": "object",
            "properties": { "pattern": { "type": "string" } },
            "required": ["pattern"]
        }),
        "read" => serde_json::json!({
            "type": "object",
            "properties": { "filePath": { "type": "string" } },
            "required": ["filePath"]
        }),
        "write" => serde_json::json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string" },
                "content": { "type": "string" }
            },
            "required": ["filePath", "content"]
        }),
        _ => serde_json::json!({
            "type": "object",
            "properties": { "input": { "type": "string" } },
            "required": ["input"]
        }),
    }
}

fn tool_description(name: &str) -> &'static str {
    match name {
        "bash" => "Run a bash command",
        "edit" => "Edit a file",
        "glob" => "Match files by pattern",
        "grep" => "Search file contents by pattern",
        "read" => "Read a file",
        "write" => "Write to a file",
        _ => "",
    }
}

/// 对齐 OpenCode CLI Identifier.descending("ses")
/// 格式：ses_ + 12位hex + 14位Base62，总长30
///
/// ⭐ 关键：counter 必须限制在 0–63（`& 0x3F`），确保 12 位 hex 的末三位
///    落在 fc0–fff。上游会把 12 位 hex 解码成 `~(ts_ms << 12 | counter)`，
///    如果 counter 超出范围，解码失败 → 判定为非官方客户端 → 403 FreeTierError。
pub fn generate_session_id() -> String {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0x3F;
    let raw = ((ts_ms & 0xFFFF_FFFF) << 12) | counter;
    let inverted = !raw & 0xFFFF_FFFF_FFFF;
    let hex_full = format!("{:012x}", inverted);
    let mut rng = rand::thread_rng();
    let suffix: String = (0..14)
        .map(|_| BASE62[rng.gen_range(0..62)] as char)
        .collect();
    format!("ses_{}{}", hex_full, suffix)
}

/// 升序消息 ID，前缀 msg_。
/// 每次调用基于当前时间戳独立生成，不与 session 绑定。
pub fn generate_request_id() -> String {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0x3F;
    let raw = ((ts_ms & 0xFFFF_FFFF) << 12) | counter;
    let hex_full = format!("{:012x}", raw);
    let mut rng = rand::thread_rng();
    let suffix: String = (0..14)
        .map(|_| BASE62[rng.gen_range(0..62)] as char)
        .collect();
    format!("msg_{}{}", hex_full, suffix)
}

/// 向请求体注入免费层工具契约。
///
/// 规则（2026-10-14 之后）：
/// - tools 必须包含官方 6 大工具：bash, edit, glob, grep, read, write
/// - 每个工具的 parameters 必须是非空 schema
/// - tool_choice 必须为 "auto"
///
/// 注意：不再注入 permission 字段。旧版（能工作的版本）没有这个字段，
/// 之前为对齐社区推测加的 `permission: {"*": "ask"}` 已移除。
pub fn inject_free_tier_tools(body: &mut serde_json::Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let tools = obj.entry("tools").or_insert_with(|| serde_json::json!([]));
    let Some(arr) = tools.as_array_mut() else {
        return;
    };

    // 1) 收集客户端已传的工具名 → 工具对象
    let mut existing: HashMap<String, serde_json::Value> = HashMap::new();
    for t in arr.drain(..) {
        if let Some(name) = t
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from)
        {
            existing.insert(name, t);
        }
    }

    // 2) 确保 6 大核心工具都在，且 parameters 非空
    for &name in &OFFICIAL_TOOLS {
        let entry = existing.entry(name.to_string()).or_insert_with(|| {
            serde_json::json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool_description(name),
                    "parameters": official_tool_schema(name)
                }
            })
        });

        if let Some(func) = entry.get_mut("function").and_then(|f| f.as_object_mut()) {
            let params = func
                .entry("parameters")
                .or_insert_with(|| serde_json::json!({}));
            let is_empty = params
                .as_object()
                .map(|o| o.is_empty())
                .unwrap_or(true);
            if is_empty {
                *params = official_tool_schema(name);
            }
        }
    }

    // 3) 客户端额外传入的工具也修复空 schema
    for (name, entry) in existing.iter_mut() {
        if OFFICIAL_TOOLS.contains(&name.as_str()) {
            continue;
        }
        if let Some(func) = entry.get_mut("function").and_then(|f| f.as_object_mut()) {
            let params = func
                .entry("parameters")
                .or_insert_with(|| serde_json::json!({}));
            let is_empty = params
                .as_object()
                .map(|o| o.is_empty())
                .unwrap_or(true);
            if is_empty {
                *params = official_tool_schema(name);
            }
        }
    }

    // 4) 官方工具按原始顺序排列，自定义工具追加在后
    let mut ordered: Vec<serde_json::Value> = Vec::with_capacity(existing.len());
    for &name in &OFFICIAL_TOOLS {
        if let Some(v) = existing.remove(name) {
            ordered.push(v);
        }
    }
    for (_, v) in existing.into_iter() {
        ordered.push(v);
    }
    *arr = ordered;

    // 5) tool_choice 必须为 auto
    if let Some(obj) = body.as_object_mut() {
        obj.entry("tool_choice")
            .or_insert_with(|| serde_json::json!("auto"));
    }
}

/// 构造请求头。`session_id` 由调用方决定（同一对话复用），
/// `request_id` 基于当前时间戳独立生成（每次递增）。
///
/// `x-opencode-project` 使用 `prj_0000000000000000`（旧版能工作的值）。
pub fn apply_headers(
    builder: wreq::RequestBuilder,
    session_id: &str,
) -> (wreq::RequestBuilder, String, String) {
    let request_id = generate_request_id();
    let builder = builder
        .header("Content-Type", "application/json")
        .header("User-Agent", OPENCODE_UA.as_str())
        .header("x-opencode-client", "cli")
        .header("x-opencode-session", session_id)
        .header("x-opencode-request", request_id.as_str())
        .header("x-opencode-project", "prj_0000000000000000")
        .header("x-session-affinity", session_id)
        .header("X-Session-Id", session_id);
    (builder, session_id.to_string(), request_id)
}