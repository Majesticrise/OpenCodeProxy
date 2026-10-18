use crate::config::OPENCODE_UA;
use rand::Rng;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

const OFFICIAL_TOOLS: [&str; 6] = ["bash", "edit", "glob", "grep", "read", "write"];

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

// ⭐ 关键修复：补回丢失的函数头
pub fn inject_free_tier_tools(body: &mut serde_json::Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let tools = obj.entry("tools").or_insert_with(|| serde_json::json!([]));
    let Some(arr) = tools.as_array_mut() else {
        return;
    };

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

    let mut sorted: Vec<(String, serde_json::Value)> = existing.into_iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    *arr = sorted.into_iter().map(|(_, v)| v).collect();

    if let Some(obj) = body.as_object_mut() {
        obj.entry("tool_choice")
            .or_insert_with(|| serde_json::json!("auto"));
    }
}

pub fn apply_headers(
    builder: wreq::RequestBuilder,
) -> (wreq::RequestBuilder, String, String) {
    let session_id = generate_session_id();
    let request_id = generate_request_id();
    let builder = builder
        .header("Content-Type", "application/json")
        .header("User-Agent", OPENCODE_UA.as_str())
        .header("x-opencode-client", "cli")
        .header("x-opencode-session", session_id.as_str())
        .header("x-opencode-request", request_id.as_str())
        .header("x-opencode-project", "prj_0000000000000000")
        .header("x-session-affinity", session_id.as_str())
        .header("X-Session-Id", session_id.as_str());
    (builder, session_id, request_id)
}