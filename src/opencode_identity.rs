use crate::config::OPENCODE_UA;
use rand::Rng;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

const OFFICIAL_TOOLS: [&str; 6] = ["bash", "edit", "glob", "grep", "read", "write"];
const SEED_CONTENT_MAX_CHARS: usize = 200;

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

/// 独立生成的 session_id，只在「无 messages 且无客户端头」等极端情况使用。
/// 正常路径走 `derive_session_from_messages`，保持同对话稳定。
#[allow(dead_code)] // 抓包验证期保留作为回退方案
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

/// 生成 request_id。
///
/// ⭐ P0 修正：回到基于当前时间戳独立生成。
///
///    上一轮根据社区注释把 msg hex 改成「session hex 的按位取反」，
///    但这会让同一 session 下所有请求的 msg hex 完全相同——真实 CLI 每条
///    消息的 msg hex 都随时间前进，全相同的模式是极强的指纹。
///    社区注释 "msg hex 是 session hex 的按位取反" 很可能只描述了首次
///    创建时的巧合，不是普遍规律。
pub fn generate_request_id(_session_id: &str) -> String {
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
    format!("msg_{}{}", hex_full, suffix)
}

fn extract_content_str(content: Option<&serde_json::Value>) -> Option<String> {
    content.and_then(|c| match c {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(parts) => {
            let mut buf = String::new();
            for p in parts {
                if let Some(t) = p.get("text").and_then(|t| t.as_str()) {
                    buf.push_str(t);
                }
            }
            if buf.is_empty() {
                None
            } else {
                Some(buf)
            }
        }
        _ => None,
    })
}

/// 从 messages 内容哈希派生一个稳定的 session_id（nonce = 0）。
pub fn derive_session_from_messages(body: &serde_json::Value) -> String {
    derive_session_from_messages_with_nonce(body, 0)
}

/// 带 nonce 的派生版本。nonce > 0 时用于 403 FreeTierError 后强制换 session。
///
/// seed 只用「首条 system + 首条 user 前 200 字符」，保证多轮对话
/// （messages 增长）和增量客户端（只发新消息）都能保持 session 稳定。
///
/// ⭐ P2 修正：当 messages 字段完全缺失时，用纳秒时间戳做一次性种子，
///    避免一批异常请求（如某些客户端的探测）共享同一个 session。
pub fn derive_session_from_messages_with_nonce(
    body: &serde_json::Value,
    nonce: u32,
) -> String {
    let mut seed_parts: Vec<String> = Vec::new();
    if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
        seed_parts.push(format!("m:{}", model));
    }
    if nonce > 0 {
        seed_parts.push(format!("n:{}", nonce));
    }

    let msgs = body.get("messages").and_then(|m| m.as_array());
    let mut content_found = false;

    if let Some(sys) = msgs.and_then(|a| {
        a.iter()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("system"))
    }) {
        if let Some(content) = extract_content_str(sys.get("content")) {
            seed_parts.push(format!(
                "sys:{}",
                content.chars().take(SEED_CONTENT_MAX_CHARS).collect::<String>()
            ));
            content_found = true;
        }
    }

    if let Some(user) = msgs.and_then(|a| {
        a.iter()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
    }) {
        if let Some(content) = extract_content_str(user.get("content")) {
            seed_parts.push(format!(
                "u:{}",
                content.chars().take(SEED_CONTENT_MAX_CHARS).collect::<String>()
            ));
            content_found = true;
        }
    }

    if !content_found && msgs.is_none() {
        // messages 完全缺失：用纳秒作为一次性种子，避免一批探测请求共享 ses_
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        seed_parts.push(format!("nanos:{}", nanos));
    } else if !content_found {
        if let Some(a) = msgs {
            seed_parts.push(format!("len:{}", a.len()));
        }
    }

    let seed = seed_parts.join("\x00");

    // FNV-1a 64-bit
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in seed.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }

    let hex_part = format!("{:012x}", hash & 0xFFFF_FFFF_FFFF);

    let mut state = hash;
    let mut suffix = String::with_capacity(14);
    for _ in 0..14 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        suffix.push(BASE62[(state >> 33) as usize % 62] as char);
    }

    format!("ses_{}{}", hex_part, suffix)
}

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

    // 保持官方工具原始顺序，自定义工具追加在后
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

    if let Some(obj) = body.as_object_mut() {
        obj.entry("tool_choice")
            .or_insert_with(|| serde_json::json!("auto"));

        // ⭐ P1 修正：改为 insert 强制覆盖。
        //    免费层下客户端意图不重要，客户端传 permission:{"*":"deny"}
        //    会导致上游拒绝；强制 ask 比保留 deny 更安全。
        //
        //    ⚠️ permission 的确切位置（顶层 vs agent 内）尚未被本地抓包验证。
        //    如果抓包发现位置不对，把这行删掉或移动到正确层级。
        //
        //    注意：serde_json::Map::insert 需要 String 而非 &str，
        //    这里必须 .to_string()（entry() 有 Into<String> 便利，insert 没有）。
        obj.insert(
            "permission".to_string(),
            serde_json::json!({ "*": "ask" }),
        );
    }
}

/// 构造请求头。`session_id` 由调用方决定（同一对话复用），
/// `request_id` 基于当前时间戳独立生成（每次递增）。
pub fn apply_headers(
    builder: wreq::RequestBuilder,
    session_id: &str,
) -> (wreq::RequestBuilder, String, String) {
    let request_id = generate_request_id(session_id);
    let builder = builder
        .header("Content-Type", "application/json")
        .header("User-Agent", OPENCODE_UA.as_str())
        .header("x-opencode-client", "cli")
        .header("x-opencode-session", session_id)
        .header("x-opencode-request", request_id.as_str())
        .header("x-opencode-project", "global")
        .header("x-session-affinity", session_id)
        .header("X-Session-Id", session_id);
    (builder, session_id.to_string(), request_id)
}