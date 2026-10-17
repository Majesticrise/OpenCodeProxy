use std::{collections::HashMap, env};
use serde::{Deserialize, Serialize};

lazy_static::lazy_static! {
    /// 免费层走 /inference/v1（2026-10-14 之后）
    pub static ref ZEN_UPSTREAM: String =
        env::var("ZEN_UPSTREAM").unwrap_or_else(|_| "https://opencode.ai/inference/v1".to_string());
    pub static ref LISTEN_HOST: String =
        env::var("LISTEN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    pub static ref LISTEN_PORT: u16 =
        env::var("LISTEN_PORT").unwrap_or_else(|_| "8910".to_string()).parse().unwrap_or(8910);
    pub static ref LOG_LEVEL: String =
        env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    pub static ref PROXY_FILE: String =
        env::var("PROXY_FILE").unwrap_or_else(|_| "proxies.txt".to_string());
    pub static ref VALIDATE_TARGET: String =
        env::var("VALIDATE_TARGET").unwrap_or_else(|_| "https://opencode.ai".to_string());
    pub static ref MIN_POOL_SIZE: usize =
        env::var("MIN_POOL_SIZE").unwrap_or_else(|_| "10".to_string()).parse().unwrap_or(10);
    pub static ref LOG_BUFFER_CAP: usize =
        env::var("LOG_BUFFER_CAP").unwrap_or_else(|_| "500".to_string()).parse().unwrap_or(500);

    /// OpenCode CLI 的 User-Agent，版本必须 >= 1.18.0
    /// 默认值来自抓包真实客户端
    pub static ref OPENCODE_UA: String = env::var("OPENCODE_UA")
        .unwrap_or_else(|_| "opencode/1.18.34 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14".to_string());

    /// 是否注入免费层身份头（默认开）
    pub static ref INJECT_FREE_TIER_HEADERS: bool = env::var("INJECT_FREE_TIER_HEADERS")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    /// 是否注入免费层 tools 契约（默认开）
    pub static ref INJECT_FREE_TIER_TOOLS: bool = env::var("INJECT_FREE_TIER_TOOLS")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingConfig {
    #[serde(default)]
    pub mapping: HashMap<String, Vec<String>>,
    #[serde(default = "default_true")]
    pub enable_fallback_chain: bool,
}

fn default_true() -> bool { true }

impl Default for RoutingConfig {
    fn default() -> Self {
        Self { mapping: HashMap::new(), enable_fallback_chain: true }
    }
}

impl RoutingConfig {
    pub fn from_env() -> Self {
        let mut mapping = HashMap::new();
        if let Ok(raw) = env::var("MODEL_MAPPING") {
            for pair in raw.split(',') {
                let pair = pair.trim();
                if pair.is_empty() { continue; }
                if let Some((k, v)) = pair.split_once(':') {
                    let k = k.trim().to_string();
                    let candidates: Vec<String> = v
                        .split('|')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !k.is_empty() && !candidates.is_empty() {
                        mapping.insert(k, candidates);
                    }
                }
            }
        }
        if let Ok(over) = env::var("MODEL_OVERRIDE") {
            let over = over.trim();
            if !over.is_empty() {
                mapping.insert("*".to_string(), vec![over.to_string()]);
            }
        }
        Self { mapping, enable_fallback_chain: true }
    }
}