use std::{collections::HashMap, env, sync::LazyLock};
use serde::{Deserialize, Serialize};

/// 免费层走 /inference/openai/v1（2026-10-14 之后）
pub static ZEN_UPSTREAM: LazyLock<String> = LazyLock::new(|| {
    env::var("ZEN_UPSTREAM")
        .unwrap_or_else(|_| "https://opencode.ai/inference/openai/v1".to_string())
});

pub static LISTEN_HOST: LazyLock<String> =
    LazyLock::new(|| env::var("LISTEN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()));

pub static LISTEN_PORT: LazyLock<u16> = LazyLock::new(|| {
    env::var("LISTEN_PORT")
        .unwrap_or_else(|_| "8910".to_string())
        .parse()
        .unwrap_or(8910)
});

pub static LOG_LEVEL: LazyLock<String> =
    LazyLock::new(|| env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()));

pub static PROXY_FILE: LazyLock<String> =
    LazyLock::new(|| env::var("PROXY_FILE").unwrap_or_else(|_| "proxies.txt".to_string()));

pub static VALIDATE_TARGET: LazyLock<String> = LazyLock::new(|| {
    env::var("VALIDATE_TARGET")
        .unwrap_or_else(|_| "https://opencode.ai/zen/v1/models".to_string())
});

pub static MIN_POOL_SIZE: LazyLock<usize> = LazyLock::new(|| {
    env::var("MIN_POOL_SIZE")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10)
});

pub static LOG_BUFFER_CAP: LazyLock<usize> = LazyLock::new(|| {
    env::var("LOG_BUFFER_CAP")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .unwrap_or(500)
});

pub static ROUTING_FILE: LazyLock<String> =
    LazyLock::new(|| env::var("ROUTING_FILE").unwrap_or_else(|_| "routing.json".to_string()));

/// OpenCode CLI 的 User-Agent，版本必须 >= 1.18.0
/// 默认值来自抓包真实客户端
pub static OPENCODE_UA: LazyLock<String> = LazyLock::new(|| {
    env::var("OPENCODE_UA").unwrap_or_else(|_| {
        "opencode/1.18.34 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14".to_string()
    })
});

/// 是否注入免费层身份头（默认开）
pub static INJECT_FREE_TIER_HEADERS: LazyLock<bool> = LazyLock::new(|| {
    env::var("INJECT_FREE_TIER_HEADERS")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true)
});

/// 是否注入免费层 tools 契约（默认开）
pub static INJECT_FREE_TIER_TOOLS: LazyLock<bool> = LazyLock::new(|| {
    env::var("INJECT_FREE_TIER_TOOLS")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true)
});

/// metrics 聚合总量的持久化文件（只存数字，不存明细）
pub static METRICS_FILE: LazyLock<String> = LazyLock::new(|| {
    env::var("METRICS_FILE").unwrap_or_else(|_| "metrics_totals.json".to_string())
});

/// 是否持久化 metrics（默认开）
pub static PERSIST_METRICS: LazyLock<bool> = LazyLock::new(|| {
    env::var("PERSIST_METRICS")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true)
});

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingConfig {
    #[serde(default)]
    pub mapping: HashMap<String, Vec<String>>,
    #[serde(default = "default_true")]
    pub enable_fallback_chain: bool,
}

fn default_true() -> bool {
    true
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            mapping: HashMap::new(),
            enable_fallback_chain: true,
        }
    }
}

impl RoutingConfig {
    pub fn from_env() -> Self {
        let mut mapping = HashMap::new();
        if let Ok(raw) = env::var("MODEL_MAPPING") {
            for pair in raw.split(',') {
                let pair = pair.trim();
                if pair.is_empty() {
                    continue;
                }
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
        Self {
            mapping,
            enable_fallback_chain: true,
        }
    }

    pub fn from_file_or_env() -> Self {
        let path = &*crate::config::ROUTING_FILE;
        if let Ok(content) = std::fs::read_to_string(path) {
            match serde_json::from_str::<RoutingConfig>(&content) {
                Ok(cfg) => {
                    tracing::info!(
                        "Loaded routing config from {} ({} rules, fallback={})",
                        path,
                        cfg.mapping.len(),
                        cfg.enable_fallback_chain
                    );
                    return cfg;
                }
                Err(e) => {
                    tracing::warn!("Corrupt routing config at {}: {}; using env", path, e);
                }
            }
        }
        Self::from_env()
    }
}