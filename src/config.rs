use std::{collections::HashMap, env};
use serde::{Deserialize, Serialize};

lazy_static::lazy_static! {
    pub static ref ZEN_UPSTREAM: String =
        env::var("ZEN_UPSTREAM").unwrap_or_else(|_| "https://opencode.ai/zen/v1".to_string());
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
}

/// 运行时路由规则（可在 WebUI 里修改，无需重启）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// 客户端模型名 → 有序上游候选列表（第一个优先）
    /// key 为 "*" 时作为兜底
    #[serde(default)]
    pub mapping: HashMap<String, Vec<String>>,
    /// 是否启用 fallback chain：模型不可用时自动切下一个候选
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
    /// 从环境变量初始化：
    ///   MODEL_OVERRIDE=big-pickle
    ///     等价于 "*": ["big-pickle"]
    ///   MODEL_MAPPING="deepseek-v4-flash-free:big-pickle|minimax-m3-free,*:minimax-m3-free"
    ///     "|" 分隔同一 key 下的候选顺序
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