use crate::config::SETTINGS_FILE;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{info, warn};

/// 运行时模式。
/// - `Anonymous`：代理池 + 免费层伪装，不带 API Key（当前默认行为）
/// - `Authenticated`：完全直连 + API Key，保留伪装
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Anonymous,
    Authenticated,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Anonymous
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuntimeSettings {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub api_key: Option<String>,
    /// 实名模式下的 Console org id（`org_...`）。
    /// 当前 `forward.rs` 优先从环境变量 `OPENCODE_ORG_ID` 读取；
    /// 这里保留字段是为了将来支持在 WebUI 里配置并落盘。
    #[serde(default)]
    pub org_id: Option<String>,
    /// 代理过滤：国家白名单（ISO 3166-1 两位码，空 = 不过滤）。
    #[serde(default)]
    pub proxy_country_whitelist: Vec<String>,
    /// 代理过滤：proxycheck 风险评分上限（None = 不过滤；未评分代理不受限）。
    #[serde(default)]
    pub proxy_max_risk: Option<u32>,
}

impl RuntimeSettings {
    pub fn load() -> Self {
        let path = &*SETTINGS_FILE;
        match std::fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<RuntimeSettings>(&content) {
                Ok(mut s) => {
                    // 环境变量优先：如果设置了 `OPENCODE_ORG_ID`，覆盖文件里的值
                    if let Ok(env_org) = std::env::var("OPENCODE_ORG_ID") {
                        let env_org = env_org.trim().to_string();
                        if !env_org.is_empty() {
                            s.org_id = Some(env_org);
                        }
                    }
                    info!(
                        "Loaded settings from {}: mode={:?}, api_key={}, org_id={}",
                        path,
                        s.mode,
                        if s.api_key.is_some() { "set" } else { "none" },
                        if s.org_id.is_some() { "set" } else { "none" }
                    );
                    s
                }
                Err(e) => {
                    warn!("Corrupt settings at {}: {}; using defaults", path, e);
                    Self::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("Settings file {} not found; using defaults", path);
                Self::default()
            }
            Err(e) => {
                warn!("Failed to read settings at {}: {}", path, e);
                Self::default()
            }
        }
    }

    /// 同步落盘（settings 只有几百字节，同步写完全可接受）。
    pub fn save(&self) -> std::io::Result<()> {
        let path = &*SETTINGS_FILE;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let tmp = Path::new(path).with_extension("json.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// 脱敏后的 API Key，用于 API 响应 / 前端展示。
    pub fn masked_api_key(&self) -> Option<String> {
        self.api_key.as_ref().map(|k| {
            if k.len() <= 8 {
                "***".to_string()
            } else {
                format!("{}...{}", &k[..4], &k[k.len() - 4..])
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_default_is_anonymous() {
        assert_eq!(Mode::default(), Mode::Anonymous);
    }

    #[test]
    fn mode_serializes_lowercase() {
        assert_eq!(serde_json::to_string(&Mode::Anonymous).unwrap(), "\"anonymous\"");
        assert_eq!(
            serde_json::to_string(&Mode::Authenticated).unwrap(),
            "\"authenticated\""
        );
    }

    #[test]
    fn mode_deserializes_from_lowercase() {
        let m: Mode = serde_json::from_str("\"authenticated\"").unwrap();
        assert_eq!(m, Mode::Authenticated);
        let m: Mode = serde_json::from_str("\"anonymous\"").unwrap();
        assert_eq!(m, Mode::Anonymous);
    }

    #[test]
    fn masked_key_normal_length() {
        let s = RuntimeSettings {
            mode: Mode::Anonymous,
            api_key: Some("sk-1234567890abcdef".to_string()),
            org_id: None,
            ..Default::default()
        };
        assert_eq!(s.masked_api_key(), Some("sk-1...cdef".to_string()));
    }

    #[test]
    fn masked_key_short() {
        let s = RuntimeSettings {
            mode: Mode::Anonymous,
            api_key: Some("short".to_string()),
            org_id: None,
            ..Default::default()
        };
        assert_eq!(s.masked_api_key(), Some("***".to_string()));
    }

    #[test]
    fn masked_key_none() {
        let s = RuntimeSettings::default();
        assert_eq!(s.masked_api_key(), None);
    }

    #[test]
    fn settings_roundtrip() {
        let original = RuntimeSettings {
            mode: Mode::Authenticated,
            api_key: Some("sk-test".to_string()),
            org_id: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: RuntimeSettings = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.mode, Mode::Authenticated);
        assert_eq!(restored.api_key, Some("sk-test".to_string()));
        assert_eq!(restored.org_id, None);
    }

    #[test]
    fn settings_roundtrip_with_org_id() {
        let original = RuntimeSettings {
            mode: Mode::Authenticated,
            api_key: Some("oc_sk_test".to_string()),
            org_id: Some("org_abc123".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: RuntimeSettings = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.org_id, Some("org_abc123".to_string()));
    }

    #[test]
    fn settings_deserializes_missing_fields() {
        // 空 JSON 应该得到默认值（无 key、无 org、匿名模式）
        let s: RuntimeSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(s.mode, Mode::Anonymous);
        assert_eq!(s.api_key, None);
        assert_eq!(s.org_id, None);
    }
}