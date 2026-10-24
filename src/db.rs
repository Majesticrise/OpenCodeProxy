//! SQLite 持久化层：代理信誉（跨重启）+ 请求明细（token 计量 / 延迟 / TTFT）。
//!
//! 设计约束：
//! - `Connection` 放在 `Mutex` 里同步短调用（WAL + synchronous=NORMAL），
//!   绝不跨 `.await` 持锁。
//! - `enabled = false`（`ZEN_DB` 为空串或打开失败）时所有方法安全退化为 no-op，
//!   调用方无需判空。
//! - 建表走 `CREATE TABLE IF NOT EXISTS` + `PRAGMA user_version`，后续加表/加列
//!   在 `MIGRATIONS` 里按版本追加。

use rusqlite::{params, Connection};
use std::sync::Mutex;
use tracing::{info, warn};

pub struct Db {
    conn: Mutex<Connection>,
    enabled: bool,
}

/// 单条请求明细（MetricsGuard::drop 时写入）。
pub struct RequestRecord {
    pub ts: i64,
    pub mode: String,
    pub model: String,
    pub model_used: String,
    pub status: u16,
    pub latency_ms: u64,
    pub ttft_ms: u64,
    pub is_stream: bool,
    pub proxy_url: Option<String>,
    pub session_id: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
    pub real_usage: bool,
}

/// IP 情报富化结果（ip-api.com / proxycheck.io）。
#[derive(Clone, Default)]
pub struct IpInfo {
    pub country: Option<String>,
    pub country_code: Option<String>,
    pub isp: Option<String>,
    pub asn: Option<String>,
    /// 使用场景：Wireless（普通宽带/移动）/ Datacenter / VPN ...
    pub ip_type: Option<String>,
    /// proxycheck.io 风险评分 0-100，越低越好；None = 未查询
    pub risk: Option<u32>,
    /// ip-api hosting 标志：true = IDC/机房 IP
    pub hosting: Option<bool>,
}

/// 从代理 URL 提取 host（IP）部分，非 IP 形式返回 None。
pub fn extract_ip(url: &str) -> Option<String> {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let host = rest.split('/').next().unwrap_or("");
    let ip = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    if ip.parse::<std::net::IpAddr>().is_ok() {
        Some(ip.to_string())
    } else {
        None
    }
}

pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS proxies (
            url               TEXT PRIMARY KEY,
            country           TEXT,
            country_code      TEXT,
            isp               TEXT,
            asn               TEXT,
            ip_type           TEXT,
            risk              INTEGER,
            hosting           INTEGER,
            latency_ms        REAL NOT NULL DEFAULT 0,
            success_count     INTEGER NOT NULL DEFAULT 0,
            fail_count        INTEGER NOT NULL DEFAULT 0,
            ban_count         INTEGER NOT NULL DEFAULT 0,
            first_seen        INTEGER NOT NULL DEFAULT 0,
            last_seen         INTEGER NOT NULL DEFAULT 0,
            last_used         INTEGER NOT NULL DEFAULT 0,
            last_success      INTEGER NOT NULL DEFAULT 0,
            cooldown_until    INTEGER NOT NULL DEFAULT 0,
            status            TEXT NOT NULL DEFAULT 'active',
            source            TEXT NOT NULL DEFAULT 'fetch'
        );
        CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status);

        CREATE TABLE IF NOT EXISTS requests (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                INTEGER NOT NULL,
            mode              TEXT NOT NULL,
            model             TEXT NOT NULL,
            model_used        TEXT NOT NULL,
            status            INTEGER NOT NULL,
            latency_ms        INTEGER NOT NULL,
            ttft_ms           INTEGER NOT NULL DEFAULT 0,
            is_stream         INTEGER NOT NULL DEFAULT 0,
            proxy_url         TEXT,
            session_id        TEXT NOT NULL DEFAULT '',
            prompt_tokens     INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens      INTEGER NOT NULL DEFAULT 0,
            cached_tokens     INTEGER NOT NULL DEFAULT 0,
            reasoning_tokens  INTEGER NOT NULL DEFAULT 0,
            real_usage        INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts);
        CREATE INDEX IF NOT EXISTS idx_requests_model ON requests(model);
        ",
    )?;
    conn.pragma_update(None, "user_version", 1)?;
    Ok(())
}

/// 启动加载用的代理行。
pub struct ProxyRow {
    pub url: String,
    pub latency: f64,
    pub success_count: u32,
    pub fail_count: u32,
    pub ban_count: u32,
    pub cooldown_until_unix: i64,
    pub info: IpInfo,
    pub last_used: i64,
}

impl Db {
    /// 打开数据库；路径为空串或打开失败时退化为禁用模式（内存库，全部 no-op）。
    pub fn open(path: &str) -> Self {
        if path.is_empty() {
            info!("Database disabled (ZEN_DB is empty)");
            return Self::disabled();
        }
        match Connection::open(path) {
            Ok(conn) => match init_schema(&conn) {
                Ok(()) => {
                    info!("Database ready at {} (WAL)", path);
                    Self {
                        conn: Mutex::new(conn),
                        enabled: true,
                    }
                }
                Err(e) => {
                    warn!("Database schema init failed at {}: {}; disabling DB", path, e);
                    Self::disabled()
                }
            },
            Err(e) => {
                warn!("Database open failed at {}: {}; disabling DB", path, e);
                Self::disabled()
            }
        }
    }

    fn disabled() -> Self {
        let conn = Connection::open_in_memory().expect("in-memory sqlite");
        Self {
            conn: Mutex::new(conn),
            enabled: false,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// 新代理入库 / 已存在的刷新延迟与活跃状态。
    pub fn upsert_proxy(&self, url: &str, latency_ms: f64, source: &str) {
        if !self.enabled {
            return;
        }
        let now = now_secs();
        if let Err(e) = self.conn.lock().unwrap().execute(
            "INSERT INTO proxies (url, latency_ms, first_seen, last_seen, status, source)
             VALUES (?1, ?2, ?3, ?3, 'active', ?4)
             ON CONFLICT(url) DO UPDATE SET
                latency_ms = ?2, last_seen = ?3, status = 'active', cooldown_until = 0",
            params![url, latency_ms, now, source],
        ) {
            warn!("db upsert_proxy failed: {}", e);
        }
    }

    /// 请求结果回写。kind: success / fail / rate_limit / ban / used（仅更新使用时间）。
    pub fn record_outcome(&self, url: &str, kind: &str, cooldown_until_unix: Option<i64>) {
        if !self.enabled {
            return;
        }
        let now = now_secs();
        let sql = match kind {
            "success" => "UPDATE proxies SET success_count = success_count + 1,
                          last_used = ?2, last_success = ?2, cooldown_until = 0 WHERE url = ?1",
            "fail" => "UPDATE proxies SET fail_count = fail_count + 1, last_used = ?2,
                       cooldown_until = MAX(cooldown_until, ?3) WHERE url = ?1",
            "rate_limit" => "UPDATE proxies SET last_used = ?2,
                             cooldown_until = MAX(cooldown_until, ?3) WHERE url = ?1",
            "ban" => "UPDATE proxies SET ban_count = ban_count + 1, last_used = ?2,
                      cooldown_until = MAX(cooldown_until, ?3) WHERE url = ?1",
            _ => "UPDATE proxies SET last_used = ?2 WHERE url = ?1",
        };
        if let Err(e) = self.conn.lock().unwrap().execute(
            sql,
            params![url, now, cooldown_until_unix.unwrap_or(0)],
        ) {
            warn!("db record_outcome({}) failed: {}", kind, e);
        }
    }

    /// 达到失败阈值被移除：保留历史行（信誉累积），标记 removed。
    pub fn mark_removed(&self, url: &str) {
        if !self.enabled {
            return;
        }
        if let Err(e) = self.conn.lock().unwrap().execute(
            "UPDATE proxies SET status = 'removed' WHERE url = ?1",
            params![url],
        ) {
            warn!("db mark_removed failed: {}", e);
        }
    }

    /// 写入 IP 情报富化结果。
    pub fn enrich_proxy(&self, url: &str, info: &IpInfo) {
        if !self.enabled {
            return;
        }
        if let Err(e) = self.conn.lock().unwrap().execute(
            "UPDATE proxies SET country = ?2, country_code = ?3, isp = ?4, asn = ?5,
                    ip_type = ?6, risk = ?7, hosting = ?8 WHERE url = ?1",
            params![
                url,
                info.country,
                info.country_code,
                info.isp,
                info.asn,
                info.ip_type,
                info.risk.map(|r| r as i64),
                info.hosting.map(|h| h as i64),
            ],
        ) {
            warn!("db enrich_proxy failed: {}", e);
        }
    }

    /// 加载所有 active 代理（含信誉与富化信息），供代理池启动恢复。
    pub fn load_active_proxies(&self) -> Vec<ProxyRow> {
        if !self.enabled {
            return Vec::new();
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT url, latency_ms, success_count, fail_count, ban_count,
                    cooldown_until, country, country_code, isp, asn, ip_type, risk,
                    hosting, last_used
             FROM proxies WHERE status = 'active'",
        ) {
            Ok(s) => s,
            Err(e) => {
                warn!("db load_active_proxies prepare failed: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([], |r| {
            Ok(ProxyRow {
                url: r.get(0)?,
                latency: r.get(1)?,
                success_count: r.get::<_, i64>(2)? as u32,
                fail_count: r.get::<_, i64>(3)? as u32,
                ban_count: r.get::<_, i64>(4)? as u32,
                cooldown_until_unix: r.get(5)?,
                info: IpInfo {
                    country: r.get(6)?,
                    country_code: r.get(7)?,
                    isp: r.get(8)?,
                    asn: r.get(9)?,
                    ip_type: r.get(10)?,
                    risk: r.get::<_, Option<i64>>(11)?.map(|v| v as u32),
                    hosting: r.get::<_, Option<i64>>(12)?.map(|v| v != 0),
                },
                last_used: r.get(13)?,
            })
        });
        match rows {
            Ok(iter) => iter.filter_map(|row| row.ok()).collect(),
            Err(e) => {
                warn!("db load_active_proxies query failed: {}", e);
                Vec::new()
            }
        }
    }

    /// 导入旧版 verified_proxies.json（Vec<(url, latency)>），仅在库为空时执行。
    pub fn import_legacy_verified(&self, path: &str) -> usize {
        if !self.enabled {
            return 0;
        }
        let existing: i64 = self
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM proxies", [], |r| r.get(0))
            .unwrap_or(0);
        if existing > 0 {
            return 0;
        }
        let Ok(content) = std::fs::read_to_string(path) else {
            return 0;
        };
        let Ok(list) = serde_json::from_str::<Vec<(String, f64)>>(&content) else {
            return 0;
        };
        let n = list.len();
        for (url, lat) in list {
            self.upsert_proxy(&url, lat, "legacy_json");
        }
        if n > 0 {
            info!("Imported {} proxies from legacy {}", n, path);
        }
        n
    }

    /// 写入一条请求明细。被 PERSIST_METRICS=false 时跳过（保持旧语义）。
    pub fn record_request(&self, rec: &RequestRecord) {
        if !self.enabled || !*crate::config::PERSIST_METRICS {
            return;
        }
        if let Err(e) = self.conn.lock().unwrap().execute(
            "INSERT INTO requests (ts, mode, model, model_used, status, latency_ms,
                ttft_ms, is_stream, proxy_url, session_id, prompt_tokens,
                completion_tokens, total_tokens, cached_tokens, reasoning_tokens, real_usage)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                rec.ts,
                rec.mode,
                rec.model,
                rec.model_used,
                rec.status,
                rec.latency_ms as i64,
                rec.ttft_ms as i64,
                rec.is_stream as i64,
                rec.proxy_url,
                rec.session_id,
                rec.prompt_tokens as i64,
                rec.completion_tokens as i64,
                rec.total_tokens as i64,
                rec.cached_tokens as i64,
                rec.reasoning_tokens as i64,
                rec.real_usage as i64,
            ],
        ) {
            warn!("db record_request failed: {}", e);
        }
    }

    /// 清理过期请求明细。days <= 0 不清理。
    pub fn prune_requests(&self, days: i64) {
        if !self.enabled || days <= 0 {
            return;
        }
        let cutoff = now_secs() - days * 86400;
        match self
            .conn
            .lock()
            .unwrap()
            .execute("DELETE FROM requests WHERE ts < ?1", params![cutoff])
        {
            Ok(n) if n > 0 => info!("Pruned {} expired request rows (>{}d)", n, days),
            Ok(_) => {}
            Err(e) => warn!("db prune_requests failed: {}", e),
        }
    }

    /// 按天聚合：(day_index, requests, success, prompt, completion, total)。
    pub fn usage_daily(&self, since_unix: i64) -> Vec<(i64, i64, i64, i64, i64, i64)> {
        self.usage_agg(
            "SELECT ts/86400, COUNT(*),
                    SUM(status BETWEEN 200 AND 399),
                    SUM(COALESCE(NULLIF(prompt_tokens,0),0)),
                    SUM(COALESCE(NULLIF(completion_tokens,0),0)),
                    SUM(COALESCE(NULLIF(total_tokens,0),0))
             FROM requests WHERE ts >= ?1 GROUP BY ts/86400 ORDER BY ts/86400",
            since_unix,
        )
    }

    /// 按模型聚合：(model, requests, success, prompt, completion, total, avg_latency, avg_ttft)。
    pub fn usage_models(&self, since_unix: i64) -> Vec<UsageModelRow> {
        if !self.enabled {
            return Vec::new();
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT model, COUNT(*), SUM(status BETWEEN 200 AND 399),
                    SUM(COALESCE(NULLIF(total_tokens,0),0)),
                    SUM(COALESCE(NULLIF(prompt_tokens,0),0)),
                    SUM(COALESCE(NULLIF(completion_tokens,0),0)),
                    CAST(AVG(latency_ms) AS INTEGER),
                    CAST(AVG(CASE WHEN ttft_ms > 0 THEN ttft_ms END) AS INTEGER)
             FROM requests WHERE ts >= ?1 GROUP BY model ORDER BY COUNT(*) DESC",
        ) {
            Ok(s) => s,
            Err(e) => {
                warn!("db usage_models prepare failed: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map(params![since_unix], |r| {
            Ok(UsageModelRow {
                model: r.get(0)?,
                requests: r.get::<_, i64>(1)?,
                success: r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                total_tokens: r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                prompt_tokens: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                completion_tokens: r.get::<_, Option<i64>>(5)?.unwrap_or(0),
                avg_latency_ms: r.get::<_, Option<i64>>(6)?.unwrap_or(0),
                avg_ttft_ms: r.get::<_, Option<i64>>(7)?.unwrap_or(0),
            })
        });
        match rows {
            Ok(iter) => iter.filter_map(|row| row.ok()).collect(),
            Err(e) => {
                warn!("db usage_models query failed: {}", e);
                Vec::new()
            }
        }
    }

    fn usage_agg(&self, sql: &str, since_unix: i64) -> Vec<(i64, i64, i64, i64, i64, i64)> {
        if !self.enabled {
            return Vec::new();
        }
        let conn = self.conn.lock().unwrap();
        let Ok(mut stmt) = conn.prepare(sql) else {
            return Vec::new();
        };
        let rows = stmt.query_map(params![since_unix], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                r.get::<_, Option<i64>>(5)?.unwrap_or(0),
            ))
        });
        match rows {
            Ok(iter) => iter.filter_map(|row| row.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }
}

pub struct UsageModelRow {
    pub model: String,
    pub requests: i64,
    pub success: i64,
    pub total_tokens: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub avg_latency_ms: i64,
    pub avg_ttft_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每个测试用独立路径：cargo test 并行跑，共用路径会互相删库（WAL/SHM 冲突）。
    fn test_db(tag: &str) -> (Db, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("zen-test-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        (Db::open(path.to_str().unwrap()), path)
    }

    fn cleanup(path: &std::path::PathBuf) {
        let _ = std::fs::remove_file(path);
        for suffix in ["-wal", "-shm"] {
            let mut p = path.clone().into_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    #[test]
    fn extract_ip_variants() {
        assert_eq!(extract_ip("http://1.2.3.4:8080").as_deref(), Some("1.2.3.4"));
        assert_eq!(extract_ip("socks5://9.10.11.12:1080").as_deref(), Some("9.10.11.12"));
        assert_eq!(extract_ip("2.3.4.5:9090").as_deref(), Some("2.3.4.5"));
        // 域名形式 / 非 IP 返回 None
        assert_eq!(extract_ip("http://example.com:80"), None);
        assert_eq!(extract_ip("garbage"), None);
    }

    #[test]
    fn disabled_db_is_noop() {
        let db = Db::open("");
        assert!(!db.enabled());
        db.upsert_proxy("http://1.1.1.1:80", 1.0, "test");
        assert!(db.load_active_proxies().is_empty());
        db.record_request(&RequestRecord {
            ts: 1,
            mode: "anonymous".into(),
            model: "m".into(),
            model_used: "m".into(),
            status: 200,
            latency_ms: 10,
            ttft_ms: 5,
            is_stream: false,
            proxy_url: None,
            session_id: String::new(),
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
            cached_tokens: 0,
            reasoning_tokens: 0,
            real_usage: true,
        });
        assert!(db.usage_daily(0).is_empty());
        assert!(db.usage_models(0).is_empty());
    }

    #[test]
    fn proxy_roundtrip_and_outcomes() {
        let (db, path) = test_db("roundtrip");
        assert!(db.enabled());

        db.upsert_proxy("http://1.2.3.4:8080", 0.5, "fetch");
        db.record_outcome("http://1.2.3.4:8080", "success", None);
        db.record_outcome("http://1.2.3.4:8080", "success", None);
        db.record_outcome("http://1.2.3.4:8080", "ban", Some(1893456000));
        db.enrich_proxy(
            "http://1.2.3.4:8080",
            &IpInfo {
                country: Some("China".into()),
                country_code: Some("CN".into()),
                isp: Some("China Mobile".into()),
                asn: Some("AS9808".into()),
                ip_type: Some("Wireless".into()),
                risk: Some(75),
                hosting: Some(false),
            },
        );

        let rows = db.load_active_proxies();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.success_count, 2);
        assert_eq!(r.ban_count, 1);
        assert_eq!(r.cooldown_until_unix, 1893456000);
        assert_eq!(r.info.country_code.as_deref(), Some("CN"));
        assert_eq!(r.info.risk, Some(75));
        assert_eq!(r.info.hosting, Some(false));

        // 移除后不再加载，但行保留（重新 upsert 会复活）
        db.mark_removed("http://1.2.3.4:8080");
        assert!(db.load_active_proxies().is_empty());
        db.upsert_proxy("http://1.2.3.4:8080", 0.4, "fetch");
        let rows = db.load_active_proxies();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].success_count, 2, "信誉应跨移除/复活保留");

        cleanup(&path);
    }

    #[test]
    fn request_log_and_usage_aggregation() {
        let (db, path) = test_db("usage");
        let now = now_secs();
        // 对齐到当天 00:01 之后，保证 4 条记录落在同一个自然日（UTC）
        let base = (now / 86400) * 86400 + 60;

        for i in 0..4 {
            db.record_request(&RequestRecord {
                ts: base + i * 60,
                mode: "anonymous".into(),
                model: "model-a".into(),
                model_used: "model-b".into(),
                status: if i == 3 { 502 } else { 200 },
                latency_ms: 100 + i as u64,
                ttft_ms: 10,
                is_stream: i % 2 == 0,
                proxy_url: Some("http://1.2.3.4:8080".into()),
                session_id: "ses_x".into(),
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                cached_tokens: 0,
                reasoning_tokens: 0,
                real_usage: true,
            });
        }

        let daily = db.usage_daily(base - 1);
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].1, 4, "requests");
        assert_eq!(daily[0].2, 3, "success");
        assert_eq!(daily[0].5, 60, "total tokens");

        let models = db.usage_models(base - 1);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model, "model-a");
        assert_eq!(models[0].requests, 4);
        assert_eq!(models[0].success, 3);
        assert_eq!(models[0].total_tokens, 60);

        // 两天前插一条旧记录；prune(1天) 只应清掉它，今天的 4 条保留
        db.record_request(&RequestRecord {
            ts: now - 2 * 86400,
            mode: "anonymous".into(),
            model: "old".into(),
            model_used: "old".into(),
            status: 200,
            latency_ms: 1,
            ttft_ms: 0,
            is_stream: false,
            proxy_url: None,
            session_id: String::new(),
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cached_tokens: 0,
            reasoning_tokens: 0,
            real_usage: false,
        });
        db.prune_requests(1);
        let daily = db.usage_daily(base - 1);
        assert_eq!(daily.len(), 1, "今天的记录应保留");
        assert_eq!(daily[0].1, 4);
        assert!(db.usage_models(0).iter().all(|m| m.model != "old"), "旧记录应被清掉");

        cleanup(&path);
    }
}
