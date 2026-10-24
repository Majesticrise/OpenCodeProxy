use crate::config::{MIN_POOL_SIZE, VALIDATE_TARGET};
use crate::db::{extract_ip, now_secs, Db, IpInfo};
use futures_util::stream::{self, StreamExt};
use rand::seq::SliceRandom;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const COOLDOWN_RATE_LIMIT: Duration = Duration::from_secs(30);
const COOLDOWN_CONNECTION: Duration = Duration::from_secs(300);
const COOLDOWN_BANNED: Duration = Duration::from_secs(900);
const MAX_FAILS_BEFORE_REMOVE: u32 = 5;
/// Banned 阈值单独设更高：403 FreeTierError 有时是 session 层面的临时拒绝，
/// 换一次 session 可能就过；攒 10 次才移除，避免代理池被瞬时误判掏空。
const MAX_BANS_BEFORE_REMOVE: u32 = 10;

/// 每验证成功多少个代理就 flush 一批到池子
const VALIDATION_BATCH_FLUSH: usize = 5;
/// 池子上限（按 latency 排序后保留前 N 个）
const MAX_POOL_SIZE: usize = 120;

fn impersonate() -> wreq_util::Profile {
    wreq_util::Profile::Chrome131
}

#[derive(Clone)]
pub struct ProxyEntry {
    pub url: String,
    pub latency: f64,
    pub cooldown_until: Option<Instant>,
    pub fail_count: u32,
    pub last_success: Option<Instant>,
    /// 累计请求成功次数。优先复用该值高的代理，避免每次盲选反复重试。
    pub success_count: u32,
    /// 累计被上游风控（403 FreeTier）次数。
    pub ban_count: u32,
    /// IP 情报（国家/ISP/ASN/使用场景/风险评分/是否机房），iplark 类信息。
    pub info: IpInfo,
    pub first_seen: i64,
    pub last_used: i64,
}

impl ProxyEntry {
    fn new(url: String, latency: f64) -> Self {
        let now = now_secs();
        Self {
            url,
            latency,
            cooldown_until: None,
            fail_count: 0,
            last_success: None,
            success_count: 0,
            ban_count: 0,
            info: IpInfo::default(),
            first_seen: now,
            last_used: 0,
        }
    }

    pub fn available(&self, now: Instant) -> bool {
        self.cooldown_until.map_or(true, |t| now >= t)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    RateLimit,
    Connection,
    Banned,
}

impl FailureKind {
    fn cooldown(self) -> Duration {
        match self {
            FailureKind::RateLimit => COOLDOWN_RATE_LIMIT,
            FailureKind::Connection => COOLDOWN_CONNECTION,
            FailureKind::Banned => COOLDOWN_BANNED,
        }
    }

    /// 该失败类型累计 fail_count 并在达到阈值时被移除；RateLimit 不计入。
    fn max_fails_before_removal(self) -> Option<u32> {
        match self {
            FailureKind::Connection => Some(MAX_FAILS_BEFORE_REMOVE),
            FailureKind::Banned => Some(MAX_BANS_BEFORE_REMOVE),
            FailureKind::RateLimit => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            FailureKind::RateLimit => "rate-limited",
            FailureKind::Connection => "connection-error",
            FailureKind::Banned => "banned",
        }
    }

    /// 数据库 record_outcome 的 kind 参数。
    fn db_kind(self) -> &'static str {
        match self {
            FailureKind::RateLimit => "rate_limit",
            FailureKind::Connection => "fail",
            FailureKind::Banned => "ban",
        }
    }
}

pub struct ProxyPoolInner {
    pub proxies: Vec<ProxyEntry>,
    pub file_path: String,
    pub is_running: AtomicBool,
    pub total_requests: AtomicU64,
}

/// 代理选择过滤规则（WebUI 可热更新，持久化在 settings.json）。
#[derive(Clone, Debug, Default)]
pub struct ProxyFilters {
    /// 国家白名单（ISO 两位码，空 = 不过滤）。
    pub country_whitelist: Vec<String>,
    /// proxycheck 风险评分上限；None = 不过滤。未评分（None risk）的代理不受限。
    pub max_risk: Option<u32>,
}

/// 过滤判定：白名单为空不过滤国家；risk 上限只拦截「已知超限」的代理。
fn entry_allowed(p: &ProxyEntry, f: &ProxyFilters) -> bool {
    if !f.country_whitelist.is_empty() {
        match &p.info.country_code {
            Some(cc) if f.country_whitelist.iter().any(|w| w.eq_ignore_ascii_case(cc)) => {}
            _ => return false,
        }
    }
    if let Some(max) = f.max_risk {
        if let Some(r) = p.info.risk {
            if r > max {
                return false;
            }
        }
    }
    true
}

#[derive(Clone)]
pub struct ProxyPool {
    pub inner: Arc<RwLock<ProxyPoolInner>>,
    pub is_fetching: Arc<AtomicBool>,
    /// SQLite 层：信誉跨重启持久化 + IP 情报。禁用时内部方法全部 no-op。
    pub db: Arc<Db>,
    /// 选择过滤规则（国家白名单 / 风险上限）。
    pub filters: Arc<std::sync::RwLock<ProxyFilters>>,
}

struct FetchingReset(Arc<AtomicBool>);
impl Drop for FetchingReset {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl ProxyPool {
    pub fn new(file_path: &str, db: Arc<Db>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ProxyPoolInner {
                proxies: Vec::new(),
                file_path: file_path.to_string(),
                is_running: AtomicBool::new(true),
                total_requests: AtomicU64::new(0),
            })),
            is_fetching: Arc::new(AtomicBool::new(false)),
            db,
            filters: Arc::new(std::sync::RwLock::new(ProxyFilters::default())),
        }
    }

    /// 更新选择过滤规则（WebUI 保存时调用，立即生效）。
    pub fn set_filters(&self, f: ProxyFilters) {
        *self.filters.write().unwrap() = f;
    }

    pub async fn load_proxies(&self) {
        let file_path = { self.inner.read().await.file_path.clone() };

        if Path::new(&file_path).exists() {
            if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
                let mut file_proxies = Vec::new();
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    let proxy_url = if !(line.starts_with("http://")
                        || line.starts_with("https://")
                        || line.starts_with("socks5://"))
                    {
                        format!("http://{}", line)
                    } else {
                        line.to_string()
                    };
                    file_proxies.push(ProxyEntry::new(proxy_url, 0.0));
                }
                if !file_proxies.is_empty() {
                    for p in &file_proxies {
                        self.db.upsert_proxy(&p.url, p.latency, "file");
                    }
                    let mut inner = self.inner.write().await;
                    let n = file_proxies.len();
                    inner.proxies = file_proxies;
                    info!("Loaded {} proxies from local file {}", n, file_path);
                    return;
                }
            }
        }

        // ── 数据库恢复：信誉/冷却/IP 情报跨重启保留 ──
        // 首次运行时顺带把旧版 verified_proxies.json 导入数据库。
        self.db.import_legacy_verified("verified_proxies.json");
        let rows = self.db.load_active_proxies();
        if !rows.is_empty() {
            let now_unix = now_secs();
            let now_inst = Instant::now();
            let entries: Vec<ProxyEntry> = rows
                .into_iter()
                .map(|r| ProxyEntry {
                    url: r.url,
                    latency: r.latency,
                    cooldown_until: if r.cooldown_until_unix > now_unix {
                        Some(
                            now_inst
                                + Duration::from_secs((r.cooldown_until_unix - now_unix) as u64),
                        )
                    } else {
                        None
                    },
                    fail_count: r.fail_count,
                    last_success: None,
                    success_count: r.success_count,
                    ban_count: r.ban_count,
                    info: r.info,
                    first_seen: 0, // DB 行有值但此处无需回填
                    last_used: r.last_used,
                })
                .collect();
            let n = entries.len();
            let mut inner = self.inner.write().await;
            inner.proxies = entries;
            info!(
                "Restored {} proxies from database (with reputation & IP intel)",
                n
            );
            return;
        }

        // ── 旧版 verified_proxies.json（数据库禁用时的兜底） ──
        if let Ok(content) = tokio::fs::read_to_string("verified_proxies.json").await {
            if let Ok(validated) = serde_json::from_str::<Vec<(String, f64)>>(&content) {
                if !validated.is_empty() {
                    let mut inner = self.inner.write().await;
                    let n = validated.len();
                    inner.proxies = validated
                        .into_iter()
                        .map(|(url, lat)| ProxyEntry::new(url, lat))
                        .collect();
                    info!("Loaded {} proxies from verified_proxies.json", n);
                    return;
                }
            }
        }

        self.fetch_free_proxies().await;
    }

    pub async fn fetch_free_proxies(&self) {
        if self
            .is_fetching
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let self_clone = self.clone();
        tokio::spawn(async move {
            let _reset = FetchingReset(self_clone.is_fetching.clone());

            info!("Fetching free proxy list from multiple public APIs...");

            let sources = vec![
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
                "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
                "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
                "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
                "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
            ];

            let mut raw_ips = HashSet::new();
            let client = wreq::Client::builder()
                .emulation(impersonate())
                .timeout(Duration::from_secs(8))
                .build()
                .unwrap();

            for url in sources {
                match client.get(url).send().await {
                    Ok(resp) if resp.status() == 200 => {
                        if let Ok(text) = resp.text().await {
                            for line in text.lines() {
                                let line = line.trim();
                                if !line.is_empty() {
                                    raw_ips.insert(line.to_string());
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to fetch from source {}: {}", url, e),
                    _ => {}
                }
            }

            if raw_ips.is_empty() {
                error!("All public proxy sources failed or returned empty list.");
                return;
            }

            let raw_proxies: Vec<String> = raw_ips
                .into_iter()
                .map(|ip| format!("http://{}", ip))
                .collect();
            info!(
                "Fetched {} unique raw proxies. Starting background validation...",
                raw_proxies.len()
            );
            self_clone.validate_proxies(raw_proxies).await;
        });
    }

    async fn validate_proxies(&self, raw_proxies: Vec<String>) {
        let sample_size = std::cmp::min(7000, raw_proxies.len());
        let mut test_targets = raw_proxies;
        test_targets.shuffle(&mut rand::thread_rng());
        test_targets.truncate(sample_size);
        let total_targets = test_targets.len();

        let existing_entries: HashMap<String, ProxyEntry> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .cloned()
                .map(|p| (p.url.clone(), p))
                .collect()
        };

        let sem = Arc::new(tokio::sync::Semaphore::new(200));
        let target = VALIDATE_TARGET.clone();

        let mut batch: Vec<(String, f64)> = Vec::with_capacity(VALIDATION_BATCH_FLUSH);
        let mut checked = 0usize;
        let mut total_validated = 0usize;
        let mut flush_batches = 0usize;

        let mut stream = stream::iter(test_targets.into_iter().map(|proxy| {
            let sem = sem.clone();
            let target = target.clone();
            async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let client = wreq::Client::builder()
                    .emulation(impersonate())
                    .proxy(wreq::Proxy::all(&proxy).ok()?)
                    .timeout(Duration::from_millis(5000))
                    .build()
                    .ok()?;
                match client.get(&target).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        Some((proxy, start.elapsed().as_secs_f64()))
                    }
                    _ => None,
                }
            }
        }))
        .buffer_unordered(200);

        while let Some(result) = stream.next().await {
            checked += 1;
            if let Some(item) = result {
                total_validated += 1;
                batch.push(item);

                if batch.len() >= VALIDATION_BATCH_FLUSH {
                    let flushed =
                        self.flush_validation_batch(&mut batch, &existing_entries).await;
                    flush_batches += 1;

                    let pool_size = self.inner.read().await.proxies.len();
                    info!(
                        "Validating: {}/{} checked, {} passed, flushed {} (batch #{}, pool now {})",
                        checked,
                        total_targets,
                        total_validated,
                        flushed,
                        flush_batches,
                        pool_size
                    );
                }
            }
        }

        if !batch.is_empty() {
            let flushed = self
                .flush_validation_batch(&mut batch, &existing_entries)
                .await;
            if flushed > 0 {
                let pool_size = self.inner.read().await.proxies.len();
                info!(
                    "Validating: final partial batch flushed {} (pool now {})",
                    flushed, pool_size
                );
            }
        }

        let (pool_size, available_count, fastest) = {
            let inner = self.inner.read().await;
            let now = Instant::now();
            let avail = inner.proxies.iter().filter(|p| p.available(now)).count();
            let fastest = inner
                .proxies
                .iter()
                .filter(|p| p.latency > 0.0)
                .map(|p| p.latency)
                .fold(f64::INFINITY, f64::min);
            (inner.proxies.len(), avail, fastest)
        };

        if total_validated == 0 {
            warn!(
                "Proxy validation complete: 0/{} passed. Pool preserved ({} nodes).",
                checked, pool_size
            );
        } else {
            info!(
                "Proxy validation complete: {}/{} passed ({} flush batches), pool={} ({} available), fastest={:.2}s",
                total_validated,
                checked,
                flush_batches,
                pool_size,
                available_count,
                if fastest.is_finite() { fastest } else { 0.0 }
            );
        }

        self.persist_verified_proxies().await;

        // 验证完成后补一轮 IP 情报富化（国家 / ISP / 评分），失败不影响池子
        self.enrich_pending().await;
    }

    async fn flush_validation_batch(
        &self,
        batch: &mut Vec<(String, f64)>,
        existing: &HashMap<String, ProxyEntry>,
    ) -> usize {
        if batch.is_empty() {
            return 0;
        }
        let count = batch.len();
        let for_db: Vec<(String, f64)> = batch.clone();

        {
            let mut inner = self.inner.write().await;
            for (url, lat) in batch.drain(..) {
                let entry = match existing.get(&url).cloned() {
                    Some(mut old) => {
                        old.latency = lat;
                        old
                    }
                    None => ProxyEntry::new(url, lat),
                };

                if let Some(pos) = inner.proxies.iter().position(|p| p.url == entry.url) {
                    inner.proxies[pos] = entry;
                } else {
                    inner.proxies.push(entry);
                }
            }

            // 未测速（latency <= 0，如刚从 proxies.txt 加载）排在已测速节点之前，
            // 防止它们被下面的 MAX_POOL_SIZE 截断掉；冷启动选择见 get_proxy（优先已测速）。
            inner.proxies.sort_by(|a, b| {
                let la = if a.latency <= 0.0 { -1.0 } else { a.latency };
                let lb = if b.latency <= 0.0 { -1.0 } else { b.latency };
                la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
            });
            if inner.proxies.len() > MAX_POOL_SIZE {
                inner.proxies.truncate(MAX_POOL_SIZE);
            }
        }

        // 池锁释放后再落库（同步短操作，避免占着池写锁做 IO）
        for (url, lat) in &for_db {
            self.db.upsert_proxy(url, *lat, "fetch");
        }
        count
    }

    async fn persist_verified_proxies(&self) {
        let now = Instant::now();
        let to_save: Vec<(String, f64)> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .filter(|p| p.available(now))
                .map(|p| (p.url.clone(), p.latency))
                .collect()
        };

        if to_save.is_empty() {
            return;
        }

        match serde_json::to_string_pretty(&to_save) {
            Ok(json_str) => {
                if let Err(e) = tokio::fs::write("verified_proxies.json", json_str).await {
                    warn!("Failed to persist proxy pool: {}", e);
                } else {
                    debug!(
                        "Proxy pool persisted to verified_proxies.json ({} nodes)",
                        to_save.len()
                    );
                }
            }
            Err(e) => warn!("Failed to serialize proxy pool: {}", e),
        }
    }

    pub async fn get_proxy(&self) -> Option<String> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let mut rng = rand::thread_rng();
        let filters = self.filters.read().unwrap().clone();

        let available: Vec<&ProxyEntry> = inner
            .proxies
            .iter()
            .filter(|p| p.available(now) && entry_allowed(p, &filters))
            .collect();

        if available.is_empty() {
            return None;
        }

        // 「成功数优先」：只在历史成功过的节点里选；没有成功记录时，
        // 在最快的前 15 个节点中冷启动盲选。
        let proven: Vec<&ProxyEntry> = available
            .iter()
            .copied()
            .filter(|p| p.success_count > 0)
            .collect();

        if !proven.is_empty() {
            // proven 是 Vec<&ProxyEntry>，choose 返回 Option<&&ProxyEntry>，
            // p.url.clone() 通过 auto-deref 得到 String。
            return proven.choose(&mut rng).map(|p| p.url.clone());
        }

        // 冷启动盲选：优先在「已测速」（latency > 0，验证时实测过）的节点里挑。
        // 排序键 = 实测延迟 + 风险惩罚（proxycheck risk 每 1 分折 0.01s，
        // 未查询按中性 0.5s 计），让低风险节点优先获得真实流量。
        let mut cold: Vec<&ProxyEntry> = {
            let measured: Vec<&ProxyEntry> = available
                .iter()
                .copied()
                .filter(|p| p.latency > 0.0)
                .collect();
            if measured.is_empty() {
                available
            } else {
                measured
            }
        };
        let risk_penalty = |p: &ProxyEntry| p.info.risk.map(|r| r as f64 * 0.01).unwrap_or(0.5);
        cold.sort_by(|a, b| {
            let ka = a.latency + risk_penalty(a);
            let kb = b.latency + risk_penalty(b);
            ka.partial_cmp(&kb).unwrap_or(std::cmp::Ordering::Equal)
        });
        let limit = std::cmp::min(15, cold.len());
        cold[..limit].choose(&mut rng).map(|p| p.url.clone())
    }
    /// 检查某个 URL 对应的代理当前是否可用（存在于池中且未处于冷却）。
    /// 用于 session → 代理绑定的可用性判定。
    pub async fn is_available(&self, proxy_url: &str) -> bool {
        let inner = self.inner.read().await;
        let now = Instant::now();
        inner
            .proxies
            .iter()
            .find(|p| p.url == proxy_url)
            .map(|p| p.available(now))
            .unwrap_or(false)
    }

    /// 该 URL 是否仍在池中（不看冷却状态）。
    /// 用于 client 缓存清理这类只关心 membership 的判断，与 `is_available` 区分开。
    pub async fn contains(&self, proxy_url: &str) -> bool {
        self.inner
            .read()
            .await
            .proxies
            .iter()
            .any(|p| p.url == proxy_url)
    }

    pub async fn report_failure(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Connection).await;
    }

    pub async fn report_rate_limit(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::RateLimit).await;
    }

    pub async fn report_banned(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Banned).await;
    }

    /// WebUI 手动拉黑：冷却 24 小时（内存 + DB），不动失败计数。
    pub async fn manual_ban(&self, proxy_url: &str) {
        let until_unix = now_secs() + 86400;
        {
            let mut inner = self.inner.write().await;
            if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
                entry.cooldown_until = Some(Instant::now() + Duration::from_secs(86400));
            }
        }
        self.db
            .record_outcome(proxy_url, "manual_ban", Some(until_unix));
        info!("Proxy manually banned for 24h: {}", proxy_url);
    }

    /// WebUI 手动解禁：清冷却并复位失败计数。
    pub async fn manual_unban(&self, proxy_url: &str) {
        {
            let mut inner = self.inner.write().await;
            if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
                entry.cooldown_until = None;
                entry.fail_count = 0;
            }
        }
        self.db.clear_cooldown(proxy_url);
        info!("Proxy manually unbanned: {}", proxy_url);
    }

    /// WebUI 手动移除：出池并标记 removed（DB 历史保留，复活时信誉仍在）。
    pub async fn manual_remove(&self, proxy_url: &str) {
        {
            let mut inner = self.inner.write().await;
            let before = inner.proxies.len();
            inner.proxies.retain(|p| p.url != proxy_url);
            if inner.proxies.len() == before {
                return;
            }
        }
        self.db.mark_removed(proxy_url);
        info!("Proxy manually removed: {}", proxy_url);
    }

    pub async fn report_success(&self, proxy_url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
            entry.fail_count = 0;
            entry.cooldown_until = None;
            entry.success_count = entry.success_count.saturating_add(1);
            entry.last_success = Some(Instant::now());
            entry.last_used = now_secs();
        }
        drop(inner);
        self.db.record_outcome(proxy_url, "success", None);
    }

    async fn report_kind(&self, proxy_url: &str, kind: FailureKind) {
        let cooldown = kind.cooldown();
        let max_fails = kind.max_fails_before_removal();
        let label = kind.label();

        let (after_len, fetching, should_remove) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();

            let mut should_remove = false;
            if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
                if max_fails.is_some() {
                    entry.fail_count += 1;
                }
                if kind == FailureKind::Banned {
                    entry.ban_count = entry.ban_count.saturating_add(1);
                }
                entry.last_used = now_secs();
                entry.cooldown_until = Some(now + cooldown);
                should_remove = max_fails
                    .map(|max| entry.fail_count >= max)
                    .unwrap_or(false);
            }

            if should_remove {
                let before = inner.proxies.len();
                inner.proxies.retain(|p| p.url != proxy_url);
                warn!(
                    "Removed repeatedly-failing proxy: {}. Pool size: {} -> {}",
                    proxy_url,
                    before,
                    inner.proxies.len()
                );
            } else {
                debug!(
                    "Proxy {} marked as {}, cooldown {:?}",
                    proxy_url, label, cooldown
                );
            }

            (
                inner.proxies.len(),
                self.is_fetching.load(Ordering::Relaxed),
                should_remove,
            )
        };

        // 池锁释放后回写数据库
        if should_remove {
            self.db.mark_removed(proxy_url);
        } else {
            let cooldown_until_unix =
                now_secs() + cooldown.as_secs() as i64;
            self.db
                .record_outcome(proxy_url, kind.db_kind(), Some(cooldown_until_unix));
        }

        if after_len < *MIN_POOL_SIZE && !fetching {
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.fetch_free_proxies().await;
            });
        }
    }

    /// 对池内缺少 IP 情报的代理做批量富化（iplark 同类信息，走免费 API）：
    /// 1) ip-api.com 批量接口（免费无 key，100 IP/请求，约 15 请求/分钟）：
    ///    国家 / ISP / ASN / 是否机房 IP
    /// 2) 可选 proxycheck.io（配置 PROXYCHECK_KEY，免费注册 1000 次/天）：风险评分 + 使用场景
    pub async fn enrich_pending(&self) {
        let pending: Vec<String> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .filter(|p| p.info.country.is_none() && p.info.risk.is_none())
                .map(|p| p.url.clone())
                .collect()
        };
        if pending.is_empty() {
            return;
        }
        info!(
            "Enriching IP intel for {} proxies (ip-api.com batch)...",
            pending.len()
        );

        let Ok(client) = wreq::Client::builder()
            .emulation(impersonate())
            .timeout(Duration::from_secs(20))
            .build()
        else {
            return;
        };

        for chunk in pending.chunks(100) {
            let ips: Vec<String> = chunk.iter().filter_map(|u| extract_ip(u)).collect();
            if ips.is_empty() {
                continue;
            }
            let url = "http://ip-api.com/batch?fields=status,query,country,countryCode,isp,as,hosting";
            match client.post(url).json(&ips).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(v) = resp.json::<Value>().await {
                        let intel = parse_ipapi_batch(&v);
                        self.apply_intel(chunk, intel).await;
                    }
                }
                Ok(resp) => {
                    warn!(
                        "ip-api batch returned {}; skipping rest of this round",
                        resp.status()
                    );
                    break;
                }
                Err(e) => {
                    warn!("ip-api batch failed: {}; skipping rest of this round", e);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await; // 限速：约 12 批/分钟
        }

        if let Some(key) = crate::config::PROXYCHECK_KEY.as_ref() {
            self.proxycheck_pass(&client, key).await;
        }
    }

    /// 把批量查询结果合并进池内存条目并落库（只覆盖查询到的新字段）。
    async fn apply_intel(&self, urls: &[String], intel: HashMap<String, IpInfo>) {
        if intel.is_empty() {
            return;
        }
        let mut merged: Vec<(String, IpInfo)> = Vec::new();
        {
            let mut inner = self.inner.write().await;
            for url in urls {
                let Some(ip) = extract_ip(url) else { continue };
                let Some(new_info) = intel.get(&ip) else { continue };
                if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == *url) {
                    merge_info(&mut entry.info, new_info);
                    merged.push((url.clone(), entry.info.clone()));
                }
            }
        }
        for (url, info) in &merged {
            self.db.enrich_proxy(url, info);
        }
        if !merged.is_empty() {
            debug!("IP intel merged for {} proxies", merged.len());
        }
    }

    /// proxycheck.io 逐个查询风险评分与使用场景（配置 PROXYCHECK_KEY 时启用）。
    async fn proxycheck_pass(&self, client: &wreq::Client, key: &str) {
        let need: Vec<String> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .filter(|p| p.info.risk.is_none())
                .map(|p| p.url.clone())
                .collect()
        };
        if need.is_empty() {
            return;
        }
        // 免费额度 1000 次/天，池子上限 120，余量充足
        info!(
            "Querying proxycheck.io risk score for {} proxies (free tier: 1000/day)",
            need.len()
        );
        for url in &need {
            let Some(ip) = extract_ip(url) else { continue };
            let api = format!("https://proxycheck.io/v2/{}?key={}&vpn=1&risk=1", ip, key);
            match client.get(&api).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(v) = resp.json::<Value>().await {
                        if let Some(patch) = parse_proxycheck(&v, &ip) {
                            let mut m = HashMap::new();
                            m.insert(ip.clone(), patch);
                            self.apply_intel(std::slice::from_ref(url), m).await;
                        }
                    }
                }
                Ok(resp) => {
                    warn!(
                        "proxycheck returned {} for {}; stopping risk pass",
                        resp.status(),
                        ip
                    );
                    break;
                }
                Err(e) => {
                    warn!("proxycheck query failed: {}; stopping risk pass", e);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1200)).await; // 限速
        }
    }
}

/// 把新查到的字段合并进已有情报（只覆盖 Some 字段，None 不动）。
fn merge_info(base: &mut IpInfo, patch: &IpInfo) {
    if patch.country.is_some() {
        base.country = patch.country.clone();
    }
    if patch.country_code.is_some() {
        base.country_code = patch.country_code.clone();
    }
    if patch.isp.is_some() {
        base.isp = patch.isp.clone();
    }
    if patch.asn.is_some() {
        base.asn = patch.asn.clone();
    }
    if patch.ip_type.is_some() {
        base.ip_type = patch.ip_type.clone();
    }
    if patch.risk.is_some() {
        base.risk = patch.risk;
    }
    if patch.hosting.is_some() {
        base.hosting = patch.hosting;
    }
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(String::from)
}

/// 解析 ip-api.com /batch 响应：`[{query, country, countryCode, isp, as, hosting}, ...]`
fn parse_ipapi_batch(v: &Value) -> HashMap<String, IpInfo> {
    let mut out = HashMap::new();
    let Some(arr) = v.as_array() else {
        return out;
    };
    for item in arr {
        if str_field(item, "status").as_deref() != Some("success") {
            continue;
        }
        let Some(ip) = str_field(item, "query") else {
            continue;
        };
        out.insert(
            ip,
            IpInfo {
                country: str_field(item, "country"),
                country_code: str_field(item, "countryCode"),
                isp: str_field(item, "isp"),
                asn: str_field(item, "as"),
                ip_type: None,
                risk: None,
                hosting: item.get("hosting").and_then(|h| h.as_bool()),
            },
        );
    }
    out
}

/// 解析 proxycheck.io v2 响应：`{"status":"ok","<ip>":{"proxy":"yes","type":"SOCKS5","risk":"92"}}`
/// risk 兼容字符串和数字两种形式。
fn parse_proxycheck(v: &Value, ip: &str) -> Option<IpInfo> {
    if v.get("status").and_then(|s| s.as_str()) != Some("ok") {
        return None;
    }
    let entry = v.get(ip)?;
    let risk = str_field(entry, "risk")
        .and_then(|r| r.parse::<u32>().ok())
        .or_else(|| entry.get("risk").and_then(|r| r.as_u64()).map(|r| r as u32));
    Some(IpInfo {
        country: None,
        country_code: None,
        isp: None,
        asn: None,
        ip_type: str_field(entry, "type"),
        risk,
        hosting: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_ipapi_batch_ok_and_failed_rows() {
        let v = json!([
            {"status":"success","query":"1.2.3.4","country":"China","countryCode":"CN",
             "isp":"China Mobile","as":"AS9808 China Mobile","hosting":false},
            {"status":"fail","query":"5.6.7.8"}
        ]);
        let m = parse_ipapi_batch(&v);
        assert_eq!(m.len(), 1, "失败行应被跳过");
        let info = &m["1.2.3.4"];
        assert_eq!(info.country_code.as_deref(), Some("CN"));
        assert_eq!(info.isp.as_deref(), Some("China Mobile"));
        assert_eq!(info.hosting, Some(false));
        assert!(info.risk.is_none());
    }

    #[test]
    fn parse_proxycheck_variants() {
        // risk 字符串形式
        let v = json!({"status":"ok","223.73.113.115":{"proxy":"no","type":"Wireless","risk":"0"}});
        let info = parse_proxycheck(&v, "223.73.113.115").unwrap();
        assert_eq!(info.risk, Some(0));
        assert_eq!(info.ip_type.as_deref(), Some("Wireless"));

        // risk 数字形式
        let v2 = json!({"status":"ok","1.1.1.1":{"proxy":"yes","type":"SOCKS5","risk":92}});
        assert_eq!(parse_proxycheck(&v2, "1.1.1.1").unwrap().risk, Some(92));

        // 状态非 ok / IP 不存在
        assert!(parse_proxycheck(&json!({"status":"error"}), "1.1.1.1").is_none());
        assert!(parse_proxycheck(&json!({"status":"ok"}), "2.2.2.2").is_none());
    }

    #[test]
    fn merge_info_only_fills_some_fields() {
        let mut base = IpInfo {
            country: Some("China".into()),
            country_code: Some("CN".into()),
            isp: None,
            asn: None,
            ip_type: None,
            risk: None,
            hosting: Some(false),
        };
        let patch = IpInfo {
            country: None,
            country_code: None,
            isp: Some("ISP X".into()),
            asn: None,
            ip_type: Some("Wireless".into()),
            risk: Some(30),
            hosting: Some(true),
        };
        merge_info(&mut base, &patch);
        assert_eq!(base.country.as_deref(), Some("China"), "None 字段不应覆盖");
        assert_eq!(base.isp.as_deref(), Some("ISP X"));
        assert_eq!(base.risk, Some(30));
        assert_eq!(base.hosting, Some(true));
    }

    #[test]
    fn entry_allowed_filter_rules() {
        let mut e = ProxyEntry::new("http://1.1.1.1:80".into(), 0.1);
        e.info.country_code = Some("CN".into());

        // 无过滤
        assert!(entry_allowed(&e, &ProxyFilters::default()));
        // 国家白名单命中（大小写不敏感）
        let f = ProxyFilters {
            country_whitelist: vec!["cn".into()],
            max_risk: None,
        };
        assert!(entry_allowed(&e, &f));
        // 国家不在白名单
        let f2 = ProxyFilters {
            country_whitelist: vec!["US".into()],
            max_risk: None,
        };
        assert!(!entry_allowed(&e, &f2));
        // 未填写国家的代理在白名单模式下被拦截
        e.info.country_code = None;
        assert!(!entry_allowed(&e, &f));
        e.info.country_code = Some("CN".into());

        // 风险上限：已知超限拦截，未评分放行
        let f3 = ProxyFilters {
            country_whitelist: vec![],
            max_risk: Some(50),
        };
        e.info.risk = Some(80);
        assert!(!entry_allowed(&e, &f3));
        e.info.risk = Some(50);
        assert!(entry_allowed(&e, &f3), "等于上限应放行");
        e.info.risk = None;
        assert!(entry_allowed(&e, &f3), "未评分不受限");
    }
}