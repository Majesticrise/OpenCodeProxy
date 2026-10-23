use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, info, warn};

const WINDOW_SECS: u64 = 3600;
const MAX_SAMPLES: usize = 5000;
const SNAPSHOT_TTL: Duration = Duration::from_millis(900);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageInfo {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
}

impl UsageInfo {
    pub fn from_json(u: &serde_json::Value) -> Self {
        Self {
            prompt_tokens: u.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            completion_tokens: u
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            total_tokens: u.get("total_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            cached_tokens: u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            reasoning_tokens: u
                .get("completion_tokens_details")
                .and_then(|d| d.get("reasoning_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        }
    }
}

#[derive(Clone)]
pub struct RequestMetric {
    pub timestamp_secs: u64,
    pub latency_ms: u64,
    pub status: u16,
    pub model: String,
    pub usage: Option<UsageInfo>,
}

pub struct TrackerState {
    pub start: Instant,
    pub model: String,
    pub is_stream: bool,
    pub status: u16,
    pub usage: Mutex<Option<UsageInfo>>,
    pub estimated_prompt_tokens: u64,
    pub streamed_chars: AtomicU64,
}

impl TrackerState {
    pub fn new(
        start: Instant,
        model: String,
        is_stream: bool,
        status: u16,
        estimated_prompt_tokens: u64,
    ) -> Self {
        Self {
            start,
            model,
            is_stream,
            status,
            usage: Mutex::new(None),
            estimated_prompt_tokens,
            streamed_chars: AtomicU64::new(0),
        }
    }
}

/// ⭐ 跨重启持久化的累计总量
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PersistedTotals {
    pub total_requests: u64,
    pub success_count: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
    pub with_usage_count: u64,
    /// model → (prompt, completion, total)
    #[serde(default)]
    pub models: HashMap<String, (u64, u64, u64)>,
    /// 上次落盘时间（诊断用）
    #[serde(default)]
    pub updated_at_secs: u64,
}

struct SnapshotCache {
    at: Instant,
    snap: MetricsSnapshot,
}

pub struct MetricsStore {
    requests: Mutex<VecDeque<RequestMetric>>,
    /// ⭐ 跨重启累计
    totals: Mutex<PersistedTotals>,
    pub active_requests: AtomicU64,
    pub active_streams: AtomicU64,
    cache: Mutex<Option<SnapshotCache>>,
    compute_lock: Mutex<()>,
    /// 累计数字有变化时置 true，定时任务负责写盘
    dirty: AtomicBool,
    persist_path: Option<PathBuf>,
}

impl MetricsStore {
    /// 纯内存模式
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_persistence(None)
    }
    /// 带持久化路径；None 等于纯内存
    pub fn with_persistence(path: Option<PathBuf>) -> Self {
        let totals = match &path {
            Some(p) => load_totals(p),
            None => PersistedTotals::default(),
        };

        Self {
            requests: Mutex::new(VecDeque::with_capacity(MAX_SAMPLES)),
            totals: Mutex::new(totals),
            active_requests: AtomicU64::new(0),
            active_streams: AtomicU64::new(0),
            cache: Mutex::new(None),
            compute_lock: Mutex::new(()),
            dirty: AtomicBool::new(false),
            persist_path: path,
        }
    }

    /// 若累计数字有变化则写盘。由外部定时任务每 5s 调用一次即可。
    pub async fn flush_if_dirty(&self) {
        let Some(path) = &self.persist_path else { return };
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return;
        }
        let totals = {
            let mut t = self.totals.lock().unwrap();
            t.updated_at_secs = now_secs();
            t.clone()
        };
        if let Err(e) = save_totals(path, &totals).await {
            warn!("Failed to persist metrics totals: {}", e);
            // 写失败：重新置 dirty，下轮再试
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// 主动触发一次写盘（阻塞式，退出前调用）
    pub fn flush_blocking(&self) {
        let Some(path) = &self.persist_path else { return };
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return;
        }
        let totals = {
            let mut t = self.totals.lock().unwrap();
            t.updated_at_secs = now_secs();
            t.clone()
        };
        let json = match serde_json::to_string_pretty(&totals) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to serialize metrics totals: {}", e);
                self.dirty.store(true, Ordering::Relaxed);
                return;
            }
        };
        // ⭐ 用 tmp + rename 保证原子性
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, json) {
            warn!("Failed to write metrics tmp file: {}", e);
            self.dirty.store(true, Ordering::Relaxed);
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            warn!("Failed to rename metrics tmp file: {}", e);
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    pub fn record(&self, metric: RequestMetric) {
        // 1) 更新窗口（用于 p95, per_minute, endpoints）
        {
            let mut reqs = self.requests.lock().unwrap();
            reqs.push_back(metric.clone());
            if reqs.len() > MAX_SAMPLES {
                reqs.pop_front();
            }
            *self.cache.lock().unwrap() = None;
        }
        // 2) 更新跨重启累计
        {
            let mut t = self.totals.lock().unwrap();
            t.total_requests += 1;
            if (200..400).contains(&metric.status) {
                t.success_count += 1;
            }
            if let Some(u) = &metric.usage {
                t.total_tokens += u.total_tokens;
                t.cached_tokens += u.cached_tokens;
                t.reasoning_tokens += u.reasoning_tokens;
                t.with_usage_count += 1;
                let e = t.models.entry(metric.model.clone()).or_insert((0, 0, 0));
                e.0 += u.prompt_tokens;
                e.1 += u.completion_tokens;
                e.2 += u.total_tokens;
            }
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    fn prune(&self, reqs: &mut VecDeque<RequestMetric>, now: u64) {
        let cutoff = now.saturating_sub(WINDOW_SECS);
        while let Some(front) = reqs.front() {
            if front.timestamp_secs < cutoff {
                reqs.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let now = Instant::now();
        if let Some(c) = self.cache.lock().unwrap().as_ref() {
            if now.duration_since(c.at) < SNAPSHOT_TTL {
                return c.snap.clone();
            }
        }
        let _guard = self.compute_lock.lock().unwrap();
        if let Some(c) = self.cache.lock().unwrap().as_ref() {
            if now.duration_since(c.at) < SNAPSHOT_TTL {
                return c.snap.clone();
            }
        }
        let snap = self.compute_snapshot();
        *self.cache.lock().unwrap() = Some(SnapshotCache {
            at: now,
            snap: snap.clone(),
        });
        snap
    }

    fn compute_snapshot(&self) -> MetricsSnapshot {
        let now = now_secs();
        let totals = self.totals.lock().unwrap().clone();
        let mut reqs = self.requests.lock().unwrap();
        self.prune(&mut reqs, now);

        // ── 窗口内：p95 延迟、per_minute、endpoints ──
        let p95 = if reqs.is_empty() {
            0
        } else {
            let mut latencies: Vec<u64> = reqs.iter().map(|r| r.latency_ms).collect();
            latencies.sort_unstable();
            let idx = (((latencies.len() as f64) * 0.95).ceil() as usize).saturating_sub(1);
            latencies.get(idx).copied().unwrap_or(0)
        };

        let mut endpoints: HashMap<u16, u64> = HashMap::new();
        let mut per_minute: HashMap<u64, (u64, u64, u64)> = HashMap::new();
        for r in reqs.iter() {
            *endpoints.entry(r.status).or_insert(0) += 1;
            if let Some(u) = &r.usage {
                let m = per_minute.entry(r.timestamp_secs / 60).or_insert((0, 0, 0));
                m.0 += u.prompt_tokens;
                m.1 += u.completion_tokens;
                m.2 += u.total_tokens;
            }
        }
        let mut per_minute_vec: Vec<(u64, (u64, u64, u64))> =
            per_minute.into_iter().collect();
        per_minute_vec.sort_by_key(|x| x.0);

        // ── 累计数字：直接用 totals ──
        let success_rate = if totals.total_requests > 0 {
            (totals.success_count as f64 / totals.total_requests as f64) * 100.0
        } else {
            0.0
        };
        let usage_coverage = if totals.total_requests > 0 {
            (totals.with_usage_count as f64 / totals.total_requests as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            total_requests: totals.total_requests,
            success_rate,
            p95_latency: p95,
            active_requests: self.active_requests.load(Ordering::Relaxed),
            active_streams: self.active_streams.load(Ordering::Relaxed),
            usage_coverage,
            total_tokens: totals.total_tokens,
            cached_tokens: totals.cached_tokens,
            reasoning_tokens: totals.reasoning_tokens,
            models: totals.models.into_iter().collect(),
            endpoints: endpoints.into_iter().collect(),
            per_minute: per_minute_vec,
        }
    }
}

impl Drop for MetricsStore {
    fn drop(&mut self) {
        self.flush_blocking();
    }
}

#[derive(Serialize, Clone)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub success_rate: f64,
    pub p95_latency: u64,
    pub active_requests: u64,
    pub active_streams: u64,
    pub usage_coverage: f64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
    pub models: Vec<(String, (u64, u64, u64))>,
    pub endpoints: Vec<(u16, u64)>,
    pub per_minute: Vec<(u64, (u64, u64, u64))>,
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ───────────────────── 持久化辅助 ─────────────────────

fn load_totals(path: &Path) -> PersistedTotals {
    match std::fs::read_to_string(path) {
        Ok(content) => match serde_json::from_str::<PersistedTotals>(&content) {
            Ok(t) => {
                info!(
                    "Loaded persisted metrics totals: {} requests, {} tokens, {} models",
                    t.total_requests,
                    t.total_tokens,
                    t.models.len()
                );
                t
            }
            Err(e) => {
                warn!("Corrupt metrics totals at {}: {}; starting fresh", path.display(), e);
                PersistedTotals::default()
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("Metrics totals file {} not found; starting fresh", path.display());
            PersistedTotals::default()
        }
        Err(e) => {
            warn!("Failed to read metrics totals at {}: {}", path.display(), e);
            PersistedTotals::default()
        }
    }
}

async fn save_totals(path: &Path, totals: &PersistedTotals) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(totals)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    // 先写 tmp 再 rename，防止写一半被杀
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, json.as_bytes()).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

// ───────────────────── usage 解析 ─────────────────────

pub fn try_extract_usage_from_chunk(chunk: &[u8], buf: &mut Vec<u8>) -> Option<UsageInfo> {
    buf.extend_from_slice(chunk);

    let mut latest: Option<UsageInfo> = None;
    let mut consumed = 0usize;
    let mut line_start = 0usize;

    for i in 0..buf.len() {
        if buf[i] != b'\n' {
            continue;
        }
        let raw_line = String::from_utf8_lossy(&buf[line_start..i]);
        let line = raw_line.trim();

        let json_str = if let Some(stripped) = line.strip_prefix("data:") {
            stripped.trim_start()
        } else if line.starts_with(':') {
            ""
        } else {
            line
        };

        if !json_str.is_empty() && json_str != "[DONE]" {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(u) = v.get("usage") {
                    if !u.is_null() {
                        latest = Some(UsageInfo::from_json(u));
                    }
                }
            }
        }
        consumed = i + 1;
        line_start = i + 1;
    }
    if consumed > 0 {
        buf.drain(..consumed);
    }

    if latest.is_none() && !buf.is_empty() {
        if let Ok(s) = std::str::from_utf8(buf) {
            let trimmed = s.trim();
            let candidate = trimmed
                .strip_prefix("data:")
                .map(|x| x.trim_start())
                .unwrap_or(trimmed);
            if !candidate.is_empty() && candidate != "[DONE]" {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(candidate) {
                    if let Some(u) = v.get("usage") {
                        if !u.is_null() {
                            latest = Some(UsageInfo::from_json(u));
                        }
                    }
                }
            }
        }
    }

    if buf.len() > 16384 {
        let cut = buf.len() - 8192;
        buf.drain(..cut);
    }

    latest
}

// ───────────────────── 指标记录 ─────────────────────

pub struct MetricsGuard {
    pub metrics: Arc<MetricsStore>,
    pub tracker: Arc<TrackerState>,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        let real_usage = self.tracker.usage.lock().unwrap().clone();
        let is_real = real_usage.is_some();

        let usage = if is_real {
            real_usage
        } else {
            let prompt = self.tracker.estimated_prompt_tokens;
            let streamed = self.tracker.streamed_chars.load(Ordering::Relaxed);
            let completion = (streamed / 3).max(1);
            let total = prompt + completion;
            if total > 0 {
                Some(UsageInfo {
                    prompt_tokens: prompt,
                    completion_tokens: completion,
                    total_tokens: total,
                    cached_tokens: 0,
                    reasoning_tokens: 0,
                })
            } else {
                None
            }
        };

        debug!(
            target: "zen_proxy::metrics",
            "metric: model={} status={} latency={}ms tokens={} real_usage={}",
            self.tracker.model,
            self.tracker.status,
            self.tracker.start.elapsed().as_millis(),
            usage.as_ref().map(|u| u.total_tokens).unwrap_or(0),
            is_real,
        );

        let metric = RequestMetric {
            timestamp_secs: now_secs(),
            latency_ms: self.tracker.start.elapsed().as_millis() as u64,
            status: self.tracker.status,
            model: self.tracker.model.clone(),
            usage,
        };
        self.metrics.record(metric);
        self.metrics
            .active_requests
            .fetch_sub(1, Ordering::Relaxed);
        if self.tracker.is_stream {
            self.metrics
                .active_streams
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_usage_from_sse_chunk() {
        let chunk = b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n";
        let mut buf = Vec::new();
        let usage = try_extract_usage_from_chunk(chunk, &mut buf);
        assert!(usage.is_some(), "should extract usage");
        let u = usage.unwrap();
        assert_eq!(u.prompt_tokens, 10);
        assert_eq!(u.completion_tokens, 5);
        assert_eq!(u.total_tokens, 15);
    }

    #[test]
    fn extract_usage_from_bare_json() {
        // 有些上游返回裸 JSON（非 SSE）
        let chunk = b"{\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}";
        let mut buf = Vec::new();
        let usage = try_extract_usage_from_chunk(chunk, &mut buf);
        assert!(usage.is_some(), "should extract from bare JSON");
        assert_eq!(usage.unwrap().total_tokens, 10);
    }

    #[test]
    fn extract_usage_with_cached_and_reasoning() {
        let chunk = br#"data: {"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150,"prompt_tokens_details":{"cached_tokens":20},"completion_tokens_details":{"reasoning_tokens":10}}}
"#;
        let mut buf = Vec::new();
        let u = try_extract_usage_from_chunk(chunk, &mut buf).unwrap();
        assert_eq!(u.cached_tokens, 20);
        assert_eq!(u.reasoning_tokens, 10);
    }

    #[test]
    fn extract_usage_no_usage_field() {
        let chunk = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
        let mut buf = Vec::new();
        let usage = try_extract_usage_from_chunk(chunk, &mut buf);
        assert!(usage.is_none(), "no usage field → None");
    }

    #[test]
    fn extract_usage_skips_done_marker() {
        let chunk = b"data: [DONE]\n\n";
        let mut buf = Vec::new();
        let usage = try_extract_usage_from_chunk(chunk, &mut buf);
        assert!(usage.is_none());
    }

    #[test]
    fn usage_info_from_json() {
        let v = serde_json::json!({
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
            "prompt_tokens_details": {"cached_tokens": 30},
            "completion_tokens_details": {"reasoning_tokens": 15}
        });
        let u = UsageInfo::from_json(&v);
        assert_eq!(u.prompt_tokens, 100);
        assert_eq!(u.completion_tokens, 50);
        assert_eq!(u.total_tokens, 150);
        assert_eq!(u.cached_tokens, 30);
        assert_eq!(u.reasoning_tokens, 15);
    }

    #[test]
    fn usage_info_from_empty_json() {
        let u = UsageInfo::from_json(&serde_json::json!({}));
        assert_eq!(u.total_tokens, 0);
        assert_eq!(u.cached_tokens, 0);
    }
}