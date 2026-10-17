use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::debug;

const WINDOW_SECS: u64 = 3600; // 60 分钟滑动窗口
const MAX_SAMPLES: usize = 5000; // 最多保留 5000 条样本
const SNAPSHOT_TTL: Duration = Duration::from_millis(900);

#[derive(Clone, Debug, Default)]
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

/// 用于跨流式 Body 传递的跟踪状态
pub struct TrackerState {
    pub start: Instant,
    pub model: String,
    pub is_stream: bool,
    pub status: u16,
    pub usage: Mutex<Option<UsageInfo>>,
    /// 上游未返回 usage 时的 fallback：prompt 侧估算
    pub estimated_prompt_tokens: u64,
    /// 上游未返回 usage 时的 fallback：流式累加的 completion 字符数
    pub streamed_chars: AtomicU64,
}

impl TrackerState {
    pub fn new(start: Instant, model: String, is_stream: bool, status: u16) -> Self {
        Self {
            start,
            model,
            is_stream,
            status,
            usage: Mutex::new(None),
            estimated_prompt_tokens: 0,
            streamed_chars: AtomicU64::new(0),
        }
    }
}

struct SnapshotCache {
    at: Instant,
    snap: MetricsSnapshot,
}

pub struct MetricsStore {
    requests: Mutex<VecDeque<RequestMetric>>,
    pub active_requests: AtomicU64,
    pub active_streams: AtomicU64,
    cache: Mutex<Option<SnapshotCache>>,
    /// 保证 compute_snapshot 串行执行，避免并发重复计算
    compute_lock: Mutex<()>,
}

impl MetricsStore {
    pub fn new() -> Self {
        Self {
            requests: Mutex::new(VecDeque::with_capacity(MAX_SAMPLES)),
            active_requests: AtomicU64::new(0),
            active_streams: AtomicU64::new(0),
            cache: Mutex::new(None),
            compute_lock: Mutex::new(()),
        }
    }

    pub fn record(&self, metric: RequestMetric) {
        let mut reqs = self.requests.lock().unwrap();
        reqs.push_back(metric);
        if reqs.len() > MAX_SAMPLES {
            reqs.pop_front();
        }
        // 新数据到达，主动让缓存失效
        *self.cache.lock().unwrap() = None;
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

        // 快速路径：缓存命中
        if let Some(c) = self.cache.lock().unwrap().as_ref() {
            if now.duration_since(c.at) < SNAPSHOT_TTL {
                return c.snap.clone();
            }
        }

        // 只有拿到 compute_lock 的线程真正重算，其余等它算完再读缓存
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
        let mut reqs = self.requests.lock().unwrap();
        self.prune(&mut reqs, now);

        let total = reqs.len() as u64;
        if total == 0 {
            return MetricsSnapshot::empty(self);
        }

        // ── P95 延迟 ──
        let mut latencies: Vec<u64> = reqs.iter().map(|r| r.latency_ms).collect();
        latencies.sort_unstable();
        let idx = (((latencies.len() as f64) * 0.95).ceil() as usize).saturating_sub(1);
        let p95 = latencies.get(idx).copied().unwrap_or(0);

        // ── 成功率 ──
        let success = reqs.iter().filter(|r| (200..400).contains(&r.status)).count() as u64;
        let success_rate = (success as f64 / total as f64) * 100.0;

        // ── Token 聚合 ──
        let with_usage = reqs.iter().filter(|r| r.usage.is_some()).count() as u64;
        let usage_coverage = (with_usage as f64 / total as f64) * 100.0;
        let (mut total_tokens, mut cached_tokens, mut reasoning_tokens) = (0u64, 0u64, 0u64);

        let mut models: std::collections::HashMap<String, (u64, u64, u64)> =
            std::collections::HashMap::new();
        let mut endpoints: std::collections::HashMap<u16, u64> = std::collections::HashMap::new();
        let mut per_minute: std::collections::HashMap<u64, (u64, u64, u64)> =
            std::collections::HashMap::new();

        for r in reqs.iter() {
            *endpoints.entry(r.status).or_insert(0) += 1;
            if let Some(u) = &r.usage {
                total_tokens += u.total_tokens;
                cached_tokens += u.cached_tokens;
                reasoning_tokens += u.reasoning_tokens;

                let e = models.entry(r.model.clone()).or_insert((0, 0, 0));
                e.0 += u.prompt_tokens;
                e.1 += u.completion_tokens;
                e.2 += u.total_tokens;

                let minute = r.timestamp_secs / 60;
                let m = per_minute.entry(minute).or_insert((0, 0, 0));
                m.0 += u.prompt_tokens;
                m.1 += u.completion_tokens;
                m.2 += u.total_tokens;
            }
        }

        // per_minute 必须按时间升序，否则前端折线图会错乱
        let mut per_minute_vec: Vec<(u64, (u64, u64, u64))> = per_minute.into_iter().collect();
        per_minute_vec.sort_by_key(|x| x.0);

        MetricsSnapshot {
            total_requests: total,
            success_rate,
            p95_latency: p95,
            active_requests: self.active_requests.load(Ordering::Relaxed),
            active_streams: self.active_streams.load(Ordering::Relaxed),
            usage_coverage,
            total_tokens,
            cached_tokens,
            reasoning_tokens,
            models: models.into_iter().collect(),
            endpoints: endpoints.into_iter().collect(),
            per_minute: per_minute_vec,
        }
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

impl MetricsSnapshot {
    fn empty(store: &MetricsStore) -> Self {
        Self {
            total_requests: 0,
            success_rate: 0.0,
            p95_latency: 0,
            active_requests: store.active_requests.load(Ordering::Relaxed),
            active_streams: store.active_streams.load(Ordering::Relaxed),
            usage_coverage: 0.0,
            total_tokens: 0,
            cached_tokens: 0,
            reasoning_tokens: 0,
            models: Vec::new(),
            endpoints: Vec::new(),
            per_minute: Vec::new(),
        }
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 从响应 chunk（原始字节）里提取 usage。
///
/// 关键修复：
/// 1. **`data:` 前缀带不带空格都能解析**——兼容 OpenAI 官方 SDK（`data: {...}`）
///    和部分网关（`data:{...}`，无空格）两种变体
/// 2. **按字节累积**——避免 chunk 边界落在多字节字符中间导致丢数据
/// 3. **每行都尝试剥 `data:` 前缀**——非流式响应整段 JSON 也能被当行解析
pub fn try_extract_usage_from_chunk(chunk: &[u8], buf: &mut Vec<u8>) -> Option<UsageInfo> {
    buf.extend_from_slice(chunk);

    let mut latest: Option<UsageInfo> = None;

    // 扫描所有完整行
    let mut consumed = 0usize;
    let mut line_start = 0usize;
    for i in 0..buf.len() {
        if buf[i] != b'\n' {
            continue;
        }
        let raw_line = String::from_utf8_lossy(&buf[line_start..i]);
        let line = raw_line.trim();

        // ⭐ 关键：data: 后空格可有可无
        let json_str = if let Some(stripped) = line.strip_prefix("data:") {
            stripped.trim_start()
        } else if line.starts_with(':') {
            // SSE 注释行（: ping），跳过
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

    // 无换行的整段 JSON（常见于非流式响应）
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

    // 防止缓冲无限增长
    if buf.len() > 16384 {
        let cut = buf.len() - 8192;
        buf.drain(..cut);
    }

    latest
}

/// 附加到 Response 的 extensions 上，Drop 时自动记录指标
pub struct MetricsGuard {
    pub metrics: Arc<MetricsStore>,
    pub tracker: Arc<TrackerState>,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        let real_usage = self.tracker.usage.lock().unwrap().clone();

        // ⭐ Fallback 估算：上游没返回 usage 时，按字符数粗估
        // 系数：英文约 4 字符/token，中文约 1.5 字符/token，混合取 3
        let usage = if real_usage.is_some() {
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

        let is_real = real_usage_available(&usage, &self.tracker);
        let tokens = usage.as_ref().map(|u| u.total_tokens).unwrap_or(0);

        debug!(
            target: "zen_proxy::metrics",
            "metric: model={} status={} latency={}ms tokens={} real_usage={}",
            self.tracker.model,
            self.tracker.status,
            self.tracker.start.elapsed().as_millis(),
            tokens,
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

/// 判断 usage 是"真"还是"估算"
fn real_usage_available(usage: &Option<UsageInfo>, tracker: &TrackerState) -> bool {
    if usage.is_none() {
        return false;
    }
    // 如果上游从没填过 usage，但 tracker 有字符统计 → 是估算
    tracker.usage.lock().map(|u| u.is_some()).unwrap_or(false)
}