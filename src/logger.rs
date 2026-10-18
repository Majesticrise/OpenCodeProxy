use std::{
    collections::VecDeque,
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context, Layer};

use crate::config::LOG_BUFFER_CAP;

pub struct LogBuffer {
    logs: Mutex<VecDeque<String>>,
    cap: usize,
}

impl LogBuffer {
    pub fn new() -> Self {
        let cap = *LOG_BUFFER_CAP;
        Self {
            logs: Mutex::new(VecDeque::with_capacity(cap)),
            cap,
        }
    }

    pub fn push(&self, msg: String) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= self.cap {
            logs.pop_front();
        }
        logs.push_back(msg);
    }

    pub fn get_all(&self) -> Vec<String> {
        self.logs.lock().unwrap().iter().cloned().collect()
    }
}

/// tracing Layer：把结构化日志写入内存环形缓冲，供 WebUI 读取。
/// 与 fmt::layer() 叠加使用：fmt 负责终端输出，本 Layer 负责 Web 缓冲。
pub struct LogLayer {
    buffer: Arc<LogBuffer>,
}

impl LogLayer {
    pub fn new(buffer: Arc<LogBuffer>) -> Self {
        Self { buffer }
    }
}

impl<S: Subscriber> Layer<S> for LogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut visitor = MsgVisitor(String::new());
        event.record(&mut visitor);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let line = format!(
            "[{}] [{:>5}] {}",
            format_utc_with_millis(now.as_secs(), now.subsec_millis()),
            meta.level(),
            visitor.0
        );
        self.buffer.push(line);
    }
}

/// 把 Unix 秒数 + 毫秒转成 "YYYY-MM-DD HH:MM:SS.mmm" 格式（UTC）
///
/// 说明：使用 UTC 而非本地时间，因为：
/// - 不依赖平台时区 API，跨平台一致
/// - 容器部署时 UTC 是常见约定
/// - 前端可以按需转换（在 WebUI 里加 `new Date(...).toLocaleString()` 等）
fn format_utc_with_millis(unix_secs: u64, millis: u32) -> String {
    let days = (unix_secs / 86400) as i64;
    let mut secs_in_day = (unix_secs % 86400) as u32;

    let hour = secs_in_day / 3600;
    secs_in_day %= 3600;
    let minute = secs_in_day / 60;
    let second = secs_in_day % 60;

    // 从 1970-01-01 开始逐年推进
    let mut year = 1970i32;
    let mut days_left = days;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days_left < days_in_year {
            break;
        }
        days_left -= days_in_year;
        year += 1;
    }

    // 从 1 月开始逐月推进
    let month_days: [i64; 12] = [
        31,
        if is_leap_year(year) { 29 } else { 28 },
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ];
    let mut month = 1u32;
    for &d in month_days.iter() {
        if days_left < d {
            break;
        }
        days_left -= d;
        month += 1;
    }
    let day = (days_left + 1) as u32;

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        year, month, day, hour, minute, second, millis
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// 从 Event 中抽取 message 以及附加字段
struct MsgVisitor(String);

impl MsgVisitor {
    fn push_sep(&mut self) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
    }
}

impl Visit for MsgVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push_sep();
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            let _ = write!(self.0, "{}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push_sep();
        if field.name() == "message" {
            // fmt::Arguments 的 Debug 会委托给 Display
            let _ = write!(self.0, "{:?}", value);
        } else {
            let _ = write!(self.0, "{}={:?}", field.name(), value);
        }
    }
}