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
        // 与 fmt::layer 默认毫秒精度对齐
        let secs = now.as_secs() % 100_000; // 保留后 5 位，避免过长
        let millis = now.subsec_millis();

        let line = format!(
            "[{:>5}.{:03}] [{:>5}] {}",
            secs,
            millis,
            meta.level(),
            visitor.0
        );
        self.buffer.push(line);
    }
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