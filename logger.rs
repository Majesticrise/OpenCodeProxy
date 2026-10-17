use std::{
    collections::VecDeque,
    io::Write,
    sync::{Arc, Mutex},
};
use tracing_subscriber::fmt::MakeWriter;

pub struct LogBuffer {
    logs: Mutex<VecDeque<String>>,
}

impl LogBuffer {
    pub fn new() -> Self {
        Self { logs: Mutex::new(VecDeque::with_capacity(500)) }
    }
    pub fn push(&self, msg: String) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= 500 {
            logs.pop_front();
        }
        logs.push_back(msg);
    }
    pub fn get_all(&self) -> Vec<String> {
        self.logs.lock().unwrap().iter().cloned().collect()
    }
}

pub struct WebLogWriter {
    pub buffer: Arc<LogBuffer>,
}

impl<'a> MakeWriter<'a> for WebLogWriter {
    type Writer = WebLogWriterGuard;
    fn make_writer(&'a self) -> Self::Writer {
        WebLogWriterGuard { buffer: self.buffer.clone() }
    }
}

pub struct WebLogWriterGuard {
    buffer: Arc<LogBuffer>,
}

impl Write for WebLogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let msg = String::from_utf8_lossy(buf).trim_end().to_string();
        if !msg.is_empty() {
            println!("{}", msg);
            self.buffer.push(msg);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}