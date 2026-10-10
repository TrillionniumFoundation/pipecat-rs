//! Incremental, bounded SSE decoder. UTF-8 may split anywhere in a network chunk.
use crate::ServiceError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

pub struct SseDecoder {
    line: Vec<u8>,
    event: Option<String>,
    data: String,
    has_data: bool,
    skip_lf: bool,
    first_line: bool,
    size: usize,
    max_bytes: usize,
}
impl SseDecoder {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            line: vec![],
            event: None,
            data: String::new(),
            has_data: false,
            skip_lf: false,
            first_line: true,
            size: 0,
            max_bytes,
        }
    }
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ServiceError> {
        let mut events = vec![];
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                self.process_line(&mut events)?;
                self.skip_lf = byte == b'\r';
            } else {
                self.size += 1;
                if self.size > self.max_bytes {
                    return Err(ServiceError::Protocol(
                        "SSE event exceeds size limit".into(),
                    ));
                }
                self.line.push(byte);
            }
        }
        Ok(events)
    }
    fn process_line(&mut self, events: &mut Vec<SseEvent>) -> Result<(), ServiceError> {
        let line_bytes = std::mem::take(&mut self.line);
        let mut line = std::str::from_utf8(&line_bytes)
            .map_err(|_| ServiceError::Protocol("invalid SSE UTF-8".into()))?;
        if self.first_line {
            self.first_line = false;
            line = line.strip_prefix('\u{feff}').unwrap_or(line);
        }
        if line.is_empty() {
            if self.has_data {
                self.data.pop();
                events.push(SseEvent {
                    event: self.event.take(),
                    data: std::mem::take(&mut self.data),
                });
            }
            self.event = None;
            self.has_data = false;
            self.size = 0;
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = Some(value.into()),
            "data" => {
                self.has_data = true;
                self.data.push_str(value);
                self.data.push('\n');
            }
            _ => {}
        }
        Ok(())
    }
    /// EOF cannot dispatch an incomplete SSE record; callers must reject truncated completions.
    pub fn has_pending_data(&self) -> bool {
        !self.line.is_empty() || self.has_data
    }
}
