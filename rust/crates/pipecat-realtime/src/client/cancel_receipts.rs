//! Same-session, single-consumption evidence for flushed cancellation requests.
//! ID payload is bounded separately from the fixed maximum of 64 receipt slots.
use super::ProviderError;
use std::collections::VecDeque;

const MAX_RECEIPTS: usize = 64;

pub(super) struct CancelReceipt {
    pub(super) request_id: Box<str>,
    pub(super) response_id: Box<str>,
    pub(super) response_generation: u64,
    pub(super) interrupt_generation: u64,
}
impl CancelReceipt {
    fn payload_bytes(&self) -> Option<usize> {
        self.request_id.len().checked_add(self.response_id.len())
    }
}

pub(super) struct CancelReceipts {
    entries: VecDeque<CancelReceipt>,
    payload_bytes: usize,
    payload_limit: usize,
}
impl CancelReceipts {
    pub(super) fn new(payload_limit: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(MAX_RECEIPTS),
            payload_bytes: 0,
            payload_limit,
        }
    }
    pub(super) fn insert(&mut self, receipt: CancelReceipt) {
        let Some(bytes) = receipt.payload_bytes() else {
            return;
        };
        if bytes > self.payload_limit
            || receipt.response_id.is_empty()
            || receipt.response_generation > receipt.interrupt_generation
        {
            return;
        }
        // Done does not retire receipts: a normal race error may arrive later.
        // The fixed FIFO/byte window, consumption and owner destruction do.
        while self.entries.len() >= MAX_RECEIPTS
            || bytes > self.payload_limit.saturating_sub(self.payload_bytes)
        {
            let Some(old) = self.entries.pop_front() else {
                return;
            };
            self.payload_bytes -= old.payload_bytes().expect("inserted receipt size");
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes)
            .expect("bounded receipt payload");
        self.entries.push_back(receipt);
    }
    pub(super) fn consume(&mut self, error: &ProviderError) -> bool {
        if error.code.as_deref() != Some("response_cancel_not_active") {
            return false;
        }
        let Some(id) = error.event_id.as_deref() else {
            return false;
        };
        let Some(index) = self
            .entries
            .iter()
            .position(|r| r.request_id.as_ref() == id)
        else {
            return false;
        };
        let receipt = self.entries.remove(index).expect("located receipt");
        self.payload_bytes -= receipt.payload_bytes().expect("inserted receipt size");
        true
    }
}
