//! Bounded process-local bookkeeping; conversation messages remain context-owned.
use pipecat_core::{
    FunctionCallInProgressFrame, NativeToolCallBatch, NativeToolCallOutcome, ProcessorError,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

pub(super) const MAX_CALLS: usize = 1024;
pub(super) const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_TERMINALS: usize = 512;

pub(super) struct ByteCounter {
    pub bytes: usize,
    pub limit: usize,
}
impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| std::io::Error::other("tool bookkeeping exceeds byte limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Legacy terminal hints, never authority for an active native lease.
#[derive(Default)]
pub(super) struct ToolTombstones {
    ids: VecDeque<String>,
    bytes: usize,
}
impl ToolTombstones {
    pub fn contains(&self, id: &str) -> bool {
        self.ids.iter().any(|old| old == id)
    }
    pub fn insert(&mut self, id: &str) {
        if id.len() > MAX_BYTES || self.contains(id) {
            return;
        }
        while self.ids.len() >= MAX_TERMINALS || self.bytes + id.len() > MAX_BYTES {
            self.bytes -= self.ids.pop_front().expect("nonempty tombstones").len();
        }
        self.bytes += id.len();
        self.ids.push_back(id.to_owned());
    }
    pub fn remove(&mut self, id: &str) -> bool {
        let Some(index) = self.ids.iter().position(|old| old == id) else {
            return false;
        };
        self.bytes -= self.ids.remove(index).expect("known tombstone").len();
        true
    }
    pub fn clear(&mut self) {
        self.ids.clear();
        self.bytes = 0;
    }
}

/// Active ordinary presentation state. Entries are rejected, never evicted.
#[derive(Default)]
pub(super) struct PendingTools {
    calls: BTreeMap<String, Option<FunctionCallInProgressFrame>>,
    bytes: usize,
}
impl std::ops::Deref for PendingTools {
    type Target = BTreeMap<String, Option<FunctionCallInProgressFrame>>;
    fn deref(&self) -> &Self::Target {
        &self.calls
    }
}
impl PendingTools {
    fn entry_bytes(
        id: &str,
        call: Option<&FunctionCallInProgressFrame>,
    ) -> Result<usize, ProcessorError> {
        if id.len() > MAX_BYTES {
            return Err(ProcessorError::new("pending tool exceeds byte capacity"));
        }
        let mut counter = ByteCounter {
            bytes: id.len(),
            limit: MAX_BYTES,
        };
        if let Some(call) = call {
            serde_json::to_writer(&mut counter, call)
                .map_err(|error| ProcessorError::new(error.to_string()))?;
        }
        Ok(counter.bytes)
    }
    pub fn check(
        &self,
        id: &str,
        call: Option<&FunctionCallInProgressFrame>,
    ) -> Result<(), ProcessorError> {
        let bytes = Self::entry_bytes(id, call)?;
        let old = self
            .calls
            .get(id)
            .map_or(Ok(0), |old| Self::entry_bytes(id, old.as_ref()))?;
        if (self.calls.len() >= MAX_CALLS && !self.calls.contains_key(id))
            || self.bytes - old + bytes > MAX_BYTES
        {
            return Err(ProcessorError::new("active pending tools exceed capacity"));
        }
        Ok(())
    }
    pub fn check_calls<'a>(
        &self,
        calls: impl Iterator<Item = &'a FunctionCallInProgressFrame>,
    ) -> Result<(), ProcessorError> {
        let mut count = self.calls.len();
        let mut bytes = self.bytes;
        for call in calls {
            let id = &call.tool_call_id;
            if let Some(old) = self.calls.get(id) {
                bytes -= Self::entry_bytes(id, old.as_ref())?;
            } else {
                count += 1;
            }
            bytes += Self::entry_bytes(id, Some(call))?;
            if count > MAX_CALLS || bytes > MAX_BYTES {
                return Err(ProcessorError::new("active pending tools exceed capacity"));
            }
        }
        Ok(())
    }
    pub fn insert(
        &mut self,
        id: String,
        call: Option<FunctionCallInProgressFrame>,
    ) -> Result<(), ProcessorError> {
        self.check(&id, call.as_ref())?;
        let bytes = Self::entry_bytes(&id, call.as_ref())?;
        if let Some(old) = self.calls.get(&id) {
            self.bytes -= Self::entry_bytes(&id, old.as_ref())?;
        }
        self.bytes += bytes;
        self.calls.insert(id, call);
        Ok(())
    }
    pub fn remove(&mut self, id: &str) {
        if let Some(old) = self.calls.remove(id) {
            self.bytes -= Self::entry_bytes(id, old.as_ref()).expect("admitted pending tool");
        }
    }
    pub fn clear(&mut self) {
        self.calls.clear();
        self.bytes = 0;
    }
}

pub(super) struct NativeCall {
    pub call: FunctionCallInProgressFrame,
    pub lineage: u64,
    pub batch: Option<Arc<NativeToolCallBatch>>,
    pub terminal: Option<(u64, bool)>,
    retained: Option<Arc<NativeToolCallOutcome>>,
    receipt_bytes: usize,
    bytes: usize,
}
#[derive(Default)]
pub(super) struct NativeTools {
    pub calls: BTreeMap<String, NativeCall>,
    completed: VecDeque<String>,
    bytes: usize,
}
impl NativeTools {
    pub fn check_batch(batch: &NativeToolCallBatch) -> Result<(), ProcessorError> {
        let mut bytes = 0usize;
        for (call, policy) in batch.members() {
            bytes = bytes
                .checked_add(batch.retained_bytes())
                .and_then(|bytes| {
                    bytes
                        .checked_add(call.tool_call_id.len().saturating_mul(2))?
                        .checked_add(
                            NativeToolCallBatch::policy_bytes(
                                call,
                                *policy,
                                Some(batch.group_id()),
                            )
                            .ok()?,
                        )
                })
                .filter(|bytes| *bytes <= MAX_BYTES)
                .ok_or_else(|| {
                    ProcessorError::new("native batch exceeds consumer tracking budget")
                })?;
        }
        Ok(())
    }
    pub fn reserve(
        &mut self,
        calls: &[FunctionCallInProgressFrame],
        lineage: u64,
        batch: Option<&Arc<NativeToolCallBatch>>,
    ) -> Result<(), ProcessorError> {
        if let Some(batch) = batch {
            if self
                .calls
                .values()
                .filter_map(|entry| entry.batch.as_ref())
                .any(|old| old.group_id() == batch.group_id() && old != batch)
            {
                return Err(ProcessorError::new(
                    "native tool group label belongs to a different batch",
                ));
            }
        }
        let mut entries = Vec::new();
        let mut needed = 0usize;
        for call in calls {
            if let Some(old) = self.calls.get(&call.tool_call_id) {
                if old.call != *call || old.lineage != lineage || old.batch.as_ref() != batch {
                    return Err(ProcessorError::new(
                        "native tool policy or context identity changed",
                    ));
                }
            } else {
                let mut counter = ByteCounter {
                    bytes: call.tool_call_id.len().saturating_mul(2),
                    limit: MAX_BYTES,
                };
                serde_json::to_writer(&mut counter, call)
                    .map_err(|error| ProcessorError::new(error.to_string()))?;
                let bytes = counter.bytes;
                let bytes = bytes.saturating_add(batch.map_or(0, |batch| batch.retained_bytes()));
                needed = needed
                    .checked_add(bytes)
                    .ok_or_else(|| ProcessorError::new("native tool tracking byte overflow"))?;
                entries.push((call, bytes));
            }
        }
        while self.calls.len().saturating_add(entries.len()) > MAX_CALLS
            || self.bytes.saturating_add(needed) > MAX_BYTES
        {
            if !self.release_receipt() && !self.evict() {
                return Err(ProcessorError::new("native tool history capacity exceeded"));
            }
        }
        for (call, bytes) in entries {
            self.bytes += bytes;
            self.calls.insert(
                call.tool_call_id.clone(),
                NativeCall {
                    call: call.clone(),
                    lineage,
                    batch: batch.cloned(),
                    terminal: None,
                    retained: None,
                    receipt_bytes: 0,
                    bytes,
                },
            );
        }
        Ok(())
    }
    fn release_receipt(&mut self) -> bool {
        for id in &self.completed {
            if let Some(entry) = self.calls.get_mut(id) {
                if entry.retained.take().is_some() {
                    self.bytes -= entry.receipt_bytes;
                    entry.receipt_bytes = 0;
                    return true;
                }
            }
        }
        false
    }
    fn evict(&mut self) -> bool {
        let candidate = self.completed.iter().position(|id| {
            let entry = &self.calls[id];
            entry.batch.as_ref().is_none_or(|batch| {
                !self.calls.values().any(|other| {
                    other.terminal.is_none()
                        && other
                            .batch
                            .as_ref()
                            .is_some_and(|active| active.group_id() == batch.group_id())
                })
            })
        });
        let Some(index) = candidate else {
            return false;
        };
        let id = self.completed.remove(index).expect("known completed entry");
        if let Some(entry) = self.calls.remove(&id) {
            self.bytes -= entry.bytes + entry.receipt_bytes;
        }
        true
    }
    pub fn finish(&mut self, outcome: Arc<NativeToolCallOutcome>, cancelled: bool) {
        let id = outcome.call().tool_call_id.clone();
        let Some(entry) = self.calls.get_mut(&id) else {
            return;
        };
        entry.terminal = Some((outcome.id(), cancelled));
        self.completed.push_back(id.clone());
        // The selected route has compact immutable replay/commit evidence.
        // Retaining a selected payload receipt here would also pin its separate
        // terminal reservation after real consumption, starving later leases.
        let bytes = if outcome.progress_terminal().is_some() {
            0
        } else {
            outcome.retained_bytes()
        };
        while self.bytes.saturating_add(bytes) > MAX_BYTES {
            if !self.release_receipt() {
                break;
            }
        }
        if outcome.progress_terminal().is_none() && self.bytes.saturating_add(bytes) <= MAX_BYTES {
            let entry = self.calls.get_mut(&id).expect("terminal entry");
            entry.retained = Some(outcome);
            entry.receipt_bytes = bytes;
            self.bytes += bytes;
        }
        while self.completed.len() > MAX_TERMINALS {
            if !self.evict() {
                break;
            }
        }
    }
    pub fn clear(&mut self) {
        self.calls.clear();
        self.completed.clear();
        self.bytes = 0;
    }
}

pub(super) struct ProvisionalCall {
    pub call: FunctionCallInProgressFrame,
    pub lineage: u64,
    pub materialized: bool,
    bytes: usize,
}
#[derive(Default)]
pub(super) struct ProvisionalTools {
    pub calls: BTreeMap<String, ProvisionalCall>,
    order: VecDeque<String>,
    bytes: usize,
}
impl ProvisionalTools {
    pub fn insert(
        &mut self,
        call: FunctionCallInProgressFrame,
        lineage: u64,
        materialized: bool,
    ) -> Result<(), ProcessorError> {
        let mut counter = ByteCounter {
            bytes: call.tool_call_id.len().saturating_mul(2),
            limit: MAX_BYTES,
        };
        serde_json::to_writer(&mut counter, &call)
            .map_err(|error| ProcessorError::new(error.to_string()))?;
        let bytes = counter.bytes;
        if bytes > MAX_BYTES {
            return Err(ProcessorError::new(
                "provisional tool history exceeds byte limit",
            ));
        }
        self.remove(&call.tool_call_id);
        while self.order.len() >= MAX_CALLS || self.bytes + bytes > MAX_BYTES {
            let Some(id) = self.order.front().cloned() else {
                break;
            };
            self.remove(&id);
        }
        self.bytes += bytes;
        self.order.push_back(call.tool_call_id.clone());
        self.calls.insert(
            call.tool_call_id.clone(),
            ProvisionalCall {
                call,
                lineage,
                materialized,
                bytes,
            },
        );
        Ok(())
    }
    pub fn remove(&mut self, id: &str) -> Option<ProvisionalCall> {
        let value = self.calls.remove(id)?;
        self.bytes -= value.bytes;
        self.order.retain(|old| old != id);
        Some(value)
    }
    pub fn clear(&mut self) {
        self.calls.clear();
        self.order.clear();
        self.bytes = 0;
    }
}

#[derive(Default)]
pub(super) struct DeclaredTools {
    pub calls: BTreeMap<String, (pipecat_core::FunctionCall, u64, usize)>,
    bytes: usize,
}
impl DeclaredTools {
    pub fn insert(
        &mut self,
        call: &pipecat_core::FunctionCall,
        lineage: u64,
    ) -> Result<(), ProcessorError> {
        if let Some((old, old_lineage, _)) = self.calls.get(&call.tool_call_id) {
            if old != call || *old_lineage != lineage {
                return Err(ProcessorError::new("conflicting tool declaration"));
            }
            return Ok(());
        }
        let bytes = NativeToolCallBatch::measure("declaration", &[(call, true)])?
            .checked_add(call.tool_call_id.len())
            .filter(|bytes| *bytes <= MAX_BYTES)
            .ok_or_else(|| ProcessorError::new("active tool declaration exceeds byte limit"))?;
        if self.calls.len() >= MAX_CALLS || self.bytes.saturating_add(bytes) > MAX_BYTES {
            return Err(ProcessorError::new(
                "active tool declarations exceed capacity",
            ));
        }
        self.bytes += bytes;
        self.calls
            .insert(call.tool_call_id.clone(), (call.clone(), lineage, bytes));
        Ok(())
    }
    pub fn remove(&mut self, id: &str) {
        if let Some((_, _, bytes)) = self.calls.remove(id) {
            self.bytes -= bytes;
        }
    }
    pub fn clear(&mut self) {
        self.calls.clear();
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tool_tombstones_bound_owned_ids_by_count_and_bytes() {
        let mut ids = ToolTombstones::default();
        for n in 0..=MAX_TERMINALS {
            ids.insert(&n.to_string());
        }
        assert_eq!(ids.ids.len(), MAX_TERMINALS);
        assert!(!ids.contains("0"));
        ids.clear();
        let a = "a".repeat(3 * 1024 * 1024);
        let b = "b".repeat(3 * 1024 * 1024);
        let c = "c".repeat(3 * 1024 * 1024);
        ids.insert(&a);
        ids.insert(&b);
        ids.insert(&c);
        assert!(!ids.contains(&a));
        assert_eq!(ids.bytes, 6 * 1024 * 1024);
        ids.insert(&c);
        assert_eq!(ids.bytes, 6 * 1024 * 1024);
        assert!(ids.remove(&b));
        assert_eq!(ids.bytes, c.len());
        ids.insert(&"x".repeat(MAX_BYTES + 1));
        assert_eq!(ids.bytes, c.len());
        ids.clear();
        assert_eq!(ids.bytes, 0);
    }
    #[test]
    fn pending_capacity_checks_batch_atomically_and_keeps_active_entries() {
        let mut pending = PendingTools::default();
        for n in 0..MAX_CALLS {
            pending.insert(n.to_string(), None).unwrap();
        }
        let policy = FunctionCallInProgressFrame {
            function_name: "f".into(),
            tool_call_id: "0".into(),
            arguments: serde_json::json!({}),
            cancel_on_interruption: true,
            group_id: Some("g".into()),
        };
        let other = FunctionCallInProgressFrame {
            tool_call_id: "new".into(),
            ..policy.clone()
        };
        let before = pending.bytes;
        assert!(pending.check_calls([&policy, &other].into_iter()).is_err());
        assert_eq!(pending.bytes, before);
        assert!(pending.get("0").unwrap().is_none());
        pending.insert("0".into(), Some(policy)).unwrap();
        assert!(pending.bytes > before);
        pending.clear();
        assert_eq!(pending.bytes, 0);
        assert!(pending.is_empty());
    }
    #[test]
    fn active_declaration_limit_never_evicts_unsettled_authority() {
        let mut declarations = DeclaredTools::default();
        for n in 0..MAX_CALLS {
            declarations
                .insert(
                    &pipecat_core::FunctionCall {
                        function_name: "f".into(),
                        tool_call_id: format!("c{n}"),
                        arguments: serde_json::json!({}),
                    },
                    7,
                )
                .unwrap();
        }
        let overflow = pipecat_core::FunctionCall {
            function_name: "f".into(),
            tool_call_id: "overflow".into(),
            arguments: serde_json::json!({}),
        };
        assert!(declarations.insert(&overflow, 7).is_err());
        assert_eq!(declarations.calls.len(), MAX_CALLS);
        assert!(declarations.calls.contains_key("c0"));
        declarations.remove("c1");
        declarations.insert(&overflow, 7).unwrap();
        assert!(declarations.calls.contains_key("c0"));
    }
}
