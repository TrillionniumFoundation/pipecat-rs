//! Provider reconciliation accepts only exact ranges committed by its selected consumer.
use super::*;
use pipecat_core::{NativeToolProgress, NativeToolProgressAdmission, NativeToolProgressRoute};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, serde::Serialize)]
pub(crate) struct SelectedContext {
    #[serde(skip)]
    route: NativeToolProgressRoute,
    #[serde(skip)]
    charges: BTreeMap<(u8, u64), pipecat_core::NativeToolProgressMetadataCharge>,
    leases: BTreeMap<String, (u64, bool)>,
    progress: BTreeMap<u64, u64>,
    terminals: BTreeSet<u64>,
    authenticated_indexes: BTreeSet<usize>,
}
impl SelectedContext {
    pub(crate) fn new(route: NativeToolProgressRoute) -> Self {
        Self {
            route,
            charges: BTreeMap::new(),
            leases: BTreeMap::new(),
            progress: BTreeMap::new(),
            terminals: BTreeSet::new(),
            authenticated_indexes: BTreeSet::new(),
        }
    }
    pub(crate) fn prepare(&mut self, messages: &[ContextMessage]) -> Result<(), ProcessorError> {
        if !self.charges.contains_key(&(3, 0)) {
            self.charges
                .insert((3, 0), self.route.reserve_metadata(64 * 1024)?);
        }
        self.authenticated_indexes.clear();
        self.authenticated_indexes
            .extend(self.route.authenticated_history_indexes(messages)?);
        Ok(())
    }
    pub(crate) fn owned(&self, index: usize) -> bool {
        self.authenticated_indexes.contains(&index)
    }
    pub(crate) fn bound(&self, limit: usize) -> Result<(), ProcessorError> {
        if self.leases.len() > MAX_MESSAGES
            || self.progress.len() > 1024
            || self.terminals.len() > MAX_MESSAGES
        {
            return Err(ProcessorError::new("selected tool ledger count exceeded"));
        }
        crate::client::bounded_serialized(&(&self.leases, &self.progress, &self.terminals), limit)
            .map_err(|_| ProcessorError::new("selected tool ledger bytes exceeded"))
    }
}
impl ContextState {
    pub(crate) fn selected_admission(
        &mut self,
        admission: &NativeToolProgressAdmission,
        limit: usize,
    ) -> Result<bool, ProcessorError> {
        self.validate_call(admission.call())?;
        let selected = self
            .selected
            .as_mut()
            .ok_or_else(|| ProcessorError::new("missing selected tool route"))?;
        let cancel = admission
            .batch()
            .members()
            .iter()
            .find(|(call, _)| call.as_ref() == admission.call())
            .map(|(_, cancel)| *cancel)
            .ok_or_else(|| ProcessorError::new("selected admission policy missing"))?;
        let record = (admission.lease_id(), cancel);
        if let Some(old) = selected.leases.get(&admission.call().tool_call_id) {
            if *old != record {
                return Err(ProcessorError::new("conflicting selected lease"));
            }
            return Ok(false);
        }
        selected.charges.insert(
            (0, admission.lease_id()),
            selected
                .route
                .reserve_metadata(2 * (128 + admission.call().tool_call_id.len()))?,
        );
        selected
            .leases
            .insert(admission.call().tool_call_id.clone(), record);
        selected.bound(limit)?;
        self.bound(limit)?;
        Ok(true)
    }
    pub(crate) fn selected_progress(
        &mut self,
        progress: &NativeToolProgress,
        limit: usize,
    ) -> Result<Option<Action>, ProcessorError> {
        self.validate_call(progress.call())?;
        if self
            .selected
            .as_ref()
            .is_some_and(|s| s.progress.contains_key(&progress.id()))
        {
            return Ok(None);
        }
        self.advance_result()?;
        let selected = self
            .selected
            .as_mut()
            .ok_or_else(|| ProcessorError::new("selected progress without route"))?;
        selected
            .charges
            .insert((1, progress.id()), selected.route.reserve_metadata(128)?);
        selected
            .progress
            .insert(progress.id(), self.result_revision);
        selected.bound(limit)?;
        let content = if progress.result().is_null() {
            "COMPLETED".to_owned()
        } else {
            progress.result().to_string()
        };
        let action = Action::ToolResult {
            call_id: progress.call().tool_call_id.clone(),
            output: Value::String(content),
            is_final: false,
            run_response: false,
        };
        crate::client::bounded_serialized(&action, limit)
            .map_err(|_| ProcessorError::new("progress wire exceeds provider limit"))?;
        self.bound(limit)?;
        Ok(Some(action))
    }
    pub(crate) fn progress_revision(&self, id: u64) -> Result<u64, ProcessorError> {
        self.selected
            .as_ref()
            .and_then(|s| s.progress.get(&id))
            .copied()
            .ok_or_else(|| ProcessorError::new("unadmitted selected progress continuation"))
    }
    pub(crate) fn selected_terminal(
        &mut self,
        id: u64,
        limit: usize,
    ) -> Result<(), ProcessorError> {
        let selected = self
            .selected
            .as_mut()
            .ok_or_else(|| ProcessorError::new("selected terminal without route"))?;
        if !selected.terminals.contains(&id) {
            selected
                .charges
                .insert((2, id), selected.route.reserve_metadata(128)?);
        }
        selected.terminals.insert(id);
        selected.bound(limit)?;
        self.bound(limit)
    }
}
