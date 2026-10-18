//! Bounded original inference intents waiting for exact provider queue facts.
//!
//! Non-inference actions retain normal source ordering. This queue does not
//! retain or regenerate a context snapshot, and it never refreshes a fence.
use super::*;
use crate::client::SelectedAlternative;
use pipecat_core::{
    NativeToolProgressMetadataCharge, NativeToolProgressPermission,
    NativeToolProgressProviderPrerequisite,
};
use std::collections::{BTreeSet, VecDeque};

const MAX_LEASE_INTENTS: usize = 32;
const MAX_INTENTS: usize = MAX_LEASE_INTENTS + 1;
const MAX_DEPENDENCIES: usize = 1024;

#[derive(Default)]
pub(super) struct DeferredProvider {
    entries: VecDeque<DeferredIntent>,
    sequence: u64,
    retired_stale: u64,
    retired_closed: u64,
    _metadata: Option<NativeToolProgressMetadataCharge>,
}
struct DeferredIntent {
    sequence: u64,
    alternative: SelectedAlternative,
    dependencies: Arc<Dependencies>,
}
struct Dependencies {
    ids: Vec<NativeToolProgressProviderPrerequisite>,
    _metadata: NativeToolProgressMetadataCharge,
}
impl DeferredProvider {
    fn initialize(&mut self, route: &NativeToolProgressRoute) -> Result<(), ProcessorError> {
        if self._metadata.is_none() {
            let bytes = std::mem::size_of::<DeferredIntent>()
                .checked_mul(MAX_INTENTS)
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() + 128))
                .ok_or_else(|| ProcessorError::fatal("deferred inference metadata overflow"))?;
            let metadata = route.reserve_metadata(bytes)?;
            self.entries = VecDeque::with_capacity(MAX_INTENTS);
            self._metadata = Some(metadata);
        }
        Ok(())
    }
    fn retire_invalid(&mut self) {
        self.entries.retain(|intent| {
            if !intent.alternative.permission.is_allowed() {
                self.retired_closed = self.retired_closed.saturating_add(1);
                false
            } else if !intent.alternative.fence.is_current() {
                self.retired_stale = self.retired_stale.saturating_add(1);
                false
            } else {
                true
            }
        });
    }
}

fn fill_options(
    alternative: &mut SelectedAlternative,
    fallback: &ResponseOptions,
) -> Result<(), ProcessorError> {
    let options = alternative.options.as_ref().unwrap_or(fallback);
    let bytes = crate::client::selected_options_bytes(options)
        .map_err(processor_error)?
        .checked_add(256)
        .ok_or_else(|| ProcessorError::fatal("selected response metadata overflow"))?;
    let metadata = alternative.permission.reserve_metadata(bytes)?;
    if alternative.options.is_none() {
        alternative.options = Some(fallback.clone());
    }
    alternative.retained_metadata = Some(metadata);
    Ok(())
}
fn copy_alternative(source: &SelectedAlternative) -> Result<SelectedAlternative, ProcessorError> {
    // intersect reserves the fixed fence/permission charge before constructing
    // the exact same origin set. No fresh invocation authority is introduced.
    let mut copy =
        SelectedAlternative::intersect(source.permission.clone(), &source.fence, &source.fence)?;
    fill_options(
        &mut copy,
        source.options.as_ref().expect("stored response options"),
    )?;
    Ok(copy)
}

impl OpenAiRealtimeProcessor {
    pub(super) fn queue_or_defer(
        &self,
        actions: Vec<Action>,
        fence: Option<InvocationFence>,
        permission: Option<NativeToolProgressPermission>,
        alternatives: Option<Vec<SelectedAlternative>>,
        dependencies: Vec<NativeToolProgressProviderPrerequisite>,
    ) -> Result<(), ProcessorError> {
        self.queue_or_defer_inner(actions, fence, permission, alternatives, dependencies)
            .map_err(|error| ProcessorError::fatal(error.to_string()))
    }
    fn queue_or_defer_inner(
        &self,
        mut actions: Vec<Action>,
        fence: Option<InvocationFence>,
        permission: Option<NativeToolProgressPermission>,
        alternatives: Option<Vec<SelectedAlternative>>,
        dependencies: Vec<NativeToolProgressProviderPrerequisite>,
    ) -> Result<(), ProcessorError> {
        let route = self
            .progress_route
            .as_ref()
            .ok_or_else(|| ProcessorError::fatal("deferred inference requires selected route"))?;
        let positions: Vec<_> = actions
            .iter()
            .enumerate()
            .filter_map(|(index, action)| {
                matches!(action, Action::CreateResponse(_)).then_some(index)
            })
            .collect();
        if positions.len() > 1 || dependencies.len() > MAX_DEPENDENCIES {
            return Err(ProcessorError::fatal(
                "selected deferred intent bound exceeded",
            ));
        }
        let Some(position) = positions.first().copied() else {
            return self
                .sender()?
                .try_send_progress_transaction(None, actions, fence, permission)
                .map_err(processor_error);
        };
        let Action::CreateResponse(options) = actions.remove(position) else {
            unreachable!()
        };
        let mut alternatives = match alternatives {
            Some(alternatives) if !alternatives.is_empty() && alternatives.len() <= MAX_INTENTS => {
                alternatives
            }
            Some(_) => {
                return Err(ProcessorError::fatal(
                    "invalid selected inference alternatives",
                ))
            }
            None => vec![SelectedAlternative::new(
                permission.ok_or_else(|| {
                    ProcessorError::fatal("selected inference permission missing")
                })?,
                fence.ok_or_else(|| ProcessorError::fatal("selected inference fence missing"))?,
            )?],
        };
        // Reserve before retaining options or sharing the dependency allocation.
        for alternative in &mut alternatives {
            fill_options(alternative, &options)?;
        }
        let dependency_bytes = dependencies
            .capacity()
            .checked_mul(std::mem::size_of::<NativeToolProgressProviderPrerequisite>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Dependencies>() + 64))
            .ok_or_else(|| ProcessorError::fatal("selected dependency metadata overflow"))?;
        let dependency_charge = route.reserve_metadata(dependency_bytes)?;
        let dependencies = Arc::new(Dependencies {
            ids: dependencies,
            _metadata: dependency_charge,
        });
        let mut deferred = self
            .deferred_provider
            .lock()
            .map_err(|_| ProcessorError::fatal("deferred provider lock poisoned"))?;
        deferred.initialize(route)?;
        deferred.retire_invalid();
        alternatives.retain(|alternative| {
            if !alternative.permission.is_allowed() {
                deferred.retired_closed = deferred.retired_closed.saturating_add(1);
                false
            } else if !alternative.fence.is_current() {
                deferred.retired_stale = deferred.retired_stale.saturating_add(1);
                false
            } else {
                true
            }
        });
        let keys: BTreeSet<_> = deferred
            .entries
            .iter()
            .map(|intent| intent.alternative.permission.lease_id())
            .chain(
                alternatives
                    .iter()
                    .map(|alternative| alternative.permission.lease_id()),
            )
            .collect();
        if keys.iter().filter(|id| id.is_some()).count() > MAX_LEASE_INTENTS
            || keys.len() > MAX_INTENTS
        {
            return Err(ProcessorError::fatal(
                "selected deferred intent capacity exceeded",
            ));
        }
        deferred
            .sequence
            .checked_add(alternatives.len() as u64)
            .ok_or_else(|| ProcessorError::fatal("selected deferred source order exhausted"))?;
        // This queue admission and the subsequent map publication serialize
        // against flush. A provider stage racing insertion is observed below,
        // even when its earlier flush found no entry and no later wake follows.
        if !actions.is_empty() {
            self.sender()?
                .try_send_progress_transaction(None, actions, None, None)
                .map_err(processor_error)?;
        }
        for alternative in alternatives {
            let key = alternative.permission.lease_id();
            deferred
                .entries
                .retain(|intent| intent.alternative.permission.lease_id() != key);
            deferred.sequence += 1;
            let sequence = deferred.sequence;
            deferred.entries.push_back(DeferredIntent {
                sequence,
                alternative,
                dependencies: dependencies.clone(),
            });
        }
        self.flush_ready_deferred(route, &mut deferred)
    }

    pub(super) fn flush_deferred(&self) -> Result<(), ProcessorError> {
        self.flush_deferred_inner()
            .map_err(|error| ProcessorError::fatal(error.to_string()))
    }
    fn flush_deferred_inner(&self) -> Result<(), ProcessorError> {
        let Some(route) = &self.progress_route else {
            return Ok(());
        };
        let mut deferred = self
            .deferred_provider
            .lock()
            .map_err(|_| ProcessorError::fatal("deferred provider lock poisoned"))?;
        self.flush_ready_deferred(route, &mut deferred)
    }
    fn flush_ready_deferred(
        &self,
        route: &NativeToolProgressRoute,
        deferred: &mut DeferredProvider,
    ) -> Result<(), ProcessorError> {
        deferred.retire_invalid();
        let ready: Vec<_> = deferred
            .entries
            .iter()
            .filter(|intent| route.provider_history_ready(&intent.dependencies.ids))
            .map(|intent| intent.sequence)
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let mut alternatives = Vec::with_capacity(ready.len());
        for intent in &deferred.entries {
            if ready.contains(&intent.sequence) {
                alternatives.push(copy_alternative(&intent.alternative)?);
            }
        }
        // All independent scopes enter one private command, oldest to newest.
        // The actor admits all debts before choosing one latest valid response.
        // On rejection the originals remain; no intent is falsely acknowledged.
        self.sender()?
            .try_send_progress_alternatives(
                None,
                vec![Action::CreateResponse(ResponseOptions::default())],
                None,
                None,
                Some(alternatives),
            )
            .map_err(processor_error)?;
        deferred
            .entries
            .retain(|intent| !ready.contains(&intent.sequence));
        Ok(())
    }
}
