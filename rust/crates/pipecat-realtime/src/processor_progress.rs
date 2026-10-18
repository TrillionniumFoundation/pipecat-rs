//! Selected provider consumer: private policy, bounded ordinal ordering, truthful queue receipts.
use super::*;
use pipecat_core::{NativeToolProgressAdmission, NativeToolProgressMetadataCharge};
use std::collections::BTreeMap;
#[derive(Default)]
pub(super) struct ProgressProvider {
    leases: BTreeMap<u64, ProviderLease>,
    pending: usize,
}
struct ProviderLease {
    cursor: u64,
    pending: BTreeMap<u64, Frame>,
    terminal: Option<Frame>,
    finished: bool,
    _metadata: NativeToolProgressMetadataCharge,
}
impl OpenAiRealtimeProcessor {
    pub(super) fn send_inference_batch(
        &self,
        actions: Vec<Action>,
        fence: Option<InvocationFence>,
    ) -> Result<(), ProcessorError> {
        if let Some(route) = &self.progress_route {
            self.sender()?
                .try_send_progress_transaction(
                    None,
                    actions,
                    fence,
                    Some(route.inference_permission()),
                )
                .map_err(processor_error)
        } else {
            self.sender()?
                .try_send_batch_with_fence(actions, fence)
                .map_err(processor_error)
        }
    }
    fn admit_progress_policy(
        &self,
        admission: &NativeToolProgressAdmission,
    ) -> Result<(), ProcessorError> {
        let mut state = self
            .context
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
        let mut next = state.clone();
        let fresh =
            next.selected_admission(admission, self.client.config().limits.max_message_bytes)?;
        if fresh {
            self.sender()?
                .try_send_progress_transaction(Some(admission.clone()), vec![], None, None)
                .map_err(processor_error)?;
        }
        *state = next;
        Ok(())
    }
    /// Returns true only for a native selected upstream receipt. Raw frames never
    /// become policy, and a retired selected frame never falls back to raw handling.
    pub(super) fn consume_progress(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<Option<Frame>, ProcessorError> {
        let progress = frame.native_tool_progress()?.cloned();
        let admission = frame.native_tool_progress_admission()?.cloned();
        let outcome = frame.native_tool_outcome()?.cloned();
        let terminal = outcome
            .as_ref()
            .and_then(|o| o.progress_terminal())
            .cloned();
        if progress.is_none() && admission.is_none() && terminal.is_none() {
            return Ok(Some(frame));
        }
        let route = self
            .progress_route
            .as_ref()
            .ok_or_else(|| ProcessorError::fatal("selected receipt without provider route"))?;
        let authority = progress
            .as_ref()
            .map(|p| p.admission())
            .or(admission.as_ref())
            .or_else(|| terminal.as_ref().map(|t| t.admission()))
            .expect("selected authority");
        route.validate_admission(ctx, authority)?;
        if direction != Direction::Upstream {
            return Err(ProcessorError::fatal(
                "selected provider receipt must travel upstream",
            ));
        }
        if route.is_closed() {
            return Ok(None);
        }
        let Some(admitted) = route.with_history_admission(|| self.admit_progress_policy(authority))
        else {
            return Ok(None);
        };
        admitted?;
        if admission.is_some() && progress.is_none() && terminal.is_none() {
            return Ok(None);
        }
        let lease_id = authority.lease_id();
        let mut consumer = self
            .progress_provider
            .lock()
            .map_err(|_| ProcessorError::fatal("progress consumer lock poisoned"))?;
        if !consumer.leases.contains_key(&lease_id) {
            if consumer.leases.len() >= 1024 {
                return Err(ProcessorError::fatal(
                    "provider selected lease count exceeded",
                ));
            }
            let metadata = route.reserve_metadata(128)?;
            consumer.leases.insert(
                lease_id,
                ProviderLease {
                    cursor: 0,
                    pending: BTreeMap::new(),
                    terminal: None,
                    finished: false,
                    _metadata: metadata,
                },
            );
        }
        if let Some(progress) = &progress {
            let state = consumer.leases.get(&lease_id).expect("lease inserted");
            if progress.ordinal() <= state.cursor {
                drop(consumer);
                route.provider_admitted(ctx, progress)?;
                return Ok(None);
            }
            if state.finished {
                return Err(ProcessorError::fatal("progress follows terminal watermark"));
            }
            if state.pending.contains_key(&progress.ordinal()) {
                return Ok(None);
            }
            if state.pending.len() >= 4 || consumer.pending >= 32 {
                return Err(ProcessorError::fatal(
                    "provider progress reorder capacity exceeded",
                ));
            }
            consumer
                .leases
                .get_mut(&lease_id)
                .expect("lease")
                .pending
                .insert(progress.ordinal(), frame);
            consumer.pending += 1;
        } else {
            let state = consumer.leases.get_mut(&lease_id).expect("lease");
            if state.finished {
                drop(consumer);
                route.terminal_consumed(ctx, outcome.as_ref().expect("terminal"))?;
                return Ok(None);
            }
            if state.terminal.is_some() {
                return Ok(None);
            }
            project_timeout(&mut frame)?;
            state.terminal = Some(frame);
        }
        loop {
            let next = {
                let state = consumer.leases.get_mut(&lease_id).expect("lease");
                state.pending.remove(&(state.cursor + 1))
            };
            let Some(next) = next else {
                break;
            };
            consumer.pending -= 1;
            let progress = next.native_tool_progress()?.expect("buffered progress");
            let Some(committed) = route.with_history_admission(|| {
                let mut state = self
                    .context
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
                let mut candidate = state.clone();
                if let Some(action) = candidate
                    .selected_progress(progress, self.client.config().limits.max_message_bytes)?
                {
                    self.sender()?
                        .try_send_progress_transaction(None, vec![action], None, None)
                        .map_err(processor_error)?;
                }
                *state = candidate;
                route.stage_provider_admitted(ctx, progress)
            }) else {
                return Ok(None);
            };
            let notification = committed?;
            consumer.leases.get_mut(&lease_id).expect("lease").cursor = progress.ordinal();
            drop(consumer);
            notification.notify()?;
            self.flush_deferred()?;
            consumer = self
                .progress_provider
                .lock()
                .map_err(|_| ProcessorError::fatal("progress consumer lock poisoned"))?;
            if !consumer.leases.contains_key(&lease_id) {
                return Ok(None);
            }
        }
        let ready = {
            let state = consumer.leases.get(&lease_id).expect("lease");
            state.terminal.as_ref().is_some_and(|f| {
                f.native_tool_outcome()
                    .ok()
                    .flatten()
                    .and_then(|o| o.progress_terminal())
                    .is_some_and(|t| t.predecessor_ordinal() == state.cursor)
            })
        };
        if ready {
            let terminal = consumer
                .leases
                .get_mut(&lease_id)
                .expect("lease")
                .terminal
                .take()
                .expect("ready terminal");
            let outcome = terminal.native_tool_outcome()?.expect("terminal outcome");
            let Some(committed) = route.with_history_admission(|| {
                let rerun = self.result(&terminal)?;
                Ok::<_, ProcessorError>((rerun, route.stage_terminal_consumed(ctx, outcome)?))
            }) else {
                return Ok(None);
            };
            let (rerun, notification) = committed?;
            consumer.leases.get_mut(&lease_id).expect("lease").finished = true;
            drop(consumer);
            notification.notify()?;
            self.flush_deferred()?;
            if let Some(fence) = rerun {
                ctx.try_push_with_invocation_fence(
                    Frame::new(FrameData::LlmRun),
                    Direction::Downstream,
                    &fence,
                )?;
            }
        }
        Ok(None)
    }
}
