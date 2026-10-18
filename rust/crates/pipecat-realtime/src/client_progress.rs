//! Private selected-route policy and socket admission. Public Actions never grant it.
use super::*;

pub(crate) struct SelectedAlternative {
    pub(crate) permission: NativeToolProgressPermission,
    pub(crate) fence: InvocationFence,
    pub(crate) options: Option<ResponseOptions>,
    pub(crate) retained_metadata: Option<pipecat_core::NativeToolProgressMetadataCharge>,
    _metadata: pipecat_core::NativeToolProgressMetadataCharge,
}
impl SelectedAlternative {
    pub(crate) fn intersect(
        permission: NativeToolProgressPermission,
        emission: &InvocationFence,
        publication: &InvocationFence,
    ) -> Result<Self, pipecat_core::ProcessorError> {
        let metadata = permission.reserve_metadata(4096)?;
        let fence = emission.intersection(publication)?;
        Ok(Self {
            permission,
            fence,
            options: None,
            retained_metadata: None,
            _metadata: metadata,
        })
    }
    pub(crate) fn new(
        permission: NativeToolProgressPermission,
        fence: InvocationFence,
    ) -> Result<Self, pipecat_core::ProcessorError> {
        let metadata = permission.reserve_metadata(4096)?;
        Ok(Self {
            permission,
            fence,
            options: None,
            retained_metadata: None,
            _metadata: metadata,
        })
    }
}
impl RealtimeSender {
    pub(crate) fn try_send_progress_transaction(
        &self,
        admission: Option<NativeToolProgressAdmission>,
        actions: Vec<Action>,
        fence: Option<InvocationFence>,
        permission: Option<NativeToolProgressPermission>,
    ) -> Result<(), RealtimeError> {
        self.try_send_progress_alternatives(admission, actions, fence, permission, None)
    }
    pub(crate) fn try_send_progress_alternatives(
        &self,
        admission: Option<NativeToolProgressAdmission>,
        actions: Vec<Action>,
        fence: Option<InvocationFence>,
        permission: Option<NativeToolProgressPermission>,
        mut alternatives: Option<Vec<SelectedAlternative>>,
    ) -> Result<(), RealtimeError> {
        if alternatives
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.len() > 33)
            || actions
                .iter()
                .filter(|a| matches!(a, Action::CreateResponse(_)))
                .count()
                > 1
        {
            return Err(RealtimeError::TooLarge);
        }
        for action in &actions {
            check_queued_action(action, &self.limits)?;
        }
        let count = actions
            .len()
            .checked_add(usize::from(admission.is_some()))
            .ok_or(RealtimeError::TooLarge)?;
        if count == 0 {
            return Ok(());
        }
        let mut permits = self.tx.try_reserve_many(count).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
        })?;
        let generation = self.generation();
        if let Some(admission) = admission {
            permits.next().expect("reserved policy").send(Command {
                action: None,
                progress_admission: Some(admission),
                selected_permission: None,
                selected_alternatives: None,
                generation,
                reply: None,
                inference_fence: None,
                camera_fence: None,
            });
        }
        for (permit, action) in permits.zip(actions) {
            let inference = matches!(action, Action::CreateResponse(_));
            permit.send(Command {
                action: Some(action),
                progress_admission: None,
                selected_permission: inference.then(|| permission.clone()).flatten(),
                selected_alternatives: if inference { alternatives.take() } else { None },
                generation,
                reply: None,
                inference_fence: inference.then(|| fence.clone()).flatten(),
                camera_fence: None,
            });
        }
        Ok(())
    }
}

pub(super) struct SelectedDebt {
    options: ResponseOptions,
    generation: u64,
    fence: Option<InvocationFence>,
    permission: NativeToolProgressPermission,
    _metadata: pipecat_core::NativeToolProgressMetadataCharge,
}
impl Actor {
    pub(super) fn defer_selected_response(
        &mut self,
        options: ResponseOptions,
        generation: u64,
        fence: Option<InvocationFence>,
        permission: NativeToolProgressPermission,
    ) -> Result<(), RealtimeError> {
        let retained_options = selected_options_bytes(&options)?;
        let epoch = self.epoch();
        if !permission.is_allowed()
            || generation != epoch
            || fence.as_ref().is_some_and(|f| !f.is_current())
        {
            return Ok(());
        }
        self.selected_debts.retain(|d| {
            d.permission.is_allowed()
                && d.generation == epoch
                && d.fence.as_ref().is_none_or(InvocationFence::is_current)
                && d.permission.lease_id() != permission.lease_id()
        });
        if self.selected_debts.len() >= 33 {
            return Err(RealtimeError::TooLarge);
        }
        let metadata = permission
            .reserve_metadata(retained_options + 4096)
            .map_err(|_| RealtimeError::TooLarge)?;
        self.selected_debts.push(SelectedDebt {
            options,
            generation,
            fence,
            permission,
            _metadata: metadata,
        });
        Ok(())
    }
    pub(super) async fn maybe_selected_continue(
        &mut self,
        socket: &mut Socket,
    ) -> Result<(), RealtimeError> {
        if self.active.is_some() || self.pending.is_some() {
            return Ok(());
        }
        loop {
            let epoch = self.epoch();
            self.selected_debts.retain(|d| {
                d.permission.is_allowed()
                    && d.generation == epoch
                    && d.fence.as_ref().is_none_or(InvocationFence::is_current)
            });
            let Some(debt) = self.selected_debts.last() else {
                return Ok(());
            };
            if !self.can_continue_selected(Some(&debt.permission)) {
                return Ok(());
            }
            let debt = self.selected_debts.pop().expect("observed pending debt");
            if !self
                .write_selected_inference(
                    socket,
                    json!({"type":"response.create", "response":debt.options}),
                    debt.fence.as_ref(),
                    Some(&debt.permission),
                )
                .await?
            {
                continue;
            }
            self.selected_debts.clear();
            self.pending_event_id = Some(format!("pc_evt_{}", self.sequence));
            self.pending = Some(ResponseContext {
                generation: debt.generation,
                inference_fence: debt.fence,
                context_id: debt
                    .options
                    .context_id
                    .or_else(|| self.config.context_id.clone()),
            });
            self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
            return Ok(());
        }
    }

    pub(super) fn admit_progress_policy(
        &mut self,
        admission: NativeToolProgressAdmission,
    ) -> Result<(), RealtimeError> {
        let call = admission.call();
        let tool = self
            .tools
            .get(&call.tool_call_id)
            .ok_or(RealtimeError::UnknownToolCall)?;
        if !tool.announced
            || tool.name != call.function_name
            || serde_json::from_str::<Value>(&tool.arguments).ok().as_ref() != Some(&call.arguments)
        {
            return Err(RealtimeError::Invalid(
                "native policy does not match announced provider call",
            ));
        }
        if let Some(old) = self.progress_policies.get(&call.tool_call_id) {
            if old.lease_id() != admission.lease_id() || old.route_id() != admission.route_id() {
                return Err(RealtimeError::Invalid("conflicting native call policy"));
            }
            return Ok(());
        }
        if self.progress_policies.len() >= self.config.limits.max_outstanding_tools {
            return Err(RealtimeError::TooLarge);
        }
        self.progress_policies
            .insert(call.tool_call_id.clone(), admission);
        if self.all_tools_selected() {
            self.settlement_deadline = None;
        }
        Ok(())
    }
    pub(super) fn all_tools_selected(&self) -> bool {
        !self.tools.is_empty()
            && self.tools.iter().all(|(id, tool)| {
                self.progress_policies
                    .get(id)
                    .is_some_and(|a| tool.announced && a.call().function_name == tool.name)
            })
    }
    pub(super) fn can_continue_selected(
        &self,
        permission: Option<&NativeToolProgressPermission>,
    ) -> bool {
        if self.tools.is_empty() {
            return true;
        }
        permission.is_some_and(|p| p.is_allowed())
            && self.tools.iter().all(|(id, tool)| {
                self.progress_policies.get(id).is_some_and(|admission| {
                    tool.announced
                        && admission.call().function_name == tool.name
                        && admission
                            .batch()
                            .members()
                            .iter()
                            .any(|(call, cancel)| call.as_ref() == admission.call() && !cancel)
                })
            })
    }
    pub(super) async fn write_selected_inference(
        &mut self,
        socket: &mut Socket,
        mut value: Value,
        fence: Option<&InvocationFence>,
        permission: Option<&NativeToolProgressPermission>,
    ) -> Result<bool, RealtimeError> {
        let Some(permission) = permission else {
            return self.write_inference(socket, value, fence).await;
        };
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RealtimeError::TooLarge)?;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        let wire = serde_json::to_string(&value)
            .map_err(|_| RealtimeError::Invalid("event serialization"))?;
        if wire.len() > self.config.limits.max_message_bytes {
            return Err(RealtimeError::TooLarge);
        }
        timeout(
            self.config.limits.operation_timeout,
            send_selected(socket, Message::Text(wire.into()), fence, permission),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
        .map_err(transport)
    }
}

/// Selected permission -> canonical invocation admission -> short readiness/start.
/// No sender-emission gate is held here and no route/control lock is entered from
/// a runtime gate. Already admitted bytes are flushed once, never retracted.
async fn send_selected<S, Item>(
    sink: &mut S,
    item: Item,
    fence: Option<&InvocationFence>,
    permission: &NativeToolProgressPermission,
) -> Result<bool, S::Error>
where
    S: Sink<Item> + Unpin,
{
    let mut item = Some(item);
    let ready = futures_util::future::poll_fn(|cx| {
        permission
            .with_allowed(|| {
                let mut ready = || match Pin::new(&mut *sink).poll_ready(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => Poll::Ready(
                        Pin::new(&mut *sink)
                            .start_send(item.take().expect("one selected admission"))
                            .map(|()| true),
                    ),
                };
                match fence {
                    Some(fence) => fence.with_current(ready).unwrap_or(Poll::Ready(Ok(false))),
                    None => ready(),
                }
            })
            .unwrap_or(Poll::Ready(Ok(false)))
    });
    let admitted = tokio::select! { biased;
        _ = permission.revoked() => false,
        result = ready => result?,
    };
    if admitted {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *sink).poll_flush(cx)).await?;
    }
    Ok(admitted)
}

/// Borrowed capacity preflight includes local-only fields skipped by serde.
pub(crate) fn selected_options_bytes(options: &ResponseOptions) -> Result<usize, RealtimeError> {
    if options
        .output_modalities
        .as_ref()
        .is_some_and(|v| v.len() != 1)
    {
        return Err(RealtimeError::Invalid(
            "exactly one response modality is required",
        ));
    }
    let bytes = std::mem::size_of::<ResponseOptions>()
        .checked_add(options.context_id.as_ref().map_or(0, String::capacity))
        .and_then(|n| n.checked_add(options.instructions.as_ref().map_or(0, String::capacity)))
        .and_then(|n| {
            n.checked_add(
                options
                    .output_modalities
                    .as_ref()
                    .map_or(0, |v| v.capacity() * std::mem::size_of::<Modality>()),
            )
        })
        .ok_or(RealtimeError::TooLarge)?;
    if bytes > 16 * 1024 {
        return Err(RealtimeError::TooLarge);
    }
    bounded_serialized(options, 16 * 1024)?;
    Ok(bytes)
}
