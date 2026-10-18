use super::*;
use crate::{
    async_trait, ChatMessage, ChatRole, ContextMessage, FrameProcessor, Pipeline, PipelineConfig,
    StartFrame,
};
use serde_json::json;

fn message(text: &str) -> ContextMessage {
    ChatMessage::new(ChatRole::Developer, text).into()
}
fn commit() -> NativeToolProgressHistoryCommit {
    NativeToolProgressHistoryCommit::new(1, 0, 1, &[message("actual")]).unwrap()
}
fn pending_receipt() -> ToolProgressReceipt {
    ToolProgressReceipt(Arc::new(ReceiptInner {
        identity: Mutex::new(ReceiptIdentity {
            event_id: 7,
            lease_id: 3,
            ordinal: 1,
            digest: [0; 32],
        }),
        state: Mutex::new(ReceiptState {
            admitted: false,
            provider_admitted: false,
            history: None,
        }),
        changed: Notify::new(),
    }))
}
#[tokio::test]
async fn history_receipt_is_repeatable_staged_and_preserves_commit_on_close() {
    let receipt = pending_receipt();
    receipt.0.stage_commit(commit()).unwrap();
    assert_eq!(receipt.history(), None);
    receipt.0.fail(NativeToolProgressFailure::Cancelled);
    receipt.0.admit();
    assert_eq!(receipt.await_history().await.unwrap(), commit());
    assert_eq!(receipt.await_history().await.unwrap(), commit());
}
#[tokio::test]
async fn dropped_waiter_does_not_cancel_history_or_consume_result() {
    let receipt = pending_receipt();
    receipt.0.admit();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), receipt.await_history())
            .await
            .is_err()
    );
    receipt.0.commit(commit()).unwrap();
    assert_eq!(receipt.await_history().await.unwrap(), commit());
    assert_eq!(receipt.await_history().await.unwrap(), commit());
}
#[test]
fn iterative_json_guard_rejects_depth_and_accumulated_spare_capacity() {
    let mut nested = json!(null);
    for _ in 0..34 {
        nested = serde_json::Value::Array(vec![nested]);
    }
    assert!(validate_json(&nested, 16 * KIB).is_err());
    let strings = (0..16)
        .map(|_| serde_json::Value::String(String::with_capacity(16 * KIB)))
        .collect();
    assert!(validate_json(&serde_json::Value::Array(strings), 16 * KIB).is_err());
    for value in [
        json!(null),
        json!(false),
        json!(0),
        json!(""),
        json!([]),
        json!({}),
    ] {
        validate_json(&value, 16 * KIB).unwrap();
    }
}
#[test]
fn history_attribution_rejects_changed_range_without_parsing_shape() {
    let original = vec![message("prefix"), message("actual")];
    let receipt = NativeToolProgressHistoryCommit::new(4, 1, 2, &original).unwrap();
    assert!(receipt.matches(&original).unwrap());
    let mut changed = original.clone();
    changed[1] = message("{\"async_tool\":true}");
    assert!(!receipt.matches(&changed).unwrap());
    assert!(!receipt.matches(&[]).unwrap());
}
#[test]
fn terminal_reason_remains_copy_and_route_metadata_has_hard_bound() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<crate::RealtimeNativeToolSchedulingReason>();
    let route = NativeToolProgressRoute::new(Default::default()).unwrap();
    let reservation = route.reserve_metadata(256 * KIB).unwrap();
    assert!(route.reserve_metadata(1).is_err());
    drop(reservation);
    assert!(route.reserve_metadata(256 * KIB).is_ok());
}

struct TestRole {
    role: NativeToolProgressRole,
    route: NativeToolProgressRoute,
    context: Mutex<Option<ProcessorContext>>,
    history: Arc<Mutex<Vec<ContextMessage>>>,
    hold_history: bool,
    history_release: Notify,
    provider_seen: Notify,
    last_progress: Mutex<Option<Frame>>,
}
#[async_trait]
impl FrameProcessor for TestRole {
    fn name(&self) -> &str {
        "selected-progress-test-role"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.route.register_role(
            self.role,
            ctx,
            matches!(
                self.role,
                NativeToolProgressRole::User | NativeToolProgressRole::Assistant
            )
            .then_some(Arc::as_ptr(&self.history) as usize),
        )?;
        if self.role == NativeToolProgressRole::Gate {
            self.route.register_gate_context(ctx)?;
        }
        if self.role == NativeToolProgressRole::Executor {
            self.route
                .register_executor_limits(1, Duration::from_secs(30))?;
        }
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() && self.role == NativeToolProgressRole::Provider {
            self.route.validate_and_open(
                ctx,
                NativeToolProgressProviderLimits {
                    response_timeout: Duration::from_secs(120),
                    operation_timeout: Duration::from_secs(5),
                    close_timeout: Duration::from_secs(2),
                    max_message_bytes: 1024 * KIB,
                    max_in_flight: 1,
                },
            )?;
        }
        if matches!(frame.data, FrameData::Cancel(_)) {
            self.route.close(NativeToolProgressFailure::Cancelled);
        }
        if let Some(progress) = frame.native_tool_progress()?.cloned() {
            match self.role {
                NativeToolProgressRole::Relay if direction == Direction::Downstream => {
                    ctx.push(frame.clone(), Direction::Upstream).await?;
                }
                NativeToolProgressRole::Provider if direction == Direction::Upstream => {
                    self.route.provider_admitted(ctx, &progress)?;
                    self.provider_seen.notify_one();
                    return Ok(());
                }
                NativeToolProgressRole::Assistant if direction == Direction::Downstream => {
                    if self.hold_history {
                        self.history_release.notified().await;
                    }
                    let notification = self
                        .route
                        .with_history_admission(|| {
                            let mut history = self.history.lock().unwrap();
                            let start = history.len();
                            history.push(message(&progress.result().to_string()));
                            let commit = NativeToolProgressHistoryCommit::new(
                                start as u64 + 1,
                                start,
                                history.len(),
                                &history,
                            )?;
                            self.route.stage_history_committed(ctx, &progress, commit)
                        })
                        .ok_or_else(|| ProcessorError::new("history route closed"))??;
                    notification.notify()?;
                    *self.last_progress.lock().unwrap() = Some(frame);
                    return Ok(());
                }
                _ => (),
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let context = self.context.lock().unwrap().take();
        drop(context);
        Ok(())
    }
}
async fn fixture(
    hold: bool,
    shutdown: Duration,
) -> Result<
    (
        crate::PipelineWorker,
        NativeToolProgressRoute,
        Vec<Arc<TestRole>>,
    ),
    crate::PipelineError,
> {
    let route = NativeToolProgressRoute::new(Default::default()).unwrap();
    let history = Arc::new(Mutex::new(Vec::new()));
    let roles = [
        NativeToolProgressRole::Gate,
        NativeToolProgressRole::User,
        NativeToolProgressRole::Provider,
        NativeToolProgressRole::Executor,
        NativeToolProgressRole::Relay,
        NativeToolProgressRole::Assistant,
    ];
    let nodes = roles
        .into_iter()
        .map(|role| {
            Arc::new(TestRole {
                role,
                route: route.clone(),
                context: Mutex::new(None),
                history: history.clone(),
                hold_history: hold,
                history_release: Notify::new(),
                provider_seen: Notify::new(),
                last_progress: Mutex::new(None),
            })
        })
        .collect::<Vec<_>>();
    let worker = Pipeline::new(
        nodes
            .iter()
            .map(|node| node.clone() as Arc<dyn FrameProcessor>)
            .collect(),
    )
    .start(PipelineConfig {
        shutdown_timeout: shutdown,
        ..Default::default()
    })
    .await?;
    Ok((worker, route, nodes))
}
#[tokio::test]
async fn actual_assistant_commit_receipt_waits_after_provider_admission_and_seals_clones() {
    let (worker, route, nodes) = fixture(true, Duration::from_secs(172)).await.unwrap();
    let ctx = nodes[3].context.lock().unwrap().as_ref().unwrap().clone();
    let call = Arc::new(FunctionCall {
        function_name: "report".into(),
        tool_call_id: "one".into(),
        arguments: json!({}),
    });
    let batch = NativeToolCallBatch::new("group".into(), vec![(call.clone(), false)]).unwrap();
    let lease = route
        .admit_call(&ctx, call, batch, true, Duration::from_secs(30))
        .unwrap();
    lease.start_execution().unwrap();
    let receipt = lease.report(json!(false), Some(false)).unwrap();
    tokio::time::timeout(Duration::from_secs(2), nodes[2].provider_seen.notified())
        .await
        .unwrap();
    assert_eq!(receipt.history(), None);
    nodes[5].history_release.notify_one();
    let committed = tokio::time::timeout(Duration::from_secs(2), receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.await_history().await.unwrap(), committed);
    assert_eq!(nodes[5].history.lock().unwrap().len(), 1);
    let frame = nodes[5]
        .last_progress
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    assert!(serde_json::to_value(&frame).is_err());
    assert!(frame.duplicate().native_tool_progress().is_err());
    let mut changed = frame.clone();
    changed.id += 1;
    assert!(changed.native_tool_progress().is_err());
    let mut stripped = frame.clone();
    stripped.take_native_tool_progress().unwrap();
    assert!(stripped.native_tool_progress().is_err());
    assert!(serde_json::to_value(&stripped).is_err());
    for data in [
        FrameData::Interruption,
        FrameData::Cancel(crate::CancelFrame {
            reason: Some("forged".into()),
        }),
        FrameData::Stop,
        FrameData::End(crate::EndFrame::default()),
    ] {
        let mut forged = frame.clone();
        forged.data = data;
        let queue = crate::FrameQueue::new(2);
        queue
            .try_send(crate::DirectedFrame::new(
                Frame::text("preserved"),
                Direction::Downstream,
            ))
            .unwrap();
        let before = queue.generation();
        assert!(queue
            .try_send(crate::DirectedFrame::new(
                forged.clone(),
                Direction::Downstream
            ))
            .is_err());
        assert_eq!(queue.generation(), before);
        assert_eq!(queue.len(), 1);
        assert!(worker
            .sender()
            .send(forged, Direction::Downstream)
            .await
            .is_err());
        assert!(lease.0.control.lock().unwrap().open);
    }
    let reason = crate::RealtimeNativeToolProgressSchedulingReason::from_progress(
        frame.native_tool_progress().unwrap().unwrap(),
    );
    let producer = crate::NativeRealtimeContextProducer::default();
    let context_frame = || Frame::new(FrameData::LlmContext(crate::LlmContextFrame::default()));
    let mut duplicate_debt = context_frame();
    assert!(producer
        .attach_assistant_progress_continuations(
            &mut duplicate_debt,
            vec![reason.clone(), reason.clone()]
        )
        .is_err());
    assert!(duplicate_debt
        .native_realtime_context_cause()
        .unwrap()
        .is_none());
    let mut continuation = context_frame();
    producer
        .attach_assistant_progress_continuations(&mut continuation, vec![reason])
        .unwrap();
    let assistant = nodes[5].context.lock().unwrap().as_ref().unwrap().clone();
    continuation
        .bind_realtime_context_invocation(&assistant)
        .unwrap();
    let cause = continuation
        .native_realtime_context_cause()
        .unwrap()
        .unwrap();
    assert_eq!(cause.assistant_native_progress_reasons().len(), 1);
    assert!(
        cause
            .inference_fence()
            .unwrap()
            .same_scope(&assistant.invocation_fence().unwrap()),
        "common scope must not be globally intersected with one alternative"
    );
    let mut forged_context = continuation.clone();
    forged_context.data = FrameData::Interruption;
    assert!(worker
        .sender()
        .send(forged_context, Direction::Downstream)
        .await
        .is_err());
    let mut stripped_context = continuation.clone();
    stripped_context
        .take_native_realtime_context_cause()
        .unwrap();
    assert!(stripped_context.native_realtime_context_cause().is_err());
    assert!(producer
        .attach(
            &mut stripped_context,
            crate::RealtimeContextIntent::Run,
            true
        )
        .is_err());
    assert!(serde_json::to_value(&stripped_context).is_err());
    let authenticated = route
        .authenticated_history_indexes(&nodes[5].history.lock().unwrap())
        .unwrap();
    assert_eq!(authenticated, vec![0]);
    assert!(route
        .pending_provider_history(&nodes[5].history.lock().unwrap())
        .unwrap()
        .is_empty());
    let frozen = lease
        .freeze_terminal(NativeToolProgressTerminalChoice::Cancel {
            reason: ToolCallEndReason::ExplicitCancel,
            run_llm: false,
        })
        .unwrap();
    assert_eq!(frozen.predecessor_ordinal(), 1);
    assert!(lease
        .freeze_terminal(NativeToolProgressTerminalChoice::Complete {
            reason: ToolCallEndReason::CallbackReturned
        })
        .is_none());
    assert!(lease.report(json!(1), Some(true)).is_err());
    assert_eq!(receipt.await_history().await.unwrap(), committed);
    worker.cancel("test done").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn insufficient_actual_shutdown_budget_fails_before_reporting_open() {
    assert!(fixture(false, Duration::from_secs(30)).await.is_err());
}

struct DuplicateSelectedSetup {
    route: NativeToolProgressRoute,
    nodes: Vec<Arc<TestRole>>,
    duplicate_role: bool,
    error: Mutex<Option<ProcessorError>>,
    cleaned: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl FrameProcessor for DuplicateSelectedSetup {
    fn name(&self) -> &str {
        "duplicate-selected-setup"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let error = if self.duplicate_role {
            self.route
                .register_role(NativeToolProgressRole::Executor, ctx, None)
        } else {
            self.route
                .register_executor_limits(2, Duration::from_secs(20))
        }
        .expect_err("duplicate selected setup must fail");
        *self.error.lock().unwrap() = Some(error.clone());
        {
            let data = self.route.0.state.lock().unwrap();
            assert_eq!(data.phase, NativeToolProgressRouteState::Failed);
            assert_eq!(data.roles.len(), 6);
            assert_eq!(data.roles[&NativeToolProgressRole::Executor].index, 3);
            assert_eq!(data.executor_limits, Some((1, Duration::from_secs(30))));
            assert!(data.leases.is_empty());
        }
        assert!(self.route.0.gate.lock().unwrap().context.is_none());
        assert!(!self.route.inference_allowed());
        assert!(!self
            .route
            .0
            .publication_open
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(self.route.with_history_admission(|| ()).is_none());
        let provider = self.nodes[2]
            .context
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let limits = NativeToolProgressProviderLimits {
            response_timeout: Duration::from_secs(120),
            operation_timeout: Duration::from_secs(5),
            close_timeout: Duration::from_secs(2),
            max_message_bytes: 1024 * KIB,
            max_in_flight: 1,
        };
        assert!(self
            .route
            .validate_configuration(&provider, limits)
            .is_err());
        assert!(self.route.validate_and_open(&provider, limits).is_err());
        // Neither another rejected registration nor owner cleanup can reset the
        // first setup failure or make the already accepted registrations retryable.
        assert!(self
            .route
            .register_role(NativeToolProgressRole::Executor, ctx, None)
            .is_err());
        assert!(self
            .route
            .register_executor_limits(1, Duration::from_secs(30))
            .is_err());
        self.route.close(NativeToolProgressFailure::OwnerDropped);
        assert_eq!(self.route.state(), NativeToolProgressRouteState::Failed);
        assert_eq!(
            self.route.failure(),
            Some(NativeToolProgressFailure::InvalidProvenance)
        );
        Err(error)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}
#[tokio::test]
async fn duplicate_selected_setup_latches_before_start_and_preserves_first_registration() {
    for duplicate_role in [true, false] {
        let route = NativeToolProgressRoute::new(Default::default()).unwrap();
        let history = Arc::new(Mutex::new(Vec::new()));
        let nodes = [
            NativeToolProgressRole::Gate,
            NativeToolProgressRole::User,
            NativeToolProgressRole::Provider,
            NativeToolProgressRole::Executor,
            NativeToolProgressRole::Relay,
            NativeToolProgressRole::Assistant,
        ]
        .into_iter()
        .map(|role| {
            Arc::new(TestRole {
                role,
                route: route.clone(),
                context: Mutex::new(None),
                history: history.clone(),
                hold_history: false,
                history_release: Notify::new(),
                provider_seen: Notify::new(),
                last_progress: Mutex::new(None),
            })
        })
        .collect::<Vec<_>>();
        let duplicate = Arc::new(DuplicateSelectedSetup {
            route: route.clone(),
            nodes: nodes.clone(),
            duplicate_role,
            error: Mutex::new(None),
            cleaned: std::sync::atomic::AtomicBool::new(false),
        });
        let mut processors = nodes
            .iter()
            .map(|node| node.clone() as Arc<dyn FrameProcessor>)
            .collect::<Vec<_>>();
        processors.push(duplicate.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            Pipeline::new(processors).start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(172),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        if let Ok(worker) = result {
            worker
                .cancel("unexpected selected setup admission")
                .await
                .unwrap();
            let _ = worker.collect().await;
            panic!("a rejected duplicate selected setup must prevent Start");
        }
        assert!(duplicate.cleaned.load(std::sync::atomic::Ordering::Acquire));
        assert!(nodes
            .iter()
            .all(|node| node.context.lock().unwrap().is_none()));
        let error = duplicate.error.lock().unwrap().clone().unwrap();
        assert_eq!(error.fatal, duplicate_role);
        assert_eq!(
            error.message,
            if duplicate_role {
                "duplicate or late tool progress role registration"
            } else {
                "selected executor limits already registered"
            }
        );
        assert_eq!(route.state(), NativeToolProgressRouteState::Failed);
        assert_eq!(
            route.failure(),
            Some(NativeToolProgressFailure::InvalidProvenance)
        );
        assert!(route.0.gate.lock().unwrap().context.is_none());
    }
}

#[test]
fn disposition_mailbox_record_bound_and_atomic_charge_release() {
    assert!(std::mem::size_of::<NativeToolProgressDispositionRecord>() * 256 <= 64 * KIB);
    // Include conservative per-entry tree bookkeeping and Arc control blocks.
    assert!(
        std::mem::size_of::<LeaseRecord>()
            + 8
            + 64
            + 2 * (std::mem::size_of::<NativeToolProgressHistoryCommit>() + 8 + 64)
            <= NativeToolProgressRoute::CORE_LEASE_BYTES
    );
    assert!(
        std::mem::size_of::<EventRecord>() + 8 + 64 + std::mem::size_of::<ReceiptInner>() + 16
            <= NativeToolProgressRoute::CORE_EVENT_BYTES
    );
    let route = NativeToolProgressRoute::new(Default::default()).unwrap();
    let metadata = route.reserve_metadata(256 * KIB).unwrap();
    // A final budget owner can be dropped by runtime queue purge. It must not
    // reacquire route.state or wake application code while that lock is held.
    let state = route.0.state.lock().unwrap();
    drop(metadata);
    drop(state);
    assert!(route.reserve_metadata(256 * KIB).is_ok());
}
#[test]
fn public_batch_never_creates_reporting_admission() {
    let call = Arc::new(FunctionCall {
        function_name: "public".into(),
        tool_call_id: "not-dispatched".into(),
        arguments: json!({}),
    });
    let batch =
        NativeToolCallBatch::new("public-group".into(), vec![(call.clone(), false)]).unwrap();
    let mut frame = Frame::new(FrameData::FunctionCallInProgress(
        crate::FunctionCallInProgressFrame {
            function_name: call.function_name.clone(),
            tool_call_id: call.tool_call_id.clone(),
            arguments: call.arguments.clone(),
            cancel_on_interruption: false,
            group_id: Some("public-group".into()),
        },
    ));
    frame.set_native_tool_batch(batch).unwrap();
    assert!(frame.native_tool_progress_admission().unwrap().is_none());
    assert!(frame.native_tool_progress().unwrap().is_none());
}
#[tokio::test]
async fn receipt_notification_wakers_reenter_outside_route_locks() {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    struct Reenter {
        route: NativeToolProgressRoute,
        woke: AtomicBool,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(
                self.route.0.state.try_lock().is_ok(),
                "receipt woke under route lock"
            );
            assert!(
                self.route.0.history_admission.try_lock().is_ok(),
                "receipt woke under history admission"
            );
            self.woke.store(true, Ordering::SeqCst);
        }
    }
    let route = NativeToolProgressRoute::new(Default::default()).unwrap();
    let receipt = pending_receipt();
    receipt.0.admit();
    let waker = Arc::new(Reenter {
        route: route.clone(),
        woke: AtomicBool::new(false),
    });
    let wake = Waker::from(waker.clone());
    let mut cx = Context::from_waker(&wake);
    let mut waiting = Box::pin(receipt.await_history());
    assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Pending));
    receipt.0.fail(NativeToolProgressFailure::OwnerDropped);
    assert!(waker.woke.load(Ordering::SeqCst));
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(Err(NativeToolProgressFailure::OwnerDropped))
    ));
}
#[tokio::test]
async fn normal_final_permission_and_compact_journal_do_not_pin_runtime_or_terminal_pool() {
    let (worker, route, nodes) = fixture(false, Duration::from_secs(172)).await.unwrap();
    let ctx = nodes[3].context.lock().unwrap().as_ref().unwrap().clone();
    let weak_runtime = Arc::downgrade(&ctx.runtime);
    let call = Arc::new(FunctionCall {
        function_name: "normal".into(),
        tool_call_id: "done".into(),
        arguments: json!({}),
    });
    let batch = NativeToolCallBatch::new("batch".into(), vec![(call.clone(), false)]).unwrap();
    let lease = route
        .admit_call(
            &ctx,
            call.clone(),
            batch.clone(),
            true,
            Duration::from_secs(30),
        )
        .unwrap();
    lease.start_execution().unwrap();
    let permission = lease.inference_permission();
    let reporter = lease.reporter();
    let terminal = lease
        .freeze_terminal(NativeToolProgressTerminalChoice::Complete {
            reason: ToolCallEndReason::CallbackReturned,
        })
        .unwrap();
    assert!(lease
        .freeze_terminal(NativeToolProgressTerminalChoice::Cancel {
            reason: ToolCallEndReason::ExplicitCancel,
            run_llm: false
        })
        .is_none());
    let full = ctx
        .tool_outcome_publisher(call, false)
        .unwrap()
        .with_batch(batch)
        .unwrap()
        .with_progress_terminal(terminal)
        .unwrap()
        .complete(
            Ok(json!("complete")),
            crate::ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    route.note_terminal_publication(&full).unwrap();
    let reason = crate::RealtimeNativeToolSchedulingReason::from_outcome(&full.outcome);
    let producer = crate::NativeRealtimeContextProducer::default();
    let mut terminal_context = Frame::new(FrameData::LlmContext(crate::LlmContextFrame::default()));
    assert!(producer
        .attach_assistant_continuation(&mut terminal_context, Some(reason))
        .is_err());
    producer
        .attach_assistant_selected_continuation(&mut terminal_context, reason, &route)
        .unwrap();
    assert!(terminal_context.has_native_tool_progress_restriction());
    assert_eq!(
        terminal_context
            .native_realtime_context_cause()
            .unwrap()
            .unwrap()
            .assistant_selected_native_terminal_route_id(),
        Some(route.id())
    );
    for data in [
        FrameData::Interruption,
        FrameData::FunctionCallCancel(crate::FunctionCallCancelFrame {
            function_name: "normal".into(),
            tool_call_id: "done".into(),
            run_llm: true,
        }),
    ] {
        let mut forged = terminal_context.clone();
        forged.data = data;
        assert!(forged.validate_selected_tool_authority().is_err());
    }
    let mut consumed = terminal_context.clone();
    let cause = consumed
        .take_native_realtime_context_cause()
        .unwrap()
        .unwrap();
    assert!(consumed.native_realtime_context_cause().is_err());
    assert!(producer
        .attach(&mut consumed, crate::RealtimeContextIntent::Run, true)
        .is_err());
    assert!(serde_json::to_value(&consumed).is_err());
    let provider_context = nodes[2].context.lock().unwrap().as_ref().unwrap().clone();
    route
        .validate_consumed_progress_context(&provider_context, &consumed, &cause)
        .unwrap();
    assert!(route
        .validate_consumed_progress_context(&ctx, &consumed, &cause)
        .is_err());
    drop(provider_context);
    let compact = full.compact_selected_journal();
    assert!(compact.outcome.is_journal_projection());
    assert!(compact.outcome.progress_terminal().is_none());
    assert!(compact.outcome.batch().is_none());
    assert!(compact
        .outcome
        .validate(
            &FrameData::FunctionCallCancel(crate::FunctionCallCancelFrame {
                function_name: "normal".into(),
                tool_call_id: "done".into(),
                run_llm: false
            }),
            false
        )
        .is_err());
    drop(lease);
    assert!(permission.is_allowed());
    worker.cancel("done").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    drop(ctx);
    drop(full);
    assert!(!permission.is_allowed());
    assert!(reporter.report(json!(1), None).is_err());
    assert!(
        weak_runtime.upgrade().is_none(),
        "retained route/reporter/journal must not retain runtime"
    );
    assert_eq!(compact.outcome.id(), compact.outcome.id());
}

#[tokio::test]
async fn queue_full_selected_failure_is_applied_only_after_owner_unlock() {
    let (worker, route, nodes) = fixture(false, Duration::from_secs(172)).await.unwrap();
    let ctx = nodes[3].context.lock().unwrap().as_ref().unwrap().clone();
    // This current-thread test does not yield between filling and admission.
    for _ in 0..ctx.runtime.config.queue_capacity {
        ctx.runtime.queues[4]
            .try_send(crate::DirectedFrame::new(
                Frame::text("queued"),
                Direction::Downstream,
            ))
            .unwrap();
    }
    let owner = route.0.state.lock().unwrap();
    let deferred = ctx.with_admission(&ctx.runtime, || {
        ctx.runtime.route_tool_progress_unlocked(
            ctx.index,
            Frame::text("rejected").with_interruptible(false),
            Direction::Downstream,
        )
    });
    assert!(!deferred.accepted());
    assert!(
        !ctx.runtime.is_finished(),
        "queue Full failed runtime while owner remained locked"
    );
    drop(owner);
    assert!(deferred.finish().is_err());
    assert!(ctx.runtime.is_finished());
    route.close(NativeToolProgressFailure::QueueFull);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn forced_owner_drop_revokes_reports_and_resolves_real_pending_receipt() {
    let (worker, route, nodes) = fixture(true, Duration::from_secs(172)).await.unwrap();
    let ctx = nodes[3].context.lock().unwrap().as_ref().unwrap().clone();
    let call = Arc::new(FunctionCall {
        function_name: "report".into(),
        tool_call_id: "owner-drop".into(),
        arguments: json!({}),
    });
    let batch = NativeToolCallBatch::new("owner".into(), vec![(call.clone(), false)]).unwrap();
    let lease = route
        .admit_call(&ctx, call, batch, true, Duration::from_secs(30))
        .unwrap();
    lease.start_execution().unwrap();
    let receipt = lease.report(json!("pending"), Some(false)).unwrap();
    drop(route.owner_guard());
    assert_eq!(
        receipt.await_history().await,
        Err(NativeToolProgressFailure::OwnerDropped)
    );
    assert_eq!(
        receipt.await_history().await,
        Err(NativeToolProgressFailure::OwnerDropped)
    );
    assert!(!lease.inference_permission().is_allowed());
    assert!(lease.report(json!("late"), None).is_err());
    worker.cancel("done").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
}
