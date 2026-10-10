//! Independent paired Grok regression probes, real local WebSocket and native runtime.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::Context;
use pipecat_turns::*;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

const WAIT: Duration = Duration::from_secs(5);
type Ws = WebSocketStream<TcpStream>;
async fn event(ws: &mut Ws, mut value: Value) {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    value["event_id"] = json!(format!(
        "independent-{}",
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn command(ws: &mut Ws) -> Value {
    let frame = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
    assert!(frame.is_text(), "expected command, got {frame:?}");
    serde_json::from_str(frame.to_text().unwrap()).unwrap()
}
async fn silence(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(80), ws.next()).await.is_err(),
        "unexpected provider command"
    );
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    event(ws, json!({"type": if done {"response.done"} else {"response.created"}, "response":{"id":id,"status":if done {"completed"} else {"in_progress"}, "output":[]}})).await;
}
#[derive(Default)]
struct Boundary {
    frames: Mutex<Vec<DirectedFrame>>,
    swallow_interruptions: AtomicBool,
    hold_upstream_context: AtomicBool,
    held_upstream: Mutex<Option<(Frame, ProcessorContext)>>,
    hold_native_outcome: AtomicBool,
    held_native: Mutex<Option<(Frame, ProcessorContext)>>,
}
#[async_trait]
impl FrameProcessor for Boundary {
    fn name(&self) -> &str {
        "IndependentGrokBoundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        if direction == Direction::Downstream
            && frame.native_tool_outcome()?.is_some()
            && self.hold_native_outcome.swap(false, Ordering::SeqCst)
        {
            *self.held_native.lock().unwrap() = Some((frame, ctx.clone()));
            return Ok(());
        }
        if direction == Direction::Upstream
            && matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
            && self.hold_upstream_context.swap(false, Ordering::SeqCst)
        {
            *self.held_upstream.lock().unwrap() = Some((frame, ctx.clone()));
            return Ok(());
        }
        if self.swallow_interruptions.load(Ordering::SeqCst)
            && matches!(frame.data, FrameData::Interruption)
        {
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
struct Fixture {
    worker: PipelineWorker,
    ws: Ws,
    pair: LLMContextAggregatorPair,
    provider: Arc<GrokRealtimeProcessor>,
    before: Arc<Boundary>,
    after: Arc<Boundary>,
    at_relay: Arc<Boundary>,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
}
impl Fixture {
    async fn start(tool_timeout: Duration, interruptions: bool) -> Self {
        Self::start_policy(tool_timeout, interruptions, false).await
    }
    async fn start_policy(
        tool_timeout: Duration,
        interruptions: bool,
        cancel_on_interruption: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("independent-offline");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.session.turn_detection = Some(Some(TurnDetection::server_vad()));
        config.session.tools = Some(vec![FunctionTool::new(
            "wait",
            "Wait",
            json!({"type":"object"}),
        )
        .into()]);
        config.limits.connect_timeout = WAIT;
        let provider =
            Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let mut user = UserAggregatorConfig::default();
        user.turn.enable_interruptions = interruptions;
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            user,
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
        );
        let before = Arc::new(Boundary::default());
        let after = Arc::new(Boundary::default());
        let at_relay = Arc::new(Boundary::default());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
        let (e, r) = (entered.clone(), release.clone());
        let mut tool = RegisteredTool::new(Arc::new(move |call| {
            let (e, r) = (e.clone(), r.clone());
            Box::pin(async move {
                e.notify_one();
                r.acquire().await.unwrap().forget();
                Ok(call.arguments)
            })
        }));
        tool.cancel_on_interruption = cancel_on_interruption;
        tool.timeout = tool_timeout;
        executor.register("wait", tool).unwrap();
        let relay = provider.tool_result_relay();
        let stages: Vec<Arc<dyn FrameProcessor>> = vec![
            pair.user(),
            before.clone(),
            provider.clone(),
            Arc::new(executor),
            at_relay.clone(),
            Arc::new(relay),
            after.clone(),
            pair.assistant(),
        ];
        let mut pipeline = PipelineConfig::default();
        pipeline.start.audio_in_sample_rate = 24000;
        pipeline.start.audio_out_sample_rate = 24000;
        let handshake = async {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            event(
                &mut ws,
                json!({"type":"conversation.created","conversation":{"id":"conv"}}),
            )
            .await;
            assert_eq!(command(&mut ws).await["type"], "session.update");
            event(
                &mut ws,
                json!({"type":"session.updated","session":{"id":"session"}}),
            )
            .await;
            ws
        };
        let (worker, ws) = tokio::join!(Pipeline::new(stages).start(pipeline), handshake);
        let mut result = Self {
            worker: worker.unwrap(),
            ws,
            pair,
            provider,
            before,
            after,
            at_relay,
            entered,
            release,
        };
        result
            .receive(|d| matches!(d, FrameData::LlmServiceMetadata(_)))
            .await;
        result.barrier().await;
        assert!(result.pair.user().realtime_service_enabled().unwrap());
        assert!(result.pair.assistant().realtime_service_enabled().unwrap());
        result
    }
    async fn send(&self, data: FrameData) {
        self.worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    async fn receive(&mut self, test: impl Fn(&FrameData) -> bool) {
        timeout(WAIT, async {
            loop {
                let frame = self.worker.recv().await.unwrap();
                if let FrameData::Error(e) = &frame.frame.data {
                    panic!("{e:?}");
                }
                if test(&frame.frame.data) {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
    async fn barrier(&mut self) {
        self.send(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("independent-barrier".into()),
        }))
        .await;
        self.receive(|d| matches!(d, FrameData::Flush(_))).await;
    }
    async fn seen(&mut self, test: impl Fn(&FrameData) -> bool) {
        timeout(WAIT, async {
            loop {
                if self
                    .after
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| test(&f.frame.data))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        self.barrier().await;
    }
    async fn result_history(&self) {
        timeout(WAIT, async {
            loop {
                let history = self.pair.context().snapshot_rich().unwrap();
                if history.messages.iter().any(|message| {
                    let MessageContent::Text(text) = &message.content else {
                        return false;
                    };
                    (message.role == ChatRole::Tool
                        && message.tool_call_id.as_deref() == Some("call")
                        && text == "{\"answer\":42}")
                        || serde_json::from_str::<Value>(text).is_ok_and(|value| {
                            value["type"] == "async_tool"
                                && value["status"] == "finished"
                                && value["tool_call_id"] == "call"
                                && value["result"] == "{\"answer\":42}"
                        })
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("genuine terminal result was not reconciled into history");
    }
    async fn tool(&mut self, id: &str) {
        response(&mut self.ws, id, false).await;
        event(&mut self.ws, json!({"type":"conversation.item.added", "item":{"type":"function_call","id":"tool-item","call_id":"call","name":"wait","arguments":""}})).await;
        event(&mut self.ws, json!({"type":"response.function_call_arguments.done","call_id":"call","name":"wait","arguments":"{\"answer\":42}"})).await;
        timeout(WAIT, self.entered.notified()).await.unwrap();
        self.seen(|d| matches!(d, FrameData::FunctionCallInProgress(_)))
            .await;
    }
    async fn finish(mut self) {
        self.worker.end().await.unwrap();
        let close = async {
            loop {
                match timeout(WAIT, self.ws.next()).await.unwrap() {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    frame => panic!("unexpected shutdown {frame:?}"),
                }
            }
            let _ = self.ws.close(None).await;
        };
        let (report, ()) = tokio::join!(self.worker.collect(), close);
        let (_, report) = report.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
}

#[tokio::test]
async fn independent_deferred_original_intent_expires_without_provider_generation_change() {
    let mut f = Fixture::start(Duration::from_secs(30), false).await;
    f.send(FrameData::InputText(TextFrame::new("first"))).await;
    assert_eq!(command(&mut f.ws).await["type"], "session.update");
    assert_eq!(
        command(&mut f.ws).await["item"]["content"][0]["text"],
        "first"
    );
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "first-response", false).await;
    f.seen(|d| matches!(d, FrameData::LlmResponseStart)).await;
    f.send(FrameData::InputText(TextFrame::new("durable deferred")))
        .await;
    assert_eq!(
        command(&mut f.ws).await["item"]["content"][0]["text"],
        "durable deferred"
    );
    f.barrier().await;
    silence(&mut f.ws).await;
    let original = f
        .before
        .frames
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find_map(|f| {
            f.frame
                .native_realtime_context_cause()
                .unwrap()
                .map(|c| c.inference_fence().unwrap().clone())
        })
        .unwrap();
    assert!(original.is_current());
    let generation = f.provider.sender().unwrap().generation();
    // Deliberately intercept the pipeline cancellation: only its original runtime fence can retire this queued inference.
    f.before.swallow_interruptions.store(true, Ordering::SeqCst);
    f.send(FrameData::Interruption).await;
    f.barrier().await;
    assert!(!original.is_current());
    assert_eq!(f.provider.sender().unwrap().generation(), generation);
    event(&mut f.ws, json!({"type":"response.text.delta","response_id":"first-response","item_id":"stale","output_index":0,"content_index":0,"delta":"stale output"})).await;
    response(&mut f.ws, "first-response", true).await;
    silence(&mut f.ws).await;
    f.barrier().await;
    assert!(!format!("{:?}", f.after.frames.lock().unwrap()).contains("stale output"));
    f.send(FrameData::LlmRun).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "fresh-response", false).await;
    response(&mut f.ws, "fresh-response", true).await;
    f.seen(|d| matches!(d, FrameData::LlmResponseEnd)).await;
    silence(&mut f.ws).await;
    f.finish().await;
}

#[tokio::test]
async fn independent_auto_pair_late_native_success_during_speech_resumes_once() {
    let mut f = Fixture::start(Duration::from_secs(30), true).await;
    f.tool("tool-response").await;
    event(&mut f.ws, json!({"type":"input_audio_buffer.speech_started","item_id":"user-audio","audio_start_ms":0})).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.cancel");
    response(&mut f.ws, "tool-response", true).await;
    f.seen(|d| matches!(d, FrameData::UserStartedSpeaking))
        .await;
    f.release.add_permits(1);
    let result = command(&mut f.ws).await;
    assert_eq!(result["item"]["call_id"], "call");
    assert_eq!(result["item"]["output"], "{\"answer\":42}");
    silence(&mut f.ws).await;
    f.seen(|d| matches!(d, FrameData::FunctionCallResult(_)))
        .await;
    assert!(format!("{:?}", f.pair.context().snapshot().unwrap()).contains("42"));
    event(&mut f.ws, json!({"type":"input_audio_buffer.speech_stopped","item_id":"user-audio","audio_end_ms":50})).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "after-success", false).await;
    response(&mut f.ws, "after-success", true).await;
    f.seen(|d| matches!(d, FrameData::LlmResponseEnd)).await;
    silence(&mut f.ws).await;
    f.finish().await;
}

#[tokio::test]
async fn independent_auto_pair_native_timeout_continues_without_reimporting_history() {
    let mut f = Fixture::start(Duration::from_millis(180), false).await;
    f.tool("timeout-response").await;
    response(&mut f.ws, "timeout-response", true).await;
    let result = command(&mut f.ws).await;
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["output"], "CANCELLED");
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "after-timeout", false).await;
    response(&mut f.ws, "after-timeout", true).await;
    f.seen(|d| matches!(d, FrameData::FunctionCallCancel(_)))
        .await;
    let history = format!("{:?}", f.pair.context().snapshot().unwrap());
    assert!(history.contains("CANCELLED"), "{history}");
    silence(&mut f.ws).await;
    f.finish().await;
}

#[tokio::test]
async fn independent_assistant_continuation_keeps_original_fence_while_response_pending() {
    let mut f = Fixture::start(Duration::from_secs(30), false).await;
    f.after.hold_upstream_context.store(true, Ordering::SeqCst);
    f.tool("first-tool").await;
    response(&mut f.ws, "first-tool", true).await;
    f.release.add_permits(1);
    let output = command(&mut f.ws).await;
    assert_eq!(output["item"]["call_id"], "call");
    timeout(WAIT, async {
        loop {
            if f.after.held_upstream.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    silence(&mut f.ws).await;
    // Start another response, then add durable no-run history so the actor has material
    // owed to the held continuation after response.created clears prior results.
    f.send(FrameData::LlmRun).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "pending-response", false).await;
    timeout(WAIT, async {
        loop {
            if f.after.frames.lock().unwrap().iter().any(|frame| {
                matches!(frame.frame.data, FrameData::LlmResponseStart)
                    && frame.frame.metadata.get("response_id") == Some(&json!("pending-response"))
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    f.send(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "new no-run history")],
        run_llm: false,
    }))
    .await;
    assert_eq!(
        command(&mut f.ws).await["item"]["content"][0]["text"],
        "new no-run history"
    );
    let (continuation, origin) = f.after.held_upstream.lock().unwrap().take().unwrap();
    assert!(!continuation.is_realtime_context_update());
    let _ = continuation.native_realtime_context_cause().unwrap();
    let continuation_id = continuation.id;
    let original = origin.invocation_fence().unwrap();
    assert!(original.is_current());
    origin
        .push(continuation, Direction::Upstream)
        .await
        .unwrap();
    timeout(WAIT, async {
        loop {
            if f.before.frames.lock().unwrap().iter().any(|frame| {
                frame.direction == Direction::Upstream && frame.frame.id == continuation_id
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    // FIFO socket acknowledgement proves the actor processed the request before cancellation.
    f.provider
        .sender()
        .unwrap()
        .send(Action::UpdateSession(SessionProperties::new().into()))
        .await
        .unwrap();
    assert_eq!(command(&mut f.ws).await["type"], "session.update");
    f.barrier().await;
    silence(&mut f.ws).await;
    let generation = f.provider.sender().unwrap().generation();
    f.after.swallow_interruptions.store(true, Ordering::SeqCst);
    f.worker
        .sender()
        .send(Frame::interruption(), Direction::Upstream)
        .await
        .unwrap();
    f.barrier().await;
    assert!(!original.is_current());
    assert_eq!(f.provider.sender().unwrap().generation(), generation);
    response(&mut f.ws, "pending-response", true).await;
    let retired_output = timeout(Duration::from_millis(80), f.ws.next()).await;
    assert!(
        retired_output.is_err(),
        "retired genuine assistant continuation emitted after response.done: {retired_output:?}"
    );
    f.send(FrameData::LlmRun).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "fresh-after-continuation", false).await;
    response(&mut f.ws, "fresh-after-continuation", true).await;
    f.barrier().await;
    silence(&mut f.ws).await;
    f.finish().await;
}

#[tokio::test]
async fn independent_late_native_completion_after_interruption_starts_fresh_inference() {
    for timed_out in [false, true] {
        let mut f = Fixture::start(
            if timed_out {
                Duration::from_millis(250)
            } else {
                Duration::from_secs(30)
            },
            false,
        )
        .await;
        f.tool("interrupted-tool").await;
        f.send(FrameData::Interruption).await;
        assert_eq!(command(&mut f.ws).await["type"], "response.cancel");
        response(&mut f.ws, "interrupted-tool", true).await;
        f.seen(|data| matches!(data, FrameData::Interruption)).await;
        if !timed_out {
            f.release.add_permits(1);
        }
        let output = command(&mut f.ws).await;
        assert_eq!(output["item"]["type"], "function_call_output");
        assert_eq!(output["item"]["call_id"], "call");
        assert_eq!(
            output["item"]["output"],
            if timed_out {
                "CANCELLED"
            } else {
                "{\"answer\":42}"
            }
        );
        // No new user frame or speech-stop supplies a fresh scope after the interruption.
        // The validated late completion itself must schedule one legitimate new request.
        assert_eq!(command(&mut f.ws).await["type"], "response.create");
        response(&mut f.ws, "late-completion", false).await;
        response(&mut f.ws, "late-completion", true).await;
        f.seen(|data| matches!(data, FrameData::LlmResponseEnd))
            .await;
        let history = format!("{:?}", f.pair.context().snapshot().unwrap());
        assert!(
            history.contains(if timed_out { "CANCELLED" } else { "42" }),
            "{history}"
        );
        silence(&mut f.ws).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn independent_delayed_native_delivery_keeps_result_but_schedules_new_inference() {
    let mut f = Fixture::start(Duration::from_secs(30), false).await;
    f.at_relay.hold_native_outcome.store(true, Ordering::SeqCst);
    f.tool("held-native").await;
    f.release.add_permits(1);
    timeout(WAIT, async {
        loop {
            if f.at_relay.held_native.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    silence(&mut f.ws).await;
    f.send(FrameData::Interruption).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.cancel");
    response(&mut f.ws, "held-native", true).await;
    f.seen(|data| matches!(data, FrameData::Interruption)).await;
    let (terminal, delivery_origin) = f.at_relay.held_native.lock().unwrap().take().unwrap();
    assert_eq!(
        terminal.native_tool_outcome().unwrap().unwrap().state(),
        ToolCallTerminalState::Completed
    );
    assert!(!delivery_origin.invocation_fence().unwrap().is_current());
    let replay = terminal.clone();
    delivery_origin
        .push(terminal, Direction::Downstream)
        .await
        .unwrap();
    let output = command(&mut f.ws).await;
    assert_eq!(output["item"]["call_id"], "call");
    assert_eq!(output["item"]["output"], "{\"answer\":42}");
    // Observe only: injecting Flush here could supply a new invocation scope.
    f.result_history().await;
    let resumed = timeout(WAIT, f.ws.next()).await;
    assert!(
        resumed.is_ok(),
        "validated late delivery restored history but failed to schedule new assistant inference"
    );
    let resumed = resumed.unwrap().unwrap().unwrap();
    let resumed: Value = serde_json::from_str(resumed.to_text().unwrap()).unwrap();
    assert_eq!(resumed["type"], "response.create");
    response(&mut f.ws, "after-held-native", false).await;
    response(&mut f.ws, "after-held-native", true).await;
    f.seen(|data| matches!(data, FrameData::LlmResponseEnd))
        .await;
    silence(&mut f.ws).await;
    delivery_origin
        .push(replay, Direction::Downstream)
        .await
        .unwrap();
    f.barrier().await;
    silence(&mut f.ws).await;
    f.finish().await;
}

#[tokio::test]
async fn independent_delayed_cancel_enabled_success_cannot_renew_stale_inference() {
    let mut f = Fixture::start_policy(Duration::from_secs(30), false, true).await;
    f.at_relay.hold_native_outcome.store(true, Ordering::SeqCst);
    f.tool("held-cancellable").await;
    f.release.add_permits(1);
    timeout(WAIT, async {
        loop {
            if f.at_relay.held_native.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    silence(&mut f.ws).await;
    f.send(FrameData::Interruption).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.cancel");
    response(&mut f.ws, "held-cancellable", true).await;
    f.seen(|data| matches!(data, FrameData::Interruption)).await;
    let (terminal, origin) = f.at_relay.held_native.lock().unwrap().take().unwrap();
    let outcome = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
    assert!(outcome.cancel_on_interruption());
    assert!(!origin.invocation_fence().unwrap().is_current());
    let replay = terminal.clone();
    origin.push(terminal, Direction::Downstream).await.unwrap();
    let output = command(&mut f.ws).await;
    assert_eq!(output["item"]["call_id"], "call");
    assert_eq!(output["item"]["output"], "{\"answer\":42}");
    f.result_history().await;
    let stale = timeout(Duration::from_millis(80), f.ws.next()).await;
    assert!(
        stale.is_err(),
        "cancel-enabled delayed terminal renewed stale inference: {stale:?}"
    );
    origin.push(replay, Direction::Downstream).await.unwrap();
    f.barrier().await;
    silence(&mut f.ws).await;
    f.send(FrameData::LlmRun).await;
    assert_eq!(command(&mut f.ws).await["type"], "response.create");
    response(&mut f.ws, "new-explicit-run", false).await;
    response(&mut f.ws, "new-explicit-run", true).await;
    f.seen(|data| matches!(data, FrameData::LlmResponseEnd))
        .await;
    silence(&mut f.ws).await;
    f.finish().await;
}
