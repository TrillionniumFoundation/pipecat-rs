use super::*;
use async_trait::async_trait;
use pipecat_core::*;
use pipecat_processors::{DtmfAggregator, DtmfAggregatorConfig};
use pipecat_protocol::{
    adapters::{FrameSerializer, TelephonyFrameSerializer},
    telephony::{EventKind, TelephonyCodec, TelephonySession},
    WireMessage,
};
use pipecat_services::{CompletionRequest, LlmClient, LlmProcessor, Message, Role};
use pipecat_transports::{Packet, SessionConfig, SessionSender, WebSocketSession};
use pipecat_turns::{AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair};
use serde_json::Value;
use tokio_tungstenite::{tungstenite::protocol::Role as WsRole, WebSocketStream};

type Serializer = Arc<Mutex<TelephonyFrameSerializer>>;
struct PhoneOutput {
    serializer: Serializer,
    sender: SessionSender,
    timeout: Duration,
    // Serializes state mutation together with publication, including clear/tail.
    output_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pause_media: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}
impl PhoneOutput {
    async fn write(&self, message: Option<WireMessage>) -> Result<(), ProcessorError> {
        if let Some(WireMessage::Text(text)) = message {
            #[cfg(test)]
            if serde_json::from_str::<Value>(&text).is_ok_and(|v| v["event"] == "media") {
                if let Some((entered, resume)) = &self.pause_media {
                    entered.notify_one();
                    resume.notified().await;
                }
            }
            tokio::time::timeout(self.timeout, self.sender.send(Packet::Text(text)))
                .await
                .map_err(|_| ProcessorError::new("phone output timeout"))?
                .map_err(|_| ProcessorError::new("phone output closed"))?;
        }
        Ok(())
    }
    async fn flush_locked(&self) -> Result<(), ProcessorError> {
        let tail = self
            .serializer
            .lock()
            .unwrap()
            .flush_output()
            .map_err(|_| ProcessorError::new("phone output resampler"))?;
        self.write(tail).await
    }
}
#[async_trait]
impl FrameProcessor for PhoneOutput {
    fn name(&self) -> &str {
        "TwilioPhoneOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            let _output = self.output_gate.lock().await;
            // A normal invocation that waited behind clear must not publish
            // its old-generation PCM after clear. Once admitted under this
            // gate, publication necessarily precedes the next clear.
            if !ctx.is_current() {
                return Ok(());
            }
            // Only these native frame kinds may produce phone wire messages.
            // In particular no inbound audio, generated text, RTVI or arbitrary
            // OutputTransportMessage is serialized onto this connection.
            if matches!(
                &frame.data,
                FrameData::TtsAudio(_) | FrameData::OutputAudio(_) | FrameData::Interruption
            ) {
                let message = self
                    .serializer
                    .lock()
                    .unwrap()
                    .serialize(&frame)
                    .map_err(|_| ProcessorError::new("phone serialization"))?;
                self.write(message).await?;
            } else if matches!(&frame.data, FrameData::TtsStopped(_)) {
                self.flush_locked().await?;
            } else if frame.data.is_cancel() {
                self.serializer.lock().unwrap().reset_audio();
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        let _output = self.output_gate.lock().await;
        self.flush_locked().await
    }
}
async fn packet(session: &mut WebSocketSession) -> Result<WireMessage, TwilioError> {
    match session.recv().await {
        Some(Packet::Text(text)) => Ok(WireMessage::Text(text)),
        Some(Packet::Binary(_)) => Err(TwilioError::Protocol("phone requires JSON text")),
        None => Err(TwilioError::Transport),
    }
}
fn json(wire: &WireMessage) -> Result<Value, TwilioError> {
    let WireMessage::Text(text) = wire else {
        return Err(TwilioError::Protocol("phone requires JSON text"));
    };
    serde_json::from_str(text).map_err(|_| TwilioError::Protocol("malformed JSON"))
}
fn protocol<T>(result: Result<T, pipecat_protocol::ProtocolError>) -> Result<T, TwilioError> {
    result.map_err(|_| TwilioError::Protocol("invalid telephony event"))
}
async fn start(
    session: &mut WebSocketSession,
    shared: &Shared,
    call: &str,
    control: &mut watch::Receiver<Control>,
) -> Result<(TelephonySession, Serializer), TwilioError> {
    let deadline = Instant::now() + shared.config.connect_timeout;
    let handshake = async {
        let connected = packet(session).await?;
        let value = json(&connected)?;
        if value["event"] != "connected"
            || value["protocol"] != "Call"
            || value["version"] != "1.0.0"
            || value.get("streamSid").is_some()
            || value.get("sequenceNumber").is_some()
        {
            return Err(TwilioError::Protocol("connected must precede start"));
        }
        let started = packet(session).await?;
        let value = json(&started)?;
        let id = value["start"]["streamSid"]
            .as_str()
            .filter(|v| security::valid_sid(v, "MZ"))
            .ok_or(TwilioError::Protocol("invalid stream identity"))?;
        if value["event"] != "start"
            || value["streamSid"] != id
            || value["start"]["accountSid"] != shared.config.account_sid
            || value["start"]["callSid"] != call
            || value["start"]["mediaFormat"]["encoding"] != "audio/x-mulaw"
            || value["start"]["mediaFormat"]["sampleRate"] != 8000
            || value["start"]["mediaFormat"]["channels"] != 1
            || value["start"]["tracks"] != serde_json::json!(["inbound"])
        {
            return Err(TwilioError::Protocol(
                "start identity or media format rejected",
            ));
        }
        let codec = protocol(TelephonyCodec::twilio(id))?
            .with_max_message_bytes(shared.config.max_message_bytes);
        let mut state = TelephonySession::new(codec.clone());
        protocol(state.accept(&connected))?;
        let event = protocol(state.accept(&started))?;
        if event.sequence != Some(1) {
            return Err(TwilioError::Protocol("start sequence must be one"));
        }
        let serializer = Arc::new(Mutex::new(protocol(TelephonyFrameSerializer::new(
            codec,
            shared.providers.input_rate,
        ))?));
        Ok((state, serializer))
    };
    if *control.borrow() != Control::Running {
        return Err(TwilioError::Protocol("canceled before start"));
    }
    tokio::select! {biased;_=control.changed()=>Err(TwilioError::Protocol("canceled before start")),result=tokio::time::timeout_at(deadline,handshake)=>result.map_err(|_|TwilioError::Deadline)?}
}
pub(super) async fn run(mut upgrade: Upgrade, shared: Arc<Shared>) -> CallSummary {
    let mut summary = CallSummary::new(upgrade.call);
    let upgraded =
        match tokio::time::timeout(shared.config.connect_timeout, upgrade.on_upgrade).await {
            Ok(Ok(stream)) => stream,
            _ => {
                summary.error = Some(TwilioError::Transport);
                return summary;
            }
        };
    let transport_config = SessionConfig {
        queue_capacity: shared.config.queue_capacity,
        max_message_bytes: shared.config.max_message_bytes,
        connect_timeout: shared.config.connect_timeout,
        write_timeout: shared.config.io_timeout,
        close_timeout: shared.config.io_timeout,
    };
    let websocket_config = match transport_config.websocket_config() {
        Ok(config) => config,
        Err(_) => {
            summary.error = Some(TwilioError::Transport);
            return summary;
        }
    };
    let websocket = WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded),
        WsRole::Server,
        Some(websocket_config),
    )
    .await;
    let mut session = match WebSocketSession::from_websocket(websocket, transport_config) {
        Ok(session) => session,
        Err(_) => {
            summary.error = Some(TwilioError::Transport);
            return summary;
        }
    };
    let mut remote_stop = false;
    let mut graceful = false;
    match start(
        &mut session,
        &shared,
        &summary.call_sid,
        &mut upgrade.control,
    )
    .await
    {
        Err(error) => summary.error = Some(error),
        Ok((state, serializer)) => {
            summary.verified_start = true;
            let result = media(
                &mut session,
                &shared,
                &summary.call_sid,
                &mut upgrade.control,
                state,
                serializer,
            )
            .await;
            summary.generated_context = result.context;
            remote_stop = result.remote_stop;
            graceful = result.graceful;
            summary.error = result.error;
            summary.pipeline_join_error = result.pipeline_join_error;
        }
    }
    // This is the single transport cleanup and hangup owner for every exit path.
    // No retries, and no hangup until bound identity and complete start pass.
    let cleanup = if graceful {
        session.finish_and_join().await
    } else {
        session.cancel_and_join().await
    };
    if cleanup.is_err() {
        summary.media_cleanup_error = Some(TwilioError::Transport);
    }
    if summary.verified_start
        && !remote_stop
        && shared.config.hangup_policy == HangupPolicy::OnLocalTermination
    {
        summary.hangup_error = hangup(&shared, &summary.call_sid).await.err();
    }
    summary
}
struct MediaResult {
    context: Context,
    remote_stop: bool,
    graceful: bool,
    error: Option<TwilioError>,
    pipeline_join_error: Option<TwilioError>,
}
async fn record_pipeline_join(worker: &mut PipelineWorker, result: &mut MediaResult) {
    match worker.join().await {
        Ok(report) if report.total_errors == 0 => {}
        Ok(_) => {
            result.error.get_or_insert(TwilioError::Pipeline);
        }
        Err(_) => {
            // A join failure can be cleanup failure/timeout, even when another
            // protocol or deadline error already owns the primary diagnostic.
            result.pipeline_join_error = Some(TwilioError::Pipeline);
            result.error.get_or_insert(TwilioError::Pipeline);
        }
    }
}
async fn bounded_end(worker: &PipelineWorker, timeout: Duration) -> Result<(), TwilioError> {
    match tokio::time::timeout(timeout, worker.end()).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(TwilioError::Pipeline),
    }
}
async fn media(
    session: &mut WebSocketSession,
    shared: &Shared,
    call: &str,
    control: &mut watch::Receiver<Control>,
    mut state: TelephonySession,
    serializer: Serializer,
) -> MediaResult {
    let mut result = MediaResult {
        context: Context::default(),
        remote_stop: false,
        graceful: false,
        error: None,
        pipeline_join_error: None,
    };
    let parts = match crate::cascade::hosted_parts(&shared.providers, shared.config.turn_timeout) {
        Ok(p) => p,
        Err(_) => {
            result.error = Some(TwilioError::Pipeline);
            return result;
        }
    };
    let pair = LLMContextAggregatorPair::new(
        parts.initial_context,
        parts.user_config,
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Text,
        },
    );
    let context = pair.context();
    let mut request = CompletionRequest::new(parts.model, Context::default());
    request.system_instruction = Some(parts.system_prompt);
    let llm = match LlmClient::new(parts.llm) {
        Ok(client) => client,
        Err(_) => {
            result.error = Some(TwilioError::Pipeline);
            return result;
        }
    };
    let budget = Arc::new(crate::processors::RetentionBudget::default());
    let output = Arc::new(PhoneOutput {
        serializer: serializer.clone(),
        sender: session.sender(),
        timeout: shared.config.io_timeout,
        output_gate: tokio::sync::Mutex::new(()),
        #[cfg(test)]
        pause_media: None,
    });
    let pipeline = Pipeline::new(vec![
        parts.stt,
        Arc::new(
            DtmfAggregator::new(DtmfAggregatorConfig::default())
                .expect("valid built-in DTMF defaults"),
        ),
        budget.clone(),
        pair.user(),
        Arc::new(LlmProcessor::new(llm, request)),
        budget,
        Arc::new(crate::processors::GeneratedContext {
            assistant: pair.assistant(),
        }),
        parts.tts,
        output,
    ]);
    let mut config = PipelineConfig {
        queue_capacity: shared.config.queue_capacity,
        system_queue_capacity: shared.config.queue_capacity,
        output_capacity: shared.config.queue_capacity,
        startup_timeout: shared.config.io_timeout,
        shutdown_timeout: shared.config.io_timeout,
        ..Default::default()
    };
    config.start.audio_in_sample_rate = shared.providers.input_rate;
    config.start.audio_out_sample_rate = shared.providers.output_rate;
    let deadline = Instant::now() + shared.config.session_timeout;
    // Do not cancel/drop startup: core owns bounded startup and its cleanup.
    // The connection task retains this future through shutdown and joins it.
    let mut worker = match pipeline.start(config).await {
        Ok(w) => w,
        Err(_) => {
            result.error = Some(TwilioError::Pipeline);
            return result;
        }
    };
    let sender = worker.sender();
    let mut graceful = false;
    if shared.config.introduce
        && *control.borrow() == Control::Running
        && (context
            .append(Message::text(
                Role::Developer,
                "Please introduce yourself to the user.",
            ))
            .is_err()
            || !matches!(
                tokio::time::timeout(
                    shared.config.io_timeout,
                    sender.send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                )
                .await,
                Ok(Ok(()))
            ))
    {
        result.error = Some(TwilioError::Pipeline);
    }
    let mut last_activity = Instant::now();
    let mut timer = tokio::time::interval(Duration::from_millis(20));
    while result.error.is_none() {
        let current = *control.borrow_and_update();
        if current != Control::Running {
            graceful = current == Control::End;
            break;
        }
        if Instant::now() >= deadline || last_activity.elapsed() >= shared.config.idle_timeout {
            result.error = Some(TwilioError::Deadline);
            break;
        }
        tokio::select! {biased;
            _=control.changed()=>{},
            _=timer.tick()=>{},
            received=worker.recv()=>match received {
                Some(frame) if frame.frame.data.as_error_frame().is_some()=>{result.error=Some(TwilioError::Pipeline);},
                None=>break,
                _=>{},
            },
            wire=packet(session)=>{
                let wire=match wire {Ok(w)=>w,Err(e)=>{result.error=Some(e);break;}};
                let event=match protocol(state.accept(&wire)){Ok(e)=>e,Err(e)=>{result.error=Some(e);break;}};
                if event.sequence.is_none()||event.stream_id.as_deref()!=state.codec().stream_id(){result.error=Some(TwilioError::Protocol("missing sequence or stream identity"));break;}
                match event.kind {
                    EventKind::Media{ref track,..} if track.as_deref()==Some("inbound")=>{},
                    EventKind::Dtmf{ref track,..} if track.as_deref()==Some("inbound_track")=>{},
                    EventKind::Mark{..}=>continue, // Not proof of playback; not user activity.
                    EventKind::Stop{ref metadata} if metadata.get("accountSid").and_then(Value::as_str)==Some(&shared.config.account_sid)&&metadata.get("callSid").and_then(Value::as_str)==Some(call)=>{result.remote_stop=true;graceful=true;break;},
                    _=>{result.error=Some(TwilioError::Protocol("unsupported or mismatched event"));break;}
                }
                last_activity=Instant::now();
                let frame=match protocol(serializer.lock().unwrap().try_deserialize(&wire)){Ok(f)=>f,Err(e)=>{result.error=Some(e);break;}};
                if let Some(frame)=frame {
                    if !matches!(tokio::time::timeout(shared.config.io_timeout,sender.send(frame,Direction::Downstream)).await,Ok(Ok(()))){result.error=Some(TwilioError::Pipeline);break;}
                }
            }
        }
    }
    if !worker.has_finished() {
        if graceful && result.error.is_none() {
            let tail = protocol(serializer.lock().unwrap().flush_input());
            match tail {
                Ok(Some(frame)) => {
                    if !matches!(
                        tokio::time::timeout(
                            shared.config.io_timeout,
                            sender.send(frame, Direction::Downstream)
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        result.error = Some(TwilioError::Pipeline);
                    }
                }
                Ok(None) => {}
                Err(e) => result.error = Some(e),
            }
            if result.error.is_none()
                && bounded_end(&worker, shared.config.io_timeout)
                    .await
                    .is_err()
            {
                result.error = Some(TwilioError::Pipeline);
            }
        }
        if !graceful || result.error.is_some() {
            serializer.lock().unwrap().reset_audio();
            let _ = worker.cancel("phone session closing").await;
        }
    }
    // Drain without collecting an unbounded Vec. A concurrent shutdown/Cancel
    // may escalate graceful End, but End can never downgrade Cancel.
    let mut cancel_sent = !graceful || result.error.is_some();
    loop {
        if !cancel_sent && *control.borrow_and_update() == Control::Cancel {
            serializer.lock().unwrap().reset_audio();
            let _ = worker.cancel("phone graceful close escalated").await;
            graceful = false;
            cancel_sent = true;
        }
        tokio::select! {biased;
            _=control.changed(),if !cancel_sent=>{},
            frame=worker.recv()=>if frame.is_none(){break;},
        }
    }
    record_pipeline_join(&mut worker, &mut result).await;
    match context.snapshot() {
        Ok(snapshot) => result.context = snapshot,
        Err(_) => {
            result.error.get_or_insert(TwilioError::Pipeline);
        }
    }
    result.graceful = graceful && result.error.is_none();
    result
}

#[cfg(test)]
mod output_tests {
    use super::*;
    struct FailingCleanup;
    #[async_trait]
    impl FrameProcessor for FailingCleanup {
        fn name(&self) -> &str {
            "FailingCleanupForDiagnosticTest"
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
            Err(ProcessorError::new("synthetic cleanup failure"))
        }
    }
    #[tokio::test]
    async fn primary_protocol_error_does_not_hide_failed_pipeline_cleanup_join() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut worker = Pipeline::new(vec![Arc::new(FailingCleanup)])
                .start(Default::default())
                .await
                .unwrap();
            worker.recv().await.unwrap();
            let primary = TwilioError::Protocol("synthetic primary protocol failure");
            let mut result = MediaResult {
                context: Context::default(),
                remote_stop: false,
                graceful: false,
                error: Some(primary.clone()),
                pipeline_join_error: None,
            };
            worker.cancel("primary protocol failure").await.unwrap();
            while worker.recv().await.is_some() {}
            record_pipeline_join(&mut worker, &mut result).await;
            assert_eq!(result.error, Some(primary));
            assert_eq!(result.pipeline_join_error, Some(TwilioError::Pipeline));
            let mut summary = CallSummary::new(format!("CA{}", "1".repeat(32)));
            summary.error = result.error;
            summary.pipeline_join_error = result.pipeline_join_error;
            let mut report = ShutdownReport::default();
            report.account_summary(&summary);
            assert_eq!(report.failed_calls, 1);
            assert_eq!(report.pipeline_join_errors, 1);
            assert_eq!(report.media_cleanup_errors, 0);
            assert!(report.failed_cleanup());
        })
        .await
        .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interrupted_paused_media_cannot_follow_clear_or_leave_a_tail() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for interruptible in [true, false] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("ws://{}", listener.local_addr().unwrap());
                let accept = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    WebSocketSession::accept(stream, SessionConfig::default())
                        .await
                        .unwrap()
                });
                let mut peer = WebSocketSession::connect(&endpoint, SessionConfig::default())
                    .await
                    .unwrap();
                let mut transport = accept.await.unwrap();
                let entered = Arc::new(tokio::sync::Notify::new());
                let resume = Arc::new(tokio::sync::Notify::new());
                let serializer = Arc::new(Mutex::new(
                    TelephonyFrameSerializer::new(
                        TelephonyCodec::twilio(format!("MZ{}", "1".repeat(32))).unwrap(),
                        16000,
                    )
                    .unwrap(),
                ));
                let output = Arc::new(PhoneOutput {
                    serializer: serializer.clone(),
                    sender: transport.sender(),
                    timeout: Duration::from_secs(1),
                    output_gate: tokio::sync::Mutex::new(()),
                    pause_media: Some((entered.clone(), resume.clone())),
                });
                let mut worker = Pipeline::new(vec![output])
                    .start(Default::default())
                    .await
                    .unwrap();
                worker.recv().await.unwrap();
                let mut audio = Frame::new(FrameData::TtsAudio(
                    AudioRawFrame::new(vec![1; 2400], 24000, 1).unwrap(),
                ));
                audio.interruptible = interruptible;
                worker
                    .sender()
                    .send(audio, Direction::Downstream)
                    .await
                    .unwrap();
                entered.notified().await;
                worker
                    .sender()
                    .send(Frame::new(FrameData::Interruption), Direction::Downstream)
                    .await
                    .unwrap();
                if !interruptible {
                    // Cancellation cannot rescue this call: the ordering gate must
                    // keep clear behind already-admitted media until its send ends.
                    assert!(tokio::time::timeout(Duration::from_millis(30), peer.recv())
                        .await
                        .is_err());
                    resume.notify_one();
                    let Packet::Text(text) = peer.recv().await.unwrap() else {
                        panic!("phone JSON only")
                    };
                    assert_eq!(
                        serde_json::from_str::<Value>(&text).unwrap()["event"],
                        "media"
                    );
                }
                let Packet::Text(text) = peer.recv().await.unwrap() else {
                    panic!("phone JSON only")
                };
                assert_eq!(
                    serde_json::from_str::<Value>(&text).unwrap()["event"],
                    "clear"
                );
                // The old normal call is canceled while blocked before publication.
                resume.notify_one();
                assert!(tokio::time::timeout(Duration::from_millis(30), peer.recv())
                    .await
                    .is_err());
                assert!(serializer.lock().unwrap().flush_output().unwrap().is_none());
                worker.cancel("done").await.unwrap();
                while worker.recv().await.is_some() {}
                worker.join().await.unwrap();
                transport.cancel_and_join().await.unwrap();
                let _ = peer.cancel_and_join().await;
            }
        })
        .await
        .unwrap();
    }
    struct HoldNormal {
        entered: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl FrameProcessor for HoldNormal {
        fn name(&self) -> &str {
            "HeldNormalForEndAdmissionTest"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            ctx.push(frame, direction).await
        }
    }
    #[tokio::test]
    async fn saturated_normal_queue_bounds_end_admission_and_cancel_joins() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let entered = Arc::new(tokio::sync::Notify::new());
            let mut worker = Pipeline::new(vec![Arc::new(HoldNormal {
                entered: entered.clone(),
            })])
            .start(PipelineConfig {
                queue_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
            worker.recv().await.unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::Text(TextFrame::new("held"))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            entered.notified().await;
            worker
                .sender()
                .send(
                    Frame::new(FrameData::Text(TextFrame::new("queued"))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            assert_eq!(
                bounded_end(&worker, Duration::from_millis(20)).await,
                Err(TwilioError::Pipeline)
            );
            worker.cancel("end admission exceeded bound").await.unwrap();
            while worker.recv().await.is_some() {}
            assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
        })
        .await
        .unwrap();
    }
}
