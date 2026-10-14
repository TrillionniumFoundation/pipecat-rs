//! Real EvalSession recording -> EvalBotHost -> native PCM fixture -> text events.
//! The fixture recognizes exact PCM bytes, not human speech.
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*, Cancellation, EvalSession, RecordedAudio, ScenarioFile, SessionOptions,
};
use pipecat_protocol::rtvi::RawAudioData;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::task::JoinHandle;
type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(5);
#[derive(Default)]
struct Trace {
    audio: Mutex<Vec<Vec<u8>>>,
    audio_received: tokio::sync::Notify,
    text: AtomicUsize,
    text_received: tokio::sync::Notify,
    configured: AtomicUsize,
    cleaned: AtomicUsize,
}
struct Recognizer(Arc<Trace>);
#[async_trait]
impl FrameProcessor for Recognizer {
    fn name(&self) -> &str {
        "pcm-recognizer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::LlmConfigureOutput { skip_tts } => {
                if *skip_tts {
                    self.0.configured.fetch_add(1, Ordering::SeqCst);
                }
            }
            FrameData::InputAudio(audio) => {
                self.0.audio.lock().unwrap().push(audio.audio.clone());
                self.0.audio_received.notify_one();
                if audio.audio == [7, 0].repeat(640) {
                    for data in [
                        FrameData::Transcription(TranscriptionFrame {
                            text: "recorded tone".into(),
                            finalized: true,
                            ..Default::default()
                        }),
                        FrameData::LlmResponseStart,
                        FrameData::LlmText(TextFrame::new("accepted PCM")),
                        FrameData::LlmResponseEnd,
                    ] {
                        ctx.push(Frame::new(data), direction).await?;
                    }
                }
            }
            FrameData::Text(_) | FrameData::LlmMessagesAppend(_) => {
                self.0.text.fetch_add(1, Ordering::SeqCst);
                self.0.text_received.notify_one();
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Factory(Arc<Trace>);
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid("skip_tts was false".into()));
        }
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Recognizer(self.0.clone()))],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
fn config() -> EvalBotHostConfig {
    EvalBotHostConfig {
        startup_timeout: Duration::from_secs(2),
        session_timeout: WAIT,
        host_timeout: Duration::from_secs(10),
        shutdown_timeout: Duration::from_secs(2),
        transport: SessionConfig {
            write_timeout: Duration::from_millis(300),
            close_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    }
}
struct Host {
    url: String,
    cancel: Cancellation,
    task: JoinHandle<Result<EvalBotHostReport, BotHostError>>,
    trace: Arc<Trace>,
}
async fn host(enabled: Option<bool>, cfg: EvalBotHostConfig) -> Host {
    let mut host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), cfg)
        .await
        .unwrap();
    if let Some(enabled) = enabled {
        host = host.with_raw_audio_input(enabled);
    }
    let url = format!("ws://{}/", host.local_addr().unwrap());
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let trace = Arc::new(Trace::default());
    let t = trace.clone();
    let task = tokio::spawn(async move { host.serve(&Factory(t), c).await });
    Host {
        url,
        cancel,
        task,
        trace,
    }
}
async fn settle(mut host: Host) -> (Result<EvalBotHostReport, BotHostError>, Arc<Trace>) {
    host.cancel.cancel();
    let joined = tokio::time::timeout(WAIT, &mut host.task).await;
    let result = match joined {
        Ok(r) => r,
        Err(_) => {
            let actual = host.task.await;
            panic!("host exceeded cleanup deadline after actual join: {actual:?}")
        }
    };
    (result.expect("host task panic"), host.trace)
}
async fn send(socket: &WebSocketSession, kind: &str, data: Value) -> Result<(), Error> {
    socket
        .sender()
        .send(Packet::Text(
            json!({"label":"rtvi-ai","id":"request","type":kind,"data":data}).to_string(),
        ))
        .await?;
    Ok(())
}
async fn ready(socket: &mut WebSocketSession) -> Result<Value, Error> {
    send(socket, "client-ready", json!({"version":"2.2.0"})).await?;
    tokio::time::timeout(WAIT, async {
        loop {
            let Some(Packet::Text(t)) = socket.recv().await else {
                return Err("closed before ready".into());
            };
            let v: Value = serde_json::from_str(&t)?;
            if v["type"] == "bot-ready" {
                return Ok(v["data"]["capabilities"].clone());
            }
        }
    })
    .await?
}
fn raw(bytes: &[u8]) -> Value {
    serde_json::to_value(RawAudioData::from_pcm16(bytes, 16000, 1).unwrap()).unwrap()
}
async fn raw_case(
    enabled: Option<bool>,
    cfg: EvalBotHostConfig,
    ready_first: bool,
    messages: Vec<(&str, Value)>,
) -> (
    Result<(), Error>,
    Result<EvalBotHostReport, BotHostError>,
    Arc<Trace>,
    Value,
) {
    let mut h = host(enabled, cfg).await;
    let mut caps = Value::Null;
    let connected = WebSocketSession::connect(&h.url, config().transport).await;
    let result = match connected {
        Err(e) => Err(e.into()),
        Ok(mut socket) => {
            let work = tokio::time::timeout(WAIT, async {
                if ready_first {
                    caps = ready(&mut socket).await?;
                }
                for (kind, data) in messages {
                    if kind == "fixture-wait-audio" {
                        h.trace.audio_received.notified().await;
                    } else if kind == "fixture-wait-text" {
                        h.trace.text_received.notified().await;
                    } else {
                        send(&socket, kind, data).await?;
                    }
                }
                while socket.recv().await.is_some() {}
                Ok::<(), Error>(())
            })
            .await;
            let finish = socket.finish();
            tokio::pin!(finish);
            let finished = match tokio::time::timeout(WAIT, &mut finish).await {
                Ok(r) => r.map_err(|e| -> Error { e.into() }),
                Err(_) => {
                    let actual = finish.await;
                    Err(format!("socket finish deadline; actual={actual:?}").into())
                }
            };
            match work {
                Ok(Ok(())) => finished,
                Ok(Err(e)) => Err(format!("client: {e}; finish={finished:?}").into()),
                Err(_) => Err(format!("client deadline; finish={finished:?}").into()),
            }
        }
    };
    // All assertions follow actual host ownership cleanup.
    let terminal = tokio::time::timeout(WAIT, &mut h.task).await;
    let report = match terminal {
        Ok(r) => r.unwrap(),
        Err(_) => {
            h.cancel.cancel();
            let r = h.task.await.unwrap();
            panic!("host deadline: {r:?}")
        }
    };
    (result, report, h.trace, caps)
}
#[tokio::test]
async fn recording_reaches_builtin_host_without_reference_text_and_reuses_fresh_pipeline() {
    let h = host(Some(true), config()).await;
    let mut results = Vec::new();
    for _ in 0..2 {
        let scenario=ScenarioFile::from_value(json!({"name":"host recording","turns":[{"user":"NEVER SEND REFERENCE","expect":[{"event":"user_transcription","text_contains":"recorded tone"},{"event":"llm_response","text_contains":"accepted PCM"}]}]})).unwrap().scenarios.remove(0);
        let session = EvalSession::new(
            scenario,
            &h.url,
            SessionOptions {
                stop_bot: false,
                default_timeout: Duration::from_secs(2),
                handshake_timeout: Duration::from_secs(2),
                max_duration: WAIT,
                transport: config().transport,
                ..Default::default()
            },
        )
        .unwrap()
        .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&[7, 0].repeat(640)).unwrap())])
        .unwrap();
        results.push(session.run(Cancellation::new()).await);
    }
    let (report, trace) = settle(h).await;
    assert!(results.iter().all(|r| r.passed), "{results:?}");
    let report = report.unwrap();
    assert_eq!(report.sessions, 2);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 2);
    assert_eq!(trace.configured.load(Ordering::SeqCst), 2);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace
            .audio
            .lock()
            .unwrap()
            .iter()
            .filter(|b| **b == [7, 0].repeat(640))
            .count(),
        2
    );
}
#[tokio::test]
async fn default_and_disabled_reject_audio_and_preserve_capability_shape() {
    for enabled in [None, Some(false)] {
        let (run, report, trace, caps) =
            raw_case(enabled, config(), true, vec![("raw-audio", raw(&[1, 0]))]).await;
        run.unwrap();
        assert!(
            matches!(report, Err(BotHostError::Invalid(_))),
            "{report:?}"
        );
        assert!(trace.audio.lock().unwrap().is_empty());
        assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(
            caps,
            json!({"eval":true,"text_only":true,"context_mode":"generated-text","playback_acknowledgements":false})
        );
    }
}
#[tokio::test]
async fn enabled_rejects_early_batch_bad_format_and_invalid_pcm() {
    let cases = vec![
        (false, "raw-audio", raw(&[1, 0])),
        (
            true,
            "raw-audio-batch",
            json!({"base64AudioBatch":["AQA="],"sampleRate":16000,"numChannels":1}),
        ),
        (
            true,
            "raw-audio",
            json!({"base64Audio":"AQA=","sampleRate":48000,"numChannels":1}),
        ),
        (
            true,
            "raw-audio",
            json!({"base64Audio":"AQ==","sampleRate":16000,"numChannels":1}),
        ),
    ];
    for (ready_first, kind, data) in cases {
        let (run, report, trace, _) =
            raw_case(Some(true), config(), ready_first, vec![(kind, data)]).await;
        run.unwrap();
        assert!(
            matches!(&report, Err(BotHostError::Invalid(e)) if e.contains("only client-ready") || e == "raw audio requires 16 kHz mono PCM16")
                || matches!(
                    &report,
                    Err(BotHostError::Eval(pipecat_evals::EvalError::Protocol(_)))
                ),
            "{report:?}"
        );
        assert!(trace.audio.lock().unwrap().is_empty());
        assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn first_audio_closes_context_and_configuration_admission() {
    for data in [
        json!({"t":"eval-context","d":{"messages":[]}}),
        json!({"t":"eval-configure","d":{}}),
    ] {
        let (run, report, trace, caps) = raw_case(
            Some(true),
            config(),
            true,
            vec![("raw-audio", raw(&[1, 0])), ("client-message", data)],
        )
        .await;
        run.unwrap();
        assert!(
            matches!(report,Err(BotHostError::Invalid(ref e)) if e.contains("must precede")),
            "{report:?}"
        );
        assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(caps["raw_audio_input"], true);
        assert_eq!(caps["audio_output"], false);
        assert_eq!(caps["text_only"], false);
    }
}
#[tokio::test]
async fn decoded_frame_budget_rejects_wire_legal_audio_and_joins_pipeline() {
    let bytes = vec![255; 2000];
    let message = pipecat_protocol::rtvi::Envelope {
        label: "rtvi-ai".into(),
        id: Some("request".into()),
        message_type: "raw-audio".into(),
        data: Some(raw(&bytes)),
    };
    assert!(message.encode().unwrap().len() < 4096);
    let frame = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(bytes.clone(), 16000, 1).unwrap(),
    ));
    assert!(serde_json::to_vec(&frame.data).unwrap().len() > 4096);
    let (run, report, trace, _) = raw_case(
        Some(true),
        config(),
        true,
        vec![
            ("raw-audio", raw(&bytes)),
            ("fixture-wait-audio", Value::Null),
            ("client-message", json!({"t":"eval-cancel","d":{}})),
        ],
    )
    .await;
    run.unwrap();
    assert_eq!(report.unwrap().last_exit, Some(EvalBotExit::EvalCancel));
    assert_eq!(*trace.audio.lock().unwrap(), vec![bytes.clone()]);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    let cfg = EvalBotHostConfig {
        transport: SessionConfig {
            max_message_bytes: 4096,
            ..config().transport
        },
        ..config()
    };
    // 2000 PCM bytes fit the base64 JSON limit but serde's numeric byte array does not.
    let (run, report, trace, _) = raw_case(
        Some(true),
        cfg,
        true,
        vec![("raw-audio", raw(&vec![255; 2000]))],
    )
    .await;
    run.unwrap();
    assert!(
        matches!(&report, Err(BotHostError::Pipeline(PipelineError::Execution(e))) if e == "eval host limit exceeded: serialized bytes"),
        "{report:?}"
    );
    assert!(trace.audio.lock().unwrap().is_empty());
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn eval_cancel_stops_enabled_host_after_audio_without_leaving_owners() {
    let (run, report, trace, _) = raw_case(
        Some(true),
        config(),
        true,
        vec![
            ("raw-audio", raw(&[1, 0])),
            ("client-message", json!({"t":"eval-cancel","d":{}})),
        ],
    )
    .await;
    run.unwrap();
    let report = report.unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
    assert_eq!(report.sessions, 1);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn enabling_audio_keeps_existing_text_input_and_text_output_contract() {
    let (run, report, trace, caps) = raw_case(
        Some(true),
        config(),
        true,
        vec![
            (
                "send-text",
                json!({"content":"ordinary text","options":{"audio_response":false}}),
            ),
            ("fixture-wait-text", Value::Null),
            ("client-message", json!({"t":"eval-cancel","d":{}})),
        ],
    )
    .await;
    run.unwrap();
    assert_eq!(report.unwrap().last_exit, Some(EvalBotExit::EvalCancel));
    assert_eq!(trace.text.load(Ordering::SeqCst), 1);
    assert_eq!(trace.configured.load(Ordering::SeqCst), 1);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert!(trace.audio.lock().unwrap().is_empty());
    assert_eq!(caps["audio_output"], false);
}
