//! Real loopback WS -> selected Gemini route -> the actual standard pair.
//! These fixtures assert observed history at the real assistant boundary. They
//! do not substitute timing, packet write completion, or fake ACKs for commits.
#![cfg(feature = "paired-history")]

#[path = "support/paired_history_peer.rs"]
mod peer;

use base64::{engine::general_purpose::STANDARD, Engine};
use peer::*;
use pipecat_core::*;
use pipecat_gemini_live::*;
use pipecat_turns::{
    AssistantAggregatorConfig, LLMContextAggregatorPair, RealtimeServiceMode, SharedContext,
    UserAggregatorConfig,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::{timeout, Instant};

fn pair(context: SharedContext, initially_muted: bool) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_shared_context_and_realtime_mode(
        context,
        UserAggregatorConfig {
            initially_muted,
            empty_user_turn: None,
            ..Default::default()
        },
        AssistantAggregatorConfig::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap()
}

fn lossless_seed_context(seed: LlmContextFrame) -> LlmContextData {
    let mut context = LlmContextData::from(seed);
    context.tools = ToolSet::NotGiven;
    context
}

struct Rig {
    worker: PipelineWorker,
    service: Option<Arc<GeminiLiveProcessor>>,
    context: SharedContext,
    before_assistant: Arc<ProbeState>,
    after_assistant: Arc<ProbeState>,
    tail: Arc<ProbeState>,
    user_name: String,
    service_entries: Arc<ProbeState>,
}
impl Rig {
    async fn start(client: GeminiClient, initially_muted: bool, hold: Option<Arc<Hold>>) -> Self {
        let context = SharedContext::default();
        Self::with_pair(client, pair(context.clone(), initially_muted), hold).await
    }
    async fn with_pair(
        client: GeminiClient,
        pair: LLMContextAggregatorPair,
        hold: Option<Arc<Hold>>,
    ) -> Self {
        Self::with_options(client, pair, hold, GeminiHistoryOptions::default()).await
    }
    async fn with_options(
        client: GeminiClient,
        pair: LLMContextAggregatorPair,
        hold: Option<Arc<Hold>>,
        options: GeminiHistoryOptions,
    ) -> Self {
        let context = pair.context();
        let user_name = pair.user().name().to_owned();
        let before_assistant = Arc::new(ProbeState::default());
        let after_assistant = Arc::new(ProbeState::default());
        let tail = Arc::new(ProbeState::default());
        let route = GeminiHistoryRoute::new(client, pair, options).unwrap();
        let service = route.service();
        let mut processors = route.into_processors(vec![Arc::new(Probe {
            name: "BeforeActualAssistant",
            context: context.clone(),
            state: before_assistant.clone(),
        })]);
        let service_entries = Arc::new(ProbeState::default());
        processors[2] = Arc::new(TappedProcessor {
            inner: processors[2].clone(),
            context: context.clone(),
            state: service_entries.clone(),
            after: false,
        });
        if let Some(hold) = hold {
            processors[1] = Arc::new(HeldProcessor {
                inner: processors[1].clone(),
                hold,
            });
        }
        let assistant_index = processors.len() - 1;
        processors[assistant_index] = Arc::new(TappedProcessor {
            inner: processors[assistant_index].clone(),
            context: context.clone(),
            state: after_assistant.clone(),
            after: true,
        });
        processors.push(Arc::new(Probe {
            name: "AfterActualAssistantForwarding",
            context: context.clone(),
            state: tail.clone(),
        }));
        let worker = Pipeline::new(processors)
            .start(pipeline_config())
            .await
            .unwrap();
        Self {
            worker,
            service,
            context,
            before_assistant,
            after_assistant,
            tail,
            user_name,
            service_entries,
        }
    }
    async fn send(&self, data: FrameData) {
        self.worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    async fn input_audio(&self) {
        self.worker
            .sender()
            .send(audio_frame(), Direction::Downstream)
            .await
            .unwrap();
    }
    async fn usage(&self, token: u64) {
        self.before_assistant
            .wait_for_in(&self.worker, |item| is_usage(item, token))
            .await;
    }
    async fn response(&self, text: &str) -> Observation {
        self.after_assistant
            .wait_for_in(
                &self.worker,
                |item| matches!(&item.frame.data, FrameData::LlmText(value) if value.text == text),
            )
            .await
    }
    async fn response_end(&self) -> Observation {
        self.after_assistant
            .wait_for_in(&self.worker, |item| {
                matches!(item.frame.data, FrameData::LlmResponseEnd)
            })
            .await
    }
    fn history(&self) -> Vec<(ChatRole, String)> {
        history_text(&self.context.snapshot_rich().unwrap())
    }
    async fn end(mut self, peer: Peer) -> WireTrace {
        let observer = self
            .service
            .as_ref()
            .and_then(|service| service.selected_join_observer());
        self.worker.end().await.unwrap();
        let report = join_worker(&mut self.worker).await.unwrap();
        assert_eq!(report.state, WorkerState::Ended, "{:?}", report.errors);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        if let Some(observer) = observer {
            assert!(
                observer.joined(),
                "selected End did not await the actual socket actor"
            );
        }
        peer.join().await
    }
    async fn cancel(mut self, peer: Peer) -> WireTrace {
        let observer = self
            .service
            .as_ref()
            .and_then(|service| service.selected_join_observer());
        self.worker
            .cancel("paired-history fixture complete")
            .await
            .unwrap();
        let report = join_worker(&mut self.worker).await.unwrap();
        assert_eq!(report.state, WorkerState::Cancelled, "{:?}", report.errors);
        if let Some(observer) = observer {
            assert!(
                observer.joined(),
                "selected Cancel did not await the actual socket actor"
            );
        }
        peer.join().await
    }
}
fn input(text: &str, token: u64) -> Value {
    let mut packet = json!({"serverContent":{"inputTranscription":{"text":text}}});
    usage(&mut packet, token);
    packet
}
fn answer(text: &str, input_text: Option<&str>, audio: bool, token: u64) -> Value {
    let mut parts = vec![json!({"text":text})];
    if audio {
        parts.push(json!({"inlineData":{"mimeType":"audio/pcm;rate=24000","data":STANDARD.encode(OUTPUT_PCM)}}));
    }
    let mut packet = json!({"serverContent":{"modelTurn":{"parts":parts},"turnComplete":true}});
    if let Some(text) = input_text {
        packet["serverContent"]["inputTranscription"] = json!({"text":text});
    }
    usage(&mut packet, token);
    packet
}
fn assert_no_seed_or_typed_feedback(trace: &WireTrace) {
    assert!(
        trace
            .input
            .iter()
            .all(|wire| wire.get("clientContent").is_none()
                && wire["realtimeInput"].get("text").is_none()),
        "unexpected seed/text echo: {:?}",
        trace.input
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_packet_audio_and_model_wait_for_actual_standard_user_consumption() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let rig = Rig::start(client, false, Some(held.clone())).await;
    rig.input_audio().await;
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["audio"].is_object())
    })
    .await;
    peer.send(answer(
        "A real answer.",
        Some("A real question."),
        true,
        101,
    ))
    .await;
    let final_frame = held.wait_entered().await;
    assert!(
        !final_frame.interruptible,
        "selected exact input must survive assistant generations"
    );
    assert!(
        matches!(&final_frame.data, FrameData::Transcription(text) if text.text == "A real question." && text.finalized && text.result.is_some())
    );
    rig.usage(101).await;
    assert!(
        rig.history().is_empty(),
        "held actual consumer cannot already have committed"
    );
    assert!(
        rig.before_assistant
            .snapshot()
            .iter()
            .all(|item| !production(&item.frame)),
        "dependent production escaped before actual consumer acceptance"
    );
    held.release();
    let observed = rig.response("A real answer.").await;
    assert_eq!(
        history_text(&observed.history),
        vec![(ChatRole::User, "A real question.".into())]
    );
    rig.response_end().await;
    assert_eq!(
        rig.history(),
        vec![
            (ChatRole::User, "A real question.".into()),
            (ChatRole::Assistant, "A real answer.".into())
        ]
    );
    let observations = rig.before_assistant.snapshot();
    assert_eq!(
        observations
            .iter()
            .filter(|item| matches!(item.frame.data, FrameData::LlmResponseStart))
            .count(),
        1
    );
    assert_eq!(
        observations
            .iter()
            .filter(|item| matches!(item.frame.data, FrameData::LlmResponseEnd))
            .count(),
        1
    );
    assert!(observations.iter().any(
        |item| matches!(&item.frame.data, FrameData::TtsAudio(audio) if audio.audio == OUTPUT_PCM)
    ));
    assert!(
        observations.iter().all(|item| !matches!(
            item.frame.data,
            FrameData::BotStartedSpeaking | FrameData::BotStoppedSpeaking
        )),
        "provider output must not invent playback acknowledgments"
    );
    assert_no_seed_or_typed_feedback(&rig.end(peer).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_runtime_user_pause_is_a_cross_queue_barrier_not_a_timer_assumption() {
    let (peer, client) = Peer::open(|_| {}).await;
    let rig = Rig::start(client, false, None).await;
    rig.send(FrameData::ProcessorPauseUrgent {
        processor: rig.user_name.clone(),
    })
    .await;
    rig.tail.wait_for_in(&rig.worker, |item| matches!(&item.frame.data, FrameData::ProcessorPauseUrgent { processor } if processor == &rig.user_name)).await;
    peer.send(answer("After resume.", Some("Before resume."), false, 102))
        .await;
    rig.usage(102).await;
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    rig.send(FrameData::ProcessorResumeUrgent {
        processor: rig.user_name.clone(),
    })
    .await;
    let observed = rig.response("After resume.").await;
    assert_eq!(
        history_text(&observed.history),
        vec![(ChatRole::User, "Before resume.".into())]
    );
    rig.response_end().await;
    assert_no_seed_or_typed_feedback(&rig.end(peer).await);
}

#[tokio::test]
async fn raw_public_model_before_input_order_is_unchanged() {
    let (peer, client) = Peer::open(|_| {}).await;
    let mut session = client.connect().await.unwrap();
    peer.send(answer("Raw answer.", Some("Raw question."), true, 103))
        .await;
    let mut events = Vec::new();
    while events.len() < 5 {
        events.push(
            timeout(WAIT, session.next_event())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .event,
        );
    }
    assert!(matches!(&events[0], Event::Text { text, thought: false } if text == "Raw answer."));
    assert!(matches!(&events[1], Event::Audio { pcm, .. } if pcm == OUTPUT_PCM));
    assert!(
        matches!(&events[2], Event::InputTranscription { transcript, is_final: false } if transcript.text == "Raw question.")
    );
    assert_eq!(events[3], Event::TurnComplete);
    assert!(matches!(events[4], Event::Usage(_)));
    session.sender().finish().await.unwrap();
    while timeout(WAIT, session.next_event()).await.unwrap().is_some() {}
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn output_without_known_input_is_not_retroactively_constrained_by_late_text() {
    let (peer, client) = Peer::open(|_| {}).await;
    let rig = Rig::start(client, false, None).await;
    peer.send(answer("Already delivered.", None, false, 104))
        .await;
    let observed = rig.response("Already delivered.").await;
    assert!(history_text(&observed.history).is_empty());
    rig.response_end().await;
    peer.send(answer(
        "Later answer.",
        Some("Late transcript."),
        false,
        105,
    ))
    .await;
    let later = rig.response("Later answer.").await;
    assert_eq!(
        history_text(&later.history),
        vec![
            (ChatRole::Assistant, "Already delivered.".into()),
            (ChatRole::User, "Late transcript.".into())
        ]
    );
    rig.end(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_does_not_force_a_suffix_before_the_source_500ms_gap() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let rig = Rig::start(client, false, Some(held.clone())).await;
    let began = Instant::now();
    peer.send(answer(
        "After source timeout.",
        Some("unfinished source"),
        false,
        106,
    ))
    .await;
    rig.usage(106).await;
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    let frame = held.wait_entered().await;
    assert!(
        began.elapsed() >= Duration::from_millis(490),
        "TurnComplete forced source finalization"
    );
    assert!(
        matches!(&frame.data, FrameData::Transcription(text) if text.text == "unfinished source" && text.result.is_none())
    );
    held.release();
    rig.response("After source timeout.").await;
    rig.end(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_fragments_concatenate_without_invented_spaces_and_ignore_interim() {
    for (fragments, expected) in [
        (vec![" Hel", "lo", " world."], "Hello world."),
        (vec![" 👋", "你好", "。"], "👋你好。"),
        (vec![" \tHello", ", ", "world!"], "Hello, world!"),
        (
            // The isolated "Dr." source final leaves its trailing space in
            // the next final. The standard TextBuffer adds its independent
            // inter-frame separator, as in the "Hello. trailing" case below.
            vec!["Dr. ", "Smith paid 3", ".14 dollars."],
            "Dr.  Smith paid 3.14 dollars.",
        ),
    ] {
        let (peer, client) = Peer::open(|_| {}).await;
        let rig = Rig::start(client, false, None).await;
        for (index, fragment) in fragments.into_iter().enumerate() {
            let mut packet = input(fragment, 110 + index as u64);
            packet["serverContent"]["interimInputTranscription"] =
                json!({"text":"NEVER APPEND THIS"});
            peer.send(packet).await;
            rig.usage(110 + index as u64).await;
        }
        peer.send(answer("Corpus answer.", None, false, 119)).await;
        let observed = rig.response("Corpus answer.").await;
        let user_text: Vec<_> = history_text(&observed.history)
            .into_iter()
            .filter_map(|(role, text)| (role == ChatRole::User).then_some(text))
            .collect();
        assert_eq!(
            user_text,
            vec![expected],
            "fragments must not become a message per piece"
        );
        rig.end(peer).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_input_after_a_timeout_is_a_new_segment_not_deduplication() {
    let (peer, client) = Peer::open(|_| {}).await;
    let rig = Rig::start(client, false, None).await;
    peer.send(answer("First.", Some("same words"), false, 120))
        .await;
    rig.response("First.").await;
    rig.response_end().await;
    peer.send(answer("Second.", Some("same words"), false, 121))
        .await;
    let second = rig.response("Second.").await;
    assert_eq!(
        history_text(&second.history),
        vec![
            (ChatRole::User, "same words".into()),
            (ChatRole::Assistant, "First.".into()),
            (ChatRole::User, "same words".into())
        ]
    );
    rig.end(peer).await;
}

async fn error_frame(worker: &mut PipelineWorker, stimulus: &str) -> ErrorFrame {
    timeout(WAIT, async {
        loop {
            // Fatal reporting records the actual error before supervisor
            // cancellation can drop its queued upstream notification.
            if let Some(error) = worker.errors().last().cloned() {
                return error;
            }
            let next = tokio::select! {
                directed = worker.recv() => directed,
                // Poll only the real recorded errors if a fatal notification
                // was dropped. Elapsed time itself never satisfies the test.
                _ = tokio::time::sleep(Duration::from_millis(5)) => continue,
            };
            match next {
                Some(directed) => {
                    if let FrameData::Error(error) | FrameData::FatalError(error) =
                        directed.frame.data
                    {
                        return error;
                    }
                }
                None => {
                    return worker
                        .errors()
                        .last()
                        .cloned()
                        .unwrap_or_else(|| panic!("runtime closed without a recorded error for {stimulus}; state: {:?}", worker.state()))
                }
            }
        }
    })
    .await
    .unwrap_or_else(|error| panic!("runtime error deadline for {stimulus}: {error}; state: {:?}; finished: {}; errors: {:#?}", worker.state(), worker.has_finished(), worker.errors()))
}

fn declaration() -> FunctionDeclaration {
    FunctionDeclaration {
        name: "lookup".into(),
        description: "Synthetic tool".into(),
        parameters: json!({"type":"object"}),
    }
}
async fn disconnected_client(
    configure: impl FnOnce(&mut GeminiConfig),
) -> (TcpListener, GeminiClient) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::new("constructor-only-synthetic-key", "gemini-3.8-live");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    configure(&mut config);
    (listener, GeminiClient::new(config).unwrap())
}
async fn no_connection(listener: &TcpListener) {
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err(),
        "rejected route connected to provider"
    );
}

#[tokio::test]
async fn every_configured_tool_policy_and_resumption_fail_before_connect() {
    for nonblocking in [false, true] {
        let (listener, mut client) =
            disconnected_client(|config| config.session.tools = vec![declaration()]).await;
        if nonblocking {
            client = client
                .with_non_blocking_tools(vec!["lookup".into()])
                .unwrap();
        }
        let result = GeminiHistoryRoute::new(
            client,
            pair(SharedContext::default(), false),
            GeminiHistoryOptions::default(),
        );
        assert!(
            result.is_err(),
            "configured tool policy was accepted: nonblocking={nonblocking}"
        );
        no_connection(&listener).await;
    }
    let (listener, client) = disconnected_client(|_| {}).await;
    let client = client
        .with_session_resumption(ResumptionPolicy::default())
        .unwrap();
    assert!(GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default()
    )
    .is_err());
    no_connection(&listener).await;
}

#[tokio::test]
async fn unsupported_mode_input_transcription_and_pair_mode_fail_before_connect() {
    let (listener, client) =
        disconnected_client(|config| config.session.input_transcription = false).await;
    assert!(GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default()
    )
    .is_err());
    no_connection(&listener).await;
    let (listener, client) = disconnected_client(|config| {
        config.mode = SessionMode::Transcription;
        config.session.modality = Modality::Text;
        config.session.output_transcription = false;
    })
    .await;
    assert!(GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default()
    )
    .is_err());
    no_connection(&listener).await;
    for mode in [RealtimeServiceMode::Auto, RealtimeServiceMode::Disabled] {
        let (listener, client) = disconnected_client(|_| {}).await;
        let pair = LLMContextAggregatorPair::with_shared_context_and_realtime_mode(
            SharedContext::default(),
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig::default(),
            mode,
        )
        .unwrap();
        assert!(GeminiHistoryRoute::new(client, pair, GeminiHistoryOptions::default()).is_err());
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsolicited_tools_reject_the_entire_packet_before_any_production_dispatch() {
    for tool in [
        json!({"toolCall":{"functionCalls":[{"id":"unexpected","name":"lookup","args":{}}]}}),
        json!({"toolCallCancellation":{"ids":["unexpected"]}}),
    ] {
        let (peer, client) = Peer::open(|_| {}).await;
        let mut rig = Rig::start(client, false, None).await;
        let mut packet = answer("Must never dispatch.", Some("Must not append."), true, 130);
        for (key, value) in tool.as_object().unwrap() {
            packet[key] = value.clone();
        }
        peer.send(packet).await;
        let _ = join_worker(&mut rig.worker).await;
        assert!(
            !rig.worker.errors().is_empty(),
            "unexpected tool packet must fail explicitly"
        );
        assert!(
            rig.before_assistant
                .snapshot()
                .iter()
                .all(|item| !production(&item.frame)
                    && !matches!(
                        item.frame.data,
                        FrameData::FunctionCallsStarted(_)
                            | FrameData::FunctionCallInProgress(_)
                            | FrameData::FunctionCallCancel(_)
                    )),
            "tool-bearing packet dispatched a callback/model effect"
        );
        assert!(rig.history().is_empty());
        peer.join().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_live_controls_fail_before_the_actual_user_mutates_history() {
    let message = ChatMessage::new(ChatRole::User, "forbidden edit");
    let controls = vec![
        FrameData::InputText(TextFrame::new("forbidden typed input")),
        FrameData::LlmRun,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![message.clone()],
            run_llm: false,
        }),
        FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![message],
            run_llm: false,
        }),
        FrameData::LlmSetTools(ToolSet::Functions(vec![])),
        FrameData::LlmSetToolChoice(ToolChoice::Auto),
        FrameData::LlmSummarizeContext(None),
        FrameData::Transcription(TranscriptionFrame {
            text: "foreign final".into(),
            finalized: true,
            ..Default::default()
        }),
        FrameData::InterimTranscription(TranscriptionFrame {
            text: "foreign interim".into(),
            ..Default::default()
        }),
        FrameData::LlmContext(LlmContextFrame::default()),
    ];
    for control in controls {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let (peer, client) = Peer::open(|_| {}).await;
            let mut rig = Rig::start(client, false, None).await;
            let initial = rig.context.snapshot_rich().unwrap();
            rig.worker
                .sender()
                .send(Frame::new(control.clone()), direction)
                .await
                .unwrap();
            let error = error_frame(&mut rig.worker, &format!("{control:?} {direction:?}")).await;
            assert!(!error.error.is_empty());
            assert_eq!(
                rig.context.snapshot_rich().unwrap(),
                initial,
                "control mutated history before rejection: {control:?} {direction:?}"
            );
            if !rig.worker.has_finished() {
                let _ = rig.worker.cancel("rejected control observed").await;
            }
            let _ = join_worker(&mut rig.worker).await;
            let trace = peer.join().await;
            assert_no_seed_or_typed_feedback(&trace);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn muted_actual_consumer_cannot_release_a_success_shaped_answer() {
    let (peer, client) = Peer::open(|_| {}).await;
    let mut rig = Rig::start(client, true, None).await;
    peer.send(answer("Never released.", Some("Muted source."), false, 131))
        .await;
    let _ = join_worker(&mut rig.worker).await;
    assert!(!rig.worker.errors().is_empty());
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    peer.join().await;
}

#[derive(Clone, Copy, Debug)]
enum TopologyError {
    MissingGate,
    ReorderedUser,
    ForeignAssistant,
    DuplicateUser,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_or_missing_route_components_fail_before_connect() {
    for invalid in [
        TopologyError::MissingGate,
        TopologyError::ReorderedUser,
        TopologyError::ForeignAssistant,
        TopologyError::DuplicateUser,
    ] {
        let (listener, client) = disconnected_client(|_| {}).await;
        let route = GeminiHistoryRoute::new(
            client,
            pair(SharedContext::default(), false),
            GeminiHistoryOptions::default(),
        )
        .unwrap();
        let mut processors = route.into_processors(vec![]);
        match invalid {
            TopologyError::MissingGate => {
                processors.remove(0);
            }
            TopologyError::ReorderedUser => processors.swap(1, 2),
            TopologyError::ForeignAssistant => {
                processors[3] = pair(SharedContext::default(), false).assistant()
            }
            TopologyError::DuplicateUser => processors.insert(2, processors[1].clone()),
        }
        let result = Pipeline::new(processors).start(pipeline_config()).await;
        assert!(
            result.is_err(),
            "invalid selected topology started: {invalid:?}"
        );
        no_connection(&listener).await;
    }
}

struct RejectSetup {
    inner: Arc<dyn FrameProcessor>,
}
#[async_trait]
impl FrameProcessor for RejectSetup {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        Err(ProcessorError::new(
            "selected assistant fixture setup rejected before binding",
        ))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_selected_assistant_setup_cannot_be_mistaken_for_completed_setup() {
    let (listener, client) = disconnected_client(|_| {}).await;
    let route = GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut processors = route.into_processors(vec![]);
    processors[3] = Arc::new(RejectSetup {
        inner: processors[3].clone(),
    });
    assert!(Pipeline::new(processors)
        .start(pipeline_config())
        .await
        .is_err());
    no_connection(&listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_and_cancel_join_the_outer_runtime_and_close_the_peer_while_input_is_held() {
    for stop in [false, true] {
        let (peer, client) = Peer::open(|_| {}).await;
        let held = Hold::new(HoldKind::FinalTranscription);
        let mut rig = Rig::start(client, false, Some(held.clone())).await;
        peer.send(answer("Drop held output.", Some("Held input."), true, 132))
            .await;
        held.wait_entered().await;
        rig.usage(132).await;
        if stop {
            // Stop is normal and does not abort an already-running ordinary
            // consumer. Release that real handler after the gate has received
            // Stop; the separate Cancel case never requires this release.
            rig.send(FrameData::Stop).await;
            peer.wait_for(|trace| trace.peer_closed).await;
            assert!(rig
                .before_assistant
                .snapshot()
                .iter()
                .all(|item| !production(&item.frame)));
            held.release();
        } else {
            rig.worker
                .cancel("cancel with real user lane held")
                .await
                .unwrap();
        }
        let report = join_worker(&mut rig.worker).await.unwrap();
        assert_eq!(
            report.state,
            if stop {
                WorkerState::Stopped
            } else {
                WorkerState::Cancelled
            }
        );
        peer.join().await;
        // The runtime join and peer closure are separate assertions. This
        // fixture alone does not claim an inner JoinHandle was awaited.
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_fence_admits_earlier_held_audio_before_finish_and_drains_post_finish_suffix() {
    let (peer, client) =
        Peer::open(|config| config.session.activity_detection.disabled = true).await;
    let context = SharedContext::default();
    let held_audio = Hold::new(HoldKind::InputAudio);
    let gate_entries = Arc::new(ProbeState::default());
    let service_completed = Arc::new(ProbeState::default());
    let after_assistant = Arc::new(ProbeState::default());
    let tail = Arc::new(ProbeState::default());
    let route = GeminiHistoryRoute::new(
        client,
        pair(context.clone(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut processors = route.into_processors(vec![]);
    processors[0] = Arc::new(TappedProcessor {
        inner: processors[0].clone(),
        context: context.clone(),
        state: gate_entries.clone(),
        after: false,
    });
    processors[1] = Arc::new(HeldProcessor {
        inner: processors[1].clone(),
        hold: held_audio.clone(),
    });
    processors[2] = Arc::new(TappedProcessor {
        inner: processors[2].clone(),
        context: context.clone(),
        state: service_completed.clone(),
        after: true,
    });
    let assistant_index = processors.len() - 1;
    processors[assistant_index] = Arc::new(TappedProcessor {
        inner: processors[assistant_index].clone(),
        context: context.clone(),
        state: after_assistant.clone(),
        after: true,
    });
    processors.push(Arc::new(Probe {
        name: "AfterEndDrainAssistantForwarding",
        context: context.clone(),
        state: tail.clone(),
    }));
    let mut worker = Pipeline::new(processors)
        .start(pipeline_config())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityStart"].is_object())
    })
    .await;
    worker
        .sender()
        .send(audio_frame(), Direction::Downstream)
        .await
        .unwrap();
    held_audio.wait_entered().await;
    let end = Frame::end();
    worker
        .sender()
        .send(end.clone(), Direction::Downstream)
        .await
        .unwrap();
    gate_entries.wait_for(|item| item.frame == end).await;
    let before_release = peer.snapshot();
    assert!(!before_release.peer_closed);
    assert!(
        before_release
            .input
            .iter()
            .all(|wire| wire["realtimeInput"].get("audio").is_none()
                && wire["realtimeInput"].get("activityEnd").is_none()),
        "finish overtook actual held audio handler"
    );
    held_audio.release();
    let wire = peer
        .wait_for(|trace| {
            trace
                .input
                .iter()
                .any(|wire| wire["realtimeInput"]["activityEnd"].is_object())
        })
        .await;
    let audio_index = wire
        .input
        .iter()
        .position(|wire| wire["realtimeInput"]["audio"].is_object())
        .unwrap();
    let finish_index = wire
        .input
        .iter()
        .position(|wire| wire["realtimeInput"]["activityEnd"].is_object())
        .unwrap();
    assert!(
        audio_index < finish_index,
        "prior input was not admitted ahead of finish"
    );
    let marker = service_completed.wait_for(|item| matches!(&item.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("gemini_history_end_input"))).await;
    assert!(!marker.frame.interruptible);
    assert_eq!(marker.direction, Direction::Downstream);
    let sent_at = Instant::now();
    peer.send(answer(
        "Drained answer.",
        Some("source arriving only after finish"),
        false,
        140,
    ))
    .await;
    // Provider completion closes the real session worker. It must not erase or
    // force-finalize the source suffix while the owner is draining history.
    peer.wait_for(|trace| trace.peer_closed).await;
    let completed = after_assistant
        .wait_for_in(&worker, |item| {
            matches!(item.frame.data, FrameData::LlmResponseEnd)
        })
        .await;
    assert!(sent_at.elapsed() >= Duration::from_millis(490));
    assert_eq!(
        history_text(&completed.history),
        vec![
            (ChatRole::User, "source arriving only after finish".into()),
            (ChatRole::Assistant, "Drained answer.".into())
        ]
    );
    let observed_end = tail
        .wait_for_in(&worker, |item| matches!(item.frame.data, FrameData::End(_)))
        .await;
    assert_eq!(
        observed_end.frame, end,
        "End was replaced instead of forwarded unchanged"
    );
    let report = join_worker(&mut worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_crossing_an_inflight_end_keeps_the_shared_shutdown_owner() {
    let (peer, client) =
        Peer::open(|config| config.session.activity_detection.disabled = true).await;
    let mut rig = Rig::start(client, false, None).await;
    rig.send(FrameData::UserStartedSpeaking).await;
    rig.input_audio().await;
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["audio"].is_object())
    })
    .await;
    rig.worker.end().await.unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityEnd"].is_object())
    })
    .await;
    rig.worker
        .cancel("cancel overtakes unfinished End drain")
        .await
        .unwrap();
    let report = join_worker(&mut rig.worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(
        rig.after_assistant
            .snapshot()
            .iter()
            .all(|item| !matches!(item.frame.data, FrameData::End(_))),
        "canceled drain forwarded a successful End"
    );
    peer.join().await;
}

struct FatalDelivery;
#[async_trait]
impl FrameProcessor for FatalDelivery {
    fn name(&self) -> &str {
        "ForcedFatalDelivery"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmText(_)) {
            return Err(ProcessorError::fatal("forced fixture delivery failure"));
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_supervisor_failure_proves_outer_join_and_peer_close_only() {
    let (peer, client) = Peer::open(|_| {}).await;
    let route = GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut worker = Pipeline::new(route.into_processors(vec![Arc::new(FatalDelivery)]))
        .start(pipeline_config())
        .await
        .unwrap();
    peer.send(answer("Force fatal delivery.", None, true, 141))
        .await;
    assert!(join_worker(&mut worker).await.is_err());
    assert!(worker
        .errors()
        .iter()
        .any(|error| error.error.contains("forced fixture delivery failure")));
    peer.join().await;
    // Intentionally no assertion of cooperative inner-worker await: core's
    // fatal supervisor aborts the receive owner and Session::drop aborts inner.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_eof_with_unfinished_session_is_failure_not_successful_end() {
    let (peer, client) = Peer::open(|_| {}).await;
    let mut rig = Rig::start(client, false, None).await;
    peer.send(input("not force finalized", 142)).await;
    rig.usage(142).await;
    peer.close().await;
    let _ = join_worker(&mut rig.worker).await;
    assert!(!rig.worker.errors().is_empty());
    assert!(
        rig.history().is_empty(),
        "abnormal EOF promoted an unfinished suffix"
    );
    assert!(rig
        .after_assistant
        .snapshot()
        .iter()
        .all(|item| !matches!(item.frame.data, FrameData::End(_))));
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_while_real_final_is_held_retires_output_but_keeps_session_input() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let rig = Rig::start(client, false, Some(held.clone())).await;
    peer.send(answer(
        "Retired answer.",
        Some("Speech survives interruption."),
        false,
        143,
    ))
    .await;
    held.wait_entered().await;
    rig.usage(143).await;
    rig.send(FrameData::Interruption).await;
    rig.tail
        .wait_for_in(&rig.worker, |item| {
            matches!(item.frame.data, FrameData::Interruption)
        })
        .await;
    held.release();
    peer.send(answer("Current answer.", None, false, 144)).await;
    let current = rig.response("Current answer.").await;
    assert_eq!(
        history_text(&current.history),
        vec![(ChatRole::User, "Speech survives interruption.".into())]
    );
    assert!(rig.before_assistant.snapshot().iter().all(|item| !matches!(&item.frame.data, FrameData::LlmText(text) if text.text == "Retired answer.")));
    rig.end(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_fragment_and_held_event_overflow_fail_closed_without_truncation() {
    for fragment_overflow in [true, false] {
        let (peer, client) = Peer::open(|_| {}).await;
        let mut rig = Rig::start(client, false, None).await;
        let packet = if fragment_overflow {
            answer(
                "Must not escape size failure.",
                Some(&"x".repeat(64 * 1024 + 1)),
                false,
                150,
            )
        } else {
            let parts: Vec<_> = (0..129)
                .map(|index| json!({"text":format!("held-{index}")}))
                .collect();
            json!({"serverContent":{"inputTranscription":{"text":"unfinished dependency"},"modelTurn":{"parts":parts},"turnComplete":true}})
        };
        peer.send(packet).await;
        let _ = join_worker(&mut rig.worker).await;
        assert!(
            !rig.worker.errors().is_empty(),
            "bounded route silently accepted overflow"
        );
        assert!(
            rig.history().is_empty(),
            "overflow silently truncated/committed a source prefix"
        );
        assert!(
            rig.before_assistant
                .snapshot()
                .iter()
                .all(|item| !production(&item.frame)),
            "dependent output escaped overflowing route"
        );
        peer.join().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whitespace_only_and_interim_only_packets_do_not_create_user_messages() {
    for source in [
        json!({"inputTranscription":{"text":" \t\n"}}),
        json!({"interimInputTranscription":{"text":"hypothesis only"}}),
        json!({"inputTranscription":{"text":""}}),
    ] {
        let (peer, client) = Peer::open(|_| {}).await;
        let rig = Rig::start(client, false, None).await;
        let mut packet = answer("No invented user text.", None, false, 151);
        for (key, value) in source.as_object().unwrap() {
            packet["serverContent"][key] = value.clone();
        }
        peer.send(packet).await;
        let observed = rig.response("No invented user text.").await;
        assert!(history_text(&observed.history).is_empty());
        rig.response_end().await;
        assert_eq!(
            rig.history(),
            vec![(ChatRole::Assistant, "No invented user text.".into())]
        );
        rig.cancel(peer).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_replacement_while_actual_final_is_held_never_satisfies_its_watermark() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let mut rig = Rig::start(client, false, Some(held.clone())).await;
    peer.send(answer(
        "Must not release against replaced context.",
        Some("Registered source."),
        false,
        152,
    ))
    .await;
    held.wait_entered().await;
    rig.usage(152).await;
    // Deliberate contract violation to verify detection. A matching public
    // user message is not a consumer-issued receipt for the registered frame.
    rig.context
        .replace_rich(LlmContextData::from(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "Registered source.")],
            ..Default::default()
        }))
        .unwrap();
    held.release();
    let _ = join_worker(&mut rig.worker).await;
    assert!(!rig.worker.errors().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    assert_eq!(
        rig.history(),
        vec![(ChatRole::User, "Registered source.".into())]
    );
    peer.join().await;
}

#[tokio::test]
async fn standard_pair_tool_declarations_and_choices_are_rejected_before_connect() {
    let tool = ToolDefinition {
        name: "lookup".into(),
        description: "Synthetic pair-side declaration".into(),
        parameters: json!({"type":"object"}),
    };
    let mut contexts = vec![LlmContextFrame {
        tools: vec![tool],
        ..Default::default()
    }];
    for choice in [
        ToolChoice::Auto,
        ToolChoice::None,
        ToolChoice::Required,
        ToolChoice::Function("lookup".into()),
    ] {
        contexts.push(LlmContextFrame {
            tool_choice: Some(choice),
            ..Default::default()
        });
    }
    for context in contexts {
        let (listener, client) = disconnected_client(|_| {}).await;
        let context = SharedContext::new_rich(context.into());
        assert!(GeminiHistoryRoute::new(
            client,
            pair(context, false),
            GeminiHistoryOptions::default()
        )
        .is_err());
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unusable_actual_user_cannot_turn_ignored_work_into_a_history_receipt() {
    let (peer, client) = Peer::open(|_| {}).await;
    let mut rig = Rig::start(client, false, None).await;
    assert!(rig.worker.processor_health(1).unwrap().set_usable(false));
    peer.send(answer(
        "Never released from unusable consumer.",
        Some("Not accepted."),
        false,
        153,
    ))
    .await;
    let _ = join_worker(&mut rig.worker).await;
    assert!(!rig.worker.errors().is_empty());
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_initial_seed_is_sent_once_and_both_local_feedback_forms_are_suppressed() {
    for rich in [false, true] {
        let (peer, client) = Peer::open(|_| {}).await;
        let seed = LlmContextFrame {
            messages: vec![
                ChatMessage::new(ChatRole::User, "Earlier question."),
                ChatMessage::new(ChatRole::Assistant, "Earlier answer."),
            ],
            ..Default::default()
        };
        let rich_context = SharedContext::new_rich(lossless_seed_context(seed.clone()));
        let context = if rich {
            rich_context
        } else {
            let mut native = rich_context.snapshot().unwrap();
            native.tools = None;
            SharedContext::new(native)
        };
        let options = GeminiHistoryOptions {
            initial_seed: Some(seed.clone()),
            ..Default::default()
        };
        let rig = Rig::with_options(client, pair(context, false), None, options).await;
        let seeded = peer
            .wait_for(|trace| {
                trace
                    .input
                    .iter()
                    .any(|wire| wire["clientContent"].is_object())
            })
            .await;
        let initial: Vec<_> = seeded
            .input
            .iter()
            .filter(|wire| wire["clientContent"].is_object())
            .collect();
        assert_eq!(initial.len(), 1);
        assert_eq!(
            initial[0]["clientContent"]["turns"],
            json!([
                {"role":"user","parts":[{"text":"Earlier question."}]},
                {"role":"model","parts":[{"text":"Earlier answer."}]}
            ])
        );
        peer.send(answer(
            "Current answer.",
            Some("Current question."),
            true,
            160,
        ))
        .await;
        let observed = rig.response("Current answer.").await;
        assert_eq!(
            history_text(&observed.history),
            vec![
                (ChatRole::User, "Earlier question.".into()),
                (ChatRole::Assistant, "Earlier answer.".into()),
                (ChatRole::User, "Current question.".into())
            ]
        );
        let feedback = rig
            .service_entries
            .wait_for(|item| {
                matches!(
                    item.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            })
            .await;
        assert!(!feedback.frame.interruptible);
        assert_eq!(feedback.direction, Direction::Downstream);
        assert_eq!(
            matches!(feedback.frame.data, FrameData::LlmContextDetailed(_)),
            rich,
            "fixture failed to exercise selected lossless feedback form"
        );
        rig.response_end().await;
        let trace = rig.end(peer).await;
        assert_eq!(
            trace
                .input
                .iter()
                .filter(|wire| wire["clientContent"].is_object())
                .count(),
            1,
            "local history feedback resent initial seed"
        );
        assert!(
            trace
                .input
                .iter()
                .all(|wire| wire["realtimeInput"].get("text").is_none()),
            "local user commit echoed as provider text"
        );
    }
}

#[tokio::test]
async fn nonempty_initial_history_requires_its_exact_explicit_seed_before_connect() {
    let actual = LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "Actual initial history.")],
        ..Default::default()
    };
    for seed in [
        None,
        Some(LlmContextFrame::default()),
        Some(LlmContextFrame {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "Different initial history.",
            )],
            ..Default::default()
        }),
    ] {
        let (listener, client) = disconnected_client(|_| {}).await;
        let options = GeminiHistoryOptions {
            initial_seed: seed,
            ..Default::default()
        };
        let context = SharedContext::new_rich(lossless_seed_context(actual.clone()));
        assert!(GeminiHistoryRoute::new(client, pair(context, false), options).is_err());
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_seed_is_revalidated_against_actual_context_at_start_before_connect() {
    let (listener, client) = disconnected_client(|_| {}).await;
    let seed = LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "Approved seed.")],
        ..Default::default()
    };
    let context = SharedContext::new_rich(lossless_seed_context(seed.clone()));
    let route = GeminiHistoryRoute::new(
        client,
        pair(context.clone(), false),
        GeminiHistoryOptions {
            initial_seed: Some(seed),
            ..Default::default()
        },
    )
    .unwrap();
    context
        .replace_rich(lossless_seed_context(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "Replaced before startup.")],
            ..Default::default()
        }))
        .unwrap();
    assert!(Pipeline::new(route.into_processors(vec![]))
        .start(pipeline_config())
        .await
        .is_err());
    no_connection(&listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicitly_empty_seed_never_sends_an_empty_client_content_request() {
    let (peer, client) = Peer::open(|_| {}).await;
    let rig = Rig::with_options(
        client,
        pair(SharedContext::default(), false),
        None,
        GeminiHistoryOptions {
            initial_seed: Some(LlmContextFrame::default()),
            ..Default::default()
        },
    )
    .await;
    peer.send(answer("Empty seed answer.", Some("Question."), false, 161))
        .await;
    rig.response("Empty seed answer.").await;
    rig.response_end().await;
    assert_no_seed_or_typed_feedback(&rig.end(peer).await);
}

#[tokio::test]
async fn invalid_zero_excessive_and_unbounded_history_options_fail_before_connect() {
    let mut invalid = Vec::new();
    for limit in [0, usize::MAX] {
        invalid.extend([
            GeminiHistoryOptions {
                max_fragment_bytes: limit,
                ..Default::default()
            },
            GeminiHistoryOptions {
                max_suffix_bytes: limit,
                ..Default::default()
            },
            GeminiHistoryOptions {
                max_queued_final_bytes: limit,
                ..Default::default()
            },
            GeminiHistoryOptions {
                max_held_events: limit,
                ..Default::default()
            },
            GeminiHistoryOptions {
                max_held_bytes: limit,
                ..Default::default()
            },
        ]);
    }
    invalid.extend([
        GeminiHistoryOptions {
            consumer_timeout: Duration::ZERO,
            ..Default::default()
        },
        GeminiHistoryOptions {
            consumer_timeout: Duration::from_secs(61),
            ..Default::default()
        },
        GeminiHistoryOptions {
            drain_timeout: Duration::ZERO,
            ..Default::default()
        },
        GeminiHistoryOptions {
            drain_timeout: Duration::from_secs(121),
            ..Default::default()
        },
        GeminiHistoryOptions {
            user_id: "u".repeat(4097),
            ..Default::default()
        },
    ]);
    for options in invalid {
        let (listener, client) = disconnected_client(|_| {}).await;
        assert!(
            GeminiHistoryRoute::new(client, pair(SharedContext::default(), false), options)
                .is_err()
        );
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_user_pause_exhausts_the_finite_consumer_deadline_without_output_release() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let mut rig = Rig::with_options(
        client,
        pair(SharedContext::default(), false),
        Some(held.clone()),
        GeminiHistoryOptions {
            consumer_timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await;
    peer.send(answer(
        "Must not time out into success.",
        Some("Held real consumer."),
        false,
        162,
    ))
    .await;
    held.wait_entered().await;
    rig.usage(162).await;
    assert!(join_worker(&mut rig.worker).await.is_err());
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_small_storage_limits_fail_at_real_receive_boundaries() {
    let cases = [
        GeminiHistoryOptions {
            max_fragment_bytes: 3,
            ..Default::default()
        },
        GeminiHistoryOptions {
            max_suffix_bytes: 3,
            ..Default::default()
        },
        GeminiHistoryOptions {
            max_queued_final_bytes: 3,
            ..Default::default()
        },
        GeminiHistoryOptions {
            max_held_events: 1,
            ..Default::default()
        },
        GeminiHistoryOptions {
            max_held_bytes: 1,
            ..Default::default()
        },
    ];
    for (index, options) in cases.into_iter().enumerate() {
        let (peer, client) = Peer::open(|_| {}).await;
        let mut rig =
            Rig::with_options(client, pair(SharedContext::default(), false), None, options).await;
        let source = if index == 2 {
            "Too long final."
        } else {
            "unfinished source"
        };
        peer.send(answer(
            "Held answer exceeds tiny limits.",
            Some(source),
            true,
            163,
        ))
        .await;
        assert!(join_worker(&mut rig.worker).await.is_err());
        assert!(rig
            .before_assistant
            .snapshot()
            .iter()
            .all(|item| !production(&item.frame)));
        peer.join().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finite_end_deadline_cancels_the_real_owner_and_never_forwards_success_end() {
    let (peer, client) =
        Peer::open(|config| config.session.activity_detection.disabled = true).await;
    let mut rig = Rig::with_options(
        client,
        pair(SharedContext::default(), false),
        None,
        GeminiHistoryOptions {
            drain_timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await;
    rig.send(FrameData::UserStartedSpeaking).await;
    rig.input_audio().await;
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["audio"].is_object())
    })
    .await;
    rig.worker.end().await.unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityEnd"].is_object())
    })
    .await;
    assert!(join_worker(&mut rig.worker).await.is_err());
    assert!(rig
        .after_assistant
        .snapshot()
        .iter()
        .all(|item| !matches!(item.frame.data, FrameData::End(_))));
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_user_label_and_normalized_punctuation_provenance_reach_actual_consumer() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let rig = Rig::with_options(
        client,
        pair(SharedContext::default(), false),
        Some(held.clone()),
        GeminiHistoryOptions {
            user_id: "selected-speaker".into(),
            ..Default::default()
        },
    )
    .await;
    let mut packet = answer("Labeled answer.", Some(" Labeled question."), false, 170);
    packet["serverContent"]["inputTranscription"]["languageCode"] = json!("en-US");
    peer.send(packet).await;
    let frame = held.wait_entered().await;
    let FrameData::Transcription(transcript) = &frame.data else {
        panic!("expected actual finalized source")
    };
    assert_eq!(transcript.user_id, "selected-speaker");
    assert_eq!(transcript.text, "Labeled question.");
    assert_eq!(transcript.language.as_deref(), Some("en-US"));
    let provenance = transcript
        .result
        .as_ref()
        .expect("punctuation final needs normalized triggering contribution");
    assert_eq!(provenance["text"], " Labeled question.");
    held.release();
    rig.response("Labeled answer.").await;
    rig.end(peer).await;
}

struct RejectDeliverySetup;
#[async_trait]
impl FrameProcessor for RejectDeliverySetup {
    fn name(&self) -> &str {
        "RejectedArbitraryDeliverySetup"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        Err(ProcessorError::new("arbitrary delivery setup failed"))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arbitrary_delivery_setup_failure_keeps_the_unchanged_core_connection_boundary() {
    let (peer, client) = Peer::open(|_| {}).await;
    let route = GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut worker = Pipeline::new(route.into_processors(vec![Arc::new(RejectDeliverySetup)]))
        .start(pipeline_config())
        .await
        .unwrap();
    let error = error_frame(&mut worker, "arbitrary nonfatal delivery setup").await;
    assert_eq!(error.error, "arbitrary delivery setup failed");
    peer.wait_for(|trace| trace.setup.is_some()).await;
    worker
        .cancel("observed nonfatal delivery setup error")
        .await
        .unwrap();
    let report = join_worker(&mut worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report
        .errors
        .iter()
        .any(|error| error.error == "arbitrary delivery setup failed"));
    let trace = peer.join().await;
    assert!(
        trace.setup.is_some(),
        "fixture did not exercise the documented arbitrary-delivery setup boundary"
    );
    // Selected setup validation is not a global guarantee about arbitrary
    // delivery setup: unchanged core surfaces this error after Start runs.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_realtime_feedback_flag_cannot_authorize_a_foreign_context() {
    for detailed in [false, true] {
        let (peer, client) = Peer::open(|_| {}).await;
        let mut rig = Rig::start(client, false, None).await;
        let context = LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "forged local commit")],
            ..Default::default()
        };
        let data = if detailed {
            FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                context: context.into(),
                speculation: false,
            })
        } else {
            FrameData::LlmContext(context)
        };
        let mut forged = Frame::new(data).with_interruptible(false);
        forged
            .metadata
            .insert("realtime_context_update".into(), json!(true));
        rig.worker
            .sender()
            .send(forged, Direction::Upstream)
            .await
            .unwrap();
        error_frame(
            &mut rig.worker,
            &format!("foreign realtime feedback Upstream detailed={detailed}"),
        )
        .await;
        assert!(rig.history().is_empty());
        if !rig.worker.has_finished() {
            let _ = rig.worker.cancel("forged feedback rejected").await;
        }
        let _ = join_worker(&mut rig.worker).await;
        assert_no_seed_or_typed_feedback(&peer.join().await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn marker_origin_string_without_the_registered_identity_cannot_close_input() {
    let (peer, client) =
        Peer::open(|config| config.session.activity_detection.disabled = true).await;
    let mut rig = Rig::start(client, false, None).await;
    rig.send(FrameData::UserStartedSpeaking).await;
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityStart"].is_object())
    })
    .await;
    let forged = Frame::new(FrameData::Flush(FlushFrame {
        returning: false,
        origin: Some("gemini_history_end_input".into()),
    }))
    .with_interruptible(false);
    let id = forged.id;
    rig.worker
        .sender()
        .send(forged, Direction::Downstream)
        .await
        .unwrap();
    rig.tail
        .wait_for_in(&rig.worker, |item| {
            item.frame.id == id && item.direction == Direction::Downstream
        })
        .await;
    rig.input_audio().await;
    let trace = peer
        .wait_for(|trace| {
            trace
                .input
                .iter()
                .any(|wire| wire["realtimeInput"]["audio"].is_object())
        })
        .await;
    assert!(
        trace
            .input
            .iter()
            .all(|wire| wire["realtimeInput"].get("activityEnd").is_none()),
        "forged origin satisfied the End input fence"
    );
    rig.worker.end().await.unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityEnd"].is_object())
    })
    .await;
    peer.send(answer("Genuine drain.", Some("Genuine input."), false, 171))
        .await;
    let report = join_worker(&mut rig.worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    peer.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_selected_component_after_service_invalidates_prior_successful_setup() {
    for (component_index, component) in [(0, "gate"), (1, "user"), (2, "service"), (3, "assistant")]
    {
        let (listener, client) = disconnected_client(|_| {}).await;
        let route = GeminiHistoryRoute::new(
            client,
            pair(SharedContext::default(), false),
            GeminiHistoryOptions::default(),
        )
        .unwrap();
        let mut processors = route.into_processors(vec![]);
        let duplicate = processors[component_index].clone();
        // The original gate/user/service stay contiguous at indices 0/1/2.
        // A repeated exact component in delivery must invalidate its previous
        // successful registration, not merely retain the first valid index.
        processors.insert(3, duplicate);
        assert!(
            Pipeline::new(processors)
                .start(pipeline_config())
                .await
                .is_err(),
            "duplicate selected {component} retained a successful startup identity"
        );
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_packet_sentence_reaches_real_user_before_its_suffix_can_release_output() {
    let (peer, client) = Peer::open(|_| {}).await;
    let held = Hold::new(HoldKind::FinalTranscription);
    let rig = Rig::start(client, false, Some(held.clone())).await;
    let sent_at = Instant::now();
    peer.send(answer(
        "After the complete source prefix.",
        Some("Hello. trailing"),
        false,
        172,
    ))
    .await;
    // This deadline checks source dispatch, never a commit ACK. The exact
    // punctuation final must reach the real user's invocation before the
    // source's independent 500 ms suffix deadline can expire.
    let first = timeout(Duration::from_millis(400), held.wait_entered())
        .await
        .expect("punctuation final was delayed behind its unfinished packet suffix");
    assert!(
        matches!(&first.data, FrameData::Transcription(text) if text.text == "Hello." && text.result.is_some())
    );
    rig.usage(172).await;
    assert!(rig.history().is_empty());
    assert!(rig
        .before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    held.release();
    let observed = rig.response("After the complete source prefix.").await;
    assert!(
        sent_at.elapsed() >= Duration::from_millis(490),
        "model release preceded source suffix finalization"
    );
    // The ordinary standard TextBuffer adds its inter-frame separator while
    // retaining the suffix's original leading space. Do not normalize source
    // text or split one selected prefix into artificial per-sentence messages.
    assert_eq!(
        history_text(&observed.history),
        vec![(ChatRole::User, "Hello.  trailing".into())]
    );
    rig.response_end().await;
    rig.end(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_user_activity_after_real_finish_is_local_only_at_the_sealed_service() {
    let (peer, client) =
        Peer::open(|config| config.session.activity_detection.disabled = true).await;
    let context = SharedContext::default();
    let user_output = Arc::new(UserOutput::default());
    let service_completed = Arc::new(ProbeState::default());
    let assistant_completed = Arc::new(ProbeState::default());
    let tail = Arc::new(ProbeState::default());
    let route = GeminiHistoryRoute::new(
        client,
        pair(context.clone(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut processors = route.into_processors(vec![]);
    processors[1] = Arc::new(CapturedUser {
        inner: processors[1].clone(),
        output: user_output.clone(),
    });
    processors[2] = Arc::new(TappedProcessor {
        inner: processors[2].clone(),
        context: context.clone(),
        state: service_completed.clone(),
        after: true,
    });
    let assistant_index = processors.len() - 1;
    processors[assistant_index] = Arc::new(TappedProcessor {
        inner: processors[assistant_index].clone(),
        context: context.clone(),
        state: assistant_completed.clone(),
        after: true,
    });
    processors.push(Arc::new(Probe {
        name: "AfterLateActivityAssistant",
        context,
        state: tail.clone(),
    }));
    let mut worker = Pipeline::new(processors)
        .start(pipeline_config())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(audio_frame(), Direction::Downstream)
        .await
        .unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["audio"].is_object())
    })
    .await;
    worker.end().await.unwrap();
    peer.wait_for(|trace| {
        trace
            .input
            .iter()
            .any(|wire| wire["realtimeInput"]["activityEnd"].is_object())
    })
    .await;
    service_completed.wait_for_in(&worker, |item| matches!(&item.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("gemini_history_end_input"))).await;

    // The separate turns unit test extracts a real timer effect before its
    // marker. Here we test the downstream service boundary with delayed local
    // effects from the actual user's setup context. These injected frames are
    // not represented as an end-to-end timer-origin witness.
    for data in [
        FrameData::UserStartedSpeaking,
        FrameData::UserStoppedSpeaking,
    ] {
        let frame = Frame::new(data);
        user_output.emit(frame.clone());
        let service = service_completed
            .wait_for_in(&worker, |item| item.frame.id == frame.id)
            .await;
        assert_eq!(service.frame, frame);
        assert_eq!(service.direction, Direction::Downstream);
        let forwarded = tail
            .wait_for_in(&worker, |item| item.frame.id == frame.id)
            .await;
        assert_eq!(forwarded.frame, frame);
        assert_eq!(forwarded.direction, Direction::Downstream);
    }
    assert!(worker.errors().is_empty(), "{:?}", worker.errors());
    peer.send(answer(
        "Drained after local activity.",
        Some("Final source."),
        false,
        181,
    ))
    .await;
    assistant_completed
        .wait_for_in(&worker, |item| {
            matches!(item.frame.data, FrameData::LlmResponseEnd)
        })
        .await;
    let report = join_worker(&mut worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        user_output.is_closed(),
        "fixture retained its setup context after cleanup"
    );
    let trace = peer.join().await;
    for key in ["activityStart", "activityEnd"] {
        assert_eq!(
            trace
                .input
                .iter()
                .filter(|wire| wire["realtimeInput"][key].is_object())
                .count(),
            1,
            "late local {key} wrote to the finished provider input: {:?}",
            trace.input
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_model_end_commits_already_consumed_source_finals_as_one_user_batch() {
    let (peer, client) = Peer::open(|_| {}).await;
    let context = SharedContext::default();
    let user_completed = Arc::new(ProbeState::default());
    let before_assistant = Arc::new(ProbeState::default());
    let tail = Arc::new(ProbeState::default());
    let route = GeminiHistoryRoute::new(
        client,
        pair(context.clone(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let mut processors = route.into_processors(vec![Arc::new(Probe {
        name: "BeforeNoModelEndAssistant",
        context: context.clone(),
        state: before_assistant.clone(),
    })]);
    processors[1] = Arc::new(TappedProcessor {
        inner: processors[1].clone(),
        context: context.clone(),
        state: user_completed.clone(),
        after: true,
    });
    processors.push(Arc::new(Probe {
        name: "AfterNoModelEndAssistant",
        context: context.clone(),
        state: tail.clone(),
    }));
    let mut worker = Pipeline::new(processors)
        .start(pipeline_config())
        .await
        .unwrap();
    for (text, token) in [("First sentence.", 182), ("Second sentence.", 183)] {
        peer.send(input(text, token)).await;
        let consumed = user_completed
            .wait_for_in(&worker, |item| {
                item.direction == Direction::Upstream
                    && matches!(&item.frame.data, FrameData::Transcription(value) if value.finalized && value.text == text)
            })
            .await;
        assert!(
            history_text(&consumed.history).is_empty(),
            "an input-only source final forced its own history message"
        );
    }
    // Successful original user.process calls prove that both finals were
    // staged. They do not fabricate a history ACK: there is no output barrier
    // yet, and the real End path must perform the ordinary single-batch commit.
    assert!(history_text(&context.snapshot_rich().unwrap()).is_empty());
    let end = Frame::end();
    worker
        .sender()
        .send(end.clone(), Direction::Downstream)
        .await
        .unwrap();
    let report = join_worker(&mut worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        history_text(&context.snapshot_rich().unwrap()),
        vec![(ChatRole::User, "First sentence. Second sentence.".into())]
    );
    assert!(tail
        .snapshot()
        .iter()
        .any(|item| item.frame == end && item.direction == Direction::Downstream));
    assert!(before_assistant
        .snapshot()
        .iter()
        .all(|item| !production(&item.frame)));
    assert_no_seed_or_typed_feedback(&peer.join().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_startup_padding_and_nudge_never_enter_real_paired_history_or_bypass_receipts() {
    for model in ["gemini-2.5-live", "gemini-3-live"] {
        for inference_on_initialization in [false, true] {
            let (peer, client) = Peer::open(|c| c.session.model = model.into()).await;
            let client = client
                .with_initial_history(InitialHistoryPolicy {
                    inference_on_initialization,
                })
                .unwrap();
            let seed = LlmContextFrame {
                messages: vec![
                    ChatMessage::new(ChatRole::User, "Earlier question."),
                    ChatMessage::new(ChatRole::Assistant, "Earlier answer."),
                ],
                ..Default::default()
            };
            let initial = vec![
                (ChatRole::User, "Earlier question.".into()),
                (ChatRole::Assistant, "Earlier answer.".into()),
            ];
            let context = SharedContext::new_rich(lossless_seed_context(seed.clone()));
            let hold = Hold::new(HoldKind::FinalTranscription);
            let rig = Rig::with_options(
                client,
                pair(context, false),
                Some(hold.clone()),
                GeminiHistoryOptions {
                    initial_seed: Some(seed),
                    ..Default::default()
                },
            )
            .await;
            let startup_count = if model == "gemini-3-live" && inference_on_initialization {
                2
            } else {
                1
            };
            let startup = peer
                .wait_for(|trace| trace.input.len() >= startup_count)
                .await;
            assert_eq!(
                startup.setup.unwrap()["setup"]["historyConfig"],
                json!({"initialHistoryInClientContent":true})
            );
            assert_eq!(
                startup.input[0]["clientContent"]["turnComplete"],
                inference_on_initialization
            );
            let turns = startup.input[0]["clientContent"]["turns"]
                .as_array()
                .unwrap();
            assert_eq!(turns.len(), if model == "gemini-2.5-live" { 3 } else { 2 });
            if model == "gemini-2.5-live" {
                assert_eq!(turns[2], json!({"role":"user","parts":[{"text":" "}]}));
            }
            if startup_count == 2 {
                assert_eq!(startup.input[1], json!({"realtimeInput":{"text":" "}}));
            }
            assert_eq!(rig.history(), initial);
            let observer = rig
                .service
                .as_ref()
                .unwrap()
                .selected_join_observer()
                .unwrap();
            assert!(!observer.joined());
            rig.input_audio().await;
            peer.wait_for(|trace| {
                trace
                    .input
                    .iter()
                    .any(|wire| wire["realtimeInput"]["audio"].is_object())
            })
            .await;
            peer.send(answer(
                "Real assistant answer.",
                Some("Real user question."),
                true,
                1261,
            ))
            .await;
            hold.wait_entered().await;
            rig.usage(1261).await;
            assert_eq!(
                rig.history(),
                initial,
                "synthetic seed text or unconsumed input entered history"
            );
            assert!(
                rig.before_assistant
                    .snapshot()
                    .iter()
                    .all(|item| !production(&item.frame)),
                "startup policy bypassed the actual user-consumption barrier"
            );
            assert!(
                !observer.joined(),
                "a held output/history receipt cannot prove actor join"
            );
            hold.release();
            let output = rig.response("Real assistant answer.").await;
            let mut committed = initial;
            committed.push((ChatRole::User, "Real user question.".into()));
            assert_eq!(history_text(&output.history), committed);
            rig.response_end().await;
            committed.push((ChatRole::Assistant, "Real assistant answer.".into()));
            assert_eq!(rig.history(), committed);
            let trace = rig.end(peer).await;
            assert!(observer.joined());
            assert_eq!(
                trace
                    .input
                    .iter()
                    .filter(|wire| wire["clientContent"].is_object())
                    .count(),
                1,
                "authentic local context feedback replayed the initial seed"
            );
            assert_eq!(
                trace
                    .input
                    .iter()
                    .filter(|wire| wire["realtimeInput"].get("text").is_some())
                    .count(),
                usize::from(startup_count == 2),
                "local transcription echoed or synthetic nudge repeated"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_implicit_and_explicit_empty_pair_are_ready_for_real_audio_with_system_configured()
{
    for explicit in [false, true] {
        let (peer, client) =
            Peer::open(|c| c.session.system_instruction = Some("Configured system".into())).await;
        let client = client
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: true,
            })
            .unwrap();
        let rig = Rig::with_options(
            client,
            pair(SharedContext::default(), false),
            None,
            GeminiHistoryOptions {
                initial_seed: explicit.then(LlmContextFrame::default),
                ..Default::default()
            },
        )
        .await;
        rig.input_audio().await;
        peer.wait_for(|trace| {
            trace
                .input
                .iter()
                .any(|wire| wire["realtimeInput"]["audio"].is_object())
        })
        .await;
        peer.send(answer(
            "Empty-pair real answer.",
            Some("Empty-pair real question."),
            false,
            1262,
        ))
        .await;
        let output = rig.response("Empty-pair real answer.").await;
        assert_eq!(
            history_text(&output.history),
            vec![(ChatRole::User, "Empty-pair real question.".into())]
        );
        rig.response_end().await;
        assert_eq!(
            rig.history(),
            vec![
                (ChatRole::User, "Empty-pair real question.".into()),
                (ChatRole::Assistant, "Empty-pair real answer.".into())
            ]
        );
        let trace = rig.end(peer).await;
        assert_no_seed_or_typed_feedback(&trace);
        assert!(
            trace
                .input
                .iter()
                .any(|wire| wire["realtimeInput"]["audio"].is_object()),
            "absence of empty seed alone does not prove the pair became ready"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_paired_cancel_and_stop_recover_the_same_actual_socket_owner() {
    for stop in [false, true] {
        let (peer, client) = Peer::open(|c| c.session.model = "gemini-2.5-live".into()).await;
        let client = client
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let seed = LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "Silent pending seed.")],
            ..Default::default()
        };
        let context = SharedContext::new_rich(lossless_seed_context(seed.clone()));
        let mut rig = Rig::with_options(
            client,
            pair(context, false),
            None,
            GeminiHistoryOptions {
                initial_seed: Some(seed),
                ..Default::default()
            },
        )
        .await;
        peer.wait_for(|trace| !trace.input.is_empty()).await;
        let observer = rig
            .service
            .as_ref()
            .unwrap()
            .selected_join_observer()
            .unwrap();
        assert!(!observer.joined());
        if stop {
            rig.send(FrameData::Stop).await;
            let report = join_worker(&mut rig.worker).await.unwrap();
            assert_eq!(report.state, WorkerState::Stopped, "{:?}", report.errors);
            assert!(
                observer.joined(),
                "Stop must settle in prepare_end without relying on cleanup"
            );
            peer.join().await;
        } else {
            rig.cancel(peer).await;
            assert!(observer.joined());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_paired_run_and_append_still_reject_before_history_mutation_and_join_on_failure() {
    for data in [
        FrameData::LlmRun,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "Forbidden append")],
            run_llm: true,
        }),
    ] {
        let (peer, client) = Peer::open(|_| {}).await;
        let client = client
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let mut rig = Rig::start(client, false, None).await;
        let observer = rig
            .service
            .as_ref()
            .unwrap()
            .selected_join_observer()
            .unwrap();
        rig.send(data).await;
        let error = error_frame(&mut rig.worker, "selected paired mutation").await;
        assert!(!error.error.is_empty());
        assert!(rig.history().is_empty());
        if !rig.worker.has_finished() {
            let _ = rig.worker.cancel("observed rejection").await;
        }
        let _ = join_worker(&mut rig.worker).await;
        assert!(observer.joined());
        assert_no_seed_or_typed_feedback(&peer.join().await);
    }
}

#[tokio::test]
async fn selected_paired_transformed_limit_and_invalid_extracted_system_fail_before_connect() {
    for invalid_system in [false, true] {
        let (listener, client) = disconnected_client(|c| {
            c.session.model = "gemini-2.5-live".into();
            c.session.system_instruction = Some("Configured".into());
        })
        .await;
        let client = client
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let messages = if invalid_system {
            let mut system = ChatMessage::new(ChatRole::System, "Configured");
            system.name = Some("unsupported metadata on omitted system".into());
            vec![system, ChatMessage::new(ChatRole::User, "Regular text")]
        } else {
            let mut messages = vec![ChatMessage::new(ChatRole::User, "x"); 1024];
            messages[1023].role = ChatRole::Assistant;
            messages
        };
        let seed = LlmContextFrame {
            messages,
            ..Default::default()
        };
        let context = SharedContext::new_rich(lossless_seed_context(seed.clone()));
        assert!(GeminiHistoryRoute::new(
            client,
            pair(context, false),
            GeminiHistoryOptions {
                initial_seed: Some(seed),
                ..Default::default()
            }
        )
        .is_err());
        no_connection(&listener).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_paired_route_cannot_be_started_a_second_time_after_real_join() {
    let (peer, client) = Peer::open(|_| {}).await;
    let client = client
        .with_initial_history(InitialHistoryPolicy {
            inference_on_initialization: false,
        })
        .unwrap();
    let route = GeminiHistoryRoute::new(
        client,
        pair(SharedContext::default(), false),
        GeminiHistoryOptions::default(),
    )
    .unwrap();
    let service = route.service().unwrap();
    let processors = route.into_processors(vec![]);
    let mut worker = Pipeline::new(processors.clone())
        .start(pipeline_config())
        .await
        .unwrap();
    let observer = service.selected_join_observer().unwrap();
    worker.end().await.unwrap();
    let report = join_worker(&mut worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended, "{:?}", report.errors);
    assert!(observer.joined());
    peer.join().await;
    assert!(Pipeline::new(processors)
        .start(pipeline_config())
        .await
        .is_err());
    assert!(
        observer.joined(),
        "second-use rejection changed the first run's join evidence"
    );
}

#[tokio::test]
async fn typed_service_observation_never_exposes_default_paired_raw_sender() {
    for selected in [false, true] {
        let (listener, client) =
            disconnected_client(|c| c.session.model = "gemini-3-live".into()).await;
        let client = if selected {
            client
                .with_initial_history(InitialHistoryPolicy {
                    inference_on_initialization: false,
                })
                .unwrap()
        } else {
            client
        };
        let route = GeminiHistoryRoute::new(
            client,
            pair(SharedContext::default(), false),
            GeminiHistoryOptions::default(),
        )
        .unwrap();
        assert_eq!(
            route.service().is_some(),
            selected,
            "typed service access must not open default paired raw sender admission"
        );
        no_connection(&listener).await;
    }
}
