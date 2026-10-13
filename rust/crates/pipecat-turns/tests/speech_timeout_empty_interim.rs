//! Existing-text SpeechTimeout interim invalidation, derived from Python af9f9da9.
//! Caller-owned monotonic times; no sleeps, model verdicts, or private state.
use pipecat_core::*;
use pipecat_services::{Message, Role};
use pipecat_turns::*;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config(explicit: bool) -> UserTurnConfig {
    UserTurnConfig {
        speech_timeout: ms(100),
        stt_timeout: ms(0),
        vad_stop_duration: ms(200),
        stop_timeout: ms(5000),
        wait_for_transcript: true,
        start_on_transcription: true,
        resolve_proposals: false,
        enable_interruptions: false,
        require_model_completion: false,
        start_strategies: None,
        stop_strategies: explicit.then(|| {
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(ms(100), true)])
                .unwrap()
        }),
    }
}
fn metadata() -> FrameData {
    FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "empty-interim-fixture".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: 0.8,
    })
}
fn start() -> FrameData {
    FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    })
}
fn stop() -> FrameData {
    FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
        stop_secs: 0.2,
        timestamp: 0.0,
    })
}
fn hello() -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: "hello".into(),
        finalized: true,
        ..Default::default()
    })
}
fn interim(text: &str, detailed_noappend: bool) -> FrameData {
    if detailed_noappend {
        FrameData::InterimTranscriptionDetailed(DetailedTranscriptionFrame {
            text: TextFrame {
                append_to_context: false,
                ..TextFrame::new(text)
            },
            user_id: "fixture-user".into(),
            timestamp: "fixture-time".into(),
            language: None,
            result: Some(serde_json::json!({"fixture": "empty-interim"})),
            finalized: false,
        })
    } else {
        FrameData::InterimTranscription(TranscriptionFrame {
            text: text.into(),
            finalized: false,
            ..Default::default()
        })
    }
}
fn stopped() -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Stopped {
        emit_speaking: true,
        reason: TurnStopReason::SpeechTimeout,
    }]
}
// All nonempty scalar and explicit blank controls complete before the scalar
// empty witness. An interim never adds its text to the committed message.
const CASES: [(&str, &str, bool, bool); 8] = [
    ("nonempty_legacy_scalar", "world", false, false),
    ("nonempty_detailed_scalar", "world", true, false),
    ("empty_legacy_explicit", "", false, true),
    ("whitespace_detailed_explicit", " \t", true, true),
    ("empty_legacy_scalar", "", false, false),
    ("empty_detailed_scalar", "", true, false),
    ("whitespace_legacy_scalar", " \t", false, false),
    ("whitespace_detailed_scalar", " \t", true, false),
];

#[test]
fn controller_pre_stop_empty_interim_revokes_finality_until_620ms() {
    for (case, text, detailed, explicit) in CASES {
        let mut controller = UserTurnController::new(config(explicit));
        let metadata = controller.process(&metadata(), ms(0));
        let started = controller.process(&start(), ms(0));
        let finalized = controller.process(&hello(), ms(10));
        let interim = controller.process(&interim(text, detailed), ms(15));
        let vad_stop = controller.process(&stop(), ms(20));
        let deadline = controller.deadline();
        let before_floor = controller.advance(ms(119));
        let floor = controller.advance(ms(120));
        let active_at_floor = controller.is_active();
        let deadline_at_floor = controller.deadline();
        let after_floor = controller.advance(ms(121));
        let before_safety = controller.advance(ms(619));
        let safety = controller.advance(ms(620));
        let active_at_safety = controller.is_active();
        let deadline_at_safety = controller.deadline();
        let after_safety = controller.advance(ms(621));
        let terminal = controller.finish(false, TurnStopReason::SessionEnd);
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        let final_deadline = controller.deadline();
        let final_active = controller.is_active();
        eprintln!("controller case={case} captured_through_cleanup: floor_120ms={floor:?}; active_120ms={active_at_floor}; deadline_20ms={deadline:?}; safety_620ms={safety:?}; terminal={terminal:?}; cleaned={cleaned:?}");

        assert!(metadata.is_empty());
        assert_eq!(
            started,
            vec![UserTurnEvent::Started {
                emit_speaking: true,
                interrupt: false,
            }]
        );
        assert!(finalized.is_empty());
        assert!(interim.is_empty());
        assert!(vad_stop.is_empty());
        assert!(before_floor.is_empty());
        // First old-red assertion is the actual premature stop event.
        assert!(
            floor.is_empty(),
            "case={case}: must not stop at 120ms: {floor:?}"
        );
        assert!(active_at_floor);
        assert_eq!(deadline, Some(ms(620)));
        assert_eq!(deadline_at_floor, Some(ms(620)));
        assert!(after_floor.is_empty());
        assert!(before_safety.is_empty());
        assert_eq!(safety, stopped(), "case={case}: must stop at 620ms");
        assert!(!active_at_safety);
        assert_eq!(deadline_at_safety, None);
        assert!(after_safety.is_empty());
        assert!(terminal.is_empty());
        assert!(cleaned.is_empty());
        assert_eq!(final_deadline, None);
        assert!(!final_active);
        eprintln!("controller case={case} all_assertions_completed");
    }
}

fn user(context: &SharedContext, explicit: bool) -> LLMUserAggregator {
    LLMUserAggregator::new(
        context.clone(),
        UserAggregatorConfig {
            turn: config(explicit),
            eager: None,
            initially_muted: false,
            user_idle_timeout: ms(0),
            mute_strategies: vec![],
            empty_user_turn: None,
        },
    )
}
fn process(
    user: &LLMUserAggregator,
    frame: Frame,
    direction: Direction,
    now: u64,
) -> AggregationOutput {
    user.process_at(frame, direction, ms(now)).unwrap()
}
fn tagged(data: FrameData, tag: &str) -> Frame {
    let mut frame = Frame::new(data);
    frame.interruptible = false;
    frame.metadata.insert("trace".into(), tag.into());
    frame
}
fn forwarded(frame: Frame, direction: Direction) -> AggregationOutput {
    AggregationOutput {
        actions: vec![AggregationAction::Push(DirectedFrame::new(
            frame, direction,
        ))],
        events: vec![],
    }
}
fn assert_committed(output: &AggregationOutput, context: &FrameData) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::UserTurnStopped {
            text: "hello".into(),
            reason: TurnStopReason::SpeechTimeout,
        }]
    );
    let [AggregationAction::Broadcast(boundary), AggregationAction::Push(inference)] =
        output.actions.as_slice()
    else {
        panic!("exactly one stop before exactly one context: {output:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
    assert_eq!(
        boundary.metadata.get("user_context_follows"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(inference.direction, Direction::Downstream);
    assert_eq!(&inference.frame.data, context);
    assert_ne!(boundary.id, inference.frame.id);
}

#[test]
fn aggregator_pre_stop_empty_interim_commits_once_at_620ms() {
    for (case, text, detailed, explicit) in CASES {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        let metadata_frame = tagged(metadata(), "stt");
        let metadata = process(&user, metadata_frame.clone(), Direction::Upstream, 0);
        let start_frame = tagged(start(), "start");
        let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
        let hello_frame = tagged(hello(), "hello");
        let finalized = process(&user, hello_frame.clone(), Direction::Downstream, 10);
        let interim_frame = tagged(interim(text, detailed), "interim");
        let interim = process(&user, interim_frame.clone(), Direction::Upstream, 15);
        let preview_at_interim = user.interim_text().unwrap();
        let transcripts_at_interim = user.transcript_frames().unwrap();
        let stop_frame = tagged(stop(), "stop");
        let vad_stop = process(&user, stop_frame.clone(), Direction::Upstream, 20);
        let deadline = user.deadline().unwrap();
        let before_floor = user.advance(ms(119)).unwrap();
        let floor = user.advance(ms(120)).unwrap();
        let history_at_floor = context.snapshot().unwrap();
        let deadline_at_floor = user.deadline().unwrap();
        let after_floor = user.advance(ms(121)).unwrap();
        let before_safety = user.advance(ms(619)).unwrap();
        let safety = user.advance(ms(620)).unwrap();
        let history_at_safety = context.snapshot().unwrap();
        let context_at_safety = context.frame_data().unwrap();
        let deadline_at_safety = user.deadline().unwrap();
        let after_safety = user.advance(ms(621)).unwrap();
        let end_frame = tagged(FrameData::End(Default::default()), "end");
        let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
        let after_end = user.advance(ms(10000)).unwrap();
        let history_after_end = context.snapshot().unwrap();
        let final_deadline = user.deadline().unwrap();
        let final_idle = user.idle_deadline().unwrap();
        eprintln!("aggregator case={case} captured_through_End: floor_120ms={floor:?}; history_120ms={:?}; deadline_20ms={deadline:?}; safety_620ms={safety:?}; End={end:?}; after_End={after_end:?}; history_after_End={:?}", history_at_floor.messages, history_after_end.messages);

        assert_eq!(metadata, forwarded(metadata_frame, Direction::Upstream));
        assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
        let [AggregationAction::Push(original), AggregationAction::Broadcast(boundary)] =
            started.actions.as_slice()
        else {
            panic!("original VAD start and exactly one generated speaking boundary: {started:?}");
        };
        assert_eq!(
            original,
            &DirectedFrame::new(start_frame.clone(), Direction::Downstream)
        );
        assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
        assert_ne!(boundary.id, start_frame.id);
        assert_eq!(finalized, AggregationOutput::default());
        assert_eq!(interim, AggregationOutput::default());
        assert_eq!(preview_at_interim, text);
        assert_eq!(transcripts_at_interim, vec![hello_frame, interim_frame]);
        assert_eq!(vad_stop, forwarded(stop_frame, Direction::Upstream));
        assert_eq!(before_floor, AggregationOutput::default());
        // First old-red assertion is the actual premature stop/commit output.
        assert_eq!(
            floor,
            AggregationOutput::default(),
            "case={case}: must not commit at 120ms"
        );
        assert!(history_at_floor.messages.is_empty());
        assert_eq!(deadline, Some(ms(620)));
        assert_eq!(deadline_at_floor, Some(ms(620)));
        assert_eq!(after_floor, AggregationOutput::default());
        assert_eq!(before_safety, AggregationOutput::default());
        assert_committed(&safety, &context_at_safety);
        assert_eq!(
            history_at_safety.messages,
            vec![Message::text(Role::User, "hello")]
        );
        assert_eq!(deadline_at_safety, None);
        assert_eq!(after_safety, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame, Direction::Downstream));
        assert_eq!(after_end, AggregationOutput::default());
        assert_eq!(history_after_end.messages, history_at_safety.messages);
        assert_eq!(final_deadline, None);
        assert_eq!(final_idle, None);
        eprintln!("aggregator case={case} all_assertions_completed");
    }
}

// Candidate-only controls follow the byte-for-byte frozen baseline above.
fn final_text(text: &str, finalized: bool) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized,
        ..Default::default()
    })
}
fn started_event() -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Started {
        emit_speaking: true,
        interrupt: false,
    }]
}
fn stopped_reason(emit_speaking: bool, reason: TurnStopReason) -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Stopped {
        emit_speaking,
        reason,
    }]
}
enum ControllerStep {
    Frame(u64, FrameData),
    Tick(u64),
    Model(u64),
}
struct ControllerTrace {
    events: Vec<Vec<UserTurnEvent>>,
    deadlines: Vec<Option<Duration>>,
}
fn check_controller(
    case: &str,
    config: UserTurnConfig,
    steps: Vec<ControllerStep>,
    effects: Vec<(usize, Vec<UserTurnEvent>)>,
) -> ControllerTrace {
    let mut controller = UserTurnController::new(config);
    let mut events = vec![];
    let mut deadlines = vec![];
    for step in steps {
        events.push(match step {
            ControllerStep::Frame(at, data) => controller.process(&data, ms(at)),
            ControllerStep::Tick(at) => controller.advance(ms(at)),
            ControllerStep::Model(at) => controller.mark_model_complete(ms(at)),
        });
        deadlines.push(controller.deadline());
    }
    let terminal = controller.finish(false, TurnStopReason::SessionEnd);
    controller.cleanup();
    let cleaned = controller.advance(ms(20000));
    let final_deadline = controller.deadline();
    let final_active = controller.is_active();
    let mut expected = vec![vec![]; events.len()];
    for (index, effect) in effects {
        expected[index] = effect;
    }
    assert_eq!(events, expected, "{case}: complete event trace");
    assert!(
        terminal.is_empty(),
        "{case}: unexpected terminal stop {terminal:?}"
    );
    assert!(cleaned.is_empty());
    assert_eq!(final_deadline, None);
    assert!(!final_active);
    ControllerTrace { events, deadlines }
}

#[test]
fn controller_blank_interim_changes_only_finality_before_adopted_stop() {
    use ControllerStep::*;
    for (text, detailed) in [("", false), (" \t", true)] {
        let trace = check_controller(
            "adopted stop remains immediate",
            config(false),
            vec![
                Frame(0, metadata()),
                Frame(0, start()),
                Frame(10, hello()),
                Frame(15, interim(text, detailed)),
                Frame(20, FrameData::UserStoppedSpeaking),
                Tick(120),
                Tick(620),
            ],
            vec![
                (1, started_event()),
                (4, stopped_reason(false, TurnStopReason::External)),
            ],
        );
        // Public deadlines hide all turn timers while VAD is speaking.
        assert_eq!(trace.deadlines[2], None);
        assert_eq!(trace.deadlines[3], None);
        assert_eq!(trace.deadlines[4], None);
    }
}

#[test]
fn controller_blank_interim_preserves_final_repair_and_zero_stt_floor() {
    use ControllerStep::*;
    for case in [
        "no_interim",
        "zero_stt",
        "early_repair",
        "late_repair",
        "empty_final",
        "interim_claims_final",
    ] {
        let mut stt = metadata();
        if case == "zero_stt" {
            let FrameData::SttMetadata(frame) = &mut stt else {
                unreachable!()
            };
            frame.ttfs_p99_latency = 0.0;
        }
        let mut steps = vec![
            Frame(0, stt),
            Frame(0, start()),
            Frame(10, final_text("hello", case != "empty_final")),
        ];
        let mut effects = vec![(1, started_event())];
        if case == "empty_final" {
            steps.push(Frame(12, final_text("", true)));
        }
        if case != "no_interim" {
            let mut data = interim(" \t", true);
            if case == "interim_claims_final" {
                let FrameData::InterimTranscriptionDetailed(frame) = &mut data else {
                    unreachable!()
                };
                frame.finalized = true;
            }
            steps.push(Frame(15, data));
        }
        steps.push(Frame(20, stop()));
        if case == "early_repair" {
            steps.push(Frame(30, final_text("", true)));
        }
        steps.push(Tick(119));
        let floor_index = steps.len();
        steps.push(Tick(120));
        if ["no_interim", "zero_stt", "early_repair"].contains(&case) {
            effects.push((floor_index, stopped()));
        }
        if case == "late_repair" {
            steps.push(Tick(199));
            effects.push((steps.len(), stopped()));
            steps.push(Frame(200, final_text("", true)));
        }
        steps.push(Tick(619));
        if ["empty_final", "interim_claims_final"].contains(&case) {
            effects.push((steps.len(), stopped()));
        }
        steps.push(Tick(620));
        steps.push(Tick(621));
        check_controller(case, config(false), steps, effects);
    }
}

#[test]
fn controller_blank_interim_keeps_inactive_no_text_and_excluded_lanes() {
    use ControllerStep::*;
    check_controller(
        "inactive",
        config(false),
        vec![
            Frame(0, metadata()),
            Frame(15, interim("", false)),
            Tick(10000),
        ],
        vec![],
    );
    check_controller(
        "no accepted text",
        config(false),
        vec![
            Frame(0, metadata()),
            Frame(0, start()),
            Frame(15, interim(" \t", true)),
            Frame(20, stop()),
            Tick(120),
            Tick(620),
            Tick(5019),
            Tick(5020),
        ],
        vec![
            (1, started_event()),
            (7, stopped_reason(true, TurnStopReason::Watchdog)),
        ],
    );
    check_controller(
        "no VAD transcript fallback",
        config(false),
        vec![
            Frame(0, metadata()),
            Frame(10, hello()),
            Frame(15, interim("", false)),
            Tick(109),
            Tick(110),
            Tick(620),
        ],
        vec![(1, started_event()), (4, stopped())],
    );
    check_controller(
        "speaking without VAD observation",
        config(false),
        vec![
            Frame(0, metadata()),
            Frame(0, FrameData::UserStartedSpeaking),
            Frame(10, hello()),
            Frame(15, interim("", false)),
            Frame(20, stop()),
            Tick(120),
            Tick(620),
        ],
        vec![
            (
                1,
                vec![UserTurnEvent::Started {
                    emit_speaking: false,
                    interrupt: false,
                }],
            ),
            (5, stopped()),
        ],
    );
    for case in ["optional", "custom_start", "model"] {
        let mut cfg = config(false);
        if case == "optional" {
            cfg.wait_for_transcript = false;
        }
        if case == "custom_start" {
            cfg.start_strategies = Some(
                UserTurnStartStrategies::new(vec![
                    UserTurnStartStrategy::vad().with_interruptions(false)
                ])
                .unwrap(),
            );
        }
        cfg.require_model_completion = case == "model";
        let mut steps = vec![
            Frame(0, metadata()),
            Frame(0, start()),
            Frame(10, hello()),
            Frame(15, interim(" \t", true)),
            Frame(20, stop()),
            Tick(120),
        ];
        let mut effects = vec![(1, started_event())];
        if case == "model" {
            effects.push((
                steps.len(),
                stopped_reason(true, TurnStopReason::ModelCompletion),
            ));
            steps.push(Model(130));
        } else {
            effects.push((5, stopped()));
        }
        steps.push(Tick(620));
        check_controller(case, cfg, steps, effects);
    }
}

#[test]
fn controller_post_stop_blank_and_nonempty_interims_keep_existing_contracts() {
    use ControllerStep::*;
    for (text, detailed, due) in [("", false, 120), (" \t", true, 120), ("tail", false, 620)] {
        let trace = check_controller(
            "post-stop compatibility",
            config(false),
            vec![
                Frame(0, metadata()),
                Frame(0, start()),
                Frame(10, hello()),
                Frame(20, stop()),
                Frame(50, interim(text, detailed)),
                Tick(119),
                Tick(120),
                Tick(619),
                Tick(620),
                Tick(621),
            ],
            vec![
                (1, started_event()),
                (if due == 120 { 6 } else { 8 }, stopped()),
            ],
        );
        assert_eq!(trace.deadlines[4], Some(ms(due)));
    }
}

#[test]
fn controller_resumed_vad_and_new_turn_do_not_inherit_blank_interim_state() {
    use ControllerStep::*;
    let trace = check_controller(
        "resumed VAD and next turn",
        config(false),
        vec![
            Frame(0, metadata()),
            Frame(0, start()),
            Frame(10, hello()),
            Frame(15, interim("", false)),
            Frame(20, stop()),
            Frame(100, start()),
            Frame(110, hello()),
            Frame(115, interim(" \t", true)),
            Frame(120, stop()),
            Tick(220),
            Tick(620),
            Tick(719),
            Tick(720),
            Frame(800, start()),
            Frame(810, hello()),
            Frame(820, stop()),
            Tick(919),
            Tick(920),
            Tick(1420),
        ],
        vec![
            (1, started_event()),
            (12, stopped()),
            (13, started_event()),
            (17, stopped()),
        ],
    );
    assert_eq!(trace.deadlines[4], Some(ms(620)));
    assert_eq!(trace.deadlines[8], Some(ms(720)));
    assert_eq!(trace.deadlines[15], Some(ms(920)));
    assert_eq!(trace.events.len(), 19);
}

#[test]
fn aggregator_frame_replay_is_rejected_but_interim_transcript_id_is_not_deduped() {
    for replay_same_frame in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, false);
        let metadata_frame = tagged(metadata(), "control-stt");
        let meta = process(&user, metadata_frame.clone(), Direction::Downstream, 0);
        let started = process(&user, tagged(start(), "start"), Direction::Downstream, 0);
        let hello_frame = tagged(hello(), "hello");
        let accepted = process(&user, hello_frame.clone(), Direction::Downstream, 10);
        let mut blank = tagged(interim(" \t", true), "blank");
        blank
            .metadata
            .insert("transcript_id".into(), "same-interim-segment".into());
        let first = process(&user, blank.clone(), Direction::Upstream, 15);
        let repair = process(
            &user,
            tagged(final_text("", true), "repair"),
            Direction::Downstream,
            16,
        );
        let second_frame = if replay_same_frame {
            blank.clone()
        } else {
            let mut fresh = tagged(interim(" \t", true), "blank");
            fresh.metadata = blank.metadata.clone();
            fresh
        };
        let second = process(&user, second_frame.clone(), Direction::Upstream, 17);
        let retained = user.transcript_frames().unwrap();
        let preview = user.interim_text().unwrap();
        let stop_frame = tagged(stop(), "stop");
        let vad_stop = process(&user, stop_frame.clone(), Direction::Upstream, 20);
        let before_floor = user.advance(ms(119)).unwrap();
        let floor = user.advance(ms(120)).unwrap();
        let floor_context = context.frame_data().unwrap();
        let floor_history = context.snapshot().unwrap().messages;
        let before_safety = user.advance(ms(619)).unwrap();
        let safety = user.advance(ms(620)).unwrap();
        let safety_context = context.frame_data().unwrap();
        let safety_history = context.snapshot().unwrap().messages;
        let after_safety = user.advance(ms(621)).unwrap();
        let end_frame = tagged(FrameData::End(Default::default()), "end");
        let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
        let after_end = user.advance(ms(10000)).unwrap();
        let final_history = context.snapshot().unwrap().messages;
        let final_deadline = user.deadline().unwrap();
        assert_eq!(meta, forwarded(metadata_frame, Direction::Downstream));
        assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
        for output in [
            accepted,
            first,
            repair,
            second,
            before_floor,
            before_safety,
            after_safety,
            after_end,
        ] {
            assert_eq!(output, AggregationOutput::default());
        }
        assert_eq!(vad_stop, forwarded(stop_frame, Direction::Upstream));
        if replay_same_frame {
            assert_eq!(retained, vec![hello_frame, blank]);
            assert_eq!(preview, "");
            assert_committed(&floor, &floor_context);
            assert_eq!(floor_history, vec![Message::text(Role::User, "hello")]);
            assert_eq!(safety, AggregationOutput::default());
        } else {
            assert_eq!(retained, vec![hello_frame, blank, second_frame]);
            assert_eq!(preview, " \t");
            assert_eq!(floor, AggregationOutput::default());
            assert!(floor_history.is_empty());
            assert_committed(&safety, &safety_context);
        }
        assert_eq!(safety_history, vec![Message::text(Role::User, "hello")]);
        assert_eq!(end, forwarded(end_frame, Direction::Downstream));
        assert_eq!(final_history, safety_history);
        assert_eq!(final_deadline, None);
    }
}

#[test]
fn aggregator_blank_interim_keeps_inactive_mute_and_terminal_admission() {
    for muted in [false, true] {
        let context = SharedContext::default();
        let user = LLMUserAggregator::new(
            context.clone(),
            UserAggregatorConfig {
                turn: config(false),
                eager: None,
                initially_muted: muted,
                user_idle_timeout: ms(0),
                mute_strategies: vec![],
                empty_user_turn: None,
            },
        );
        let blank = tagged(interim(" \t", true), "inactive-blank");
        let interim_output = process(&user, blank.clone(), Direction::Downstream, 15);
        let preview = user.interim_text().unwrap();
        let retained = user.transcript_frames().unwrap();
        let before_end = user.advance(ms(620)).unwrap();
        let end_frame = tagged(FrameData::End(Default::default()), "end");
        let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
        let late = process(
            &user,
            tagged(interim("", false), "late"),
            Direction::Upstream,
            701,
        );
        let after_end = user.advance(ms(10000)).unwrap();
        let history = context.snapshot().unwrap().messages;
        let after_retained = user.transcript_frames().unwrap();
        let after_preview = user.interim_text().unwrap();
        let deadline = user.deadline().unwrap();
        for output in [interim_output, before_end, late, after_end] {
            assert_eq!(output, AggregationOutput::default());
        }
        assert_eq!(preview, if muted { "" } else { " \t" });
        assert_eq!(retained, if muted { vec![] } else { vec![blank] });
        assert_eq!(end, forwarded(end_frame, Direction::Downstream));
        assert_eq!(after_retained, retained);
        assert_eq!(after_preview, "");
        assert!(history.is_empty());
        assert_eq!(deadline, None);
    }
}

#[test]
fn aggregator_eager_configuration_keeps_its_external_stop_lane() {
    let context = SharedContext::default();
    let user = LLMUserAggregator::new(
        context.clone(),
        UserAggregatorConfig {
            turn: config(false),
            eager: Some(EagerUserTurnConfig::default()),
            initially_muted: false,
            user_idle_timeout: ms(0),
            mute_strategies: vec![],
            empty_user_turn: None,
        },
    );
    let start_frame = tagged(FrameData::ProposedUserStartedSpeaking, "external-start");
    let started = process(&user, start_frame, Direction::Downstream, 0);
    let vad_start_frame = tagged(start(), "vad-start");
    let vad_start = process(&user, vad_start_frame.clone(), Direction::Downstream, 1);
    let final_frame = tagged(hello(), "hello");
    let accepted = process(&user, final_frame.clone(), Direction::Downstream, 10);
    let blank = tagged(interim(" \t", true), "blank");
    let interim = process(&user, blank.clone(), Direction::Upstream, 15);
    let retained = user.transcript_frames().unwrap();
    let preview = user.interim_text().unwrap();
    let stop_frame = tagged(stop(), "vad-stop");
    let vad_stop = process(&user, stop_frame.clone(), Direction::Downstream, 20);
    let floor = user.advance(ms(120)).unwrap();
    let safety = user.advance(ms(620)).unwrap();
    let before_end = context.snapshot().unwrap().messages;
    let end_frame = tagged(FrameData::End(Default::default()), "end");
    let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
    let after_end = user.advance(ms(10000)).unwrap();
    let history = context.snapshot().unwrap().messages;
    let final_context = context.frame_data().unwrap();
    let deadline = user.deadline().unwrap();
    assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
    assert_eq!(vad_start, forwarded(vad_start_frame, Direction::Downstream));
    for output in [accepted, interim, floor, safety, after_end] {
        assert_eq!(output, AggregationOutput::default());
    }
    assert_eq!(retained, vec![final_frame, blank]);
    assert_eq!(preview, " \t");
    assert_eq!(vad_stop, forwarded(stop_frame, Direction::Downstream));
    assert!(before_end.is_empty());
    assert_eq!(
        end.events,
        vec![AggregationEvent::UserTurnStopped {
            text: "hello".into(),
            reason: TurnStopReason::SessionEnd
        }]
    );
    let [AggregationAction::Push(inference), AggregationAction::Push(terminal)] =
        end.actions.as_slice()
    else {
        panic!("one terminal context followed by End: {end:?}");
    };
    assert_eq!(inference.direction, Direction::Downstream);
    assert_eq!(inference.frame.data, final_context);
    assert_eq!(
        terminal,
        &DirectedFrame::new(end_frame, Direction::Downstream)
    );
    assert_eq!(history, vec![Message::text(Role::User, "hello")]);
    assert_eq!(deadline, None);
}
