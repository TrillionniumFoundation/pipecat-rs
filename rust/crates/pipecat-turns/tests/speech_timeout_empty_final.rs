//! Existing-text SpeechTimeout finalization, source-derived from Python af9f9da9.
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
            service_name: "empty-final-fixture".into(),
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
fn transcript(text: &str, finalized: bool, detailed_noappend: bool) -> FrameData {
    if detailed_noappend {
        FrameData::TranscriptionDetailed(DetailedTranscriptionFrame {
            text: TextFrame {
                append_to_context: false,
                ..TextFrame::new(text)
            },
            user_id: "fixture-user".into(),
            timestamp: "fixture-time".into(),
            language: None,
            result: Some(serde_json::json!({"fixture": "empty-final"})),
            finalized,
        })
    } else {
        FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized,
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
// Complete both genuine nonempty-final controls before the first empty witness.
// The explicit policy has the same old bug and is not an empty-final control.
const CASES: [(&str, &str, bool, &str); 4] = [
    ("nonempty_legacy", "world", false, "hello world"),
    ("empty_legacy", "", false, "hello"),
    ("empty_detailed_noappend", "", true, "hello"),
    ("whitespace_detailed_noappend", " \t", true, "hello"),
];

#[test]
fn controller_existing_text_empty_final_releases_stt_at_200ms() {
    for (case, text, detailed, _) in CASES {
        for explicit in [false, true] {
            let mut controller = UserTurnController::new(config(explicit));
            let metadata = controller.process(&metadata(), ms(0));
            let started = controller.process(&start(), ms(0));
            let hello = controller.process(&transcript("hello", false, false), ms(10));
            let vad_stop = controller.process(&stop(), ms(20));
            let deadline = controller.deadline();
            let before_floor = controller.advance(ms(119));
            let floor = controller.advance(ms(120));
            let before_final = controller.advance(ms(199));
            let finalized = controller.process(&transcript(text, true, detailed), ms(200));
            let active_at_final = controller.is_active();
            let deadline_at_final = controller.deadline();
            let later = controller.advance(ms(201));
            let before_safety = controller.advance(ms(619));
            let old_safety = controller.advance(ms(620));
            let after_safety = controller.advance(ms(621));
            let terminal = controller.finish(false, TurnStopReason::SessionEnd);
            controller.cleanup();
            let cleaned = controller.advance(ms(10000));
            let final_deadline = controller.deadline();
            let final_active = controller.is_active();
            eprintln!("controller case={case} explicit={explicit} captured_through_cleanup: final_200ms={finalized:?}; active_200ms={active_at_final}; deadline_200ms={deadline_at_final:?}; old_620ms={old_safety:?}; terminal={terminal:?}; cleaned={cleaned:?}");

            assert!(metadata.is_empty());
            assert_eq!(
                started,
                vec![UserTurnEvent::Started {
                    emit_speaking: true,
                    interrupt: false,
                }]
            );
            assert!(hello.is_empty());
            assert!(vad_stop.is_empty());
            assert_eq!(deadline, Some(ms(620)));
            assert!(before_floor.is_empty());
            assert!(floor.is_empty());
            assert!(before_final.is_empty());
            // First old-red assertion is a public event, not just a deadline.
            assert_eq!(
                finalized,
                stopped(),
                "case={case} explicit={explicit}: final must stop at 200ms"
            );
            assert!(!active_at_final);
            assert_eq!(deadline_at_final, None);
            assert!(later.is_empty());
            assert!(before_safety.is_empty());
            assert!(old_safety.is_empty());
            assert!(after_safety.is_empty());
            assert!(terminal.is_empty());
            assert!(cleaned.is_empty());
            assert_eq!(final_deadline, None);
            assert!(!final_active);
            eprintln!("controller case={case} explicit={explicit} all_assertions_completed");
        }
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
fn assert_committed(output: &AggregationOutput, text: &str, context: &FrameData) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::UserTurnStopped {
            text: text.into(),
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
fn aggregator_existing_text_empty_final_commits_once_at_200ms() {
    for (case, text, detailed, expected_text) in CASES {
        for explicit in [false, true] {
            let context = SharedContext::default();
            let user = user(&context, explicit);
            let metadata_frame = tagged(metadata(), "stt");
            let metadata = process(&user, metadata_frame.clone(), Direction::Upstream, 0);
            let start_frame = tagged(start(), "start");
            let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
            let mut hello_frame = tagged(transcript("hello", false, false), "hello");
            hello_frame
                .metadata
                .insert("transcript_id".into(), "hello-segment".into());
            let hello = process(&user, hello_frame, Direction::Downstream, 10);
            let stop_frame = tagged(stop(), "stop");
            let vad_stop = process(&user, stop_frame.clone(), Direction::Upstream, 20);
            let deadline = user.deadline().unwrap();
            let before_floor = user.advance(ms(119)).unwrap();
            let floor = user.advance(ms(120)).unwrap();
            let before_final = user.advance(ms(199)).unwrap();
            let history_before_final = context.snapshot().unwrap();
            let mut final_frame = tagged(transcript(text, true, detailed), "final");
            final_frame
                .metadata
                .insert("transcript_id".into(), "final-segment".into());
            let finalized = process(&user, final_frame.clone(), Direction::Upstream, 200);
            let history_at_final = context.snapshot().unwrap();
            let context_at_final = context.frame_data().unwrap();
            let deadline_at_final = user.deadline().unwrap();
            let repeated_frame = process(&user, final_frame, Direction::Upstream, 201);
            let before_safety = user.advance(ms(619)).unwrap();
            let old_safety = user.advance(ms(620)).unwrap();
            let after_safety = user.advance(ms(621)).unwrap();
            let end_frame = tagged(FrameData::End(Default::default()), "end");
            let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
            let after_end = user.advance(ms(10000)).unwrap();
            let history_after_end = context.snapshot().unwrap();
            let final_deadline = user.deadline().unwrap();
            let final_idle = user.idle_deadline().unwrap();
            eprintln!("aggregator case={case} explicit={explicit} captured_through_End: final_200ms={finalized:?}; history_200ms={:?}; deadline_200ms={deadline_at_final:?}; old_620ms={old_safety:?}; End={end:?}; after_End={after_end:?}; history_after_End={:?}", history_at_final.messages, history_after_end.messages);

            assert_eq!(metadata, forwarded(metadata_frame, Direction::Upstream));
            assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
            let [AggregationAction::Push(original), AggregationAction::Broadcast(boundary)] =
                started.actions.as_slice()
            else {
                panic!(
                    "original VAD start and exactly one generated speaking boundary: {started:?}"
                );
            };
            assert_eq!(
                original,
                &DirectedFrame::new(start_frame.clone(), Direction::Downstream)
            );
            assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
            assert_ne!(boundary.id, start_frame.id);
            assert_eq!(hello, AggregationOutput::default());
            assert_eq!(vad_stop, forwarded(stop_frame, Direction::Upstream));
            assert_eq!(deadline, Some(ms(620)));
            assert_eq!(before_floor, AggregationOutput::default());
            assert_eq!(floor, AggregationOutput::default());
            assert_eq!(before_final, AggregationOutput::default());
            assert!(history_before_final.messages.is_empty());
            // First old-red assertion is the semantic stop/commit event.
            assert_eq!(
                finalized.events,
                vec![AggregationEvent::UserTurnStopped {
                    text: expected_text.into(),
                    reason: TurnStopReason::SpeechTimeout,
                }],
                "case={case} explicit={explicit}: final must commit at 200ms"
            );
            assert_committed(&finalized, expected_text, &context_at_final);
            assert_eq!(
                history_at_final.messages,
                vec![Message::text(Role::User, expected_text)]
            );
            assert_eq!(deadline_at_final, None);
            assert_eq!(repeated_frame, AggregationOutput::default());
            assert_eq!(before_safety, AggregationOutput::default());
            assert_eq!(old_safety, AggregationOutput::default());
            assert_eq!(after_safety, AggregationOutput::default());
            assert_eq!(end, forwarded(end_frame, Direction::Downstream));
            assert_eq!(after_end, AggregationOutput::default());
            assert_eq!(history_after_end.messages, history_at_final.messages);
            assert_eq!(final_deadline, None);
            assert_eq!(final_idle, None);
            eprintln!("aggregator case={case} explicit={explicit} all_assertions_completed");
        }
    }
}

// Candidate-only controls. The complete frozen baseline above stays unchanged.
fn controls_config(explicit: bool) -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: config(explicit),
        eager: None,
        initially_muted: false,
        user_idle_timeout: ms(0),
        mute_strategies: vec![],
        empty_user_turn: None,
    }
}
fn input(at: u64, data: FrameData) -> (u64, Option<Frame>) {
    (at, Some(Frame::new(data)))
}
fn tick(at: u64) -> (u64, Option<Frame>) {
    (at, None)
}
struct ControlTrace {
    frames: Vec<Option<Frame>>,
    outputs: Vec<AggregationOutput>,
    histories: Vec<Vec<Message>>,
    contexts: Vec<FrameData>,
    deadlines: Vec<Option<Duration>>,
    interims: Vec<String>,
    retained: Vec<Frame>,
    end_frame: Frame,
    end: AggregationOutput,
    after_end: AggregationOutput,
    late_final: AggregationOutput,
    stop_frame: Frame,
    late_stop: AggregationOutput,
    history: Vec<Message>,
    deadline: Option<Duration>,
    idle: Option<Duration>,
}
fn control_trace(config: UserAggregatorConfig, steps: Vec<(u64, Option<Frame>)>) -> ControlTrace {
    let context = SharedContext::default();
    let user = LLMUserAggregator::new(context.clone(), config);
    let mut frames = vec![];
    let mut outputs = vec![];
    let mut histories = vec![];
    let mut contexts = vec![];
    let mut deadlines = vec![];
    let mut interims = vec![];
    let mut now = 0;
    for (at, frame) in steps {
        now = at;
        let out = match &frame {
            Some(frame) => process(&user, frame.clone(), Direction::Downstream, at),
            None => user.advance(ms(at)).unwrap(),
        };
        frames.push(frame);
        outputs.push(out);
        histories.push(context.snapshot().unwrap().messages);
        contexts.push(context.frame_data().unwrap());
        deadlines.push(user.deadline().unwrap());
        interims.push(user.interim_text().unwrap());
    }
    let end_frame = tagged(FrameData::End(Default::default()), "control-end");
    let end = process(&user, end_frame.clone(), Direction::Downstream, now + 1);
    let after_end = user.advance(ms(now + 10000)).unwrap();
    let late_final = process(
        &user,
        Frame::new(transcript("", true, false)),
        Direction::Downstream,
        now + 10001,
    );
    let stop_frame = tagged(FrameData::Stop, "control-late-stop");
    let late_stop = process(&user, stop_frame.clone(), Direction::Upstream, now + 10002);
    ControlTrace {
        frames,
        outputs,
        histories,
        contexts,
        deadlines,
        interims,
        retained: user.transcript_frames().unwrap(),
        end_frame,
        end,
        after_end,
        late_final,
        stop_frame,
        late_stop,
        history: context.snapshot().unwrap().messages,
        deadline: user.deadline().unwrap(),
        idle: user.idle_deadline().unwrap(),
    }
}
#[derive(Clone, Copy)]
enum Expected {
    Quiet,
    Forward,
    Start,
    TranscriptStart,
    Commit(&'static str, TurnStopReason),
}
fn assert_control_trace(trace: &ControlTrace, expected: &[Expected]) {
    // All public operations, End and post-terminal probes ran before assertions.
    assert_eq!(trace.outputs.len(), expected.len());
    let mut messages = vec![];
    for (index, expected) in expected.iter().enumerate() {
        let out = &trace.outputs[index];
        match expected {
            Expected::Quiet => assert_eq!(*out, AggregationOutput::default(), "step {index}"),
            Expected::Forward => assert_eq!(
                *out,
                forwarded(trace.frames[index].clone().unwrap(), Direction::Downstream),
                "step {index}"
            ),
            Expected::Start | Expected::TranscriptStart => {
                assert_eq!(
                    out.events,
                    vec![AggregationEvent::UserTurnStarted],
                    "step {index}"
                );
                let boundary = if matches!(expected, Expected::Start) {
                    let [AggregationAction::Push(original), AggregationAction::Broadcast(boundary)] =
                        out.actions.as_slice()
                    else {
                        panic!("one original and one start: {out:?}");
                    };
                    assert_eq!(
                        *original,
                        DirectedFrame::new(
                            trace.frames[index].clone().unwrap(),
                            Direction::Downstream
                        )
                    );
                    boundary
                } else {
                    let [AggregationAction::Broadcast(boundary)] = out.actions.as_slice() else {
                        panic!("one transcript start: {out:?}");
                    };
                    boundary
                };
                assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
                assert_ne!(boundary.id, trace.frames[index].as_ref().unwrap().id);
            }
            Expected::Commit(text, reason) => {
                assert_eq!(
                    out.events,
                    vec![AggregationEvent::UserTurnStopped {
                        text: (*text).into(),
                        reason: *reason
                    }],
                    "step {index}"
                );
                let boundary = if text.is_empty() {
                    let [AggregationAction::Broadcast(boundary)] = out.actions.as_slice() else {
                        panic!("empty stop without context: {out:?}");
                    };
                    boundary
                } else {
                    let [AggregationAction::Broadcast(boundary), AggregationAction::Push(context)] =
                        out.actions.as_slice()
                    else {
                        panic!("one stop then one context: {out:?}");
                    };
                    assert_eq!(context.direction, Direction::Downstream);
                    assert_eq!(context.frame.data, trace.contexts[index]);
                    assert_ne!(boundary.id, context.frame.id);
                    messages.push(Message::text(Role::User, *text));
                    boundary
                };
                assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
                assert_eq!(
                    boundary.metadata.get("user_context_follows"),
                    Some(&serde_json::Value::Bool(!text.is_empty()))
                );
            }
        }
        assert_eq!(trace.histories[index], messages, "history at step {index}");
    }
    assert_eq!(
        trace.end,
        forwarded(trace.end_frame.clone(), Direction::Downstream)
    );
    assert_eq!(trace.after_end, AggregationOutput::default());
    assert_eq!(trace.late_final, AggregationOutput::default());
    assert_eq!(
        trace.late_stop,
        forwarded(trace.stop_frame.clone(), Direction::Upstream)
    );
    assert_eq!(trace.history, messages);
    assert_eq!(trace.deadline, None);
    assert_eq!(trace.idle, None);
}

#[test]
fn empty_final_observed_during_speech_or_before_floor_never_skips_silence() {
    use Expected::*;
    for explicit in [false, true] {
        for (text, detailed) in [("", false), (" \t", true)] {
            for during_speech in [false, true] {
                let mut steps = vec![
                    input(0, metadata()),
                    input(0, start()),
                    input(10, transcript("hello", false, false)),
                ];
                if during_speech {
                    steps.push(input(15, transcript(text, true, detailed)));
                    steps.push(input(20, stop()));
                } else {
                    steps.push(input(20, stop()));
                    steps.push(input(50, transcript(text, true, detailed)));
                }
                steps.extend([tick(119), tick(120), tick(620)]);
                // Exercise the public standalone controller on the exact same inputs.
                let mut controller = UserTurnController::new(config(explicit));
                let events: Vec<_> = steps
                    .iter()
                    .map(|(at, frame)| match frame {
                        Some(frame) => controller.process(&frame.data, ms(*at)),
                        None => controller.advance(ms(*at)),
                    })
                    .collect();
                let finish = controller.finish(false, TurnStopReason::SessionEnd);
                controller.cleanup();
                let cleaned = controller.advance(ms(10000));
                let trace = control_trace(controls_config(explicit), steps);
                assert_eq!(
                    events,
                    vec![
                        vec![],
                        vec![UserTurnEvent::Started {
                            emit_speaking: true,
                            interrupt: false
                        }],
                        vec![],
                        vec![],
                        vec![],
                        vec![],
                        stopped(),
                        vec![]
                    ]
                );
                assert!(finish.is_empty());
                assert!(cleaned.is_empty());
                let order = if during_speech {
                    [Quiet, Forward]
                } else {
                    [Forward, Quiet]
                };
                assert_control_trace(
                    &trace,
                    &[
                        Forward,
                        Start,
                        Quiet,
                        order[0],
                        order[1],
                        Quiet,
                        Commit("hello", TurnStopReason::SpeechTimeout),
                        Quiet,
                    ],
                );
                assert_eq!(trace.deadlines[4], Some(ms(120)));
            }
        }
    }
}

#[test]
fn resumed_vad_and_nonempty_interim_revoke_prior_blank_finality() {
    use Expected::*;
    for explicit in [false, true] {
        for resumed in [false, true] {
            let (steps, expected, due) = if resumed {
                (
                    vec![
                        input(0, metadata()),
                        input(0, start()),
                        input(10, transcript("hello", false, false)),
                        input(20, stop()),
                        input(50, transcript("", true, false)),
                        input(100, start()),
                        input(140, stop()),
                        tick(240),
                        tick(620),
                        tick(739),
                        tick(740),
                        tick(741),
                    ],
                    vec![
                        Forward,
                        Start,
                        Quiet,
                        Forward,
                        Quiet,
                        Forward,
                        Forward,
                        Quiet,
                        Quiet,
                        Quiet,
                        Commit("hello", TurnStopReason::SpeechTimeout),
                        Quiet,
                    ],
                    740,
                )
            } else {
                (
                    vec![
                        input(0, metadata()),
                        input(0, start()),
                        input(10, transcript("hello", false, false)),
                        input(15, transcript("", true, false)),
                        input(
                            17,
                            FrameData::InterimTranscription(TranscriptionFrame {
                                text: "still arriving".into(),
                                ..Default::default()
                            }),
                        ),
                        input(20, stop()),
                        tick(120),
                        tick(619),
                        tick(620),
                        tick(621),
                    ],
                    vec![
                        Forward,
                        Start,
                        Quiet,
                        Quiet,
                        Quiet,
                        Forward,
                        Quiet,
                        Quiet,
                        Commit("hello", TurnStopReason::SpeechTimeout),
                        Quiet,
                    ],
                    620,
                )
            };
            let mut controller = UserTurnController::new(config(explicit));
            let events: Vec<_> = steps
                .iter()
                .map(|(at, frame)| match frame {
                    Some(frame) => controller.process(&frame.data, ms(*at)),
                    None => controller.advance(ms(*at)),
                })
                .collect();
            let finish = controller.finish(false, TurnStopReason::SessionEnd);
            controller.cleanup();
            let cleaned = controller.advance(ms(10000));
            let trace = control_trace(controls_config(explicit), steps);
            for (index, out) in events.iter().enumerate() {
                let want = if index == 1 {
                    vec![UserTurnEvent::Started {
                        emit_speaking: true,
                        interrupt: false,
                    }]
                } else if index == events.len() - 2 {
                    stopped()
                } else {
                    vec![]
                };
                assert_eq!(*out, want, "resumed={resumed} due={due} step={index}");
            }
            assert!(finish.is_empty());
            assert!(cleaned.is_empty());
            assert_control_trace(&trace, &expected);
        }
    }
}

#[test]
fn duplicate_frame_or_transcript_identity_cannot_complete_the_wait() {
    use Expected::*;
    for explicit in [false, true] {
        let mut hello = Frame::new(transcript("hello", false, false));
        hello
            .metadata
            .insert("transcript_id".into(), "accepted".into());
        let mut duplicate_frame = hello.clone();
        duplicate_frame.data = transcript("", true, false);
        duplicate_frame
            .metadata
            .insert("transcript_id".into(), "fresh-but-same-frame".into());
        let mut duplicate_transcript = Frame::new(transcript(" \t", true, true));
        duplicate_transcript
            .metadata
            .insert("transcript_id".into(), "accepted".into());
        let mut accepted = Frame::new(transcript("", true, true));
        accepted
            .metadata
            .insert("transcript_id".into(), "fresh-final".into());
        let repeated = accepted.clone();
        let mut repeated_transcript = Frame::new(transcript("", true, false));
        repeated_transcript
            .metadata
            .insert("transcript_id".into(), "fresh-final".into());
        let trace = control_trace(
            controls_config(explicit),
            vec![
                input(0, metadata()),
                input(0, start()),
                (10, Some(hello)),
                input(20, stop()),
                tick(120),
                (200, Some(duplicate_frame)),
                (220, Some(duplicate_transcript)),
                tick(299),
                (300, Some(accepted)),
                (301, Some(repeated)),
                (302, Some(repeated_transcript)),
                tick(620),
            ],
        );
        assert_control_trace(
            &trace,
            &[
                Forward,
                Start,
                Quiet,
                Forward,
                Quiet,
                Quiet,
                Quiet,
                Quiet,
                Commit("hello", TurnStopReason::SpeechTimeout),
                Quiet,
                Quiet,
                Quiet,
            ],
        );
        assert_eq!(trace.deadlines[5], Some(ms(620)));
        assert_eq!(trace.deadlines[6], Some(ms(620)));
    }
}

#[test]
fn inactive_no_text_and_nonappended_only_turns_keep_their_existing_gates() {
    use Expected::*;
    for explicit in [false, true] {
        for text in ["", " \t"] {
            let inactive = control_trace(
                controls_config(explicit),
                vec![
                    input(0, metadata()),
                    input(200, transcript(text, true, false)),
                    tick(620),
                    tick(10000),
                ],
            );
            assert_control_trace(&inactive, &[Forward, Quiet, Quiet, Quiet]);
            for wait in [false, true] {
                let mut cfg = controls_config(explicit);
                cfg.turn.wait_for_transcript = wait;
                cfg.turn.stop_strategies = explicit.then(|| {
                    UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
                        ms(100),
                        wait,
                    )])
                    .unwrap()
                });
                let trace = control_trace(
                    cfg,
                    vec![
                        input(0, metadata()),
                        input(0, start()),
                        input(20, stop()),
                        input(50, transcript(text, true, false)),
                        tick(120),
                        tick(620),
                        tick(5020),
                        tick(5021),
                    ],
                );
                let (at_safety, at_watchdog) = if wait {
                    (Quiet, Commit("", TurnStopReason::Watchdog))
                } else {
                    (Commit("", TurnStopReason::SpeechTimeout), Quiet)
                };
                assert_control_trace(
                    &trace,
                    &[
                        Forward,
                        Start,
                        Forward,
                        Quiet,
                        Quiet,
                        at_safety,
                        at_watchdog,
                        Quiet,
                    ],
                );
            }
            // A previous append=false nonempty frame is not buffered context text.
            let trace = control_trace(
                controls_config(explicit),
                vec![
                    input(0, metadata()),
                    input(0, start()),
                    input(10, transcript("hello", false, true)),
                    input(20, stop()),
                    input(200, transcript(text, true, true)),
                    tick(620),
                    tick(621),
                ],
            );
            assert_control_trace(
                &trace,
                &[
                    Forward,
                    Start,
                    Quiet,
                    Forward,
                    Quiet,
                    Commit("", TurnStopReason::SpeechTimeout),
                    Quiet,
                ],
            );
        }
    }
}

#[test]
fn no_vad_blank_nonfinal_and_blank_interim_paths_are_not_broadened() {
    use Expected::*;
    for explicit in [false, true] {
        let no_vad = control_trace(
            controls_config(explicit),
            vec![
                input(0, metadata()),
                input(10, transcript("hello", false, false)),
                input(50, transcript("", true, false)),
                tick(109),
                tick(110),
                tick(620),
            ],
        );
        assert_control_trace(
            &no_vad,
            &[
                Forward,
                TranscriptStart,
                Quiet,
                Quiet,
                Commit("hello", TurnStopReason::SpeechTimeout),
                Quiet,
            ],
        );
        for data in [
            transcript("", false, false),
            transcript(" \t", false, true),
            FrameData::InterimTranscription(TranscriptionFrame::default()),
        ] {
            let trace = control_trace(
                controls_config(explicit),
                vec![
                    input(0, metadata()),
                    input(0, start()),
                    input(10, transcript("hello", false, false)),
                    input(20, stop()),
                    input(200, data),
                    tick(619),
                    tick(620),
                    tick(621),
                ],
            );
            assert_control_trace(
                &trace,
                &[
                    Forward,
                    Start,
                    Quiet,
                    Forward,
                    Quiet,
                    Quiet,
                    Commit("hello", TurnStopReason::SpeechTimeout),
                    Quiet,
                ],
            );
        }
    }
}

#[test]
fn custom_min_words_keeps_blank_final_as_noop() {
    use Expected::*;
    for explicit in [false, true] {
        let mut cfg = controls_config(explicit);
        cfg.turn.start_strategies = Some(
            UserTurnStartStrategies::new(vec![
                UserTurnStartStrategy::vad().with_interruptions(false),
                UserTurnStartStrategy::min_words(3, true).with_interruptions(false),
            ])
            .unwrap(),
        );
        let trace = control_trace(
            cfg,
            vec![
                input(0, metadata()),
                input(0, start()),
                input(10, transcript("hello", false, false)),
                input(20, stop()),
                input(200, transcript(" \t", true, true)),
                tick(619),
                tick(620),
                tick(621),
            ],
        );
        assert_control_trace(
            &trace,
            &[
                Forward,
                Start,
                Quiet,
                Forward,
                Quiet,
                Quiet,
                Commit("hello", TurnStopReason::SpeechTimeout),
                Quiet,
            ],
        );
    }
}

#[test]
fn model_flag_and_mixed_detectors_keep_original_stt_or_watchdog_behavior() {
    use Expected::*;
    for explicit in [false, true] {
        let mut cfg = controls_config(explicit);
        cfg.turn.require_model_completion = true;
        let trace = control_trace(
            cfg,
            vec![
                input(0, metadata()),
                input(0, start()),
                input(10, transcript("hello", false, false)),
                input(20, stop()),
                input(200, transcript("", true, false)),
                tick(620),
                tick(5020),
                tick(5021),
            ],
        );
        let (safety, watchdog) = if explicit {
            (Commit("hello", TurnStopReason::SpeechTimeout), Quiet)
        } else {
            (Quiet, Commit("hello", TurnStopReason::Watchdog))
        };
        assert_control_trace(
            &trace,
            &[
                Forward, Start, Quiet, Forward, Quiet, safety, watchdog, Quiet,
            ],
        );
    }
    for policies in [
        vec![
            UserTurnStopStrategy::speech_timeout(ms(100), true),
            UserTurnStopStrategy::turn_analyzer(true),
        ],
        vec![
            UserTurnStopStrategy::speech_timeout(ms(100), true),
            UserTurnStopStrategy::speech_timeout(ms(150), true),
        ],
    ] {
        let mut cfg = controls_config(false);
        cfg.turn.stop_strategies = Some(UserTurnStopStrategies::new(policies).unwrap());
        let trace = control_trace(
            cfg,
            vec![
                input(0, metadata()),
                input(0, start()),
                input(10, transcript("hello", false, false)),
                input(20, stop()),
                input(200, transcript("", true, false)),
                tick(619),
                tick(620),
                tick(621),
            ],
        );
        assert_control_trace(
            &trace,
            &[
                Forward,
                Start,
                Quiet,
                Forward,
                Quiet,
                Quiet,
                Commit("hello", TurnStopReason::SpeechTimeout),
                Quiet,
            ],
        );
    }
}

#[test]
fn eager_external_blank_finals_preserve_timer_preview_and_exact_match() {
    for text in ["", " \t"] {
        let context = SharedContext::default();
        let mut cfg = controls_config(false);
        // Eager configuration replaces caller start/stop settings with External.
        cfg.eager =
            Some(EagerUserTurnConfig::new(EagerMatchPolicy::Exact, ms(5000), 1024).unwrap());
        let user = LLMUserAggregator::new(context.clone(), cfg);
        let metadata_frame = Frame::new(metadata());
        let metadata = process(&user, metadata_frame.clone(), Direction::Downstream, 0);
        let started = process(
            &user,
            Frame::new(FrameData::ProposedUserStartedSpeaking),
            Direction::Downstream,
            0,
        );
        let vad_start_frame = Frame::new(start());
        let vad_start = process(&user, vad_start_frame.clone(), Direction::Downstream, 1);
        let prediction = process(
            &user,
            Frame::new(FrameData::EagerTranscription(TranscriptionFrame {
                text: "hello".into(),
                finalized: true,
                ..Default::default()
            })),
            Direction::Downstream,
            5,
        );
        // Optional evidence capture must not prevent End when an oracle is wrong.
        let capability = prediction.actions.iter().find_map(|action| match action {
            AggregationAction::Push(frame) => frame.frame.native_speculation().cloned(),
            _ => None,
        });
        let hello_frame = Frame::new(transcript("hello", false, false));
        let hello = process(&user, hello_frame.clone(), Direction::Downstream, 10);
        let preview_frame = Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
            text: "unfinished preview".into(),
            ..Default::default()
        }));
        let preview = process(&user, preview_frame.clone(), Direction::Downstream, 15);
        let stop_frame = Frame::new(stop());
        let vad_stop = process(&user, stop_frame.clone(), Direction::Downstream, 20);
        let waiting_stop = process(
            &user,
            Frame::new(FrameData::ProposedUserStoppedSpeaking),
            Direction::Downstream,
            30,
        );
        let preview_before_blank = user.interim_text().unwrap();
        let blank_frame = Frame::new(transcript(text, true, true));
        let blank = process(&user, blank_frame.clone(), Direction::Downstream, 200);
        let preview_after_blank = user.interim_text().unwrap();
        let pending = capability.as_ref().map(NativeSpeculation::status);
        let deadline = user.deadline().unwrap();
        let repeated = process(&user, blank_frame.clone(), Direction::Downstream, 201);
        let deadline_after_repeat = user.deadline().unwrap();
        let at_speech_safety = user.advance(ms(620)).unwrap();
        let before_external_timeout = user.advance(ms(699)).unwrap();
        let history_before_stop = context.snapshot().unwrap();
        let committed = user.advance(ms(700)).unwrap();
        let outcome = capability.as_ref().map(NativeSpeculation::status);
        let context_at_commit = context.frame_data().unwrap();
        let later = user.advance(ms(701)).unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), Direction::Downstream, 800);
        let after_end = user.advance(ms(10000)).unwrap();
        let history = context.snapshot().unwrap();
        let retained = user.transcript_frames().unwrap();
        let final_preview = user.interim_text().unwrap();
        let final_deadline = user.deadline().unwrap();
        let final_idle = user.idle_deadline().unwrap();

        // Every behavioral assertion, including capability admission, is after End.
        assert_eq!(metadata, forwarded(metadata_frame, Direction::Downstream));
        assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
        let [AggregationAction::Broadcast(boundary)] = started.actions.as_slice() else {
            panic!("proposed External start is consumed and emits one boundary: {started:?}");
        };
        assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
        assert_eq!(vad_start, forwarded(vad_start_frame, Direction::Downstream));
        assert!(prediction.events.is_empty());
        let [AggregationAction::Push(provisional)] = prediction.actions.as_slice() else {
            panic!("one admitted speculative context: {prediction:?}");
        };
        assert_eq!(provisional.direction, Direction::Downstream);
        let FrameData::LlmContextDetailed(provisional_data) = &provisional.frame.data else {
            panic!("rich speculative context");
        };
        assert!(provisional_data.speculation);
        assert_eq!(provisional_data.context.messages.len(), 1);
        assert_eq!(provisional_data.context.messages[0].role, ChatRole::User);
        assert_eq!(
            provisional_data.context.messages[0].content,
            MessageContent::Text("hello".into())
        );
        let capability =
            capability.expect("an admitted eager prediction must carry native speculation");
        assert_eq!(hello, AggregationOutput::default());
        assert_eq!(preview, AggregationOutput::default());
        assert_eq!(vad_stop, forwarded(stop_frame, Direction::Downstream));
        assert_eq!(waiting_stop, AggregationOutput::default());
        assert_eq!(preview_before_blank, "unfinished preview");
        assert_eq!(blank, AggregationOutput::default());
        assert_eq!(preview_after_blank, "");
        assert_eq!(pending, Some(SpeculationStatus::Pending));
        assert_eq!(
            deadline,
            Some(ms(700)),
            "External timeout is 500ms after the accepted blank"
        );
        assert_eq!(repeated, AggregationOutput::default());
        assert_eq!(deadline_after_repeat, Some(ms(700)));
        assert_eq!(at_speech_safety, AggregationOutput::default());
        assert_eq!(before_external_timeout, AggregationOutput::default());
        assert!(history_before_stop.messages.is_empty());
        assert_eq!(
            committed.events,
            vec![AggregationEvent::UserTurnStopped {
                text: "hello".into(),
                reason: TurnStopReason::External
            }]
        );
        if text.is_empty() {
            assert_eq!(outcome, Some(SpeculationStatus::Confirmed));
            let [AggregationAction::Broadcast(boundary)] = committed.actions.as_slice() else {
                panic!("exact empty final confirms without new inference: {committed:?}");
            };
            assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
            assert_eq!(boundary.native_speculation(), Some(&capability));
            assert_eq!(
                boundary.metadata.get("user_context_follows"),
                Some(&serde_json::Value::Bool(false))
            );
        } else {
            assert_eq!(outcome, Some(SpeculationStatus::Cancelled));
            let [AggregationAction::Broadcast(cancel), AggregationAction::Broadcast(boundary), AggregationAction::Push(context)] =
                committed.actions.as_slice()
            else {
                panic!("raw whitespace mismatch cancels, stops, then infers accepted hello: {committed:?}");
            };
            assert_eq!(cancel.data, FrameData::EagerEndOfTurnCancel);
            assert_eq!(cancel.native_speculation(), Some(&capability));
            assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
            assert_eq!(boundary.native_speculation(), None);
            assert_eq!(
                boundary.metadata.get("user_context_follows"),
                Some(&serde_json::Value::Bool(true))
            );
            assert_eq!(context.direction, Direction::Downstream);
            assert_eq!(context.frame.data, context_at_commit);
        }
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame, Direction::Downstream));
        assert_eq!(after_end, AggregationOutput::default());
        assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
        assert_eq!(retained, vec![hello_frame, preview_frame, blank_frame]);
        assert_eq!(final_preview, "");
        assert_eq!(final_deadline, None);
        assert_eq!(final_idle, None);
    }
}

#[test]
fn blank_final_preserves_a_deferred_request_until_its_correlated_verdict() {
    let context = SharedContext::default();
    let mut cfg = controls_config(false);
    cfg.turn.stop_strategies = Some(
        UserTurnStopStrategies::new(vec![
            UserTurnStopStrategy::speech_timeout(ms(100), true).deferred(),
            UserTurnStopStrategy::external_completion(),
        ])
        .unwrap(),
    );
    let user = LLMUserAggregator::new(context.clone(), cfg);
    let metadata_frame = Frame::new(metadata());
    let metadata = process(&user, metadata_frame.clone(), Direction::Downstream, 0);
    let start_frame = Frame::new(start());
    let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
    let hello = process(
        &user,
        Frame::new(transcript("hello", false, false)),
        Direction::Downstream,
        10,
    );
    let stop_frame = Frame::new(stop());
    let vad_stop = process(&user, stop_frame.clone(), Direction::Downstream, 20);
    let blank = process(
        &user,
        Frame::new(transcript("", true, false)),
        Direction::Downstream,
        200,
    );
    let requested = user.advance(ms(620)).unwrap();
    let request = requested
        .events
        .iter()
        .find_map(|event| match event {
            AggregationEvent::UserTurnInferenceRequested(request) => Some(request.clone()),
            _ => None,
        })
        .unwrap();
    let pending_before = user.pending_inference_request().unwrap();
    let blank_pending = process(
        &user,
        Frame::new(transcript(" \t", true, true)),
        Direction::Downstream,
        650,
    );
    let pending_after = user.pending_inference_request().unwrap();
    let before_verdict = context.snapshot().unwrap();
    let completed = user
        .complete_inference_at(request.request_id, ms(700))
        .unwrap();
    let context_at_commit = context.frame_data().unwrap();
    let pending_completed = user.pending_inference_request().unwrap();
    let later = user.advance(ms(800)).unwrap();
    let end_frame = Frame::new(FrameData::End(Default::default()));
    let end = process(&user, end_frame.clone(), Direction::Downstream, 900);
    let after_end = user.advance(ms(10000)).unwrap();
    let history = context.snapshot().unwrap();
    let deadline = user.deadline().unwrap();

    assert_eq!(metadata, forwarded(metadata_frame, Direction::Downstream));
    assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
    let [AggregationAction::Push(original), AggregationAction::Broadcast(boundary)] =
        started.actions.as_slice()
    else {
        panic!("one original VAD start and boundary");
    };
    assert_eq!(
        *original,
        DirectedFrame::new(start_frame, Direction::Downstream)
    );
    assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
    assert_eq!(hello, AggregationOutput::default());
    assert_eq!(vad_stop, forwarded(stop_frame, Direction::Downstream));
    assert_eq!(blank, AggregationOutput::default());
    assert!(requested.actions.is_empty());
    assert_eq!(
        requested.events,
        vec![AggregationEvent::UserTurnInferenceRequested(
            UserTurnInferenceRequest {
                request_id: request.request_id,
                strategy_index: 0,
                text: "hello".into(),
                reason: TurnStopReason::SpeechTimeout
            }
        )]
    );
    assert_eq!(pending_before, Some(request.request_id));
    assert_eq!(blank_pending, AggregationOutput::default());
    assert_eq!(pending_after, pending_before);
    assert!(before_verdict.messages.is_empty());
    assert_eq!(
        completed.events,
        vec![AggregationEvent::UserTurnStopped {
            text: "hello".into(),
            reason: TurnStopReason::ExternalCompletion
        }]
    );
    let [AggregationAction::Broadcast(boundary), AggregationAction::Push(context)] =
        completed.actions.as_slice()
    else {
        panic!("one stop and one context: {completed:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
    assert_eq!(
        boundary.metadata.get("user_context_follows"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(context.direction, Direction::Downstream);
    assert_eq!(context.frame.data, context_at_commit);
    assert_eq!(pending_completed, None);
    assert_eq!(later, AggregationOutput::default());
    assert_eq!(end, forwarded(end_frame, Direction::Downstream));
    assert_eq!(after_end, AggregationOutput::default());
    assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
    assert_eq!(deadline, None);
}

#[test]
fn mute_and_terminal_admission_still_precede_blank_finalization() {
    for explicit in [false, true] {
        for boundary in [
            FrameData::UserMuteStarted,
            FrameData::End(Default::default()),
            FrameData::Stop,
            FrameData::Cancel(Default::default()),
        ] {
            let muted = matches!(boundary, FrameData::UserMuteStarted);
            let cancelled = matches!(boundary, FrameData::Cancel(_));
            let context = SharedContext::default();
            let user = LLMUserAggregator::new(context.clone(), controls_config(explicit));
            let metadata_frame = Frame::new(metadata());
            let metadata = process(&user, metadata_frame.clone(), Direction::Downstream, 0);
            let start_frame = Frame::new(start());
            let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
            let hello = process(
                &user,
                Frame::new(transcript("hello", false, false)),
                Direction::Downstream,
                10,
            );
            let stop_frame = Frame::new(stop());
            let vad_stop = process(&user, stop_frame.clone(), Direction::Downstream, 20);
            let boundary_frame = tagged(boundary, "pre-final-boundary");
            let closed = process(&user, boundary_frame.clone(), Direction::Downstream, 50);
            let context_at_close = context.frame_data().unwrap();
            let blank = process(
                &user,
                Frame::new(transcript("", true, false)),
                Direction::Downstream,
                200,
            );
            let old_safety = user.advance(ms(620)).unwrap();
            let end_frame = Frame::new(FrameData::End(Default::default()));
            let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
            let after_end = user.advance(ms(10000)).unwrap();
            let history = context.snapshot().unwrap();
            let deadline = user.deadline().unwrap();

            assert_eq!(metadata, forwarded(metadata_frame, Direction::Downstream));
            assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
            let [AggregationAction::Push(original), AggregationAction::Broadcast(started_boundary)] =
                started.actions.as_slice()
            else {
                panic!("original start and generated boundary");
            };
            assert_eq!(
                *original,
                DirectedFrame::new(start_frame, Direction::Downstream)
            );
            assert_eq!(started_boundary.data, FrameData::UserStartedSpeaking);
            assert_eq!(hello, AggregationOutput::default());
            assert_eq!(vad_stop, forwarded(stop_frame, Direction::Downstream));
            assert_eq!(
                closed.events,
                vec![AggregationEvent::UserTurnStopped {
                    text: "hello".into(),
                    reason: if muted {
                        TurnStopReason::Muted
                    } else {
                        TurnStopReason::SessionEnd
                    }
                }]
            );
            if muted {
                let [AggregationAction::Broadcast(stopped), AggregationAction::Push(original)] =
                    closed.actions.as_slice()
                else {
                    panic!("mute stop and original control: {closed:?}");
                };
                assert_eq!(stopped.data, FrameData::UserStoppedSpeaking);
                assert_eq!(
                    stopped.metadata.get("user_context_follows"),
                    Some(&serde_json::Value::Bool(false))
                );
                assert_eq!(
                    *original,
                    DirectedFrame::new(boundary_frame, Direction::Downstream)
                );
            } else if cancelled {
                assert_eq!(
                    closed.actions,
                    forwarded(boundary_frame, Direction::Downstream).actions
                );
            } else {
                let [AggregationAction::Push(context), AggregationAction::Push(original)] =
                    closed.actions.as_slice()
                else {
                    panic!("terminal context flush then original control: {closed:?}");
                };
                assert_eq!(context.direction, Direction::Downstream);
                assert_eq!(context.frame.data, context_at_close);
                assert_eq!(
                    *original,
                    DirectedFrame::new(boundary_frame, Direction::Downstream)
                );
            }
            assert_eq!(blank, AggregationOutput::default());
            assert_eq!(old_safety, AggregationOutput::default());
            assert_eq!(end, forwarded(end_frame, Direction::Downstream));
            assert_eq!(after_end, AggregationOutput::default());
            assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
            assert_eq!(deadline, None);
        }
    }
}

#[test]
fn accepted_blank_final_clears_preview_before_floor_but_rejections_do_not() {
    use Expected::*;
    for explicit in [false, true] {
        let mut hello = Frame::new(transcript("hello", false, false));
        hello
            .metadata
            .insert("transcript_id".into(), "hello".into());
        let preview = Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
            text: "unfinished preview".into(),
            ..Default::default()
        }));
        let mut same_frame = hello.clone();
        same_frame.data = transcript("", true, false);
        let mut same_transcript = Frame::new(transcript("", true, true));
        same_transcript
            .metadata
            .insert("transcript_id".into(), "hello".into());
        let trace = control_trace(
            controls_config(explicit),
            vec![
                input(0, metadata()),
                input(0, start()),
                (10, Some(hello.clone())),
                (15, Some(preview.clone())),
                input(20, stop()),
                (40, Some(same_frame)),
                (45, Some(same_transcript)),
                input(50, transcript(" \t", true, true)),
                tick(119),
                tick(120),
                tick(620),
                tick(621),
            ],
        );
        assert_control_trace(
            &trace,
            &[
                Forward,
                Start,
                Quiet,
                Quiet,
                Forward,
                Quiet,
                Quiet,
                Quiet,
                Quiet,
                Commit("hello", TurnStopReason::SpeechTimeout),
                Quiet,
                Quiet,
            ],
        );
        for index in [3, 4, 5, 6] {
            assert_eq!(trace.interims[index], "unfinished preview");
        }
        for index in [7, 8, 11] {
            assert_eq!(trace.interims[index], "");
        }
        assert_eq!(trace.retained, vec![hello, preview]);
    }
}

#[test]
fn pending_adopted_external_stop_is_not_released_by_blank_completion() {
    let context = SharedContext::default();
    let user = LLMUserAggregator::new(context.clone(), controls_config(false));
    let metadata_frame = Frame::new(metadata());
    let metadata = process(&user, metadata_frame.clone(), Direction::Downstream, 0);
    let start_frame = Frame::new(start());
    let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
    let hello = process(
        &user,
        Frame::new(transcript("hello", false, false)),
        Direction::Downstream,
        10,
    );
    let interim = process(
        &user,
        Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
            text: "tail".into(),
            ..Default::default()
        })),
        Direction::Downstream,
        15,
    );
    let external_frame = Frame::new(FrameData::UserStoppedSpeaking);
    let external = process(&user, external_frame.clone(), Direction::Upstream, 20);
    let blank = process(
        &user,
        Frame::new(transcript("", true, false)),
        Direction::Downstream,
        200,
    );
    let preview = user.interim_text().unwrap();
    let uncommitted = context.snapshot().unwrap();
    let completed = process(
        &user,
        Frame::new(transcript("world", true, false)),
        Direction::Downstream,
        300,
    );
    let committed = context.frame_data().unwrap();
    let later = user.advance(ms(620)).unwrap();
    let end_frame = Frame::new(FrameData::End(Default::default()));
    let end = process(&user, end_frame.clone(), Direction::Downstream, 700);
    let after_end = user.advance(ms(10000)).unwrap();
    let history = context.snapshot().unwrap();
    let deadline = user.deadline().unwrap();

    assert_eq!(metadata, forwarded(metadata_frame, Direction::Downstream));
    assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
    let [AggregationAction::Push(original), AggregationAction::Broadcast(boundary)] =
        started.actions.as_slice()
    else {
        panic!("original VAD start and generated boundary");
    };
    assert_eq!(
        *original,
        DirectedFrame::new(start_frame, Direction::Downstream)
    );
    assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
    assert_eq!(hello, AggregationOutput::default());
    assert_eq!(interim, AggregationOutput::default());
    assert_eq!(external, forwarded(external_frame, Direction::Upstream));
    assert_eq!(blank, AggregationOutput::default());
    assert_eq!(preview, "tail");
    assert!(uncommitted.messages.is_empty());
    assert_eq!(
        completed.events,
        vec![AggregationEvent::UserTurnStopped {
            text: "hello world".into(),
            reason: TurnStopReason::External
        }]
    );
    let [AggregationAction::Push(context)] = completed.actions.as_slice() else {
        panic!("adopted stop emits context without a duplicate boundary: {completed:?}");
    };
    assert_eq!(context.direction, Direction::Downstream);
    assert_eq!(context.frame.data, committed);
    assert_eq!(later, AggregationOutput::default());
    assert_eq!(end, forwarded(end_frame, Direction::Downstream));
    assert_eq!(after_end, AggregationOutput::default());
    assert_eq!(
        history.messages,
        vec![Message::text(Role::User, "hello world")]
    );
    assert_eq!(deadline, None);
}
