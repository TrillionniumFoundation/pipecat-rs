use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use pipecat_vad::{
    MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnCancellation, SmartTurnParams,
    SmartTurnPrediction, SmartTurnPredictor, VadError,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct Call {
    samples: Vec<i16>,
    cancellation: SmartTurnCancellation,
    reply: mpsc::Sender<SmartTurnPrediction>,
}
struct Controlled {
    calls: UnboundedSender<Call>,
    active: Arc<AtomicUsize>,
}
impl SmartTurnPredictor for Controlled {
    fn predict(
        &mut self,
        samples: &[i16],
        rate: u32,
        cancellation: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        assert_eq!(rate, 16_000);
        self.active.fetch_add(1, Ordering::SeqCst);
        let (reply, receive) = mpsc::channel();
        self.calls
            .send(Call {
                samples: samples.to_vec(),
                cancellation: cancellation.clone(),
                reply,
            })
            .unwrap();
        let result = loop {
            if cancellation.is_cancelled() {
                break Err(VadError::Cancelled);
            }
            match receive.recv_timeout(Duration::from_millis(1)) {
                Ok(value) => break Ok(value),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => break Err(VadError::Cancelled),
            }
        };
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}
fn complete(call: &Call, complete: bool) {
    call.reply
        .send(SmartTurnPrediction {
            probability: if complete { 0.9 } else { 0.1 },
            complete,
        })
        .unwrap();
}
fn configured(
    timeout: Duration,
) -> (
    Arc<SmartTurnUserAggregator>,
    UnboundedReceiver<Call>,
    Arc<AtomicUsize>,
) {
    configured_params(timeout, SmartTurnParams::default(), true)
}
fn configured_params(
    timeout: Duration,
    params: SmartTurnParams,
    wait_for_transcript: bool,
) -> (
    Arc<SmartTurnUserAggregator>,
    UnboundedReceiver<Call>,
    Arc<AtomicUsize>,
) {
    configured_policies(timeout, params, wait_for_transcript, None)
}
fn configured_policies(
    timeout: Duration,
    params: SmartTurnParams,
    wait_for_transcript: bool,
    stop_strategies: Option<UserTurnStopStrategies>,
) -> (
    Arc<SmartTurnUserAggregator>,
    UnboundedReceiver<Call>,
    Arc<AtomicUsize>,
) {
    let (calls, receive) = unbounded_channel();
    let active = Arc::new(AtomicUsize::new(0));
    let analyzer = SmartTurnAnalyzer::with_predictor(
        Controlled {
            calls,
            active: active.clone(),
        },
        16_000,
        params,
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    let user = Arc::new(SmartTurnUserAggregator::new(
        SharedContext::new(Context::default()),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                stt_timeout: timeout,
                stop_timeout: Duration::from_secs(30),
                wait_for_transcript,
                stop_strategies,
                ..Default::default()
            },
            ..Default::default()
        },
        analyzer,
    ));
    (user, receive, active)
}
fn start() -> Frame {
    Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    }))
}
fn stop() -> Frame {
    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    }))
}
fn audio(sample: i16) -> Frame {
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new([sample.to_le_bytes(); 320].concat(), 16_000, 1).unwrap(),
    ))
}
fn transcript(text: &str, finalized: bool) -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized,
        ..Default::default()
    }))
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn recv_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let next = worker.recv().await.expect("pipeline output");
            if let FrameData::Error(error) = &next.frame.data {
                panic!("processor error: {error:?}");
            }
            if predicate(&next.frame.data) {
                return next;
            }
        }
    })
    .await
    .expect("frame deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        Frame::new(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("barrier".into()),
        })),
    )
    .await;
    recv_until(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn boot(user: Arc<SmartTurnUserAggregator>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![user])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |data| data.is_start()).await;
    worker
}
async fn call(calls: &mut UnboundedReceiver<Call>) -> Call {
    tokio::time::timeout(Duration::from_secs(5), calls.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn end(worker: &mut PipelineWorker) {
    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}

#[tokio::test]
async fn smart_turn_forwards_idle_updates_and_owns_idle_callbacks() {
    let (user, _, _) = configured(Duration::ZERO);
    user.user()
        .on_user_turn_idle(|ctx| async move {
            ctx.push(
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("idle"))),
                Direction::Downstream,
            )
            .await
        })
        .unwrap();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |data| data.is_start()).await;
    send(
        &worker,
        Frame::new(FrameData::UserIdleTimeoutUpdate { timeout: 0.02 }),
    )
    .await;
    send(&worker, Frame::new(FrameData::BotStoppedSpeaking)).await;
    recv_until(
        &mut worker,
        |data| matches!(data, FrameData::TtsSpeak(text) if text.text == "idle"),
    )
    .await;
    assert!(user.user().waiting_for_user().unwrap());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(
        &worker,
        Frame::new(FrameData::UserIdleTimeoutUpdate { timeout: 60.0 }),
    )
    .await;
    barrier(&mut worker).await;
    end(&mut worker).await;
    assert_eq!(user.user().idle_deadline().unwrap(), None);
}

#[tokio::test]
async fn smart_turn_empty_model_completion_rearms_idle_without_bot_speech() {
    let (user, mut calls, active) =
        configured_params(Duration::ZERO, SmartTurnParams::default(), false);
    user.user()
        .on_user_turn_idle(|ctx| async move {
            ctx.push(
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("empty-turn idle"))),
                Direction::Downstream,
            )
            .await
        })
        .unwrap();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |data| data.is_start()).await;
    send(
        &worker,
        Frame::new(FrameData::UserIdleTimeoutUpdate { timeout: 0.02 }),
    )
    .await;
    send(&worker, start()).await;
    send(&worker, audio(7)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    complete(&call(&mut calls).await, true);
    recv_until(
        &mut worker,
        |data| matches!(data, FrameData::TtsSpeak(text) if text.text == "empty-turn idle"),
    )
    .await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    end(&mut worker).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn final_transcript_and_model_verdict_gate_in_both_arrival_orders() {
    for final_first in [false, true] {
        let (user, mut calls, _) = configured(Duration::from_secs(10));
        let mut worker = boot(user.clone()).await;
        send(&worker, start()).await;
        send(&worker, audio(7)).await;
        barrier(&mut worker).await;
        send(&worker, stop()).await;
        let prediction = call(&mut calls).await;
        assert_eq!(prediction.samples, vec![7; 320]);
        if final_first {
            send(&worker, transcript("ready", true)).await;
            barrier(&mut worker).await;
            assert!(user.context().snapshot().unwrap().messages.is_empty());
            complete(&prediction, true);
        } else {
            complete(&prediction, true);
            recv_until(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
            assert!(user.context().snapshot().unwrap().messages.is_empty());
            send(&worker, transcript("ready", true)).await;
        }
        recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
        assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
        assert_eq!(user.buffered_samples().await, 0);
        end(&mut worker).await;
    }
}

#[tokio::test]
async fn delayed_prediction_does_not_restart_physical_stop_p99_budget() {
    let (user, mut calls, _) = configured(Duration::from_millis(350));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("late model", false)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    let released = tokio::time::Instant::now();
    complete(&prediction, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert!(
        released.elapsed() < Duration::from_millis(100),
        "expired P99 must not start a fresh wait"
    );
    end(&mut worker).await;
}

#[tokio::test]
async fn resumed_speech_cancels_stale_result_and_retains_audio() {
    let (user, mut calls, _) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(11)).await;
    send(&worker, transcript("first", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let stale = call(&mut calls).await;
    send(&worker, start()).await;
    send(&worker, audio(22)).await;
    barrier(&mut worker).await;
    assert!(stale.cancellation.is_cancelled());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, stop()).await;
    let current = call(&mut calls).await;
    assert_eq!(current.samples, [vec![11; 320], vec![22; 320]].concat());
    complete(&current, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
    assert!(
        user.context().snapshot().unwrap().messages.is_empty(),
        "old finalized flag must be cleared on resume"
    );
    send(&worker, transcript("second", true)).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("first second".into()))
    );
    end(&mut worker).await;
}

#[tokio::test]
async fn silence_timeout_cancels_prediction_and_stops_without_fake_model_metrics() {
    let (user, mut calls, _) = configured_params(
        Duration::from_secs(10),
        SmartTurnParams {
            stop_secs: 0.02,
            ..Default::default()
        },
        true,
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(3)).await;
    send(&worker, transcript("timeout", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    send(&worker, audio(0)).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(user.buffered_samples().await, 0);
    end(&mut worker).await;
}

#[tokio::test]
async fn end_cancels_native_inference_joins_and_flushes_once() {
    let (user, mut calls, active) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("accepted", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    end(&mut worker).await;
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(user.buffered_samples().await, 0);
}

#[tokio::test]
async fn cancel_cleans_inference_without_llm_response() {
    let (user, mut calls, active) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("accepted", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    worker.cancel("test").await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    while let Some(frame) = worker.recv().await {
        assert!(!matches!(frame.frame.data, FrameData::LlmContext(_)));
    }
}

#[tokio::test]
async fn detailed_vad_timestamp_and_metadata_discount_delivery_delay() {
    let (user, mut calls, _) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(
        &worker,
        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: "synthetic".into(),
                user_turn_strategies: None,
            },
            ttfs_p99_latency: 0.4,
        })),
    )
    .await;
    send(
        &worker,
        Frame::new(FrameData::VadUserStartedSpeakingDetailed(
            VadUserStartedSpeakingFrame {
                start_secs: 0.1,
                timestamp: 0.0,
            },
        )),
    )
    .await;
    send(&worker, audio(1)).await;
    send(
        &worker,
        Frame::new(FrameData::TranscriptionDetailed(
            DetailedTranscriptionFrame {
                text: TextFrame::new("detailed"),
                finalized: false,
                user_id: String::new(),
                timestamp: String::new(),
                language: None,
                result: None,
            },
        )),
    )
    .await;
    barrier(&mut worker).await;
    send(
        &worker,
        Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
            VadUserStoppedSpeakingFrame {
                stop_secs: 0.2,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64()
                    - 0.3,
            },
        )),
    )
    .await;
    let prediction = call(&mut calls).await;
    let released = tokio::time::Instant::now();
    complete(&prediction, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert!(released.elapsed() < Duration::from_millis(100));
    end(&mut worker).await;
}

#[tokio::test]
async fn model_completion_without_transcript_can_end_realtime_turn() {
    let (user, mut calls, _) =
        configured_params(Duration::from_secs(10), SmartTurnParams::default(), false);
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    complete(&prediction, true);
    recv_until(&mut worker, |data| {
        matches!(data, FrameData::UserStoppedSpeaking)
    })
    .await;
    assert_eq!(user.buffered_samples().await, 0);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    end(&mut worker).await;
}

#[tokio::test]
async fn transcript_only_fallback_waits_for_inactivity_without_model() {
    let (user, mut calls, _) = configured(Duration::from_millis(350));
    let mut worker = boot(user.clone()).await;
    send(&worker, transcript("without VAD", false)).await;
    barrier(&mut worker).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert!(calls.try_recv().is_err());
    end(&mut worker).await;
}

#[tokio::test]
async fn repeated_resume_uses_one_managed_task_and_one_native_predictor() {
    let (user, mut calls, active) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    for _ in 0..30 {
        send(&worker, start()).await;
        send(&worker, audio(1)).await;
        barrier(&mut worker).await;
        send(&worker, stop()).await;
        let prediction = call(&mut calls).await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        send(&worker, start()).await;
        barrier(&mut worker).await;
        assert!(prediction.cancellation.is_cancelled());
    }
    end(&mut worker).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn interim_after_final_revokes_finalization_without_vad_resume() {
    let (user, mut calls, _) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("first", true)).await;
    send(
        &worker,
        Frame::new(FrameData::InterimTranscriptionDetailed(
            DetailedTranscriptionFrame {
                text: TextFrame::new("second pending"),
                user_id: String::new(),
                timestamp: String::new(),
                language: None,
                result: None,
                finalized: false,
            },
        )),
    )
    .await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    complete(&prediction, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, transcript("second", true)).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("first second".into()))
    );
    end(&mut worker).await;
}

#[tokio::test]
async fn external_stop_invalidates_model_and_clears_audio() {
    let (user, mut calls, _) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("external", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    send(&worker, Frame::new(FrameData::UserStoppedSpeaking)).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(user.buffered_samples().await, 0);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

#[tokio::test]
async fn user_audio_subclass_feeds_model_and_mute_cancels_buffered_turn() {
    let (user, mut calls, _) = configured(Duration::from_secs(10));
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    let FrameData::InputAudio(user_audio) = audio(8).data else {
        unreachable!()
    };
    send(
        &worker,
        Frame::new(FrameData::UserAudio(UserAudioRawFrame {
            audio: user_audio,
            user_id: "speaker".into(),
        })),
    )
    .await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    assert_eq!(prediction.samples, vec![8; 320]);
    send(&worker, Frame::new(FrameData::UserMuteStarted)).await;
    barrier(&mut worker).await;
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(user.buffered_samples().await, 0);
    send(&worker, start()).await;
    let FrameData::InputAudio(user_audio) = audio(9).data else {
        unreachable!()
    };
    send(
        &worker,
        Frame::new(FrameData::UserAudio(UserAudioRawFrame {
            audio: user_audio,
            user_id: "speaker".into(),
        })),
    )
    .await;
    barrier(&mut worker).await;
    assert_eq!(user.buffered_samples().await, 0);
    end(&mut worker).await;
}
#[tokio::test]
async fn incomplete_verdict_keeps_turn_open_past_p99_then_resume_reanalyzes() {
    let (user, mut calls, _) = configured(Duration::ZERO);
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("not finished", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let first = call(&mut calls).await;
    complete(&first, false);
    recv_until(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
    barrier(&mut worker).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(user.buffered_samples().await, 320);
    send(&worker, start()).await;
    send(&worker, audio(2)).await;
    send(&worker, transcript("now finished", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let second = call(&mut calls).await;
    complete(&second, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

struct DeferredCancellation(UnboundedSender<Call>);
impl SmartTurnPredictor for DeferredCancellation {
    fn predict(
        &mut self,
        samples: &[i16],
        _: u32,
        cancellation: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        let (reply, receive) = mpsc::channel();
        self.0
            .send(Call {
                samples: samples.to_vec(),
                cancellation: cancellation.clone(),
                reply,
            })
            .unwrap();
        receive
            .recv_timeout(Duration::from_secs(3))
            .map_err(|_| VadError::Cancelled)
    }
}
fn wrap_predictor(
    predictor: impl SmartTurnPredictor,
    config: UserTurnConfig,
) -> Arc<SmartTurnUserAggregator> {
    Arc::new(SmartTurnUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: config,
            ..Default::default()
        },
        SmartTurnAnalyzer::with_predictor(
            predictor,
            16_000,
            SmartTurnParams::default(),
            Arc::new(MonotonicSmartTurnClock::default()),
            None,
        )
        .unwrap(),
    ))
}

#[tokio::test]
async fn empty_model_verdict_uses_default_recovery_without_restarting_idle_or_model_work() {
    let (calls, mut received) = unbounded_channel();
    let active = Arc::new(AtomicUsize::new(0));
    let user = wrap_predictor(
        Controlled {
            calls,
            active: active.clone(),
        },
        UserTurnConfig {
            wait_for_transcript: false,
            stt_timeout: Duration::ZERO,
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(3)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut received).await;
    complete(&prediction, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    let messages = user.context().snapshot().unwrap().messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, pipecat_services::Role::Developer);
    assert_eq!(
        messages[0].content,
        Some(Content::Text(
            DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT.into()
        ))
    );
    assert!(!user.user().waiting_for_user().unwrap());
    assert_eq!(user.user().idle_deadline().unwrap(), None);
    assert_eq!(user.user().deadline().unwrap(), None);
    barrier(&mut worker).await;
    assert!(received.try_recv().is_err());
    end(&mut worker).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn resumed_stop_coalesces_until_old_predictor_acknowledges_cancellation() {
    let (send_calls, mut calls) = unbounded_channel();
    let user = wrap_predictor(
        DeferredCancellation(send_calls),
        UserTurnConfig {
            enable_interruptions: false,
            stt_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(3)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let first = call(&mut calls).await;
    send(&worker, start()).await;
    send(&worker, audio(4)).await;
    send(&worker, transcript("latest turn", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    barrier(&mut worker).await;
    assert!(first.cancellation.is_cancelled());
    assert!(
        calls.try_recv().is_err(),
        "do not launch overlapping prediction work"
    );
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    complete(&first, true);
    let second = call(&mut calls).await;
    assert_eq!(second.samples, [vec![3; 320], vec![4; 320]].concat());
    assert!(
        user.context().snapshot().unwrap().messages.is_empty(),
        "canceled COMPLETE was stale"
    );
    complete(&second, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

#[tokio::test]
async fn end_discards_coalesced_request_and_joins_slow_cancellation() {
    let (send_calls, mut calls) = unbounded_channel();
    let user = wrap_predictor(
        DeferredCancellation(send_calls),
        UserTurnConfig {
            enable_interruptions: false,
            stt_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(3)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let first = call(&mut calls).await;
    send(&worker, start()).await;
    send(&worker, audio(4)).await;
    send(&worker, transcript("accepted before end", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    barrier(&mut worker).await;
    worker.end().await.unwrap();
    // End must invalidate queued inference before the slow native run exits.
    tokio::time::sleep(Duration::from_millis(10)).await;
    complete(&first, true);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Ended
    );
    assert!(calls.try_recv().is_err());
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(user.buffered_samples().await, 0);
}

#[tokio::test]
async fn supervisor_survives_user_start_interruption_and_emits_context() {
    let (send_calls, mut calls) = unbounded_channel();
    let user = wrap_predictor(
        Controlled {
            calls: send_calls,
            active: Arc::new(AtomicUsize::new(0)),
        },
        UserTurnConfig {
            enable_interruptions: true,
            stt_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
    barrier(&mut worker).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("after interruption", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    complete(&prediction, true);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

struct FailOnce(bool);
impl SmartTurnPredictor for FailOnce {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        if !std::mem::replace(&mut self.0, true) {
            Err(VadError::Model("injected failure".into()))
        } else {
            Ok(SmartTurnPrediction {
                probability: 0.9,
                complete: true,
            })
        }
    }
}

#[tokio::test]
async fn model_error_is_nonfatal_and_supervisor_processes_next_utterance() {
    let user = wrap_predictor(
        FailOnce(false),
        UserTurnConfig {
            enable_interruptions: false,
            stt_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("first", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let output = worker.recv().await.unwrap();
            if let FrameData::Error(error) = output.frame.data {
                assert!(error.error.contains("injected failure"));
                assert!(!error.fatal);
                assert_eq!(output.direction, Direction::Upstream);
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, start()).await;
    send(&worker, audio(2)).await;
    send(&worker, transcript("second", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

#[tokio::test]
async fn watchdog_clears_incomplete_analyzer_when_policy_forces_stop() {
    let (send_calls, mut calls) = unbounded_channel();
    let user = wrap_predictor(
        Controlled {
            calls: send_calls,
            active: Arc::new(AtomicUsize::new(0)),
        },
        UserTurnConfig {
            enable_interruptions: false,
            stop_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(1)).await;
    send(&worker, transcript("watchdog", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    complete(&prediction, false);
    recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(user.buffered_samples().await, 0);
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    end(&mut worker).await;
}

#[tokio::test(start_paused = true)]
async fn smart_turn_terminal_boundaries_cancel_an_inflight_idle_handler() {
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for boundary in ["end", "stop", "cancel", "cleanup"] {
        let (user, _, active) = configured(Duration::ZERO);
        let entered = Arc::new(tokio::sync::Notify::new());
        let destroyed = Arc::new(AtomicUsize::new(0));
        let began = entered.clone();
        let dropped = destroyed.clone();
        user.user()
            .on_user_turn_idle(move |_| {
                let began = began.clone();
                let dropped = dropped.clone();
                async move {
                    let _guard = Dropped(dropped);
                    began.notify_one();
                    std::future::pending::<Result<(), ProcessorError>>().await
                }
            })
            .unwrap();
        let mut worker = Pipeline::new(vec![user.clone()])
            .start(PipelineConfig {
                max_background_tasks: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        recv_until(&mut worker, |data| data.is_start()).await;
        send(
            &worker,
            Frame::new(FrameData::UserIdleTimeoutUpdate { timeout: 10.0 }),
        )
        .await;
        send(&worker, Frame::new(FrameData::BotStoppedSpeaking)).await;
        barrier(&mut worker).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        match boundary {
            "end" => worker.end().await.unwrap(),
            "stop" => send(&worker, Frame::new(FrameData::Stop)).await,
            "cancel" => worker.cancel("idle handler shutdown").await.unwrap(),
            "cleanup" => {
                user.cleanup().await.unwrap();
                worker.end().await.unwrap();
            }
            _ => unreachable!(),
        }
        let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.state,
            match boundary {
                "cancel" => WorkerState::Cancelled,
                "stop" => WorkerState::Stopped,
                _ => WorkerState::Ended,
            }
        );
        assert_eq!(destroyed.load(Ordering::SeqCst), 1, "{boundary}");
        assert_eq!(active.load(Ordering::SeqCst), 0, "{boundary}");
        assert_eq!(user.user().idle_deadline().unwrap(), None);
    }
}

#[tokio::test]
async fn composed_deferred_analyzer_notifies_without_context_then_correlated_completion_clears_audio(
) {
    let (user, mut calls, active) = configured_policies(
        Duration::ZERO,
        SmartTurnParams::default(),
        true,
        Some(
            UserTurnStopStrategies::new(vec![
                UserTurnStopStrategy::turn_analyzer(true).deferred(),
                UserTurnStopStrategy::external_completion(),
            ])
            .unwrap(),
        ),
    );
    let (tx, mut rx) = unbounded_channel();
    user.user()
        .on_user_turn_inference_requested(move |request| {
            tx.send(request).map_err(|_| ProcessorError::new("closed"))
        })
        .unwrap();
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(100)).await;
    send(&worker, transcript("real accepted text", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    complete(&prediction, true);
    let request = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.text, "real accepted text");
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, request.completion_frame()).await;
    recv_until(&mut worker, |d| matches!(d, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(user.buffered_samples().await, 0);
    end(&mut worker).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn composed_external_completion_cancels_pending_native_analysis() {
    let (user, mut calls, active) = configured_policies(
        Duration::ZERO,
        SmartTurnParams::default(),
        true,
        Some(
            UserTurnStopStrategies::new(vec![
                UserTurnStopStrategy::turn_analyzer(true),
                UserTurnStopStrategy::external_completion(),
            ])
            .unwrap(),
        ),
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, start()).await;
    send(&worker, audio(100)).await;
    send(&worker, transcript("accepted", true)).await;
    barrier(&mut worker).await;
    send(&worker, stop()).await;
    let prediction = call(&mut calls).await;
    send(&worker, Frame::new(FrameData::UserTurnInferenceCompleted)).await;
    recv_until(&mut worker, |d| matches!(d, FrameData::LlmContext(_))).await;
    assert!(prediction.cancellation.is_cancelled());
    assert_eq!(user.buffered_samples().await, 0);
    end(&mut worker).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
