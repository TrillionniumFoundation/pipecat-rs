//! Native supervisors, bounded inference ownership, and strategy lifecycle.
use pipecat_core::*;
use pipecat_turns::*;
use pipecat_vad::{
    MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnCancellation, SmartTurnParams,
    SmartTurnPrediction, SmartTurnPredictor, VadError,
};
use serde_json::json;
use std::{
    sync::{mpsc, Arc},
    time::Duration,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn chain(s: Vec<UserTurnStartStrategy>) -> Option<UserTurnStartStrategies> {
    Some(UserTurnStartStrategies::new(s).unwrap())
}
fn wake() -> UserTurnStartStrategy {
    UserTurnStartStrategy::wake_phrase(vec!["hey pipecat".into()], ms(100), false).unwrap()
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn vad(start: bool) -> FrameData {
    let v = VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    };
    if start {
        FrameData::VadUserStartedSpeaking(v)
    } else {
        FrameData::VadUserStoppedSpeaking(v)
    }
}
fn audio(sample: i16) -> FrameData {
    FrameData::InputAudio(
        AudioRawFrame::new([sample.to_le_bytes(); 320].concat(), 16_000, 1).unwrap(),
    )
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let f = worker.recv().await.expect("pipeline frame");
            if let FrameData::Error(error) = &f.frame.data {
                panic!("{error:?}");
            }
            if wanted(&f.frame.data) {
                return f;
            }
        }
    })
    .await
    .expect("output deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("strategy barrier".into()),
        }),
    )
    .await;
    receive(worker, |f| matches!(f, FrameData::Flush(_))).await;
}
async fn boot(processor: Arc<dyn FrameProcessor>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, |f| f.is_start()).await;
    worker
}
async fn end(worker: &mut PipelineWorker) {
    worker.end().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Ended
    );
}
#[tokio::test(start_paused = true)]
async fn wake_deadline_survives_turn_stop_expires_without_input_and_reuses_one_timer() {
    let user = Arc::new(LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                start_strategies: chain(vec![wake(), UserTurnStartStrategy::transcription(true)]),
                enable_interruptions: false,
                speech_timeout: ms(10),
                ..Default::default()
            },
            ..Default::default()
        },
    ));
    let mut worker = boot(user.clone()).await;
    for n in 0..20 {
        send(&worker, transcript("hey pipecat")).await;
        barrier(&mut worker).await;
        tokio::time::advance(ms(10)).await;
        receive(&mut worker, |f| matches!(f, FrameData::LlmContext(_))).await;
        assert!(user.deadline().unwrap().is_some());
        tokio::time::advance(ms(100)).await;
        tokio::task::yield_now().await;
        barrier(&mut worker).await;
        assert_eq!(user.deadline().unwrap(), None);
        send(&worker, transcript("unrelated background")).await;
        barrier(&mut worker).await;
        assert_eq!(user.context().snapshot().unwrap().messages.len(), n + 1);
    }
    end(&mut worker).await;
    assert_eq!(user.deadline().unwrap(), None);
}
#[tokio::test(start_paused = true)]
async fn terminal_cleanup_cancels_wake_keepalive_and_never_rearms() {
    for cancel in [false, true] {
        let user = Arc::new(LLMUserAggregator::new(
            SharedContext::default(),
            UserAggregatorConfig {
                turn: UserTurnConfig {
                    start_strategies: chain(vec![wake()]),
                    enable_interruptions: false,
                    speech_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
                ..Default::default()
            },
        ));
        let mut worker = boot(user.clone()).await;
        send(&worker, transcript("hey pipecat")).await;
        barrier(&mut worker).await;
        assert!(user.deadline().unwrap().is_some());
        if cancel {
            worker.cancel("test").await.unwrap();
            worker.join().await.unwrap();
        } else {
            end(&mut worker).await;
        }
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(user.deadline().unwrap(), None);
        assert!(user
            .advance(Duration::from_secs(60))
            .unwrap()
            .events
            .is_empty());
    }
}
struct Call {
    samples: Vec<i16>,
    cancellation: SmartTurnCancellation,
    reply: mpsc::Sender<bool>,
}
struct Controlled(UnboundedSender<Call>);
impl SmartTurnPredictor for Controlled {
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
        loop {
            if cancellation.is_cancelled() {
                return Err(VadError::Cancelled);
            }
            match receive.recv_timeout(ms(1)) {
                Ok(complete) => {
                    return Ok(SmartTurnPrediction {
                        probability: if complete { 0.9 } else { 0.1 },
                        complete,
                    })
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => return Err(VadError::Cancelled),
            }
        }
    }
}
fn smart(
    starts: Vec<UserTurnStartStrategy>,
    mutes: Vec<UserMuteStrategy>,
) -> (Arc<SmartTurnUserAggregator>, UnboundedReceiver<Call>) {
    let (send, receive) = unbounded_channel();
    let analyzer = SmartTurnAnalyzer::with_predictor(
        Controlled(send),
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    (
        Arc::new(SmartTurnUserAggregator::new(
            SharedContext::default(),
            UserAggregatorConfig {
                turn: UserTurnConfig {
                    start_strategies: chain(starts),
                    enable_interruptions: false,
                    stop_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
                mute_strategies: mutes,
                ..Default::default()
            },
            analyzer,
        )),
        receive,
    )
}
async fn next_call(calls: &mut UnboundedReceiver<Call>) -> Call {
    tokio::time::timeout(Duration::from_secs(3), calls.recv())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn minwords_mid_vad_stays_active_until_native_model_completion() {
    let (user, mut calls) = smart(vec![UserTurnStartStrategy::min_words(3, true)], vec![]);
    let mut worker = boot(user.clone()).await;
    send(&worker, FrameData::BotStartedSpeaking).await;
    send(&worker, vad(true)).await;
    send(&worker, audio(7)).await;
    send(&worker, transcript("no")).await;
    barrier(&mut worker).await;
    send(&worker, transcript("one two three")).await;
    receive(&mut worker, |f| matches!(f, FrameData::UserStartedSpeaking)).await;
    assert_eq!(user.user().deadline().unwrap(), None);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, audio(8)).await;
    send(&worker, transcript("more words")).await;
    barrier(&mut worker).await;
    send(&worker, vad(false)).await;
    let call = next_call(&mut calls).await;
    assert_eq!(call.samples, [vec![7; 320], vec![8; 320]].concat());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    call.reply.send(true).unwrap();
    receive(&mut worker, |f| matches!(f, FrameData::LlmContext(_))).await;
    let messages = user.context().snapshot().unwrap().messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].content,
        Some(pipecat_services::Content::Text(
            "one two three more words".into()
        ))
    );
    end(&mut worker).await;
}
#[tokio::test]
async fn function_mute_invalidates_inflight_model_and_fresh_turn_has_fresh_audio() {
    let (user, mut calls) = smart(
        vec![
            UserTurnStartStrategy::vad(),
            UserTurnStartStrategy::transcription(true),
        ],
        vec![UserMuteStrategy::FunctionCall],
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, vad(true)).await;
    send(&worker, audio(11)).await;
    send(&worker, transcript("accepted before tool")).await;
    barrier(&mut worker).await;
    send(&worker, vad(false)).await;
    let first = next_call(&mut calls).await;
    send(
        &worker,
        FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "a".into(),
            arguments: json!({}),
        }]),
    )
    .await;
    receive(&mut worker, |f| matches!(f, FrameData::UserMuteStarted)).await;
    assert!(first.cancellation.is_cancelled());
    assert_eq!(user.buffered_samples().await, 0);
    send(&worker, audio(99)).await;
    send(&worker, transcript("suppressed speech")).await;
    barrier(&mut worker).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    send(
        &worker,
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "a".into(),
            arguments: json!({}),
            result: json!("done"),
            run_llm: Some(false),
            is_final: true,
            error: None,
        }),
    )
    .await;
    receive(&mut worker, |f| matches!(f, FrameData::UserMuteStopped)).await;
    send(&worker, vad(true)).await;
    send(&worker, audio(22)).await;
    send(&worker, transcript("fresh speech")).await;
    barrier(&mut worker).await;
    send(&worker, vad(false)).await;
    let second = next_call(&mut calls).await;
    assert_eq!(second.samples, vec![22; 320]);
    second.reply.send(true).unwrap();
    receive(&mut worker, |f| matches!(f, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 2);
    end(&mut worker).await;
}
#[tokio::test(start_paused = true)]
async fn smart_supervisor_also_owns_wake_expiry_without_a_model_request() {
    let (user, mut calls) = smart(
        vec![wake(), UserTurnStartStrategy::transcription(true)],
        vec![],
    );
    let mut worker = boot(user.clone()).await;
    send(&worker, transcript("hey pipecat")).await;
    receive(&mut worker, |f| matches!(f, FrameData::LlmContext(_))).await;
    assert!(user.user().deadline().unwrap().is_some());
    tokio::time::advance(ms(101)).await;
    tokio::task::yield_now().await;
    barrier(&mut worker).await;
    assert_eq!(user.user().deadline().unwrap(), None);
    send(&worker, transcript("background speech")).await;
    barrier(&mut worker).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert!(calls.try_recv().is_err());
    end(&mut worker).await;
}
