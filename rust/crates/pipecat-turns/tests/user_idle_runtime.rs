//! Owned idle timers and callbacks under deterministic Tokio scheduling.
use pipecat_core::*;
use pipecat_turns::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

const IDLE: Duration = Duration::from_secs(10);
fn configured(timeout: Duration) -> (Arc<LLMUserAggregator>, Arc<AtomicUsize>) {
    let user = Arc::new(LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: timeout,
            turn: UserTurnConfig {
                enable_interruptions: false,
                stop_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            ..Default::default()
        },
    ));
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    user.on_user_turn_idle(move |ctx| {
        let seen = seen.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            ctx.push(
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new(
                    "Are you still there?",
                ))),
                Direction::Downstream,
            )
            .await
        }
    })
    .unwrap();
    (user, count)
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.expect("pipeline output");
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("processor error: {error:?}");
            }
            if wanted(&frame.frame.data) {
                return frame;
            }
        }
    })
    .await
    .expect("output deadline")
}
async fn start(user: Arc<LLMUserAggregator>, max_background_tasks: usize) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![user])
        .start(PipelineConfig {
            max_background_tasks,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, |frame| frame.is_start()).await;
    worker
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("idle-test".into()),
        }),
    )
    .await;
    receive(worker, |frame| matches!(frame, FrameData::Flush(_))).await;
}
async fn tick(worker: &mut PipelineWorker, duration: Duration) {
    tokio::time::advance(duration).await;
    // Yield lets a due timer finish before the frame fence reaches the sink.
    tokio::task::yield_now().await;
    barrier(worker).await;
}
async fn end(mut worker: PipelineWorker) {
    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .expect("idle task must not delay End")
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}

#[tokio::test(start_paused = true)]
async fn idle_callback_pushes_a_reminder_once_per_bot_cycle() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user.clone(), 1).await;
    tick(&mut worker, IDLE * 2).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tokio::time::advance(IDLE).await;
    let reminder = receive(&mut worker, |f| matches!(f, FrameData::TtsSpeak(_))).await;
    assert!(
        matches!(reminder.frame.data, FrameData::TtsSpeak(ref text) if text.text == "Are you still there?")
    );
    assert!(user.waiting_for_user().unwrap());
    tick(&mut worker, IDLE * 3).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    send(&worker, FrameData::BotStartedSpeaking).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_updates_restart_a_single_owned_timer_without_stale_callbacks() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user, 1).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    for _ in 0..200 {
        send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 10.0 }).await;
        barrier(&mut worker).await;
    }
    tick(&mut worker, Duration::from_secs(9)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 20.0 }).await;
    barrier(&mut worker).await;
    tick(&mut worker, Duration::from_secs(1)).await; // The obsolete deadline.
    assert_eq!(count.load(Ordering::SeqCst), 0);
    tick(&mut worker, Duration::from_secs(19)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn runtime_disabled_detection_can_enable_and_disable_in_the_same_waiting_window() {
    let (user, count) = configured(Duration::ZERO);
    let mut worker = start(user.clone(), 1).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    assert!(user.waiting_for_user().unwrap());
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 10.0 }).await;
    barrier(&mut worker).await;
    tick(&mut worker, Duration::from_secs(9)).await;
    send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 0.0 }).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE * 2).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 10.0 }).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn interruption_suppresses_idle_until_the_bot_actually_responds() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user.clone(), 2).await;
    send(&worker, FrameData::BotStartedSpeaking).await;
    send(&worker, FrameData::UserStartedSpeaking).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE * 3).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(
        &worker,
        FrameData::Transcription(TranscriptionFrame {
            text: "question".into(),
            finalized: true,
            ..Default::default()
        }),
    )
    .await;
    barrier(&mut worker).await;
    send(&worker, FrameData::UserStoppedSpeaking).await;
    barrier(&mut worker).await;
    send(&worker, FrameData::UserIdleTimeoutUpdate { timeout: 10.0 }).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE * 2).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(!user.waiting_for_user().unwrap());
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn empty_turn_watchdog_rearms_idle_without_a_bot_response() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user.clone(), 2).await;
    send(&worker, FrameData::UserStartedSpeaking).await;
    send(&worker, FrameData::UserStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, Duration::from_secs(5)).await;
    assert!(user.waiting_for_user().unwrap());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn explicit_wait_can_rearm_from_an_idle_handler_without_deadlocking() {
    let (user, count) = configured(IDLE);
    let weak = Arc::downgrade(&user);
    let rounds = Arc::new(AtomicUsize::new(0));
    user.on_user_turn_idle(move |ctx| {
        let weak = weak.clone();
        let rounds = rounds.clone();
        async move {
            if rounds.fetch_add(1, Ordering::SeqCst) == 0 {
                weak.upgrade().unwrap().wait_for_user(&ctx)?;
            }
            Ok(())
        }
    })
    .unwrap();
    let mut worker = start(user, 1).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    tick(&mut worker, IDLE * 2).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn end_stop_cancel_and_cleanup_prevent_idle_after_session_close() {
    for boundary in ["end", "stop", "cancel", "cleanup"] {
        let (user, count) = configured(IDLE);
        let mut worker = start(user.clone(), 1).await;
        send(&worker, FrameData::BotStoppedSpeaking).await;
        barrier(&mut worker).await;
        let before = tokio::time::Instant::now();
        match boundary {
            "end" => worker.end().await.unwrap(),
            "stop" => send(&worker, FrameData::Stop).await,
            "cancel" => worker.cancel("test").await.unwrap(),
            "cleanup" => {
                user.cleanup().await.unwrap();
                worker.end().await.unwrap();
            }
            _ => unreachable!(),
        }
        let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
            .await
            .expect("terminal drain must cancel idle timer")
            .unwrap();
        assert_eq!(
            report.state,
            match boundary {
                "cancel" => WorkerState::Cancelled,
                "stop" => WorkerState::Stopped,
                _ => WorkerState::Ended,
            }
        );
        assert!(before.elapsed() < Duration::from_secs(1));
        tokio::time::advance(IDLE * 2).await;
        assert_eq!(count.load(Ordering::SeqCst), 0, "{boundary}");
        assert_eq!(user.idle_deadline().unwrap(), None);
    }
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test(start_paused = true)]
async fn terminal_shutdown_cancels_and_joins_an_inflight_idle_handler() {
    for boundary in ["end", "stop", "cancel", "cleanup"] {
        let (user, _) = configured(IDLE);
        let began = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let entered = began.clone();
        let destroyed = dropped.clone();
        user.on_user_turn_idle(move |_| {
            let entered = entered.clone();
            let destroyed = destroyed.clone();
            async move {
                let _guard = Dropped(destroyed);
                entered.notify_one();
                std::future::pending::<Result<(), ProcessorError>>().await
            }
        })
        .unwrap();
        let mut worker = start(user.clone(), 1).await;
        send(&worker, FrameData::BotStoppedSpeaking).await;
        barrier(&mut worker).await;
        tokio::time::advance(IDLE).await;
        began.notified().await;
        match boundary {
            "end" => worker.end().await.unwrap(),
            "stop" => send(&worker, FrameData::Stop).await,
            "cancel" => worker.cancel("test").await.unwrap(),
            "cleanup" => {
                user.cleanup().await.unwrap();
                worker.end().await.unwrap();
            }
            _ => unreachable!(),
        }
        tokio::time::timeout(Duration::from_secs(1), worker.join())
            .await
            .expect("handler is owned and cancellable")
            .unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_nonfatal_handler_error_does_not_drop_later_handlers_or_an_explicit_rearm() {
    let user = Arc::new(LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: IDLE,
            ..Default::default()
        },
    ));
    let weak = Arc::downgrade(&user);
    let attempts = Arc::new(AtomicUsize::new(0));
    let invoked = attempts.clone();
    user.on_user_turn_idle(move |ctx| {
        let user = weak.upgrade().unwrap();
        let invoked = invoked.clone();
        async move {
            if invoked.fetch_add(1, Ordering::SeqCst) == 0 {
                user.wait_for_user(&ctx)?;
                return Err(ProcessorError::new("test callback failed"));
            }
            Ok(())
        }
    })
    .unwrap();
    let successes = Arc::new(AtomicUsize::new(0));
    let seen = successes.clone();
    user.on_user_turn_idle(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    })
    .unwrap();
    let mut worker = start(user, 1).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tokio::time::advance(IDLE).await;
    let error = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let output = worker.recv().await.unwrap();
            if let FrameData::Error(error) = output.frame.data {
                break error;
            }
        }
    })
    .await
    .unwrap();
    assert!(!error.fatal);
    assert_eq!(error.error, "test callback failed");
    tokio::task::yield_now().await;
    assert_eq!(successes.load(Ordering::SeqCst), 1);
    tick(&mut worker, IDLE).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(successes.load(Ordering::SeqCst), 2);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn runtime_tool_replays_and_partial_results_never_prompt_over_a_pending_call() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user.clone(), 1).await;
    let declarations = || {
        FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "active".into(),
            arguments: serde_json::json!({}),
        }])
    };
    let result = |id: &str, is_final: bool| {
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: id.into(),
            arguments: serde_json::json!({}),
            result: serde_json::json!("result"),
            run_llm: None,
            is_final,
            error: None,
        })
    };
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    send(&worker, declarations()).await;
    send(&worker, declarations()).await;
    send(&worker, result("unknown", true)).await;
    send(&worker, result("active", false)).await;
    barrier(&mut worker).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    tick(&mut worker, IDLE * 3).await;
    assert!(user.function_calls_in_progress().unwrap());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    send(&worker, result("active", true)).await;
    send(&worker, result("active", true)).await;
    barrier(&mut worker).await;
    assert!(!user.function_calls_in_progress().unwrap());
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 0); // Completion alone is not a response.
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    send(&worker, declarations()).await; // Recently completed declaration replay.
    barrier(&mut worker).await;
    tick(&mut worker, IDLE).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}

#[tokio::test(start_paused = true)]
async fn same_deadline_rearms_and_disabled_windows_never_duplicate_callbacks() {
    let (user, count) = configured(IDLE);
    let mut worker = start(user, 1).await;
    send(&worker, FrameData::BotStoppedSpeaking).await;
    barrier(&mut worker).await;
    // No clock advances: both 10-second settings have the same absolute
    // deadline with an intervening disarmed window and a different deadline.
    for timeout in [0.0, 20.0, 10.0, 0.0, 10.0] {
        send(&worker, FrameData::UserIdleTimeoutUpdate { timeout }).await;
    }
    barrier(&mut worker).await;
    tick(&mut worker, IDLE - Duration::from_millis(1)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    tick(&mut worker, Duration::from_millis(1)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    tick(&mut worker, IDLE * 3).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    end(worker).await;
}
