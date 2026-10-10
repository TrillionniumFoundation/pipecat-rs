//! Independent eval-host ownership controls; no synthetic echo acceptance target.
use pipecat_core::{async_trait, FrameProcessor, IdentityProcessor};
use pipecat_evals::{
    bot_host::{
        BotHostError, EvalBotFactory, EvalBotHost, EvalBotHostConfig, EvalBotPipeline,
        EvalBotSettings,
    },
    Cancellation,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

fn config() -> EvalBotHostConfig {
    EvalBotHostConfig {
        startup_timeout: Duration::from_secs(3),
        session_timeout: Duration::from_secs(5),
        host_timeout: Duration::from_secs(10),
        shutdown_timeout: Duration::from_millis(400),
        transport: SessionConfig {
            connect_timeout: Duration::from_millis(400),
            write_timeout: Duration::from_millis(200),
            close_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn plan() -> EvalBotPipeline {
    EvalBotPipeline {
        context: SharedContext::default(),
        processors: vec![Arc::new(IdentityProcessor) as Arc<dyn FrameProcessor>],
        assistant: Arc::new(IdentityProcessor),
        introduction: None,
    }
}

struct CreateGuard(Arc<AtomicUsize>);
impl Drop for CreateGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Factory {
    creates: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    block_first: bool,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        assert!(settings.skip_tts);
        let index = self.creates.fetch_add(1, Ordering::SeqCst);
        let _guard = CreateGuard(self.dropped.clone());
        self.entered.notify_one();
        if self.block_first && index == 0 {
            std::future::pending::<()>().await;
        }
        Ok(plan())
    }
}
fn factory(block_first: bool) -> Arc<Factory> {
    Arc::new(Factory {
        creates: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(Notify::new()),
        block_first,
    })
}
async fn send(socket: &WebSocketSession, id: &str, kind: &str, data: Value) {
    socket
        .sender()
        .send(Packet::Text(
            json!({
                "label": "rtvi-ai", "id": id, "type": kind, "data": data,
            })
            .to_string(),
        ))
        .await
        .unwrap();
}
async fn ready(socket: &mut WebSocketSession, id: &str) {
    send(socket, id, "client-ready", json!({"version": "2.2.0"})).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let Some(Packet::Text(text)) = socket.recv().await else {
                panic!("text host closed before correlated readiness");
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "bot-ready" {
                assert_eq!(value["id"], id);
                assert_eq!(value["data"]["version"], "2.2.0");
                return;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn eval_cancel_during_pending_factory_drops_it_without_waiting_for_startup_timeout() {
    let factory = factory(true);
    let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config())
        .await
        .unwrap();
    let url = format!("ws://{}/?skip_tts=true", host.local_addr().unwrap());
    let cancellation = Cancellation::new();
    let owned = factory.clone();
    let host_cancel = cancellation.clone();
    let mut serving = tokio::spawn(async move { host.serve(owned.as_ref(), host_cancel).await });
    let socket = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), factory.entered.notified())
        .await
        .unwrap();
    send(
        &socket,
        "cancel-startup",
        "client-message",
        json!({"t": "eval-cancel", "d": {}}),
    )
    .await;
    let stopped = tokio::time::timeout(Duration::from_millis(800), &mut serving).await;
    if stopped.is_err() {
        cancellation.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), &mut serving).await;
    }
    assert!(
        stopped.is_ok(),
        "eval-cancel was buffered behind the pending factory"
    );
    assert!(stopped.unwrap().unwrap().is_ok());
    assert_eq!(factory.creates.load(Ordering::SeqCst), 1);
    assert_eq!(factory.dropped.load(Ordering::SeqCst), 1);
    let _ = socket.cancel().await;
}

#[tokio::test(flavor = "current_thread")]
async fn startup_disconnect_drops_factory_then_next_scenario_gets_fresh_readiness() {
    let factory = factory(true);
    let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config())
        .await
        .unwrap();
    let url = format!("ws://{}/?skip_tts=true", host.local_addr().unwrap());
    let cancellation = Cancellation::new();
    let owned = factory.clone();
    let host_cancel = cancellation.clone();
    let serving = tokio::spawn(async move { host.serve(owned.as_ref(), host_cancel).await });
    let socket = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), factory.entered.notified())
        .await
        .unwrap();
    socket.cancel().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while factory.dropped.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut next = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    ready(&mut next, "fresh-second").await;
    assert_eq!(factory.creates.load(Ordering::SeqCst), 2);
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
    let _ = next.cancel().await;
}

#[tokio::test(flavor = "current_thread")]
async fn end_releases_event_channel_and_next_session_remains_usable() {
    let factory = factory(false);
    let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config())
        .await
        .unwrap();
    let url = format!("ws://{}/?skip_tts=true", host.local_addr().unwrap());
    let cancellation = Cancellation::new();
    let owned = factory.clone();
    let host_cancel = cancellation.clone();
    let serving = tokio::spawn(async move { host.serve(owned.as_ref(), host_cancel).await });
    let mut first = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    ready(&mut first, "first-id").await;
    send(&first, "end", "disconnect-bot", json!({})).await;
    let closed = tokio::time::timeout(Duration::from_secs(1), async {
        while first.recv().await.is_some() {}
    })
    .await;
    if closed.is_err() {
        cancellation.cancel();
    }
    assert!(
        closed.is_ok(),
        "closed observer channel starved End completion"
    );
    let mut second = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    ready(&mut second, "second-id").await;
    assert_eq!(factory.creates.load(Ordering::SeqCst), 2);
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
    let _ = first.cancel().await;
    let _ = second.cancel().await;
}

struct SetupProbeFactory {
    entered: Arc<Notify>,
    setup_dropped: Arc<AtomicUsize>,
    cleanup_entered: Arc<AtomicUsize>,
    cleanup_completed: Arc<AtomicUsize>,
    cleanup_dropped: Arc<AtomicUsize>,
    block_setup: bool,
    cleanup_delay: Option<Duration>,
}
struct SetupProbe {
    entered: Arc<Notify>,
    setup_dropped: Arc<AtomicUsize>,
    cleanup_entered: Arc<AtomicUsize>,
    cleanup_completed: Arc<AtomicUsize>,
    cleanup_dropped: Arc<AtomicUsize>,
    block_setup: bool,
    cleanup_delay: Option<Duration>,
}
#[async_trait]
impl FrameProcessor for SetupProbe {
    fn name(&self) -> &str {
        "IndependentStartupCleanupProbe"
    }
    async fn setup(
        &self,
        _config: &pipecat_core::StartFrame,
        _ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        let _guard = CreateGuard(self.setup_dropped.clone());
        self.entered.notify_one();
        if self.block_setup {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: pipecat_core::Frame,
        direction: pipecat_core::Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), pipecat_core::ProcessorError> {
        let _guard = CreateGuard(self.cleanup_dropped.clone());
        self.cleanup_entered.fetch_add(1, Ordering::SeqCst);
        match self.cleanup_delay {
            Some(delay) => tokio::time::sleep(delay).await,
            None => std::future::pending::<()>().await,
        }
        self.cleanup_completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl EvalBotFactory for SetupProbeFactory {
    async fn create(&self, _settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        let mut result = plan();
        result.processors = vec![Arc::new(SetupProbe {
            entered: self.entered.clone(),
            setup_dropped: self.setup_dropped.clone(),
            cleanup_entered: self.cleanup_entered.clone(),
            cleanup_completed: self.cleanup_completed.clone(),
            cleanup_dropped: self.cleanup_dropped.clone(),
            block_setup: self.block_setup,
            cleanup_delay: self.cleanup_delay,
        })];
        Ok(result)
    }
}
fn setup_factory(block_setup: bool, cleanup_delay: Option<Duration>) -> Arc<SetupProbeFactory> {
    Arc::new(SetupProbeFactory {
        entered: Arc::new(Notify::new()),
        setup_dropped: Arc::new(AtomicUsize::new(0)),
        cleanup_entered: Arc::new(AtomicUsize::new(0)),
        cleanup_completed: Arc::new(AtomicUsize::new(0)),
        cleanup_dropped: Arc::new(AtomicUsize::new(0)),
        block_setup,
        cleanup_delay,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_during_processor_setup_joins_its_cleanup_before_host_returns() {
    let factory = setup_factory(true, Some(Duration::from_millis(100)));
    let mut limits = config();
    limits.shutdown_timeout = Duration::from_secs(1);
    let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), limits)
        .await
        .unwrap();
    let url = format!("ws://{}/?skip_tts=true", host.local_addr().unwrap());
    let cancellation = Cancellation::new();
    let owned = factory.clone();
    let host_cancel = cancellation.clone();
    let serving = tokio::spawn(async move { host.serve(owned.as_ref(), host_cancel).await });
    let socket = WebSocketSession::connect(&url, config().transport)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), factory.entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "cooperative startup cancellation failed: {result:?}"
    );
    assert_eq!(factory.setup_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(factory.cleanup_entered.load(Ordering::SeqCst), 1);
    assert_eq!(
        factory.cleanup_completed.load(Ordering::SeqCst),
        1,
        "host returned before inherited startup cleanup completed"
    );
    assert_eq!(factory.cleanup_dropped.load(Ordering::SeqCst), 1);
    let _ = socket.cancel().await;
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_processor_cleanup_is_bounded_and_reported_as_failure() {
    for block_setup in [false, true] {
        let factory = setup_factory(block_setup, None);
        let mut limits = config();
        limits.shutdown_timeout = Duration::from_secs(1);
        let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), limits)
            .await
            .unwrap();
        let url = format!("ws://{}/?skip_tts=true", host.local_addr().unwrap());
        let cancellation = Cancellation::new();
        let owned = factory.clone();
        let host_cancel = cancellation.clone();
        let serving = tokio::spawn(async move { host.serve(owned.as_ref(), host_cancel).await });
        let mut socket = WebSocketSession::connect(&url, config().transport)
            .await
            .unwrap();
        if block_setup {
            tokio::time::timeout(Duration::from_secs(1), factory.entered.notified())
                .await
                .unwrap();
            cancellation.cancel();
        } else {
            ready(&mut socket, "cleanup-timeout").await;
            send(
                &socket,
                "stop",
                "client-message",
                json!({"t": "eval-cancel", "d": {}}),
            )
            .await;
        }
        let result = tokio::time::timeout(Duration::from_secs(2), serving)
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.is_err(),
            "blocked cleanup was falsely reported as successful (startup={block_setup})"
        );
        assert_eq!(factory.cleanup_entered.load(Ordering::SeqCst), 1);
        assert_eq!(factory.cleanup_completed.load(Ordering::SeqCst), 0);
        assert_eq!(
            factory.cleanup_dropped.load(Ordering::SeqCst),
            1,
            "cleanup future remained alive after host returned"
        );
        let _ = socket.cancel().await;
    }
}
