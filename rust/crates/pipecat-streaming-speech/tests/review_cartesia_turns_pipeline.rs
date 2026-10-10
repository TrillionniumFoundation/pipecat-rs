//! Independent Cartesia Turns socket -> real user-aggregator contract tests.
//! Synthetic loopback only. No model, credential, external service, or upstream code.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_streaming_speech::*;
use pipecat_turns::*;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn bounded<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), work)
        .await
        .expect("review operation deadline")
}

#[derive(Default)]
struct Seen(Mutex<Vec<DirectedFrame>>);
impl Seen {
    fn frames(&self, direction: Direction) -> Vec<Frame> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.direction == direction)
            .map(|f| f.frame.clone())
            .collect()
    }
    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.frames(Direction::Downstream)
            .iter()
            .filter(|f| predicate(&f.data))
            .count()
    }
}
#[async_trait]
impl FrameProcessor for Seen {
    fn name(&self) -> &str {
        "CartesiaTurnsIndependentRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}

struct Batch {
    messages: Vec<&'static str>,
    flushed: oneshot::Sender<()>,
}
struct Peer {
    client: CartesiaTurnsClient,
    batches: mpsc::Sender<Batch>,
    wire: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn batch(&self, messages: Vec<&'static str>) {
        let (flushed, done) = oneshot::channel();
        bounded(self.batches.send(Batch { messages, flushed }))
            .await
            .unwrap();
        bounded(done).await.unwrap();
    }
}
async fn peer() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaTurnsConfig::new("synthetic-review-key");
    config.endpoint = format!(
        "ws://{}/stt/turns/websocket",
        listener.local_addr().unwrap()
    );
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(100);
    let (batches, mut commands) = mpsc::channel::<Batch>(4);
    let (wire, reads) = mpsc::channel(32);
    let task = tokio::spawn(async move {
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        let mut socket = bounded(accept_async(tcp)).await.unwrap();
        socket
            .send(Message::Text(
                r#"{"type":"connected","request_id":"independent-session"}"#.into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {
                batch = commands.recv() => {
                    let Some(batch) = batch else { break; };
                    // Deliberately one network flush, with no pipeline interleaving barriers.
                    for json in batch.messages { socket.feed(Message::Text(json.into())).await.unwrap(); }
                    socket.flush().await.unwrap();
                    let _ = batch.flushed.send(());
                }
                incoming = socket.next() => match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(message)) => {
                        let closing = message.to_text().is_ok_and(|s| s == r#"{"type":"close"}"#);
                        bounded(wire.send(message)).await.unwrap();
                        if closing { let _ = socket.close(None).await; break; }
                    }
                }
            }
        }
    });
    Peer {
        client: CartesiaTurnsClient::new(config).unwrap(),
        batches,
        wire: reads,
        task,
    }
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
}
async fn flush(worker: &PipelineWorker) {
    let event = LocalFlushEvent::default();
    bounded(worker.sender().send(
        Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(event.clone()),
            returning: false,
            origin: Some("independent Cartesia Turns test".into()),
        })),
        Direction::Downstream,
    ))
    .await
    .unwrap();
    bounded(event.wait()).await;
}
async fn audio(worker: &PipelineWorker, peer: &mut Peer, byte: u8) {
    bounded(worker.sender().send(
        Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(vec![byte; 320], 16000, 1).unwrap(),
        )),
        Direction::Downstream,
    ))
    .await
    .unwrap();
    let frame = bounded(peer.wire.recv()).await.unwrap();
    assert!(frame.is_binary(), "PCM must remain raw binary: {frame:?}");
    assert_eq!(frame.into_data().as_ref(), [byte; 320].as_slice());
}
async fn finish(worker: PipelineWorker, mut peer: Peer) {
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let close = bounded(peer.wire.recv()).await.unwrap();
    assert_eq!(close.to_text().unwrap(), r#"{"type":"close"}"#);
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn independent_coalesced_turns_adopt_metadata_and_survive_their_start_interruption() {
    for interrupt in [true, false] {
        let mut peer = peer().await;
        let processor = Arc::new(
            CartesiaTurnsProcessor::new(peer.client.clone())
                .with_should_interrupt(interrupt)
                .with_user_id("fixed-review-user")
                .unwrap(),
        );
        let context = SharedContext::default();
        let user = Arc::new(LLMUserAggregator::new(
            context.clone(),
            UserAggregatorConfig::default(),
        ));
        let upstream = Arc::new(Seen::default());
        let raw = Arc::new(Seen::default());
        let downstream = Arc::new(Seen::default());
        let mut config = PipelineConfig::default();
        config.start.enable_usage_metrics = true;
        let worker = bounded(
            Pipeline::new(vec![
                upstream.clone(),
                processor,
                raw.clone(),
                user,
                downstream.clone(),
            ])
            .start(config),
        )
        .await
        .unwrap();
        flush(&worker).await;
        let opening = raw.frames(Direction::Downstream);
        let start = opening
            .iter()
            .position(|f| matches!(f.data, FrameData::Start(_)))
            .unwrap();
        let metadata = opening
            .iter()
            .position(|f| matches!(f.data, FrameData::SttMetadata(_)))
            .unwrap();
        assert!(start < metadata, "Start must precede service metadata");
        let FrameData::SttMetadata(meta) = &opening[metadata].data else {
            unreachable!()
        };
        assert_eq!(meta.ttfs_p99_latency, 0.0);
        let recommendations = meta.base.user_turn_strategies.as_ref().unwrap();
        assert_eq!(
            recommendations.start,
            Some(vec![UserTurnStrategy::External {
                enable_interruptions: interrupt
            }])
        );
        assert_eq!(
            recommendations.stop,
            Some(vec![UserTurnStrategy::External {
                enable_interruptions: interrupt
            }])
        );
        let up_metadata = upstream
            .frames(Direction::Upstream)
            .into_iter()
            .find(|f| matches!(f.data, FrameData::SttMetadata(_)))
            .unwrap();
        assert_eq!(opening[metadata].broadcast_sibling_id, Some(up_metadata.id));
        assert_eq!(up_metadata.broadcast_sibling_id, Some(opening[metadata].id));

        for (index, (batch, expected)) in [
            (vec![r#"{"type":"turn.start","transcript":"Never commit this start"}"#,
                r#"{"type":"turn.update","transcript":"Hello","is_final":true}"#,
                r#"{"type":"turn.update","transcript":"Hello world"}"#,
                r#"{"type":"turn.eager_end","transcript":"Never commit this eager"}"#,
                r#"{"type":"turn.resume"}"#,
                r#"{"type":"turn.end","transcript":"Hello world.","review_marker":17}"#], "Hello world."),
            (vec![r#"{"type":"turn.start"}"#,
                r#"{"type":"turn.update","transcript":"Second"}"#,
                r#"{"type":"turn.end","transcript":"Second committed turn.","review_marker":17}"#], "Second committed turn."),
        ].into_iter().enumerate() {
            audio(&worker, &mut peer, index as u8 + 1).await;
            peer.batch(batch).await;
            wait_until(|| downstream.count(|f| matches!(f, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_))) == index + 1).await;
            flush(&worker).await;
            let history = context.snapshot().unwrap();
            assert_eq!(history.messages.len(), index + 1);
            assert_eq!(history.messages[index].role, Role::User);
            assert_eq!(history.messages[index].content, Some(Content::Text(expected.into())));
            assert_eq!(downstream.count(|f| matches!(f, FrameData::Interruption)), if interrupt { index + 1 } else { 0 });
            assert_eq!(downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)), index + 1);
            assert_eq!(downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)), index + 1);
        }
        let frames = raw.frames(Direction::Downstream);
        let mut ordered = vec![];
        for frame in &frames {
            match &frame.data {
                FrameData::Metrics(metrics) => {
                    for metric in metrics {
                        if let Metric::SttUsage { audio_seconds, .. } = metric {
                            assert_eq!(*audio_seconds, 0.01);
                            ordered.push("usage");
                        }
                    }
                }
                FrameData::Transcription(text) => {
                    assert!(
                        frame.interruptible,
                        "committed text must retain normal interruption semantics"
                    );
                    assert!(text.finalized);
                    assert_eq!(text.user_id, "fixed-review-user");
                    assert_eq!(text.language.as_deref(), Some("en"));
                    assert!(text.timestamp.ends_with('Z') || text.timestamp.ends_with("+00:00"));
                    assert_eq!(text.result.as_ref().unwrap()["review_marker"], 17);
                    ordered.push("final");
                }
                FrameData::ProposedUserStoppedSpeaking => ordered.push("stop"),
                FrameData::EagerTranscription(_)
                | FrameData::EagerTranscriptionDetailed(_)
                | FrameData::EagerEndOfTurnCancel => {
                    panic!("committed-only service emitted eager work")
                }
                _ => {}
            }
        }
        assert_eq!(
            ordered,
            ["usage", "final", "stop", "usage", "final", "stop"]
        );
        let stops: Vec<_> = frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::ProposedUserStoppedSpeaking))
            .collect();
        let upstream_stops: Vec<_> = upstream
            .frames(Direction::Upstream)
            .into_iter()
            .filter(|f| matches!(f.data, FrameData::ProposedUserStoppedSpeaking))
            .collect();
        assert_eq!(upstream_stops.len(), 2);
        for (down, up) in stops.iter().zip(upstream_stops.iter()) {
            assert_eq!(down.broadcast_sibling_id, Some(up.id));
            assert_eq!(up.broadcast_sibling_id, Some(down.id));
        }
        finish(worker, peer).await;
    }
}

#[tokio::test]
async fn independent_empty_end_waits_for_default_outer_watchdog_without_fabricated_text() {
    let peer = peer().await;
    let processor =
        Arc::new(CartesiaTurnsProcessor::new(peer.client.clone()).with_should_interrupt(false));
    let context = SharedContext::default();
    let user = Arc::new(LLMUserAggregator::new(
        context.clone(),
        UserAggregatorConfig::default(),
    ));
    let raw = Arc::new(Seen::default());
    let downstream = Arc::new(Seen::default());
    let worker = bounded(
        Pipeline::new(vec![processor, raw.clone(), user, downstream.clone()])
            .start(Default::default()),
    )
    .await
    .unwrap();
    flush(&worker).await;
    peer.batch(vec![
        r#"{"type":"turn.start"}"#,
        r#"{"type":"turn.update","transcript":"unfinished hypothesis"}"#,
        r#"{"type":"turn.eager_end","transcript":"uncommitted guess"}"#,
        r#"{"type":"turn.end","transcript":""}"#,
    ])
    .await;
    wait_until(|| raw.count(|f| matches!(f, FrameData::ProposedUserStoppedSpeaking)) == 1).await;
    flush(&worker).await;
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        1
    );
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)),
        0
    );
    assert_eq!(
        raw.count(|f| matches!(
            f,
            FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
        )),
        0
    );
    assert!(context.snapshot().unwrap().messages.is_empty());
    // Network input and start roundtrip are settled before pausing native time.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)),
        0
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    wait_until(|| downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)) == 1).await;
    assert!(context.snapshot().unwrap().messages.is_empty());
    assert_eq!(
        downstream.count(|f| matches!(
            f,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        )),
        0
    );
    tokio::time::resume();
    finish(worker, peer).await;
}

#[tokio::test]
async fn independent_explicit_start_and_stop_policies_are_not_replaced_by_service_metadata() {
    let peer = peer().await;
    let processor =
        Arc::new(CartesiaTurnsProcessor::new(peer.client.clone()).with_should_interrupt(true));
    let context = SharedContext::default();
    let config = UserAggregatorConfig {
        turn: UserTurnConfig {
            start_strategies: Some(
                UserTurnStartStrategies::new(vec![
                    UserTurnStartStrategy::external().with_interruptions(false)
                ])
                .unwrap(),
            ),
            stop_strategies: Some(
                UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
                    Duration::from_secs(10),
                    true,
                )])
                .unwrap(),
            ),
            stop_timeout: Duration::from_secs(20),
            ..Default::default()
        },
        ..Default::default()
    };
    let user = Arc::new(LLMUserAggregator::new(context.clone(), config));
    let raw = Arc::new(Seen::default());
    let downstream = Arc::new(Seen::default());
    let worker = bounded(
        Pipeline::new(vec![processor, raw.clone(), user, downstream.clone()])
            .start(Default::default()),
    )
    .await
    .unwrap();
    flush(&worker).await;
    peer.batch(vec![
        r#"{"type":"turn.start"}"#,
        r#"{"type":"turn.end","transcript":"Explicit policy waits."}"#,
    ])
    .await;
    wait_until(|| raw.count(|f| matches!(f, FrameData::ProposedUserStoppedSpeaking)) == 1).await;
    flush(&worker).await;
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        1
    );
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::Interruption)),
        0
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)),
        0
    );
    assert!(context.snapshot().unwrap().messages.is_empty());
    tokio::time::resume();
    finish(worker, peer).await;
}
