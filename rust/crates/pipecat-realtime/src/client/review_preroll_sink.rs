//! Deterministic model-sink qualification, separate from real WS/pair tests.
use super::*;
use std::task::Context;

struct SinkModel {
    values: Vec<Value>,
    fail_flush: Option<usize>,
    delay: Option<Duration>,
    sleeping: Option<Pin<Box<tokio::time::Sleep>>>,
    interrupt_after_flush: Option<(usize, Arc<AtomicU64>)>,
}
impl SinkModel {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            fail_flush: None,
            delay: None,
            sleeping: None,
            interrupt_after_flush: None,
        }
    }
}
impl Sink<Message> for SinkModel {
    type Error = tungstenite::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let Message::Text(text) = item else {
            panic!("expected JSON text");
        };
        self.values.push(serde_json::from_str(&text).unwrap());
        if let Some(delay) = self.delay {
            self.sleeping = Some(Box::pin(tokio::time::sleep(delay)));
        }
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.fail_flush == Some(self.values.len()) {
            return Poll::Ready(Err(tungstenite::Error::Io(std::io::Error::other(
                "injected flush failure",
            ))));
        }
        if let Some(sleep) = &mut self.sleeping {
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
        self.sleeping = None;
        if let Some((at, generation)) = &self.interrupt_after_flush {
            if *at == self.values.len() {
                generation.fetch_add(1, Ordering::AcqRel);
                self.interrupt_after_flush = None;
            }
        }
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
fn actor() -> Actor {
    let mut config = RealtimeConfig::new("local-model-only");
    config.limits.max_audio_chunk_bytes = 2;
    let (tx, _rx) = mpsc::channel(8);
    let mut actor = Actor::new(
        config,
        Arc::new(AtomicU64::new(1)),
        Arc::new(Mutex::new(())),
        tx,
    );
    actor.pipeline_turns = true;
    actor.ready = true;
    actor.active = Some("model-response".into());
    let mut ring = Preroll::new(6);
    ring.append(&[1, 2, 3, 4, 5, 6]);
    actor.audio_preroll = Some(ring);
    actor
}
fn control(generation: u64) -> Control {
    Control {
        generation,
        ..Control::default()
    }
}
#[tokio::test]
async fn manual_preroll_clear_and_replay_flush_failure_stops_transaction() {
    for failure in [1, 2] {
        let mut actor = actor();
        let mut sink = SinkModel::new();
        sink.fail_flush = Some(failure);
        let result = actor.interrupt(&mut sink, control(1)).await;
        assert_eq!(result, Err(RealtimeError::Transport));
        assert_eq!(sink.values.len(), failure);
        assert_eq!(sink.values[0]["type"], "input_audio_buffer.clear");
        assert!(!sink.values.iter().any(|v| v["type"] == "response.cancel"));
        assert_eq!(
            actor.audio_preroll.as_ref().unwrap().snapshot(),
            [1, 2, 3, 4, 5, 6]
        );
    }
}
#[tokio::test]
async fn manual_preroll_all_chunks_share_one_operation_deadline() {
    let mut actor = actor();
    actor.config.limits.operation_timeout = Duration::from_millis(100);
    let mut sink = SinkModel::new();
    sink.delay = Some(Duration::from_millis(40));
    // No spawned task: every pending flush remains owned by this awaited transaction.
    let result = actor.interrupt(&mut sink, control(1)).await;
    assert_eq!(result, Err(RealtimeError::Timeout("write")));
    assert_eq!(sink.values[0]["type"], "input_audio_buffer.clear");
    assert!(
        sink.values.len() < 5,
        "per-chunk timeout must not renew the budget"
    );
    assert!(!sink.values.iter().any(|v| v["type"] == "response.cancel"));
}
#[tokio::test]
async fn manual_preroll_superseded_replay_restarts_clear_and_preserves_ring() {
    let mut actor = actor();
    let mut sink = SinkModel::new();
    sink.interrupt_after_flush = Some((2, actor.generation.clone()));
    let first = actor.interrupt(&mut sink, control(1)).await;
    let boundary = sink.values.len();
    let second = actor.interrupt(&mut sink, control(2)).await;
    assert_eq!(first, Ok(()));
    assert_eq!(second, Ok(()));
    assert_eq!(boundary, 2);
    assert_eq!(sink.values[boundary]["type"], "input_audio_buffer.clear");
    let replay: Vec<u8> = sink.values[boundary + 1..]
        .iter()
        .filter(|v| v["type"] == "input_audio_buffer.append")
        .flat_map(|v| STANDARD.decode(v["audio"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(replay, [1, 2, 3, 4, 5, 6]);
    assert_eq!(actor.audio_preroll.as_ref().unwrap().snapshot(), replay);
    assert_eq!(sink.values.last().unwrap()["type"], "response.cancel");
}
