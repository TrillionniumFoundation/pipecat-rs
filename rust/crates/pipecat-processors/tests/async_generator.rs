//! Forward-first source contract and bounded native stream lifecycle witnesses.
use pipecat_core::*;
use pipecat_processors::{
    AsyncGeneratorProcessor, AsyncGeneratorProcessorConfig, AsyncGeneratorReceiver,
    AsyncGeneratorSerializerFactory, AsyncGeneratorStreamError,
};
use pipecat_protocol::{adapters::FrameSerializer, ProtocolError, WireMessage};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

type Hook = Arc<dyn Fn(&Frame) -> Result<Option<WireMessage>, ProtocolError> + Send + Sync>;
struct HookSerializer {
    hook: Hook,
    drops: Arc<AtomicUsize>,
}
impl FrameSerializer for HookSerializer {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
        (self.hook)(frame)
    }
    fn deserialize(&mut self, _: &WireMessage) -> Result<Option<Frame>, ProtocolError> {
        panic!("the outbound generator must never deserialize")
    }
}
impl Drop for HookSerializer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn factory(hook: Hook, drops: Arc<AtomicUsize>) -> AsyncGeneratorSerializerFactory {
    Arc::new(move || {
        Ok(Box::new(HookSerializer {
            hook: hook.clone(),
            drops: drops.clone(),
        }))
    })
}
fn processor(
    config: AsyncGeneratorProcessorConfig,
    hook: impl Fn(&Frame) -> Result<Option<WireMessage>, ProtocolError> + Send + Sync + 'static,
) -> Arc<AsyncGeneratorProcessor> {
    Arc::new(
        AsyncGeneratorProcessor::new(
            config,
            factory(Arc::new(hook), Arc::new(AtomicUsize::new(0))),
        )
        .unwrap(),
    )
}
fn text(frame: &Frame) -> Option<&str> {
    match &frame.data {
        FrameData::Text(text) | FrameData::InputText(text) => Some(&text.text),
        _ => None,
    }
}
fn echo(frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
    Ok(text(frame).map(|text| WireMessage::Text(text.to_owned())))
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("bounded generator operation")
}
async fn until(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn recv(worker: &mut PipelineWorker) -> DirectedFrame {
    bounded(worker.recv()).await.expect("pipeline frame")
}
async fn wire(receiver: &mut AsyncGeneratorReceiver) -> WireMessage {
    bounded(receiver.recv()).await.expect("wire message")
}
async fn start(processor: Arc<AsyncGeneratorProcessor>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    assert!(recv(&mut worker).await.frame.data.is_start());
    worker
}
async fn send(worker: &mut PipelineWorker, frame: Frame, direction: Direction) -> DirectedFrame {
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    loop {
        let result = recv(worker).await;
        if result.frame.id == id {
            return result;
        }
        assert!(matches!(result.frame.data, FrameData::Error(_)));
    }
}
async fn finish(worker: PipelineWorker, terminal: FrameData) -> WorkerReport {
    worker
        .sender()
        .send(Frame::new(terminal), Direction::Downstream)
        .await
        .unwrap();
    bounded(worker.collect()).await.unwrap().1
}
fn terminals() -> Vec<FrameData> {
    vec![
        FrameData::End(Default::default()),
        FrameData::EndDetailed(Default::default()),
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
        FrameData::Stop,
    ]
}

#[test]
fn positive_finite_configuration_limits_and_defaults() {
    let good = AsyncGeneratorProcessorConfig::default();
    assert_eq!(good.max_queued_messages, 1024);
    assert_eq!(good.max_message_bytes, 1024 * 1024);
    assert_eq!(good.max_queued_bytes, 4 * 1024 * 1024);
    for bad in [
        AsyncGeneratorProcessorConfig {
            max_queued_messages: 0,
            ..good.clone()
        },
        AsyncGeneratorProcessorConfig {
            max_queued_messages: 65_537,
            ..good.clone()
        },
        AsyncGeneratorProcessorConfig {
            max_message_bytes: 0,
            ..good.clone()
        },
        AsyncGeneratorProcessorConfig {
            max_message_bytes: 16 * 1024 * 1024 + 1,
            ..good.clone()
        },
        AsyncGeneratorProcessorConfig {
            max_queued_bytes: 0,
            ..good.clone()
        },
        AsyncGeneratorProcessorConfig {
            max_queued_bytes: 64 * 1024 * 1024 + 1,
            ..good.clone()
        },
    ] {
        assert!(AsyncGeneratorProcessor::new(
            bad,
            factory(Arc::new(echo), Arc::new(AtomicUsize::new(0)))
        )
        .is_err());
    }
    let maximal = AsyncGeneratorProcessorConfig {
        max_queued_messages: 65_536,
        max_message_bytes: 16 * 1024 * 1024,
        max_queued_bytes: 64 * 1024 * 1024,
    };
    let processor = processor(maximal, echo);
    assert!(processor.take_receiver().is_none());
    assert!(processor.current_snapshot().is_none());
}

#[tokio::test]
async fn forwards_original_identity_fields_and_direction_before_serializing() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_hook = seen.clone();
    let processor = processor(Default::default(), move |frame| {
        seen_hook
            .lock()
            .unwrap()
            .push((frame.id, serde_json::to_value(frame).unwrap()));
        echo(frame)
    });
    let mut worker = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    assert!(processor.take_receiver().is_none());
    for direction in [Direction::Upstream, Direction::Downstream] {
        let mut frame = Frame::text("é你好");
        frame.name = "original-name".into();
        frame.pts = Some(123);
        frame
            .metadata
            .insert("keep".into(), serde_json::json!([1, "value"]));
        frame.transport_source = Some("origin".into());
        frame.transport_destination = Some("destination".into());
        frame.broadcast_sibling_id = Some(37);
        frame.interruptible = false;
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        let expected = serde_json::to_value(&frame).unwrap();
        let id = frame.id;
        let actual = send(&mut worker, frame, direction).await;
        assert_eq!(actual.direction, direction);
        assert_eq!(actual.frame.id, id);
        assert_eq!(serde_json::to_value(&actual.frame).unwrap(), expected);
        assert!(tag.contains(&actual.frame));
        assert_eq!(wire(&mut receiver).await, WireMessage::Text("é你好".into()));
        assert!(seen.lock().unwrap().contains(&(id, expected)));
    }
    assert_eq!(seen.lock().unwrap().len(), 3, "Start is serialized too");
    assert_eq!(
        finish(worker, FrameData::End(Default::default()))
            .await
            .total_errors,
        0
    );
    assert!(bounded(receiver.recv()).await.is_none());
}

#[tokio::test]
async fn none_empty_text_and_empty_binary_are_ignored_but_types_and_utf8_survive() {
    let processor = processor(Default::default(), |frame| {
        Ok(match text(frame) {
            Some("none") => None,
            Some("empty-text") => Some(WireMessage::Text(String::with_capacity(2 * 1024 * 1024))),
            Some("empty-binary") => Some(WireMessage::Binary(Vec::with_capacity(2 * 1024 * 1024))),
            Some("text") => Some(WireMessage::Text("雪é".into())),
            Some("binary") => Some(WireMessage::Binary(vec![0, 255, 128, 1])),
            _ => None,
        })
    });
    let mut worker = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    for input in ["none", "empty-text", "empty-binary", "text", "binary"] {
        send(&mut worker, Frame::text(input), Direction::Downstream).await;
    }
    assert_eq!(
        finish(worker, FrameData::End(Default::default()))
            .await
            .total_errors,
        0
    );
    assert_eq!(receiver.snapshot().queued_messages, 2);
    assert_eq!(wire(&mut receiver).await, WireMessage::Text("雪é".into()));
    assert_eq!(
        wire(&mut receiver).await,
        WireMessage::Binary(vec![0, 255, 128, 1])
    );
    assert!(bounded(receiver.recv()).await.is_none());
    assert!(bounded(receiver.recv()).await.is_none());
    assert_eq!(receiver.error_reason(), None);
}

#[tokio::test]
async fn failed_forwarding_admission_never_calls_serializer_and_closes_even_cancel() {
    // End/Stop are staged separately by core, so a full output queue does not
    // reject their forwarding admission. Cancel uses the ordinary output lane.
    for data in [
        FrameData::Text(TextFrame::new("overflow")),
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = calls.clone();
        let processor = processor(Default::default(), move |_| {
            hook_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        });
        // Leave Start in the only output slot, making the next push fail before
        // the serializer. No downstream-receipt claim is involved.
        let worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                output_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut receiver = processor.take_receiver().unwrap();
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
        until(|| receiver.is_closed()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            receiver.error_reason(),
            Some(AsyncGeneratorStreamError::ForwardingFailed)
        );
        assert!(bounded(receiver.recv()).await.is_none());
        assert!(bounded(worker.collect()).await.is_err());
    }
}

#[tokio::test]
async fn codec_error_and_panic_are_after_forward_failures_and_do_not_retract_original() {
    for panic in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = calls.clone();
        let processor = processor(Default::default(), move |frame| {
            if text(frame).is_some() {
                hook_calls.fetch_add(1, Ordering::SeqCst);
                assert!(!panic, "synthetic private codec panic");
                return Err(ProtocolError::InvalidField("private codec error"));
            }
            Ok(None)
        });
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        send(
            &mut worker,
            Frame::text("already forwarded"),
            Direction::Upstream,
        )
        .await;
        until(|| receiver.is_closed()).await;
        assert_eq!(
            receiver.error_reason(),
            Some(if panic {
                AsyncGeneratorStreamError::SerializerPanicked
            } else {
                AsyncGeneratorStreamError::SerializerFailed
            })
        );
        send(
            &mut worker,
            Frame::text("later still forwards"),
            Direction::Downstream,
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "closed codec is never retried"
        );
        assert!(bounded(receiver.recv()).await.is_none());
        let report = finish(worker, FrameData::End(Default::default())).await;
        assert_eq!(report.total_errors, 1);
        assert!(report.errors[0]
            .error
            .contains("after original-frame forwarding admission"));
        assert!(!report.errors[0].error.contains("private codec"));
    }
}

#[tokio::test]
async fn exact_count_and_capacity_limits_release_budget_on_receive_not_consumer_drop() {
    let config = AsyncGeneratorProcessorConfig {
        max_queued_messages: 2,
        max_message_bytes: 4,
        max_queued_bytes: 8,
    };
    let processor = processor(config, |frame| {
        Ok(text(frame).map(|_| WireMessage::Binary(vec![0; 4])))
    });
    let mut worker = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    for value in ["a", "b"] {
        send(&mut worker, Frame::text(value), Direction::Downstream).await;
    }
    until(|| receiver.snapshot().queued_messages == 2).await;
    assert_eq!(receiver.snapshot().queued_bytes, 8);
    let retained_by_consumer = wire(&mut receiver).await;
    assert_eq!(receiver.snapshot().queued_bytes, 4);
    send(&mut worker, Frame::text("c"), Direction::Downstream).await;
    until(|| receiver.snapshot().queued_messages == 2).await;
    assert_eq!(receiver.snapshot().queued_bytes, 8);
    assert_eq!(
        finish(worker, FrameData::End(Default::default()))
            .await
            .total_errors,
        0
    );
    assert!(
        receiver.is_closed(),
        "a full queue must close without inserting a sentinel"
    );
    assert_eq!(wire(&mut receiver).await, retained_by_consumer);
    assert_eq!(wire(&mut receiver).await, retained_by_consumer);
    assert!(bounded(receiver.recv()).await.is_none());
    assert_eq!(receiver.snapshot().queued_bytes, 0);
}

#[tokio::test]
async fn full_count_or_byte_budget_closes_after_forward_and_drains_prior_fifo() {
    for (config, expected, accepted) in [
        (
            AsyncGeneratorProcessorConfig {
                max_queued_messages: 2,
                max_message_bytes: 4,
                max_queued_bytes: 12,
            },
            AsyncGeneratorStreamError::QueueFull,
            2,
        ),
        (
            AsyncGeneratorProcessorConfig {
                max_queued_messages: 3,
                max_message_bytes: 4,
                max_queued_bytes: 7,
            },
            AsyncGeneratorStreamError::QueueByteLimit,
            1,
        ),
    ] {
        let processor = processor(config, |frame| {
            Ok(text(frame).map(|value| WireMessage::Binary(vec![value.as_bytes()[0]; 4])))
        });
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        for index in 0..=accepted {
            send(
                &mut worker,
                Frame::text(char::from(b'a' + index as u8).to_string()),
                Direction::Downstream,
            )
            .await;
        }
        until(|| receiver.is_closed()).await;
        assert_eq!(receiver.error_reason(), Some(expected));
        assert_eq!(receiver.snapshot().queued_messages, accepted);
        for index in 0..accepted {
            assert_eq!(
                wire(&mut receiver).await,
                WireMessage::Binary(vec![b'a' + index as u8; 4])
            );
        }
        assert!(bounded(receiver.recv()).await.is_none());
        assert_eq!(
            finish(worker, FrameData::End(Default::default()))
                .await
                .total_errors,
            1
        );
    }
}

#[tokio::test]
async fn utf8_and_spare_string_and_vector_capacity_are_counted_without_truncation() {
    for binary in [false, true] {
        for queued_limit in [false, true] {
            let config = AsyncGeneratorProcessorConfig {
                max_queued_messages: 4,
                max_message_bytes: if queued_limit { 64 } else { 4 },
                max_queued_bytes: if queued_limit { 63 } else { 128 },
            };
            let processor = processor(config, move |frame| {
                Ok(text(frame).map(|_| {
                    if binary {
                        let mut bytes = Vec::with_capacity(64);
                        bytes.push(0);
                        WireMessage::Binary(bytes)
                    } else {
                        let mut text = String::with_capacity(64);
                        text.push('é');
                        WireMessage::Text(text)
                    }
                }))
            });
            let mut worker = start(processor.clone()).await;
            let mut receiver = processor.take_receiver().unwrap();
            send(
                &mut worker,
                Frame::text("small length, large allocation"),
                Direction::Downstream,
            )
            .await;
            until(|| receiver.is_closed()).await;
            assert_eq!(
                receiver.error_reason(),
                Some(if queued_limit {
                    AsyncGeneratorStreamError::QueueByteLimit
                } else {
                    AsyncGeneratorStreamError::MessageTooLarge
                })
            );
            assert_eq!(receiver.snapshot().queued_bytes, 0);
            assert!(bounded(receiver.recv()).await.is_none());
            assert_eq!(
                finish(worker, FrameData::End(Default::default()))
                    .await
                    .total_errors,
                1
            );
        }
    }
    let processor = processor(
        AsyncGeneratorProcessorConfig {
            max_message_bytes: 4,
            ..Default::default()
        },
        echo,
    );
    let mut worker = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    send(&mut worker, Frame::text("éé"), Direction::Downstream).await;
    assert_eq!(wire(&mut receiver).await, WireMessage::Text("éé".into()));
    send(&mut worker, Frame::text("éé!"), Direction::Downstream).await;
    until(|| receiver.is_closed()).await;
    assert_eq!(
        receiver.error_reason(),
        Some(AsyncGeneratorStreamError::MessageTooLarge)
    );
    assert!(bounded(receiver.recv()).await.is_none());
    assert_eq!(
        finish(worker, FrameData::End(Default::default()))
            .await
            .total_errors,
        1
    );
}

#[tokio::test]
async fn every_terminal_closes_full_queue_releases_codec_and_joined_reuse_isolates_receivers() {
    for terminal in terminals() {
        let creations = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let factory: AsyncGeneratorSerializerFactory = {
            let creations = creations.clone();
            let drops = drops.clone();
            let calls = calls.clone();
            Arc::new(move || {
                let run = creations.fetch_add(1, Ordering::SeqCst);
                let calls = calls.clone();
                Ok(Box::new(HookSerializer {
                    drops: drops.clone(),
                    hook: Arc::new(move |frame| {
                        calls
                            .lock()
                            .unwrap()
                            .push((run, frame.data.source_class_name()));
                        Ok(text(frame).map(|value| WireMessage::Text(format!("{run}:{value}"))))
                    }),
                }))
            })
        };
        let processor = Arc::new(
            AsyncGeneratorProcessor::new(
                AsyncGeneratorProcessorConfig {
                    max_queued_messages: 1,
                    ..Default::default()
                },
                factory,
            )
            .unwrap(),
        );
        let mut first = start(processor.clone()).await;
        let mut old = processor.take_receiver().unwrap();
        send(&mut first, Frame::text("old"), Direction::Downstream).await;
        until(|| old.snapshot().queued_messages == 1).await;
        assert_eq!(finish(first, terminal.clone()).await.total_errors, 0);
        assert!(old.is_closed());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(old.snapshot().queued_messages, 1);
        assert_eq!(
            calls.lock().unwrap().len(),
            if matches!(terminal, FrameData::Stop) {
                3
            } else {
                2
            }
        );
        let mut second = start(processor.clone()).await;
        let mut new = processor.take_receiver().unwrap();
        send(&mut second, Frame::text("new"), Direction::Downstream).await;
        assert_eq!(wire(&mut old).await, WireMessage::Text("0:old".into()));
        assert!(bounded(old.recv()).await.is_none());
        assert_eq!(wire(&mut new).await, WireMessage::Text("1:new".into()));
        assert_eq!(
            finish(second, FrameData::End(Default::default()))
                .await
                .total_errors,
            0
        );
        assert!(bounded(new.recv()).await.is_none());
        assert_eq!(creations.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        processor.cleanup().await.unwrap();
        processor.cleanup().await.unwrap();
        assert!(bounded(old.recv()).await.is_none());
        assert!(bounded(new.recv()).await.is_none());
    }
}

#[tokio::test]
async fn native_stop_codec_failure_still_closes_and_keeps_preceding_data() {
    for failure in [0, 1] {
        let processor = processor(
            AsyncGeneratorProcessorConfig {
                max_queued_messages: 1,
                ..Default::default()
            },
            move |frame| {
                if matches!(frame.data, FrameData::Stop) {
                    return if failure == 0 {
                        Err(ProtocolError::InvalidField("stop failed"))
                    } else {
                        Ok(Some(WireMessage::Text("cannot fit".into())))
                    };
                }
                echo(frame)
            },
        );
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        send(&mut worker, Frame::text("queued"), Direction::Downstream).await;
        until(|| receiver.snapshot().queued_messages == 1).await;
        let report = finish(worker, FrameData::Stop).await;
        assert!(
            report.total_errors <= 1,
            "terminal output can finish before error reporting"
        );
        assert!(receiver.is_closed());
        assert_eq!(
            receiver.error_reason(),
            Some(if failure == 0 {
                AsyncGeneratorStreamError::SerializerFailed
            } else {
                AsyncGeneratorStreamError::QueueFull
            })
        );
        assert_eq!(
            wire(&mut receiver).await,
            WireMessage::Text("queued".into())
        );
        assert!(bounded(receiver.recv()).await.is_none());
    }
}

#[tokio::test]
async fn receiver_drop_clears_queue_drops_idle_codec_and_future_frames_still_forward() {
    let drops = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = calls.clone();
    let processor = Arc::new(
        AsyncGeneratorProcessor::new(
            Default::default(),
            factory(
                Arc::new(move |frame| {
                    hook_calls.fetch_add(1, Ordering::SeqCst);
                    echo(frame)
                }),
                drops.clone(),
            ),
        )
        .unwrap(),
    );
    let mut worker = start(processor.clone()).await;
    let receiver = processor.take_receiver().unwrap();
    send(&mut worker, Frame::text("queued"), Direction::Downstream).await;
    until(|| receiver.snapshot().queued_messages == 1).await;
    drop(receiver);
    let snapshot = processor.current_snapshot().unwrap();
    assert!(snapshot.closed);
    assert_eq!(
        snapshot.error_reason,
        Some(AsyncGeneratorStreamError::ReceiverDropped)
    );
    assert_eq!(snapshot.queued_messages, 0);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    send(
        &mut worker,
        Frame::text("forward only"),
        Direction::Upstream,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        finish(worker, FrameData::End(Default::default()))
            .await
            .total_errors,
        0
    );
    assert_eq!(
        processor.current_snapshot().unwrap().error_reason,
        Some(AsyncGeneratorStreamError::ReceiverDropped)
    );
}

#[tokio::test]
async fn worker_owner_drop_and_repeated_cleanup_wake_receiver_and_preserve_queued_data() {
    let processor = processor(Default::default(), echo);
    let mut worker = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    send(
        &mut worker,
        Frame::text("before drop"),
        Direction::Downstream,
    )
    .await;
    until(|| receiver.snapshot().queued_messages == 1).await;
    drop(worker);
    until(|| receiver.is_closed()).await;
    assert_eq!(
        wire(&mut receiver).await,
        WireMessage::Text("before drop".into())
    );
    assert!(bounded(receiver.recv()).await.is_none());
    processor.cleanup().await.unwrap();
    processor.cleanup().await.unwrap();
    drop(processor);
    assert!(bounded(receiver.recv()).await.is_none());
}

struct Block {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}
impl Block {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
    fn wait(&self) {
        self.entered.notify_one();
        let (released, timeout) = self
            .wake
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(4),
                |released| !*released,
            )
            .unwrap();
        assert!(
            !timeout.timed_out() && *released,
            "test codec must be released promptly"
        );
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_preserves_queued_fifo_but_stale_original_invocation_cannot_publish() {
    for kind in [0, 1, 2] {
        let block = Arc::new(Block::new());
        let hook_block = block.clone();
        let processor = processor(Default::default(), move |frame| {
            if text(frame) == Some("stale") {
                hook_block.wait();
            }
            if matches!(frame.data, FrameData::Interruption) {
                return Ok(Some(WireMessage::Text("clear".into())));
            }
            echo(frame)
        });
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        send(&mut worker, Frame::text("queued"), Direction::Downstream).await;
        until(|| receiver.snapshot().queued_messages == 1).await;
        let mut stale = if kind == 1 {
            Frame::new(FrameData::InputText(TextFrame::new("stale")))
        } else {
            Frame::text("stale")
        };
        if kind == 2 {
            stale.interruptible = false;
        }
        worker
            .sender()
            .send(stale, Direction::Downstream)
            .await
            .unwrap();
        bounded(block.entered.notified()).await;
        // Ingress admission advances the epoch even while the synchronous
        // system codec is still running. No refreshed epoch may be sampled.
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        block.release();
        assert_eq!(
            wire(&mut receiver).await,
            WireMessage::Text("queued".into())
        );
        assert_eq!(wire(&mut receiver).await, WireMessage::Text("clear".into()));
        worker
            .sender()
            .send(Frame::text("fresh"), Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(wire(&mut receiver).await, WireMessage::Text("fresh".into()));
        assert_eq!(
            finish(worker, FrameData::End(Default::default()))
                .await
                .total_errors,
            0
        );
        assert!(bounded(receiver.recv()).await.is_none());
        assert_eq!(receiver.error_reason(), None);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_cancel_is_cooperative_but_receiver_drop_closes_during_synchronous_codec() {
    for receiver_drop in [false, true] {
        let block = Arc::new(Block::new());
        let hook_block = block.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let processor = Arc::new(
            AsyncGeneratorProcessor::new(
                Default::default(),
                factory(
                    Arc::new(move |frame| {
                        if text(frame) == Some("held") {
                            hook_block.wait();
                        }
                        echo(frame)
                    }),
                    drops.clone(),
                ),
            )
            .unwrap(),
        );
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        send(&mut worker, Frame::text("queued"), Direction::Downstream).await;
        until(|| receiver.snapshot().queued_messages == 1).await;
        worker
            .sender()
            .send(Frame::text("held"), Direction::Downstream)
            .await
            .unwrap();
        bounded(block.entered.notified()).await;
        if receiver_drop {
            drop(receiver);
            assert_eq!(
                processor.current_snapshot().unwrap().error_reason,
                Some(AsyncGeneratorStreamError::ReceiverDropped)
            );
            assert_eq!(processor.current_snapshot().unwrap().queued_bytes, 0);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "a synchronous call cannot be preempted"
            );
            block.release();
            until(|| drops.load(Ordering::SeqCst) == 1).await;
            assert_eq!(
                finish(worker, FrameData::End(Default::default()))
                    .await
                    .total_errors,
                0
            );
        } else {
            worker.cancel("close while codec runs").await.unwrap();
            // Core joins both processing lanes in one actor task. A synchronous
            // codec blocks that actor, so enqueuing Cancel is not yet processing
            // it or linearizing stream closure. Let this invocation finish.
            assert!(!receiver.is_closed());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            block.release();
            until(|| receiver.is_closed()).await;
            assert_eq!(
                wire(&mut receiver).await,
                WireMessage::Text("queued".into())
            );
            assert_eq!(
                wire(&mut receiver).await,
                WireMessage::Text("held".into()),
                "a result completed before Cancel processing remains queued"
            );
            assert!(bounded(receiver.recv()).await.is_none());
            let report = bounded(worker.collect()).await.unwrap().1;
            assert_eq!(report.total_errors, 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(bounded(receiver.recv()).await.is_none());
        }
    }
}

#[tokio::test]
async fn shared_native_authority_guard_runs_after_forward_but_before_custom_serializer() {
    let speculation = SpeculationOwner::new();
    let mut speculative = Frame::text("private");
    speculative
        .set_native_speculation(speculation.capability())
        .unwrap();
    let mut continuation = Frame::new(FrameData::LlmResponseEnd);
    continuation
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::new(7, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
        ))
        .unwrap();
    let producer = NativeRealtimeContextProducer::default();
    let mut context = Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "private")],
        ..Default::default()
    }));
    producer
        .attach(&mut context, RealtimeContextIntent::Run, true)
        .unwrap();
    let eager = EagerSourceSessionOwner::new(4096).unwrap();
    let mut source = Frame::new(FrameData::UserStartedSpeaking);
    source
        .set_native_eager_source(NativeEagerSource::Start(eager.begin_turn().unwrap()))
        .unwrap();
    for original in [
        speculative,
        continuation,
        context,
        source,
        Frame::new(FrameData::LlmContinuationCheckpoint),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = calls.clone();
        let processor = processor(Default::default(), move |_| {
            hook_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(WireMessage::Binary(vec![1])))
        });
        let mut worker = start(processor.clone()).await;
        let mut receiver = processor.take_receiver().unwrap();
        assert_eq!(
            wire(&mut receiver).await,
            WireMessage::Binary(vec![1]),
            "Start is a normal codec input"
        );
        let id = original.id;
        let actual = send(&mut worker, original, Direction::Downstream).await;
        assert_eq!(actual.frame.id, id);
        assert!(
            actual.frame.require_no_native_llm_continuation().is_err(),
            "forwarding must preserve authority"
        );
        until(|| receiver.is_closed()).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "custom serializer cannot strip native authority"
        );
        assert_eq!(
            receiver.error_reason(),
            Some(AsyncGeneratorStreamError::NativeAuthority)
        );
        assert!(bounded(receiver.recv()).await.is_none());
        assert_eq!(
            finish(worker, FrameData::End(Default::default()))
                .await
                .total_errors,
            1
        );
    }
}

#[tokio::test]
async fn failed_factory_start_is_joined_before_successful_fresh_setup() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let factory: AsyncGeneratorSerializerFactory = {
        let attempts = attempts.clone();
        let drops = drops.clone();
        Arc::new(move || {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(ProcessorError::new("private factory failure"));
            }
            Ok(Box::new(HookSerializer {
                hook: Arc::new(echo),
                drops: drops.clone(),
            }))
        })
    };
    let processor = Arc::new(AsyncGeneratorProcessor::new(Default::default(), factory).unwrap());
    // There is no session for Start to process after the failed first factory.
    // Core's failed-start path joins its worker before returning this error.
    let first = bounded(Pipeline::new(vec![processor.clone()]).start(Default::default())).await;
    assert!(first.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(processor.current_snapshot().is_none());
    assert!(processor.take_receiver().is_none());

    let mut second = start(processor.clone()).await;
    let mut receiver = processor.take_receiver().unwrap();
    send(
        &mut second,
        Frame::text("fresh after failed setup"),
        Direction::Downstream,
    )
    .await;
    assert_eq!(
        wire(&mut receiver).await,
        WireMessage::Text("fresh after failed setup".into())
    );
    assert_eq!(
        finish(second, FrameData::End(Default::default()))
            .await
            .total_errors,
        0
    );
    assert!(bounded(receiver.recv()).await.is_none());
    assert_eq!(receiver.error_reason(), None);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
