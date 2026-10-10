// Tests run against the actual crate module, without recompiling queue.rs in a synthetic crate.
use super::MediaQueue;
use pipecat_core::{FlushFrame, Frame, FrameData, PipelineFlushFrame};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

async fn is_pending<T>(mut future: Pin<&mut impl Future<Output = T>>) -> bool {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

fn marker(detailed: bool) -> Frame {
    Frame::new(if detailed {
        FrameData::FlushDetailed(PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        })
    } else {
        FrameData::Flush(FlushFrame {
            origin: Some("pipecat.flows".into()),
            returning: false,
        })
    })
}

#[tokio::test]
async fn review_dropping_blocked_fence_leaves_every_queue_unchanged() {
    let first = MediaQueue::new(1, 1024);
    let second = MediaQueue::new(1, 1024);
    let existing = Frame::text("occupied");
    let existing_id = existing.id;
    second.push(existing).unwrap();
    let queues = [first.clone(), second.clone()];
    let mut wait = Box::pin(MediaQueue::fence_wait(
        &queues,
        marker(false),
        Duration::from_secs(5),
    ));
    assert!(is_pending(wait.as_mut()).await);
    assert!(first.is_empty());
    drop(wait);
    assert_eq!(second.pop().unwrap().frame.id, existing_id);
    assert!(first.is_empty());
    assert!(second.is_empty());
}

#[tokio::test(start_paused = true)]
async fn review_fence_notifications_do_not_extend_absolute_timeout() {
    let first = MediaQueue::new(1, 1024);
    let second = MediaQueue::new(1, 1024);
    second.push(Frame::text("occupied")).unwrap();
    let queues = [first.clone(), second.clone()];
    let wait = MediaQueue::fence_wait(&queues, marker(true), Duration::from_millis(50));
    tokio::pin!(wait);
    assert!(is_pending(wait.as_mut()).await);
    for round in 0..4 {
        tokio::time::advance(Duration::from_millis(10)).await;
        first.pause(round % 2 == 0);
        second.pause(round % 2 != 0);
        assert!(is_pending(wait.as_mut()).await);
        assert!(first.is_empty());
    }
    tokio::time::advance(Duration::from_millis(10)).await;
    let error = wait.await.unwrap_err();
    assert!(error.fatal);
    assert!(error.message.contains("admission timed out"));
    assert!(first.is_empty());
}

#[tokio::test]
async fn review_close_on_an_unblocked_destination_wakes_fence() {
    let first = MediaQueue::new(1, 1024);
    let second = MediaQueue::new(1, 1024);
    second.push(Frame::text("occupied")).unwrap();
    let queues = [first.clone(), second.clone()];
    let wait = MediaQueue::fence_wait(&queues, marker(false), Duration::from_secs(5));
    tokio::pin!(wait);
    assert!(is_pending(wait.as_mut()).await);
    first.end();
    let error = tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("closing any destination must wake fence admission")
        .unwrap_err();
    assert!(error.message.contains("closed"));
    assert!(first.is_empty());
    assert!(second.pop().unwrap().fence.is_none());
}

#[tokio::test]
async fn review_fence_deduplicates_destination_handles() {
    let queue = MediaQueue::new(1, 1024);
    let frame = marker(true);
    let id = frame.id;
    MediaQueue::fence_wait(
        &[queue.clone(), queue.clone()],
        frame,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let item = queue.pop().unwrap();
    assert_eq!(item.frame.id, id);
    assert!(item.fence.is_some());
    assert!(queue.is_empty());
}

#[tokio::test]
async fn review_cancelling_one_waiter_does_not_steal_capacity_notification() {
    let queue = MediaQueue::new(1, 1024);
    queue.push(Frame::text("occupied")).unwrap();
    let mut cancelled =
        Box::pin(queue.push_wait(Frame::text("cancelled"), None, Duration::from_secs(1)));
    let frame = Frame::text("survives");
    let id = frame.id;
    let survivor = queue.push_wait(frame, None, Duration::from_secs(1));
    tokio::pin!(survivor);
    assert!(is_pending(cancelled.as_mut()).await);
    assert!(is_pending(survivor.as_mut()).await);
    drop(cancelled);
    queue.pop().unwrap();
    tokio::time::timeout(Duration::from_millis(50), survivor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.pop().unwrap().frame.id, id);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn review_registered_capacity_wakes_survive_repeated_pop_before_repoll() {
    let queue = MediaQueue::new(1, 1024);
    for _ in 0..500 {
        queue.push(Frame::text("occupied")).unwrap();
        let next = Frame::text("next");
        let id = next.id;
        let wait = queue.push_wait(next, None, Duration::from_secs(1));
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        queue.pop().unwrap();
        tokio::time::timeout(Duration::from_millis(50), wait)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queue.pop().unwrap().frame.id, id);
    }
}

#[tokio::test]
async fn review_interruptible_fence_is_not_republished_in_a_new_epoch() {
    for detailed in [false, true] {
        let first = MediaQueue::new(1, 1024);
        let second = MediaQueue::new(1, 1024);
        second.push(Frame::text("occupied")).unwrap();
        let queues = [first.clone(), second.clone()];
        let wait = MediaQueue::fence_wait(
            &queues,
            marker(detailed).with_interruptible(true),
            Duration::from_secs(1),
        );
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        first.interrupt();
        second.interrupt();
        let _ = wait.await;
        assert!(
            first.is_empty() && second.is_empty(),
            "stale explicitly interruptible fence was published after interruption"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn review_oversized_fence_metadata_is_rejected_before_publication() {
    for detailed in [false, true] {
        let values = [
            serde_json::Value::String("x".repeat(4096)),
            serde_json::Value::String(String::with_capacity(4096)),
            serde_json::Value::Array(vec![serde_json::Value::Null; 4096]),
            serde_json::Value::Array(Vec::with_capacity(4096)),
            serde_json::Value::Array(vec![serde_json::Value::Object(serde_json::Map::new()); 256]),
        ];
        for (kind, value) in values.into_iter().enumerate() {
            let first = MediaQueue::new(1, 2048);
            let second = MediaQueue::new(1, 2048);
            let mut frame = marker(detailed);
            frame.metadata.insert("retained".into(), value);
            let before = Instant::now();
            let result = MediaQueue::fence_wait(
                &[first.clone(), second.clone()],
                frame,
                Duration::from_secs(1),
            )
            .await;
            assert!(
                result.is_err(),
                "oversized fence metadata kind {kind} bypassed its byte budget"
            );
            assert_eq!(Instant::now(), before);
            assert!(first.is_empty() && second.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn review_empty_fences_still_admit_at_full_media_byte_capacity() {
    for detailed in [false, true] {
        let first = MediaQueue::new(2, 16);
        let second = MediaQueue::new(2, 16);
        first.push(Frame::text("1234567890123456")).unwrap();
        second.push(Frame::text("1234567890123456")).unwrap();
        let before = Instant::now();
        MediaQueue::fence_wait(
            &[first.clone(), second.clone()],
            marker(detailed),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(Instant::now(), before);
        for queue in [&first, &second] {
            assert!(queue.pop().unwrap().fence.is_none());
            assert!(queue.pop().unwrap().fence.is_some());
            assert!(queue.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn review_dynamic_fence_metadata_keys_are_bounded_in_both_policies() {
    for detailed in [false, true] {
        for fail_fast in [false, true] {
            let queue = MediaQueue::new(1, 2048);
            let mut frame = marker(detailed);
            frame
                .metadata
                .insert("k".repeat(4096), serde_json::Value::Null);
            let result = if fail_fast {
                MediaQueue::fence(std::slice::from_ref(&queue), frame)
            } else {
                MediaQueue::fence_wait(std::slice::from_ref(&queue), frame, Duration::from_secs(1))
                    .await
            };
            assert!(result.is_err());
            assert!(queue.is_empty());
        }
    }
}

#[tokio::test]
async fn review_default_uninterruptible_fence_survives_destination_interrupt() {
    for detailed in [false, true] {
        let first = MediaQueue::new(1, 1024);
        let second = MediaQueue::new(1, 1024);
        second.push(Frame::text("occupied")).unwrap();
        let queues = [first.clone(), second.clone()];
        let frame = marker(detailed);
        assert!(!frame.interruptible);
        let id = frame.id;
        let wait = MediaQueue::fence_wait(&queues, frame, Duration::from_secs(1));
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        second.interrupt();
        wait.await.unwrap();
        for queue in [&first, &second] {
            let item = queue.pop().unwrap();
            assert_eq!(item.frame.id, id);
            assert!(item.fence.is_some());
            assert!(queue.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn review_small_fence_metadata_preserves_identity_and_payload() {
    for detailed in [false, true] {
        let first = MediaQueue::new(1, 2048);
        let second = MediaQueue::new(1, 2048);
        let mut frame = marker(detailed);
        frame.metadata.insert("trace".into(), serde_json::json!(7));
        let expected = frame.clone();
        MediaQueue::fence_wait(
            &[first.clone(), second.clone()],
            frame,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        for queue in [&first, &second] {
            let item = queue.pop().unwrap();
            assert_eq!(item.frame, expected);
            assert!(item.fence.is_some());
        }
    }
}
