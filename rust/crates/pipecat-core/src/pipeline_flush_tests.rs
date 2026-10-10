// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause

use super::*;
use std::task::{Context, Wake, Waker};

struct CallbackWake(Box<dyn Fn() + Send + Sync>);

impl Wake for CallbackWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        (self.0)();
    }
}

async fn started() -> PipelineWorker {
    let mut worker = Pipeline::new(vec![])
        .start(PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    worker
}

fn returning_flush(event: &LocalFlushEvent) -> Frame {
    Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        event: Some(event.clone()),
        returning: true,
        origin: Some("local".into()),
    }))
}

#[tokio::test]
async fn flush_completion_is_visible_when_output_receiver_is_woken() {
    let mut worker = started().await;
    let event = LocalFlushEvent::default();
    let observed = Arc::new(Mutex::new(vec![]));
    let waker = Waker::from(Arc::new(CallbackWake(Box::new({
        let event = event.clone();
        let observed = observed.clone();
        move || observed.lock().unwrap().push(event.is_set())
    }))));
    // A synchronous waker observes the publication boundary, before the sender
    // can run any instructions following publication on another thread.
    assert!(worker
        .output
        .poll_recv(&mut Context::from_waker(&waker))
        .is_pending());
    let marker = returning_flush(&event);
    worker
        .runtime
        .deliver(marker.clone(), Direction::Downstream, None)
        .unwrap();
    assert_eq!(*observed.lock().unwrap(), [true]);
    let returned = worker.try_recv().unwrap();
    assert_eq!(returned.frame, marker);
    assert!(event.is_set());
    worker.end().await.unwrap();
    assert_eq!(worker.collect().await.unwrap().1.state, WorkerState::Ended);
}

#[tokio::test]
async fn rejected_flush_output_admission_does_not_set_completion() {
    for closed in [false, true] {
        let mut worker = started().await;
        let event = LocalFlushEvent::default();
        if closed {
            worker.output.close();
        } else {
            worker
                .runtime
                .deliver(Frame::text("occupy output"), Direction::Downstream, None)
                .unwrap();
        }
        let error = worker
            .runtime
            .deliver(returning_flush(&event), Direction::Downstream, None)
            .unwrap_err();
        assert!(error.fatal);
        assert!(!event.is_set());
        assert_eq!(worker.state(), WorkerState::Failed);
        while let Ok(frame) = worker.try_recv() {
            assert!(!matches!(frame.frame.data, FrameData::FlushDetailed(_)));
        }
        assert!(worker.join().await.is_err());
        assert!(!event.is_set());
    }
}

#[tokio::test]
async fn receiver_closure_after_flush_admission_does_not_revoke_completion() {
    for drop_receiver in [false, true] {
        let mut worker = started().await;
        let (_, placeholder) = mpsc::channel(1);
        let receiver = Arc::new(Mutex::new(Some(std::mem::replace(
            &mut worker.output,
            placeholder,
        ))));
        let event = LocalFlushEvent::default();
        let observed = Arc::new(Mutex::new(vec![]));
        let waker = Waker::from(Arc::new(CallbackWake(Box::new({
            let receiver = receiver.clone();
            let observed = observed.clone();
            move || {
                let mut receiver = receiver.lock().unwrap();
                observed
                    .lock()
                    .unwrap()
                    .push(receiver.as_ref().unwrap().is_empty());
                if drop_receiver {
                    receiver.take();
                } else {
                    receiver.as_mut().unwrap().close();
                }
            }
        }))));
        let waiting = event.wait();
        tokio::pin!(waiting);
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        let marker = returning_flush(&event);
        worker
            .runtime
            .deliver(marker.clone(), Direction::Downstream, None)
            .unwrap();
        // Completion wakes the waiter after reservation and before publication.
        // Closing or dropping the receiver here cannot reject that admission.
        assert_eq!(*observed.lock().unwrap(), [true]);
        assert!(event.is_set());
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready());
        if !drop_receiver {
            let returned = receiver
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .try_recv()
                .unwrap();
            assert_eq!(returned.frame, marker);
        }
        assert_eq!(worker.state(), WorkerState::Running);
        worker.runtime.finish(WorkerState::Cancelled);
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
    }
}
