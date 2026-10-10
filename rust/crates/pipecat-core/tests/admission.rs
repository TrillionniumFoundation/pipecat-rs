use pipecat_core::*;
use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Semaphore;

struct CancelGate {
    normal_entered: Semaphore,
    cancel_entered: Semaphore,
    cancel_release: Semaphore,
}
#[async_trait]
impl FrameProcessor for CancelGate {
    fn name(&self) -> &str {
        "cancel-admission-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "active") {
            self.normal_entered.add_permits(1);
            std::future::pending::<()>().await;
        }
        if frame.data.is_cancel() {
            self.cancel_entered.add_permits(1);
            self.cancel_release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_normal_and_end_senders_are_rejected_before_cancel_handler_finishes() {
    for waiting in [Frame::text("late"), Frame::end()] {
        let gate = Arc::new(CancelGate {
            normal_entered: Semaphore::new(0),
            cancel_entered: Semaphore::new(0),
            cancel_release: Semaphore::new(0),
        });
        let mut worker = Pipeline::new(vec![gate.clone()])
            .start(PipelineConfig {
                queue_capacity: 1,
                ..PipelineConfig::default()
            })
            .await
            .unwrap();
        worker.recv().await.unwrap();
        let sender = worker.sender();
        sender
            .send(Frame::text("active"), Direction::Downstream)
            .await
            .unwrap();
        gate.normal_entered.acquire().await.unwrap().forget();
        sender
            .send(Frame::text("queued"), Direction::Downstream)
            .await
            .unwrap();
        let mut pending_send = Box::pin(sender.send(waiting, Direction::Downstream));
        poll_fn(|cx| match pending_send.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => panic!("sender must be backpressured, got {result:?}"),
        })
        .await;
        worker
            .cancel("close input before clearing normal queue")
            .await
            .unwrap();
        gate.cancel_entered.acquire().await.unwrap().forget();
        assert!(
            !worker.has_finished(),
            "Cancel handler remains deliberately held"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pending_send)
                .await
                .unwrap(),
            Err(PipelineError::Closed)
        );
        gate.cancel_release.add_permits(1);
        let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Cancelled);
        assert!(report.errors.is_empty());
        assert!(frames
            .iter()
            .all(|frame| !matches!(frame.frame.data, FrameData::Text(_) | FrameData::End(_))));
    }
}

struct SystemGate {
    entered: Semaphore,
    release: Semaphore,
    hold_queued: bool,
    queued_entered: Semaphore,
    queued_release: Semaphore,
}
#[async_trait]
impl FrameProcessor for SystemGate {
    fn name(&self) -> &str {
        "system-admission-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::InputText(text) if text.text == "held") {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if self.hold_queued
            && matches!(&frame.data, FrameData::InputText(text) if text.text == "queued")
        {
            self.queued_entered.add_permits(1);
            self.queued_release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_ordinary_system_senders_wake_closed_without_capacity_after_end_or_cancel() {
    for cancelling in [false, true] {
        for waiting in [
            Frame::new(FrameData::InputText(TextFrame::new("late input"))),
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 320], 16000, 1).unwrap(),
            )),
        ] {
            let late_id = waiting.id;
            let gate = Arc::new(SystemGate {
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
                hold_queued: false,
                queued_entered: Semaphore::new(0),
                queued_release: Semaphore::new(0),
            });
            let mut worker = Pipeline::new(vec![gate.clone()])
                .start(PipelineConfig {
                    system_queue_capacity: 1,
                    ..PipelineConfig::default()
                })
                .await
                .unwrap();
            worker.recv().await.unwrap();
            let sender = worker.sender();
            sender
                .send(
                    Frame::new(FrameData::InputText(TextFrame::new("held"))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            gate.entered.acquire().await.unwrap().forget();
            sender
                .send(
                    Frame::new(FrameData::InputText(TextFrame::new("queued"))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            let mut pending_send = Box::pin(sender.send(waiting, Direction::Downstream));
            poll_fn(|cx| match pending_send.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => {
                    panic!("system sender must be backpressured, got {result:?}")
                }
            })
            .await;
            let terminal = if cancelling {
                Frame::cancel("system capacity")
            } else {
                Frame::end()
            };
            let mut terminal_send = Box::pin(sender.send(terminal, Direction::Downstream));
            poll_fn(|cx| match terminal_send.as_mut().poll(cx) {
                Poll::Pending => {
                    assert!(cancelling, "End has available normal capacity");
                    Poll::Ready(())
                }
                Poll::Ready(result) => {
                    assert!(!cancelling, "Cancel is waiting for held system capacity");
                    assert_eq!(result, Ok(()));
                    Poll::Ready(())
                }
            })
            .await;
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), pending_send)
                    .await
                    .unwrap(),
                Err(PipelineError::Closed)
            );
            assert!(!worker.has_finished());
            gate.release.add_permits(1);
            if cancelling {
                tokio::time::timeout(Duration::from_secs(1), terminal_send)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                report.state,
                if cancelling {
                    WorkerState::Cancelled
                } else {
                    WorkerState::Ended
                }
            );
            assert!(report.errors.is_empty());
            assert!(!frames.iter().any(|frame| frame.frame.id == late_id));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn administrative_system_senders_can_wait_for_capacity_during_end_drain() {
    for administrative in [
        Frame::interruption(),
        Frame::new(FrameData::ProcessorResume {
            processor: "system-admission-gate".into(),
        }),
    ] {
        let admin_id = administrative.id;
        let gate = Arc::new(SystemGate {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            hold_queued: true,
            queued_entered: Semaphore::new(0),
            queued_release: Semaphore::new(0),
        });
        let mut worker = Pipeline::new(vec![gate.clone()])
            .start(PipelineConfig {
                system_queue_capacity: 1,
                ..PipelineConfig::default()
            })
            .await
            .unwrap();
        worker.recv().await.unwrap();
        let sender = worker.sender();
        sender
            .send(
                Frame::new(FrameData::InputText(TextFrame::new("held"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        gate.entered.acquire().await.unwrap().forget();
        sender
            .send(
                Frame::new(FrameData::InputText(TextFrame::new("queued"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let mut pending_send = Box::pin(sender.send(administrative, Direction::Downstream));
        poll_fn(|cx| match pending_send.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => {
                panic!("administrative sender must wait for capacity, got {result:?}")
            }
        })
        .await;
        worker.end().await.unwrap();
        poll_fn(|cx| match pending_send.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => {
                panic!("End must leave administrative input available, got {result:?}")
            }
        })
        .await;
        gate.release.add_permits(1);
        gate.queued_entered.acquire().await.unwrap().forget();
        tokio::time::timeout(Duration::from_secs(1), pending_send)
            .await
            .unwrap()
            .unwrap();
        gate.queued_release.add_permits(1);
        let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty());
        assert!(frames.iter().any(|frame| frame.frame.id == admin_id));
        assert!(frames.last().unwrap().frame.is_terminal());
    }
}
