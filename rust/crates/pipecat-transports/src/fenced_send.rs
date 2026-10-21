//! Original invocation admission at the socket boundary, not playback evidence.
use futures_util::Sink;
use pipecat_core::InvocationFence;
use std::{pin::Pin, task::Poll};

pub(crate) async fn send<S: Sink<Item> + Unpin, Item>(
    sink: &mut S,
    item: Item,
    fence: Option<&InvocationFence>,
) -> Result<bool, S::Error> {
    let mut item = Some(item);
    let admitted = {
        let admission = std::future::poll_fn(|cx| {
            let mut admit = || match Pin::new(&mut *sink).poll_ready(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => Poll::Ready(
                    Pin::new(&mut *sink)
                        .start_send(item.take().expect("one socket admission"))
                        .map(|()| true),
                ),
            };
            match fence {
                Some(fence) => fence.with_current(admit).unwrap_or(Poll::Ready(Ok(false))),
                None => admit(),
            }
        });
        tokio::pin!(admission);
        if fence.is_some() {
            // InvocationFence is non-owning and has no retirement notification.
            // Recheck a stale head even if an unwritable socket never wakes us.
            let mut recheck = tokio::time::interval(std::time::Duration::from_millis(20));
            loop {
                tokio::select! {
                    biased;
                    result = &mut admission => break result?,
                    _ = recheck.tick() => {},
                }
            }
        } else {
            admission.await?
        }
    };
    if admitted {
        std::future::poll_fn(|cx| Pin::new(&mut *sink).poll_flush(cx)).await?;
    }
    Ok(admitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::*;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        task::{Context, Waker},
        time::Duration,
    };
    use tokio::time::timeout;
    #[derive(Default)]
    struct Capture(Mutex<Option<InvocationFence>>);
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "transport-fence-capture"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
            }
            ctx.push(frame, direction).await
        }
    }
    async fn source() -> (PipelineWorker, InvocationFence) {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::text("fence"), Direction::Downstream)
            .await
            .unwrap();
        loop {
            if matches!(
                timeout(Duration::from_secs(2), worker.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .frame
                    .data,
                FrameData::Text(_)
            ) {
                break;
            }
        }
        let fence = capture.0.lock().unwrap().take().unwrap();
        (worker, fence)
    }
    async fn interrupt(worker: &mut PipelineWorker) {
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        loop {
            if matches!(
                timeout(Duration::from_secs(2), worker.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .frame
                    .data,
                FrameData::Interruption
            ) {
                break;
            }
        }
    }
    async fn finish(mut worker: PipelineWorker) {
        worker.cancel("fixture complete").await.unwrap();
        worker.collect().await.unwrap();
    }
    #[derive(Default)]
    struct State {
        ready: AtomicBool,
        flush: AtomicBool,
        polls: AtomicUsize,
        admitted: AtomicUsize,
        flushed: AtomicUsize,
        wake: Mutex<Option<Waker>>,
    }
    struct Controlled(Arc<State>);
    impl Sink<u8> for Controlled {
        type Error = std::io::Error;
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.polls.fetch_add(1, Ordering::SeqCst);
            *self.0.wake.lock().unwrap() = Some(cx.waker().clone());
            if self.0.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn start_send(self: Pin<&mut Self>, _item: u8) -> Result<(), Self::Error> {
            self.0.admitted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            *self.0.wake.lock().unwrap() = Some(cx.waker().clone());
            if self.0.flush.load(Ordering::SeqCst) {
                self.0.flushed.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }
    impl State {
        fn wake(&self) {
            if let Some(waker) = self.wake.lock().unwrap().take() {
                waker.wake();
            }
        }
        async fn reached(&self, count: &AtomicUsize) {
            timeout(Duration::from_secs(2), async {
                while count.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }
    #[tokio::test]
    async fn stale_before_readiness_is_not_admitted_or_flushed() {
        let (mut worker, fence) = source().await;
        interrupt(&mut worker).await;
        let state = Arc::new(State::default());
        assert!(!send(&mut Controlled(state.clone()), 1, Some(&fence))
            .await
            .unwrap());
        assert_eq!(state.polls.load(Ordering::SeqCst), 0);
        assert_eq!(state.admitted.load(Ordering::SeqCst), 0);
        assert_eq!(state.flushed.load(Ordering::SeqCst), 0);
        finish(worker).await;
    }
    #[tokio::test]
    async fn stale_while_sink_pending_is_rechecked_under_original_fence() {
        let (mut worker, fence) = source().await;
        let state = Arc::new(State::default());
        let pending = state.clone();
        let task = tokio::spawn(async move {
            send(&mut Controlled(pending), 1, Some(&fence))
                .await
                .unwrap()
        });
        state.reached(&state.polls).await;
        interrupt(&mut worker).await;
        state.ready.store(true, Ordering::SeqCst);
        state.flush.store(true, Ordering::SeqCst);
        state.wake();
        assert!(!timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(state.admitted.load(Ordering::SeqCst), 0);
        finish(worker).await;
    }
    #[tokio::test]
    async fn admitted_item_flushes_once_even_after_interruption() {
        let (mut worker, fence) = source().await;
        let state = Arc::new(State::default());
        state.ready.store(true, Ordering::SeqCst);
        let pending = state.clone();
        let task = tokio::spawn(async move {
            send(&mut Controlled(pending), 1, Some(&fence))
                .await
                .unwrap()
        });
        state.reached(&state.admitted).await;
        interrupt(&mut worker).await;
        state.flush.store(true, Ordering::SeqCst);
        state.wake();
        assert!(timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(state.admitted.load(Ordering::SeqCst), 1);
        assert_eq!(state.flushed.load(Ordering::SeqCst), 1);
        finish(worker).await;
    }
    #[tokio::test]
    async fn finished_owner_invalidates_weak_fence_but_unfenced_send_stays_compatible() {
        let (worker, fence) = source().await;
        finish(worker).await;
        let state = Arc::new(State::default());
        state.ready.store(true, Ordering::SeqCst);
        state.flush.store(true, Ordering::SeqCst);
        assert!(!send(&mut Controlled(state.clone()), 1, Some(&fence))
            .await
            .unwrap());
        assert!(send(&mut Controlled(state.clone()), 2, None).await.unwrap());
        assert_eq!(state.admitted.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn bounded_session_queue_discards_stale_packet_and_joins_normal_close() {
        use crate::{Packet, SessionConfig, WebSocketSession};
        let (mut worker, fence) = source().await;
        interrupt(&mut worker).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut socket = WebSocketSession::accept(
                listener.accept().await.unwrap().0,
                SessionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(2), socket.recv())
                    .await
                    .unwrap(),
                Some(Packet::Text("current".into()))
            );
            assert!(timeout(Duration::from_secs(2), socket.recv())
                .await
                .unwrap()
                .is_none());
            let _ = socket.finish().await;
        });
        let client = WebSocketSession::connect(
            &format!("ws://{address}"),
            SessionConfig {
                queue_capacity: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        client
            .sender()
            .send_fenced(Packet::Text("stale".into()), fence)
            .await
            .unwrap();
        client
            .sender()
            .send(Packet::Text("current".into()))
            .await
            .unwrap();
        client.finish().await.unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        finish(worker).await;
    }
    #[tokio::test]
    async fn interruption_alone_retires_pending_sink_without_manual_wake() {
        let (mut worker, fence) = source().await;
        let state = Arc::new(State::default());
        let pending = state.clone();
        let task = tokio::spawn(async move {
            send(&mut Controlled(pending), 1, Some(&fence))
                .await
                .unwrap()
        });
        state.reached(&state.polls).await;
        interrupt(&mut worker).await;
        // Deliberately neither readiness nor a sink waker is changed.
        assert!(!timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(state.admitted.load(Ordering::SeqCst), 0);
        finish(worker).await;
    }

    #[tokio::test]
    async fn graceful_pipeline_end_waits_for_fenced_tail_barrier_before_retiring() {
        use crate::{Packet, SessionConfig, SessionSender, WebSocketSession};
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{
            tungstenite::{protocol::Role, Message},
            WebSocketStream,
        };
        struct Output(SessionSender);
        #[async_trait]
        impl FrameProcessor for Output {
            fn name(&self) -> &str {
                "barrier-output"
            }
            async fn process(
                &self,
                frame: Frame,
                direction: Direction,
                ctx: &ProcessorContext,
            ) -> Result<(), ProcessorError> {
                if matches!(frame.data, FrameData::Text(_)) {
                    self.0
                        .send_fenced(
                            Packet::Binary(vec![7; 65536].into()),
                            ctx.invocation_fence()?,
                        )
                        .await
                        .map_err(|e| ProcessorError::new(e.to_string()))?;
                }
                ctx.push(frame, direction).await
            }
            async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
                timeout(Duration::from_secs(2), self.0.flush())
                    .await
                    .map_err(|_| ProcessorError::new("barrier timeout"))?
                    .map_err(|e| ProcessorError::new(e.to_string()))
            }
        }
        let (write, read) = tokio::io::duplex(64);
        let config = SessionConfig::default();
        let socket = WebSocketStream::from_raw_socket(
            write,
            Role::Server,
            Some(config.websocket_config().unwrap()),
        )
        .await;
        let session = WebSocketSession::from_websocket(socket, config).unwrap();
        let mut peer = WebSocketStream::from_raw_socket(read, Role::Client, None).await;
        let mut worker = Pipeline::new(vec![Arc::new(Output(session.sender()))])
            .start(Default::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::text("tail"), Direction::Downstream)
            .await
            .unwrap();
        loop {
            if matches!(worker.recv().await.unwrap().frame.data, FrameData::Text(_)) {
                break;
            }
        }
        worker.end().await.unwrap();
        let mut collect = tokio::spawn(async move { worker.collect().await.unwrap() });
        assert!(
            timeout(Duration::from_millis(50), &mut collect)
                .await
                .is_err(),
            "worker retired before unwritable tail was drained"
        );
        let Some(Ok(Message::Binary(bytes))) =
            timeout(Duration::from_secs(2), peer.next()).await.unwrap()
        else {
            panic!("tail absent")
        };
        assert_eq!(bytes.as_ref(), vec![7; 65536]);
        let (_, report) = timeout(Duration::from_secs(2), collect)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.total_errors, 0);
        let close = tokio::spawn(async move { session.finish().await });
        while let Some(message) = peer.next().await {
            if matches!(message.unwrap(), Message::Close(_)) {
                break;
            }
        }
        let _ = peer.close(None).await;
        timeout(Duration::from_secs(2), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_flush_waiter_does_not_cancel_queue_or_retry() {
        use crate::{Outbound, Packet, SessionSender};
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let sender = SessionSender { tx, max: 128 };
        let abandoned = sender.clone();
        let task = tokio::spawn(async move { abandoned.flush().await });
        let Some(Outbound::Flush(reply)) = rx.recv().await else {
            panic!("barrier missing")
        };
        task.abort();
        let _ = task.await;
        assert!(reply.send(()).is_err());
        sender.send(Packet::Text("after".into())).await.unwrap();
        assert!(
            matches!(rx.recv().await, Some(Outbound::Packet { packet:Packet::Text(value), fence:None }) if value=="after")
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dropped_writer_or_cancelled_barrier_cannot_report_flush_success() {
        use crate::{Outbound, SessionSender, TransportError};
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let sender = SessionSender { tx, max: 128 };
        let task = tokio::spawn(async move { sender.flush().await });
        let Some(Outbound::Flush(reply)) = rx.recv().await else {
            panic!("barrier missing")
        };
        drop(reply); // peer close, write failure, cancel, or writer drop
        drop(rx);
        assert!(matches!(task.await.unwrap(), Err(TransportError::Closed)));
    }
}
