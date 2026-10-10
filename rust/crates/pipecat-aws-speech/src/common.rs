use futures_util::SinkExt;
use pipecat_services::CancellationToken;
use std::future::Future;
use tokio::{net::TcpStream, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderValue, Request},
        protocol::WebSocketConfig,
        Message,
    },
    MaybeTlsStream, WebSocketStream,
};

pub(crate) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

use crate::{StreamingError, StreamingLimits};

pub(crate) fn validate_limits(limits: &StreamingLimits) -> Result<(), StreamingError> {
    if limits.queue_capacity == 0
        || limits.queue_capacity > 65536
        || limits.max_message_bytes == 0
        || limits.connect_timeout.is_zero()
        || limits.write_timeout.is_zero()
        || limits.response_timeout.is_zero()
        || limits.close_timeout.is_zero()
    {
        return Err(StreamingError::Invalid(
            "limits must be positive and queue capacity <= 65536",
        ));
    }
    Ok(())
}

pub(crate) fn request(
    url: &str,
    headers: &[(&'static str, &str)],
) -> Result<Request<()>, StreamingError> {
    let mut request = url
        .into_client_request()
        .map_err(|_| StreamingError::Invalid("WebSocket request"))?;
    for &(name, value) in headers {
        let mut value = HeaderValue::from_str(value)
            .map_err(|_| StreamingError::Invalid("authentication header"))?;
        value.set_sensitive(true);
        request.headers_mut().insert(name, value);
    }
    Ok(request)
}
pub(crate) fn transport(error: tokio_tungstenite::tungstenite::Error) -> StreamingError {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(response) => StreamingError::Http(response.status().as_u16()),
        Error::Capacity(_) => StreamingError::TooLarge,
        Error::AlreadyClosed | Error::ConnectionClosed => StreamingError::Closed,
        _ => StreamingError::Transport,
    }
}
pub(crate) async fn connect(
    request: Request<()>,
    limits: &StreamingLimits,
) -> Result<Socket, StreamingError> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(limits.max_message_bytes))
        .max_frame_size(Some(limits.max_message_bytes));
    tokio::time::timeout(
        limits.connect_timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(config), true),
    )
    .await
    .map_err(|_| StreamingError::Timeout("connect"))?
    .map(|(socket, _)| socket)
    .map_err(transport)
}
pub(crate) async fn write(
    socket: &mut Socket,
    message: Message,
    limits: &StreamingLimits,
) -> Result<(), StreamingError> {
    tokio::time::timeout(limits.write_timeout, socket.send(message))
        .await
        .map_err(|_| StreamingError::Timeout("write"))?
        .map_err(transport)
}
pub(crate) async fn close(socket: &mut Socket, limits: &StreamingLimits) {
    let _ = tokio::time::timeout(limits.close_timeout, socket.close(None)).await;
}
pub(crate) async fn cancelled<F, T>(
    cancel: &CancellationToken,
    future: F,
) -> Result<T, StreamingError>
where
    F: Future<Output = Result<T, StreamingError>>,
{
    tokio::select! { biased; _ = cancel.cancelled() => Err(StreamingError::Cancelled), result = future => result }
}

pub(crate) struct EventStream<E> {
    pub(crate) rx: mpsc::Receiver<E>,
    pub(crate) cancel: CancellationToken,
    pub(crate) task: JoinHandle<Result<(), StreamingError>>,
    pub(crate) ended: bool,
}
impl<E> EventStream<E> {
    pub(crate) async fn next(&mut self) -> Option<Result<E, StreamingError>> {
        if self.ended {
            return None;
        }
        if self.cancel.is_cancelled() {
            self.rx.close();
            while self.rx.try_recv().is_ok() {}
        } else {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => { self.rx.close(); while self.rx.try_recv().is_ok() {} }
                event = self.rx.recv() => { if let Some(event) = event { return Some(Ok(event)); } }
            }
        }
        // Waiting on a JoinHandle is cancellation-safe. Do not make the stream
        // terminal before the await: a caller can drop next() in a select or
        // timeout while bounded socket cleanup is still running, then retry.
        let result = (&mut self.task).await;
        self.ended = true;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(StreamingError::Transport)),
        }
    }
}
impl<E> Drop for EventStream<E> {
    fn drop(&mut self) {
        // The actor observes cancellation outside queue waits, then performs
        // bounded protocol cleanup before its detached task exits.
        self.cancel.cancel();
    }
}

pub(crate) struct Input<C> {
    pub(crate) tx: mpsc::Sender<C>,
    pub(crate) cancel: CancellationToken,
    pub(crate) limits: StreamingLimits,
    pub(crate) finished: std::sync::Arc<std::sync::Mutex<bool>>,
}
impl<C> Clone for Input<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            cancel: self.cancel.clone(),
            limits: self.limits.clone(),
            finished: self.finished.clone(),
        }
    }
}
impl<C> Input<C> {
    pub(crate) async fn send(&self, command: C, terminal: bool) -> Result<(), StreamingError> {
        if *self.finished.lock().map_err(|_| StreamingError::Closed)? {
            return Err(StreamingError::Closed);
        }
        cancelled(&self.cancel, async {
            let permit = tokio::time::timeout(self.limits.write_timeout, self.tx.reserve())
                .await
                .map_err(|_| StreamingError::Timeout("input queue"))?
                .map_err(|_| StreamingError::Closed)?;
            let mut finished = self.finished.lock().map_err(|_| StreamingError::Closed)?;
            if *finished {
                return Err(StreamingError::Closed);
            }
            if self.cancel.is_cancelled() {
                return Err(StreamingError::Cancelled);
            }
            permit.send(command);
            *finished = terminal;
            Ok(())
        })
        .await
    }
    pub(crate) fn try_send(&self, command: C, terminal: bool) -> Result<(), StreamingError> {
        if self.cancel.is_cancelled() {
            return Err(StreamingError::Cancelled);
        }
        let mut finished = self.finished.lock().map_err(|_| StreamingError::Closed)?;
        if *finished {
            return Err(StreamingError::Closed);
        }
        self.tx.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => StreamingError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => StreamingError::Closed,
        })?;
        *finished = terminal;
        Ok(())
    }
}
