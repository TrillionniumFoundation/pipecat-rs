use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

#[derive(Debug)]
pub enum Response {
    Json(Value),
    Status(u16),
    Raw { declared: usize, body: Vec<u8> },
    Hang,
}
#[derive(Debug)]
pub struct Request {
    pub headers: String,
    pub body: Value,
    pub respond: oneshot::Sender<Response>,
}
pub struct Peer {
    pub url: String,
    pub requests: mpsc::Receiver<Request>,
    pub count: Arc<AtomicUsize>,
    pub active: Arc<AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<usize>>,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Peer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (send, requests) = mpsc::channel(8);
        let (stop, mut cancelled) = oneshot::channel();
        let count = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let accepted = count.clone();
        let active_task = active.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            let mut joined = 0;
            loop {
                tokio::select! { biased;
                    _ = &mut cancelled => break,
                    Some(result) = children.join_next(), if !children.is_empty() => { result.unwrap(); joined += 1; },
                    incoming = listener.accept(), if children.len() < 8 => {
                        let (socket, _) = incoming.unwrap();
                        accepted.fetch_add(1, Ordering::SeqCst);
                        let send = send.clone(); let active = active_task.clone();
                        children.spawn(async move {
                            active.fetch_add(1, Ordering::SeqCst);
                            let _active = Active(active);
                            // Entire peer operation, including a deliberately stalled body, has a bound.
                            let _ = tokio::time::timeout(Duration::from_secs(10), handle(socket, send)).await;
                        });
                    }
                }
            }
            children.abort_all();
            while let Some(result) = children.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled(), "peer child panicked: {error}");
                }
                joined += 1;
            }
            joined
        });
        Self {
            url,
            requests,
            count,
            active,
            stop: Some(stop),
            task: Some(task),
        }
    }
    pub async fn request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub async fn finish(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(3), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(joined, self.count.load(Ordering::SeqCst));
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
async fn handle(mut socket: TcpStream, send: mpsc::Sender<Request>) {
    let mut bytes = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() <= 64 * 1024, "bounded request headers");
        if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(length <= 64 * 1024, "bounded request body");
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() <= end + length + chunk.len());
    }
    let body = serde_json::from_slice(&bytes[end..end + length]).unwrap();
    let (respond, receive) = oneshot::channel();
    send.try_send(Request {
        headers,
        body,
        respond,
    })
    .expect("bounded HTTP request journal");
    let Ok(response) = receive.await else {
        return;
    };
    let (status, declared, body) = match response {
        Response::Json(value) => {
            let body = serde_json::to_vec(&value).unwrap();
            (200, body.len(), body)
        }
        Response::Status(status) => (status, 2, b"{}".to_vec()),
        Response::Raw { declared, body } => (200, declared, body),
        Response::Hang => {
            std::future::pending::<()>().await;
            unreachable!()
        }
    };
    let headers = format!("HTTP/1.1 {status} Synthetic\r\nContent-Type: application/json\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n");
    if socket.write_all(headers.as_bytes()).await.is_ok() {
        let _ = socket.write_all(&body).await;
    }
}
