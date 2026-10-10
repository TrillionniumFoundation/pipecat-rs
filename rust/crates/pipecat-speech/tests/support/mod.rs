use pipecat_speech::SpeechConfig;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
#[derive(Debug)]
pub struct Request {
    pub head: String,
    pub body: Vec<u8>,
}
impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.trim()))
    }
    pub fn url(&self) -> reqwest::Url {
        reqwest::Url::parse(&format!(
            "http://localhost{}",
            self.head
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
        ))
        .unwrap()
    }
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}
pub async fn read_request(socket: &mut TcpStream) -> Request {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let end = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(key, val)| {
            key.eq_ignore_ascii_case("content-length")
                .then(|| val.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    Request {
        head,
        body: bytes[end..end + length].to_vec(),
    }
}
pub async fn server(
    status: &str,
    mime: &str,
    chunks: Vec<Vec<u8>>,
    truncated: bool,
    delay: Duration,
) -> (SpeechConfig, oneshot::Receiver<Request>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::elevenlabs("synthetic-secret");
    config.base_url = format!("http://{}/proxy", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(2);
    let (sender, request) = oneshot::channel();
    let headers = format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = sender.send(read_request(&mut socket).await);
        tokio::time::sleep(delay).await;
        if socket.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        for chunk in chunks {
            assert!(!chunk.is_empty());
            if socket
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
                || socket.write_all(&chunk).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        if !truncated {
            let _ = socket.write_all(b"0\r\n\r\n").await;
        }
    });
    (config, request, task)
}
