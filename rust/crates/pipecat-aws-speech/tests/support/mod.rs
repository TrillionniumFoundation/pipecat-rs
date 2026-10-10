use tokio::{io::AsyncReadExt, net::TcpStream};
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
