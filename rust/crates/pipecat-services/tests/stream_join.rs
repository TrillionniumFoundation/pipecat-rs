//! Actual HTTP request producer ownership, including blocked terminal delivery.
use pipecat_services::*;
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
async fn request(socket: &mut TcpStream) {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let length = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&chunk[..count]);
    }
}
#[tokio::test]
async fn actual_http_preheader_owner_cancels_and_joins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        let _ = tx.send(());
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
    });
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = endpoint;
    config.channel_capacity = 1;
    let mut stream = LlmClient::new(config)
        .unwrap()
        .stream(CompletionRequest::new(
            "synthetic",
            Context::new(vec![Message::text(Role::User, "hello")]),
        ))
        .unwrap();
    rx.await.unwrap();
    stream
        .cancel_and_join(Duration::from_secs(1))
        .await
        .unwrap();
    stream.cancel_and_join(Duration::ZERO).await.unwrap();
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_http_backpressured_progress_queue_joins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"stop"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        let _ = tx.send(());
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
    });
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = endpoint;
    config.channel_capacity = 1;
    let mut stream = LlmClient::new(config)
        .unwrap()
        .stream(CompletionRequest::new(
            "synthetic",
            Context::new(vec![Message::text(Role::User, "hello")]),
        ))
        .unwrap();
    rx.await.unwrap();
    // Do not drain Started: the first provider progress event is backpressured.
    // Closing admission must unblock producer retirement and error delivery.
    stream
        .cancel_and_join(Duration::from_secs(1))
        .await
        .unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn actual_http_successful_final_usage_delivery_is_backpressured_and_joined() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        // No choices means no ResponseProgress or text events. Successful
        // decoder completion next publishes Usage then Finished, outside run.
        let body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
        let _ = tx.send(());
    });
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = endpoint;
    config.channel_capacity = 1;
    let mut stream = LlmClient::new(config)
        .unwrap()
        .stream(CompletionRequest::new(
            "synthetic",
            Context::new(vec![Message::text(Role::User, "hello")]),
        ))
        .unwrap();
    // Started still occupies the only slot. Peer EOF is evidence that HTTP
    // ownership ended; Usage finalization cannot enter the full receiver.
    rx.await.unwrap();
    stream
        .cancel_and_join(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(StreamEvent::Started))
    ));
    assert!(stream.next().await.is_none());
    peer.await.unwrap();
}
