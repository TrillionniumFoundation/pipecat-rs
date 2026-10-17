//! Ordinary Gemini Parts, raw backpressure, and record-atomic publication.
//! Also included by the turns fixture, so its existing dependency graph covers
//! both raw HTTP events and the real assistant consumer.
use pipecat_services::{adapters::request_body, protocol::ProtocolDecoder, sse::SseEvent, *};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

pub(crate) const WAIT: Duration = Duration::from_secs(10);
pub(crate) type Check<T> = Result<T, String>;

pub(crate) fn candidate(parts: Value) -> Value {
    json!({"candidates":[{"index":0,"content":{"role":"model","parts":parts}}]})
}
pub(crate) fn stopped() -> Value {
    json!({"candidates":[{"index":0,"finishReason":"STOP"}]})
}
pub(crate) fn wire(records: &[Value]) -> Vec<u8> {
    records
        .iter()
        .map(|r| format!("data: {r}\n\n"))
        .collect::<String>()
        .into_bytes()
}
pub(crate) fn config(listener: &TcpListener) -> ClientConfig {
    let mut config = ClientConfig::new(Provider::Gemini, "synthetic-thought-fixture-key");
    config.endpoint = format!(
        "http://{}/models/{{model}}:streamGenerateContent?alt=sse",
        listener.local_addr().unwrap()
    );
    config.channel_capacity = 1;
    config.request_timeout = Duration::from_secs(30);
    config.idle_timeout = Duration::from_secs(20);
    config
}
pub(crate) fn request() -> CompletionRequest {
    CompletionRequest::new(
        "gemini-3-fixture",
        Context::new(vec![Message::text(Role::User, "question")]),
    )
}
pub(crate) async fn read_request(socket: &mut TcpStream) -> Check<Value> {
    let mut bytes = vec![];
    let mut part = [0; 4096];
    let end = loop {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request headers".into());
        }
        bytes.extend_from_slice(&part[..n]);
        if bytes.len() > 1024 * 1024 {
            return Err("oversize request".into());
        }
        if let Some(i) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse::<usize>())
        })
        .ok_or("missing request length")?
        .map_err(|e| e.to_string())?;
    while bytes.len() < end + length {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request body".into());
        }
        bytes.extend_from_slice(&part[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())
}
pub(crate) async fn header(socket: &mut TcpStream) -> Check<()> {
    socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n").await.map_err(|e| e.to_string())
}
pub(crate) async fn chunk(socket: &mut TcpStream, records: &[Value]) -> Check<()> {
    let bytes = wire(records);
    socket
        .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    socket.write_all(&bytes).await.map_err(|e| e.to_string())?;
    socket.write_all(b"\r\n").await.map_err(|e| e.to_string())
}
pub(crate) async fn eof(socket: &mut TcpStream) -> Check<()> {
    socket
        .write_all(b"0\r\n\r\n")
        .await
        .map_err(|e| e.to_string())
}
pub(crate) async fn closed(socket: &mut TcpStream) -> Check<()> {
    let mut byte = [0];
    match socket.read(&mut byte).await {
        Ok(0) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        other => Err(format!("expected client EOF/reset, got {other:?}")),
    }
}
pub(crate) async fn join_peer<T: std::fmt::Debug>(mut peer: JoinHandle<Check<T>>) -> Check<T> {
    match timeout(WAIT, &mut peer).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            peer.abort();
            let result = peer.await;
            Err(format!("peer timeout; aborted and joined: {result:?}"))
        }
    }
}
fn decode(decoder: &mut ProtocolDecoder, value: Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn signed_parts() -> Value {
    json!([
        {"text":"first","thought":true,"thoughtSignature":"first+/=="},
        {"text":"","thought":true,"thoughtSignature":"empty+/=="},
        {"text":" \t ","thought":true,"thoughtSignature":"second+/=="},
        {"text":"answer","thoughtSignature":"answer+/=="},
        {"functionCall":{"id":"call","name":"echo","args":{"n":1}},"thoughtSignature":"tool+/=="}
    ])
}
fn visible_events() -> Vec<StreamEvent> {
    vec![
        StreamEvent::ResponseProgress,
        StreamEvent::ThoughtStart,
        StreamEvent::ThoughtDelta("first".into()),
        StreamEvent::ThoughtEnd,
        StreamEvent::ThoughtStart,
        StreamEvent::ThoughtDelta(" \t ".into()),
        StreamEvent::ThoughtEnd,
        StreamEvent::TextDelta("answer".into()),
        StreamEvent::ToolCall(ToolCall {
            id: "call".into(),
            name: "echo".into(),
            arguments: json!({"n":1}),
        }),
    ]
}
#[test]
fn each_nonempty_thought_part_closes_before_record_checkpoint_and_tail() {
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    let mut events = decode(&mut decoder, candidate(signed_parts())).unwrap();
    let StreamEvent::GeminiCheckpoint(history) = events.pop().expect("record checkpoint") else {
        panic!("checkpoint must be last")
    };
    assert_eq!(events, visible_events());
    assert_eq!(
        request_body(
            Provider::Gemini,
            &CompletionRequest::new("gemini-3-fixture", Context::new(history))
        )
        .unwrap()["contents"][0]["parts"],
        signed_parts()
    );
    assert!(decode(&mut decoder, stopped()).unwrap().is_empty());
    let terminal = decoder.end_of_stream().unwrap();
    assert!(matches!(
        terminal.as_slice(),
        [StreamEvent::GeminiHistory(_)]
    ));
    assert!(
        decoder.end_of_stream().unwrap().is_empty(),
        "normal Gemini must not add another End"
    );
}
#[test]
fn cross_record_parts_keep_separate_boundaries_and_empty_unsigned_parts_emit_none() {
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    assert_eq!(
        decode(&mut decoder, candidate(json!([{"text":"","thought":true}]))).unwrap(),
        vec![StreamEvent::ResponseProgress]
    );
    for text in ["one", " ", "two"] {
        assert_eq!(
            decode(
                &mut decoder,
                candidate(json!([{"text":text,"thought":true}]))
            )
            .unwrap(),
            vec![
                StreamEvent::ThoughtStart,
                StreamEvent::ThoughtDelta(text.into()),
                StreamEvent::ThoughtEnd
            ]
        );
    }
    assert!(
        decode(&mut decoder, candidate(json!([{"text":"","thought":true}])))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        decode(&mut decoder, candidate(json!([{"text":"tail"}]))).unwrap(),
        vec![StreamEvent::TextDelta("tail".into())]
    );
    decode(&mut decoder, stopped()).unwrap();
    decoder.end_of_stream().unwrap();
    assert!(decoder.end_of_stream().unwrap().is_empty());
}

async fn stalled() -> Check<(ClientConfig, JoinHandle<Check<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        read_request(&mut socket).await?;
        header(&mut socket).await?;
        chunk(
            &mut socket,
            &[candidate(json!([
                {"text":"private","thought":true,"thoughtSignature":"unpublished+/=="},
                {"text":"must-not-leak"}
            ]))],
        )
        .await?;
        closed(&mut socket).await
    });
    Ok((config, peer))
}
async fn take(stream: &mut LlmStream, expected: &[StreamEvent]) -> Check<()> {
    for want in expected {
        let actual = stream
            .next()
            .await
            .ok_or("raw EOF before expected event")?
            .map_err(|e| e.to_string())?;
        if &actual != want {
            return Err(format!("expected {want:?}, got {actual:?}"));
        }
    }
    Ok(())
}
async fn drain(stream: &mut LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    let mut events = vec![];
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    events
}
// Current-thread polling matters: cancellation follows the returned receipt
// synchronously, before the producer can fill the newly freed capacity-one slot.
#[tokio::test]
async fn capacity_one_cancel_balances_only_successfully_enqueued_boundaries() {
    let prefix = [
        StreamEvent::Started,
        StreamEvent::ResponseProgress,
        StreamEvent::ThoughtStart,
        StreamEvent::ThoughtDelta("private".into()),
        StreamEvent::ThoughtEnd,
    ];
    for count in [2, 3, 4, 5] {
        let (config, peer) = stalled().await.unwrap();
        let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
        let observed = timeout(WAIT, take(&mut stream, &prefix[..count])).await;
        stream.cancel();
        // Draining to None proves the raw event channel has closed, not that a
        // private HTTP JoinHandle was joined (LlmStream exposes none). cancel()
        // alone cannot settle finalization while its consumer is backpressured.
        let terminal = timeout(WAIT, drain(&mut stream)).await;
        drop(stream);
        let peer = join_peer(peer).await;
        assert!(
            matches!(observed, Ok(Ok(()))),
            "receipt={observed:?}; peer={peer:?}"
        );
        peer.unwrap();
        let terminal = terminal.expect("raw owner must close after drain");
        let mut expected = vec![];
        if count == 3 || count == 4 {
            expected.push("end");
        }
        expected.extend(["cancelled", "finished"]);
        let actual: Vec<_> = terminal
            .iter()
            .map(|event| match event {
                Ok(StreamEvent::ThoughtEnd) => "end",
                Err(ServiceError::Cancelled) => "cancelled",
                Ok(StreamEvent::Finished { .. }) => "finished",
                other => panic!("undelivered event escaped cancellation: {other:?}"),
            })
            .collect();
        assert_eq!(actual, expected, "consumed prefix length {count}");
    }
}
#[tokio::test]
async fn capacity_one_deadline_closes_an_enqueued_start_before_error_and_finished() {
    let (mut config, peer) = stalled().await.unwrap();
    config.request_timeout = Duration::from_secs(2);
    let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
    let observed = timeout(
        WAIT,
        take(
            &mut stream,
            &[
                StreamEvent::Started,
                StreamEvent::ResponseProgress,
                StreamEvent::ThoughtStart,
            ],
        ),
    )
    .await;
    // Keep the receiver undrained until the actual HTTP owner closes its socket.
    // Delta may fill the slot; End cannot. The outer request deadline interrupts
    // the blocked send and finalization waits for the consumer to drain.
    let peer = join_peer(peer).await;
    let terminal = timeout(WAIT, drain(&mut stream)).await;
    stream.cancel();
    drop(stream);
    assert!(
        matches!(observed, Ok(Ok(()))),
        "receipt={observed:?}; peer={peer:?}"
    );
    peer.unwrap();
    let mut terminal = terminal.expect("raw owner must close after deadline drain");
    if matches!(terminal.first(), Some(Ok(StreamEvent::ThoughtDelta(text))) if text == "private") {
        assert!(
            matches!(terminal.remove(0), Ok(StreamEvent::ThoughtDelta(text)) if text == "private")
        );
    }
    assert!(
        matches!(
            terminal.as_slice(),
            [
                Ok(StreamEvent::ThoughtEnd),
                Err(ServiceError::Timeout),
                Ok(StreamEvent::Finished { .. })
            ]
        ),
        "{terminal:?}"
    );
}
#[tokio::test]
async fn dropping_raw_receiver_closes_http_without_promising_an_end_receipt() {
    let (config, peer) = stalled().await.unwrap();
    let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
    let observed = timeout(
        WAIT,
        take(
            &mut stream,
            &[
                StreamEvent::Started,
                StreamEvent::ResponseProgress,
                StreamEvent::ThoughtStart,
            ],
        ),
    )
    .await;
    drop(stream);
    let peer = join_peer(peer).await;
    assert!(
        matches!(observed, Ok(Ok(()))),
        "receipt={observed:?}; peer={peer:?}"
    );
    peer.unwrap();
}
#[tokio::test]
async fn malformed_record_publishes_no_thought_boundary_text_or_checkpoint() {
    for bad in [
        json!({"text":"bad","thoughtSignature":false}),
        json!({"text":false}),
        json!({"functionCall":{"id":"bad","name":"echo","args":[]}}),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            header(&mut socket).await?;
            chunk(&mut socket, &[candidate(json!([{"text":"never-published","thought":true,"thoughtSignature":"never-committed+/=="}, bad]))]).await?;
            closed(&mut socket).await
        });
        let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
        let events = timeout(WAIT, drain(&mut stream)).await;
        stream.cancel();
        drop(stream);
        let peer = join_peer(peer).await;
        peer.unwrap();
        assert!(matches!(
            events.expect("raw owner EOF").as_slice(),
            [
                Ok(StreamEvent::Started),
                Err(ServiceError::Protocol(_)),
                Ok(StreamEvent::Finished { .. })
            ]
        ));
    }
}
