mod support;
use pipecat_aws_bedrock::protocol::BedrockEvent;
use pipecat_aws_bedrock::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
#[tokio::test]
async fn actual_signed_stream_decodes_single_byte_http_splits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BedrockClient::new(support::config(&listener)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let r = support::read_request(&mut socket).await;
        support::respond(
            &mut socket,
            "application/vnd.amazon.eventstream",
            &support::text_response("héllo"),
            1,
        )
        .await;
        r
    });
    let mut s = client.stream(support::request()).unwrap();
    let mut events = vec![];
    while let Some(e) = s.next().await {
        events.push(e.unwrap());
    }
    assert!(events
        .iter()
        .any(|e| matches!(e,BedrockEvent::TextDelta(s) if s=="héllo")));
    assert!(matches!(
        events.last(),
        Some(BedrockEvent::Completed { .. })
    ));
    let request = server.await.unwrap();
    assert_eq!(
        request.route,
        "POST /model/us.amazon.nova-lite-v1%3A0/converse-stream HTTP/1.1"
    );
    assert!(request.headers["authorization"].contains("/us-east-1/bedrock/aws4_request"));
    assert_eq!(request.headers["x-amz-security-token"], "FAKETOKEN/+==");
    assert_eq!(request.json()["messages"][0]["content"][0]["text"], "Hello");
}
#[tokio::test]
async fn converse_uses_nonstream_route_and_first_nonempty_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BedrockClient::new(support::config(&listener)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let r = support::read_request(&mut socket).await;
        support::respond(&mut socket,"application/json",br#"{"output":{"message":{"content":[{"text":""},{"text":"first"},{"text":"second"}]}}}"#,3).await;
        r
    });
    assert_eq!(
        client.converse(support::request(), 10).await.unwrap(),
        Some("first".into())
    );
    assert!(server.await.unwrap().route.contains("/converse HTTP"));
}
#[test]
fn sigv4_arn_route_matches_independent_hmac_and_double_encoding() {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    let client =
        BedrockClient::new(BedrockConfig::new(support::credentials(), "us-east-1")).unwrap();
    let mut input = support::request();
    input.model =
        "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.anthropic.claude-v1:0".into();
    let r = client
        .signed_request(&input, true, "20241010T010203Z")
        .unwrap();
    let route="/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Ainference-profile%2Fus.anthropic.claude-v1%3A0/converse-stream";
    assert_eq!(r.url().path(), route);
    let hash = |b: &[u8]| -> String {
        Sha256::digest(b)
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect()
    };
    let bodyhash = hash(r.body().unwrap().as_bytes().unwrap());
    let canonical=format!("POST\n{}\n\ncontent-type:application/json\nhost:bedrock-runtime.us-east-1.amazonaws.com\nx-amz-content-sha256:{bodyhash}\nx-amz-date:20241010T010203Z\nx-amz-security-token:FAKETOKEN/+==\n\ncontent-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token\n{bodyhash}",route.replace('%',"%25"));
    let mac = |k: &[u8], v: &str| {
        let mut h = Hmac::<Sha256>::new_from_slice(k).unwrap();
        h.update(v.as_bytes());
        h.finalize().into_bytes().to_vec()
    };
    let day = mac(b"AWS4FAKESECRET", "20241010");
    let region = mac(&day, "us-east-1");
    let service = mac(&region, "bedrock");
    let signing = mac(&service, "aws4_request");
    let sig = mac(
        &signing,
        &format!(
            "AWS4-HMAC-SHA256\n20241010T010203Z\n20241010/us-east-1/bedrock/aws4_request\n{}",
            hash(canonical.as_bytes())
        ),
    )
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect::<String>();
    assert!(r.headers()["authorization"]
        .to_str()
        .unwrap()
        .ends_with(&format!("Signature={sig}")));
    assert!(r.headers()["authorization"].is_sensitive());
    assert!(!format!("{:?}", r.headers()).contains("FAKETOKEN"));
}
#[tokio::test]
async fn cancellation_breaks_stalled_headers_and_drains_no_stale_output() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BedrockClient::new(support::config(&listener)).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        tx.send(()).unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(2), s.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let mut stream = client.stream(support::request()).unwrap();
    rx.await.unwrap();
    stream.cancellation_token().cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(ServiceError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
    assert_eq!(server.await.unwrap(), 0);
}
#[tokio::test]
async fn bad_crc_truncation_and_exception_never_emit_completed() {
    for mut data in [
        support::text_response("hi"),
        support::text_response("hi"),
        support::raw_record(
            "exception",
            ":exception-type",
            "validationException",
            br#"{"message":"SECRET"}"#.to_vec(),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if data.0 == 0 {
            data.1[8] ^= 1;
        }
        if data.0 == 1 {
            data.1.truncate(data.1.len() - 4);
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BedrockClient::new(support::config(&listener)).unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            support::read_request(&mut s).await;
            support::respond(&mut s, "application/vnd.amazon.eventstream", &data.1, 1000).await;
        });
        let mut stream = client.stream(support::request()).unwrap();
        let mut failed = false;
        while let Some(e) = stream.next().await {
            match e {
                Err(e) => {
                    failed = true;
                    assert!(!format!("{e:?}").contains("SECRET"));
                }
                Ok(BedrockEvent::Completed { .. }) => panic!("must not complete"),
                _ => {}
            }
        }
        assert!(failed);
        server.await.unwrap();
    }
}
#[tokio::test]
async fn redirects_are_not_followed_and_error_body_is_redacted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BedrockClient::new(support::config(&listener)).unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        s.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/steal\r\nContent-Length: 6\r\n\r\nSECRET").await.unwrap();
    });
    let mut stream = client.stream(support::request()).unwrap();
    let e = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(e, ServiceError::Http { status: 307, .. }));
    assert!(!format!("{e:?}").contains("SECRET"));
    server.await.unwrap();
}
#[tokio::test]
async fn idle_timeout_is_distinct_from_cancellation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(&listener);
    config.limits.idle_timeout = Duration::from_millis(30);
    let client = BedrockClient::new(config).unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.amazon.eventstream\r\nContent-Length: 999\r\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let mut stream = client.stream(support::request()).unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(ServiceError::Timeout))
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn valid_crc_oversized_prelude_fails_before_waiting_for_the_declared_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(&listener);
    config.limits.max_event_bytes = 1024;
    let client = BedrockClient::new(config).unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.amazon.eventstream\r\nTransfer-Encoding: chunked\r\n\r\nC\r\n").await.unwrap();
        let mut prelude = vec![];
        prelude.extend_from_slice(&((1024 * 1024u32).to_be_bytes()));
        prelude.extend_from_slice(&0u32.to_be_bytes());
        prelude.extend_from_slice(&support::crc(&prelude).to_be_bytes());
        s.write_all(&prelude).await.unwrap();
        s.write_all(b"\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    let mut stream = client.stream(support::request()).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .unwrap(),
        Some(Err(ServiceError::Protocol(_)))
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn optional_header_timeout_retry_preserves_the_exact_request_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(&listener);
    config.retry = pipecat_services::RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(30),
    };
    let client = BedrockClient::new(config).unwrap();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let a = support::read_request(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let b = support::read_request(&mut second).await;
        assert_eq!(a.body, b.body);
        assert_eq!(a.route, b.route);
        support::respond(
            &mut second,
            "application/vnd.amazon.eventstream",
            &support::text_response("retry"),
            1000,
        )
        .await;
    });
    let mut stream = client.stream(support::request()).unwrap();
    let mut completed = false;
    while let Some(event) = stream.next().await {
        completed |= matches!(event.unwrap(), BedrockEvent::Completed { .. });
    }
    assert!(completed);
    server.await.unwrap();
}
#[tokio::test]
async fn cancellation_breaks_a_full_consumer_queue_without_replaying_queued_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(&listener);
    config.limits.queue_capacity = 1;
    let client = BedrockClient::new(config).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        support::respond(
            &mut s,
            "application/vnd.amazon.eventstream",
            &support::text_response("queued"),
            1000,
        )
        .await;
        tx.send(()).unwrap();
    });
    let mut stream = client.stream(support::request()).unwrap();
    rx.await.unwrap();
    stream.cancellation_token().cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .unwrap(),
        Some(Err(ServiceError::Cancelled))
    ));
    server.await.unwrap();
}
#[test]
fn oversized_request_is_rejected_synchronously_without_a_network_task() {
    let mut config = BedrockConfig::new(support::credentials(), "us-east-1");
    config.limits.max_request_bytes = 128;
    let client = BedrockClient::new(config).unwrap();
    let mut request = support::request();
    request.context.messages[0].content = Some(pipecat_services::Content::Text("x".repeat(1024)));
    assert!(matches!(
        client.stream(request),
        Err(ServiceError::InvalidRequest(_))
    ));
}
#[tokio::test]
async fn cancellation_after_producer_completion_still_discards_queued_events_as_cancelled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BedrockClient::new(support::config(&listener)).unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        support::read_request(&mut s).await;
        support::respond(
            &mut s,
            "application/vnd.amazon.eventstream",
            &support::text_response("stale"),
            4096,
        )
        .await;
    });
    let mut stream = client.stream(support::request()).unwrap();
    server.await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    stream.cancellation_token().cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(ServiceError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
}
#[test]
fn valid_stream_outside_tokio_returns_an_explicit_error_without_panicking() {
    let client =
        BedrockClient::new(BedrockConfig::new(support::credentials(), "us-east-1")).unwrap();
    assert!(
        matches!(client.stream(support::request()),Err(ServiceError::InvalidRequest(message)) if message.contains("Tokio runtime"))
    );
}
