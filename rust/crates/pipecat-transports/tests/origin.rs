use pipecat_transports::*;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, StatusCode},
    Error,
};
#[tokio::test]
async fn loopback_origin_policy_rejects_external_null_and_duplicate_before_upgrade() {
    for origin in [
        vec![],
        vec!["http://localhost:8080"],
        vec!["https://127.0.0.1"],
        vec!["http://[::1]:8080"],
        vec!["null"],
        vec!["https://evil.example"],
        vec!["http://127.0.0.1.evil.example"],
        vec!["http://localhost/evil"],
        vec!["http://localhost", "https://evil.example"],
    ] {
        let accepted = origin.is_empty()
            || (origin.len() == 1
                && [
                    "http://localhost:8080",
                    "https://127.0.0.1",
                    "http://[::1]:8080",
                ]
                .contains(&origin[0]));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let result = WebSocketSession::accept_with_origin_policy(
                socket,
                SessionConfig::default(),
                OriginPolicy::LoopbackOnlyAllowMissing,
            )
            .await;
            if accepted {
                result.unwrap().cancel().await.unwrap();
            } else {
                assert!(result.is_err());
            }
        });
        let mut request = format!("ws://{addr}").into_client_request().unwrap();
        for origin in origin {
            request
                .headers_mut()
                .append("origin", HeaderValue::from_str(origin).unwrap());
        }
        let result = tokio_tungstenite::connect_async(request).await;
        if accepted {
            assert!(result.is_ok());
        } else {
            match result {
                Err(Error::Http(response)) => assert_eq!(response.status(), StatusCode::FORBIDDEN),
                other => panic!("unexpected handshake: {other:?}"),
            }
        }
        server.await.unwrap();
    }
}
