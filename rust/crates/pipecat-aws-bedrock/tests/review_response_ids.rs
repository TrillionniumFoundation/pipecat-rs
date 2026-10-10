mod support;
use pipecat_aws_bedrock::*;
use pipecat_core::*;
use pipecat_services::{ClientConfig, LlmClient, LlmProcessor, Provider};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn actual_anthropic_gemini_and_bedrock_admissions_share_unique_response_ids() {
    let mut ids = BTreeSet::new();
    for provider in [Some(Provider::Anthropic), Some(Provider::Gemini), None] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let processor: Arc<dyn FrameProcessor> = match provider {
            Some(provider) => {
                let mut config = ClientConfig::new(provider, "fake-test-key");
                config.endpoint = format!("http://{}/", listener.local_addr().unwrap());
                Arc::new(LlmProcessor::new(
                    LlmClient::new(config).unwrap(),
                    support::request(),
                ))
            }
            None => Arc::new(BedrockProcessor::new(
                BedrockClient::new(support::config(&listener)).unwrap(),
                support::request(),
            )),
        };
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        let mut worker = Pipeline::new(vec![processor])
            .start(Default::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        worker.end().await.unwrap();
        let id = tokio::time::timeout(Duration::from_secs(3), async {
            let mut id = None;
            while let Some(frame) = worker.recv().await {
                if matches!(frame.frame.data, FrameData::LlmResponseStart) {
                    assert!(id.is_none());
                    id = Some(
                        frame
                            .frame
                            .native_llm_continuation()
                            .unwrap()
                            .unwrap()
                            .response_id(),
                    );
                }
            }
            worker.join().await.unwrap();
            id.expect("provider must admit a response before HTTP fails")
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert!(
            ids.insert(id),
            "provider {provider:?} reused response id {id}"
        );
    }
    assert_eq!(ids.len(), 3);
}
