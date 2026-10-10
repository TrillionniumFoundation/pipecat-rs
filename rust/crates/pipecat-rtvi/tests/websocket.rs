use pipecat_core::*;
use pipecat_rtvi::*;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn websocket_client_handshake_text_and_disconnect_cross_native_pipeline() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();
        let server=tokio::spawn(async move {
            let (stream,_)=listener.accept().await.unwrap();
            let mut socket=WebSocketSession::accept(stream,SessionConfig::default()).await.unwrap();
            let processor=Arc::new(RtviProcessor::new(RtviConfig::default()).unwrap());let handle=processor.handle();
            let mut worker=Pipeline::new(vec![processor]).start(PipelineConfig::default()).await.unwrap();
            handle.set_bot_ready(None).unwrap();
            loop {
                tokio::select! {
                    packet=socket.recv()=>{
                        let Some(Packet::Text(text))=packet else { break };
                        let value:Value=serde_json::from_str(&text).unwrap();
                        worker.sender().send(Frame::new(FrameData::InputTransportMessage(value)),Direction::Downstream).await.unwrap();
                    }
                    directed=worker.recv()=>{
                        let Some(directed)=directed else { break };
                        match directed.frame.data {
                            FrameData::OutputTransportMessageUrgent(value)=>socket.sender().send(Packet::Text(value.to_string())).await.unwrap(),
                            FrameData::LlmMessagesAppend(messages)=>{
                                assert_eq!(messages.messages[0].content,"from websocket");
                                handle.send_server_message(json!({"received":messages.messages[0].content})).unwrap();
                            }
                            FrameData::End(_)=>break,
                            _=>{}
                        }
                    }
                }
            }
            let report=worker.join().await.unwrap();assert_eq!(report.state,WorkerState::Ended);
            socket.finish().await.unwrap();
        });
        let mut client=WebSocketSession::connect(&format!("ws://{address}"),SessionConfig::default()).await.unwrap();
        let tx=client.sender();
        tx.send(Packet::Text(json!({"label":"rtvi-ai","type":"client-ready","id":"ws-1","data":{"version":"2.2.0"}}).to_string())).await.unwrap();
        let Some(Packet::Text(text))=client.recv().await else { panic!("ready response missing") };let ready:Value=serde_json::from_str(&text).unwrap();assert_eq!(ready["type"],"bot-ready");assert_eq!(ready["id"],"ws-1");
        tx.send(Packet::Text(json!({"label":"rtvi-ai","type":"send-text","id":"ws-2","data":{"content":"from websocket"}}).to_string())).await.unwrap();
        let Some(Packet::Text(text))=client.recv().await else { panic!("text response missing") };let response:Value=serde_json::from_str(&text).unwrap();assert_eq!(response["data"]["received"],"from websocket");
        tx.send(Packet::Text(json!({"label":"rtvi-ai","type":"disconnect-bot","id":"ws-3"}).to_string())).await.unwrap();
        assert!(client.recv().await.is_none());client.finish().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}
