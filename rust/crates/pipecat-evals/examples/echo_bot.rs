//! A native RTVI eval target: cargo run -p pipecat-evals --example echo_bot -- 7860
//! This deterministic test target echoes text; it is not an LLM or parity claim.
use pipecat_evals::serializer::{decode_control, EvalControl};
use pipecat_protocol::rtvi::{Envelope, MESSAGE_LABEL};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::json;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).unwrap_or("7860".into());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    eprintln!("native echo eval target: ws://{}", listener.local_addr()?);
    loop {
        let (stream, _) = listener.accept().await?;
        let mut socket = WebSocketSession::accept(stream, SessionConfig::default()).await?;
        let mut stop = false;
        while let Some(packet) = socket.recv().await {
            let bytes = match packet {
                Packet::Text(s) => s.into_bytes(),
                Packet::Binary(b) => b.to_vec(),
            };
            match decode_control(&bytes, 1024 * 1024)? {
                EvalControl::Cancel => {
                    stop = true;
                    break;
                }
                EvalControl::Rtvi(message) => {
                    let replies = match message.message_type.as_str() {
                        "client-ready" => vec![("bot-ready", json!({"version":"2.2.0"}))],
                        "send-text" => vec![
                            ("bot-llm-started", json!({})),
                            (
                                "bot-llm-text",
                                json!({"text":message.data.as_ref().and_then(|d|d.get("content")).and_then(|v|v.as_str()).unwrap_or("")}),
                            ),
                            ("bot-llm-stopped", json!({})),
                        ],
                        _ => vec![],
                    };
                    for (kind, data) in replies {
                        let event = Envelope {
                            label: MESSAGE_LABEL.into(),
                            message_type: kind.into(),
                            id: message.id.clone(),
                            data: Some(data),
                        };
                        socket.sender().send(Packet::Text(event.encode()?)).await?;
                    }
                }
                _ => {}
            }
        }
        let _ = socket.finish().await;
        if stop {
            break;
        }
    }
    Ok(())
}
