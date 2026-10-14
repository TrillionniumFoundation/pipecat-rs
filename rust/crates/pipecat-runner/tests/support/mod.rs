//! Synthetic wire fixtures; every provider listens only on loopback.
use pipecat_cli::AgentError;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
const USER: &str = "hello native agent";
const ASSISTANT: &str = "Hello from Rust!";
pub async fn deepgram(listener: &TcpListener, expect_audio: bool) -> Result<(), AgentError> {
    let (stream, _) = listener.accept().await?;
    let mut session = WebSocketSession::accept(stream, SessionConfig::default()).await?;
    let mut transcribed = false;
    let mut drained_tail = false;
    while let Some(packet) = session.recv().await {
        match packet {
            Packet::Binary(audio) => {
                if transcribed {
                    if drained_tail || audio != vec![0; 68] {
                        return Err(AgentError::Smoke(
                            "STT did not receive the exact terminal PCM tail",
                        ));
                    }
                    drained_tail = true;
                    continue;
                }
                if audio != vec![0; 956] {
                    return Err(AgentError::Smoke("STT did not receive exact input PCM"));
                }
                transcribed = true;
                session.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"speech_final":true,"start":0.0,"duration":0.032,"channel":{"alternatives":[{"transcript":USER,"confidence":1.0,"words":[]}]}}).to_string())).await?;
            }
            Packet::Text(text) => {
                let value: Value = serde_json::from_str(&text)
                    .map_err(|_| AgentError::Smoke("invalid STT request"))?;
                if value["type"] == "CloseStream" {
                    break;
                }
                if value["type"] == "Finalize" {
                    session.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}}).to_string())).await?;
                }
            }
        }
    }
    let _ = session.finish().await;
    if transcribed != expect_audio || drained_tail != expect_audio {
        return Err(AgentError::Smoke("STT never received audio"));
    }
    Ok(())
}
pub async fn openai(listener: &TcpListener, introduction: bool) -> Result<(), AgentError> {
    openai_expected(listener, introduction, USER).await
}
pub async fn openai_expected(
    listener: &TcpListener,
    introduction: bool,
    expected_user: &str,
) -> Result<(), AgentError> {
    let (role, content) = if introduction {
        ("developer", json!("Please introduce yourself to the user."))
    } else {
        ("user", json!(expected_user))
    };
    openai_expected_content(listener, role, content).await
}
pub async fn openai_expected_content(
    listener: &TcpListener,
    role: &str,
    content: Value,
) -> Result<(), AgentError> {
    let (mut socket, _) = listener.accept().await?;
    let body = read_http_json(&mut socket).await?;
    let message_present = body["messages"].as_array().is_some_and(|messages| {
        messages
            .iter()
            .any(|m| m["role"] == role && m["content"] == content)
    });
    if body["stream"] != true
        || !message_present
        || body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["role"] == "user")
            .count()
            != 1
        || body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "assistant")
    {
        return Err(AgentError::Smoke(
            "LLM did not receive fresh conversation context",
        ));
    }
    let chunks = [
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{"content":"Hello "}}]})
        ),
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{"content":"from Rust!"}}]})
        ),
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}})
        ),
        "data: [DONE]\n\n".into(),
    ];
    let length: usize = chunks.iter().map(String::len).sum();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    for chunk in chunks {
        socket.write_all(chunk.as_bytes()).await?;
        socket.flush().await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}
async fn read_http_json(socket: &mut TcpStream) -> Result<Value, AgentError> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    let end = loop {
        let n = socket.read(&mut buffer).await?;
        if n == 0 || bytes.len() + n > 1024 * 1024 {
            return Err(AgentError::Smoke("invalid loopback HTTP request"));
        }
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end])
        .map_err(|_| AgentError::Smoke("invalid HTTP headers"))?;
    let len = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|s| s.trim().parse::<usize>().ok())
        })
        .filter(|n| *n < 1024 * 1024)
        .ok_or(AgentError::Smoke("missing HTTP length"))?;
    while bytes.len() < end + len {
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Err(AgentError::Smoke("truncated HTTP body"));
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len])
        .map_err(|_| AgentError::Smoke("invalid HTTP JSON"))
}
pub async fn cartesia(listener: &TcpListener) -> Result<(), AgentError> {
    let (stream, _) = listener.accept().await?;
    let mut session = WebSocketSession::accept(stream, SessionConfig::default()).await?;
    let mut transcript = String::new();
    let mut context_id = None;
    while let Some(packet) = session.recv().await {
        let Packet::Text(text) = packet else {
            return Err(AgentError::Smoke("TTS received non-JSON request"));
        };
        let value: Value =
            serde_json::from_str(&text).map_err(|_| AgentError::Smoke("invalid TTS request"))?;
        let id = value["context_id"]
            .as_str()
            .ok_or(AgentError::Smoke("missing TTS context ID"))?;
        if context_id.as_ref().is_some_and(|old| old != id) {
            return Err(AgentError::Smoke("TTS turn changed context ID"));
        }
        context_id = Some(id.to_owned());
        transcript.push_str(
            value["transcript"]
                .as_str()
                .ok_or(AgentError::Smoke("missing TTS transcript"))?,
        );
        if value["continue"] == false {
            if transcript != ASSISTANT || value["output_format"]["sample_rate"] != 24000 {
                return Err(AgentError::Smoke(
                    "TTS did not receive complete LLM text and audio format",
                ));
            }
            for message in [
                json!({"type":"chunk","context_id":id,"data":"AAABAP//AgA="}),
                json!({"type":"timestamps","context_id":id,"word_timestamps":{"words":["Hello","from","Rust!"],"start":[0.0,0.00003,0.00006],"end":[0.00003,0.00006,0.0001]}}),
                json!({"type":"done","context_id":id}),
            ] {
                session
                    .sender()
                    .send(Packet::Text(message.to_string()))
                    .await?;
            }
            let _ = session.finish().await;
            return Ok(());
        }
    }
    Err(AgentError::Smoke(
        "TTS text stream ended without a finish marker",
    ))
}
