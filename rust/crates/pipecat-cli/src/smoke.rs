//! Credential-free real loopback sockets with synthetic provider responses.
//! This verifies protocol and pipeline integration, not live model quality or
//! physical microphone/speaker behavior.
use crate::{
    config::{Options, ProviderConfig, VadPaths, DEFAULT_SYSTEM},
    wire, AbortTask, AgentError,
};
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_services::{Content, Role};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
const USER: &str = "hello native agent";
const ASSISTANT: &str = "Hello from Rust!";
const INPUT_BYTES: usize = 1024;
const PCM: &[u8] = &[0, 0, 1, 0, 255, 255, 2, 0];
#[derive(Clone, Copy)]
#[allow(dead_code)]
enum CompletionMode {
    Acknowledge,
    Introduction,
    EndUnplayed,
    Interrupt,
    Disconnect,
}
pub struct SmokeReport {
    pub audio_bytes: usize,
}

pub async fn run() -> Result<SmokeReport, AgentError> {
    tokio::time::timeout(
        Duration::from_secs(20),
        run_inner(CompletionMode::Acknowledge),
    )
    .await
    .map_err(|_| AgentError::Smoke("full pipeline timed out"))?
}
/// Real Silero model/processor in the same synthetic-provider loopback pipeline.
pub async fn run_with_vad(paths: VadPaths) -> Result<SmokeReport, AgentError> {
    tokio::time::timeout(
        Duration::from_secs(30),
        run_inner_with_vad(CompletionMode::Acknowledge, Some(paths)),
    )
    .await
    .map_err(|_| AgentError::Smoke("model-backed pipeline timed out"))?
}
async fn run_inner(mode: CompletionMode) -> Result<SmokeReport, AgentError> {
    run_inner_with_vad(mode, None).await
}
async fn run_inner_with_vad(
    mode: CompletionMode,
    vad: Option<VadPaths>,
) -> Result<SmokeReport, AgentError> {
    let dg = TcpListener::bind("127.0.0.1:0").await?;
    let llm = TcpListener::bind("127.0.0.1:0").await?;
    let tts = TcpListener::bind("127.0.0.1:0").await?;
    let agent = TcpListener::bind("127.0.0.1:0").await?;
    let agent_url = format!("ws://{}", agent.local_addr()?);
    let providers = ProviderConfig {
        deepgram_key: "synthetic-only".into(),
        openai_key: "synthetic-only".into(),
        cartesia_key: "synthetic-only".into(),
        deepgram_endpoint: format!("ws://{}/v1/listen", dg.local_addr()?),
        openai_endpoint: format!("http://{}/v1/chat/completions", llm.local_addr()?),
        cartesia_endpoint: format!("ws://{}/tts/websocket", tts.local_addr()?),
        model: "synthetic-model".into(),
        voice_id: "synthetic-voice".into(),
        system_prompt: DEFAULT_SYSTEM.into(),
        input_rate: 16000,
        output_rate: 24000,
    };
    let mut dg_task = AbortTask(tokio::spawn(deepgram(
        dg,
        !matches!(mode, CompletionMode::Introduction),
    )));
    let mut llm_task = AbortTask(tokio::spawn(openai(
        llm,
        matches!(mode, CompletionMode::Introduction),
    )));
    let mut tts_task = AbortTask(tokio::spawn(cartesia(tts)));
    let mut server = AbortTask(tokio::spawn(async move {
        let (stream, _) = agent.accept().await?;
        let options = Options {
            vad,
            introduce: matches!(mode, CompletionMode::Introduction),
            turn_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        crate::run_session(stream, &options, &providers).await
    }));
    let mut client = WebSocketSession::connect(&agent_url, SessionConfig::default()).await?;
    if !matches!(mode, CompletionMode::Introduction) {
        client
            .sender()
            .send(wire::packet(Kind::Audio(pb::AudioRawFrame {
                id: 1,
                name: "InputAudioRawFrame#1".into(),
                audio: vec![0; INPUT_BYTES],
                sample_rate: 16000,
                num_channels: 1,
                pts: Some(0),
            }))?)
            .await?;
    }
    let mut audio = Vec::new();
    let mut playback_id = None;
    let mut acked = false;
    while let Some(packet) = client.recv().await {
        let Packet::Binary(bytes) = packet else {
            return Err(AgentError::Smoke(
                "agent must return protobuf binary packets",
            ));
        };
        match pb::Frame::parse(&bytes)?.frame {
            Some(Kind::Audio(pcm)) => {
                if playback_id.is_none() || pcm.sample_rate != 24000 || pcm.num_channels != 1 {
                    return Err(AgentError::Smoke(
                        "output PCM framing or context order differs",
                    ));
                }
                audio.extend_from_slice(&pcm.audio);
            }
            Some(Kind::Message(message)) => {
                let message: Value = serde_json::from_str(&message.data)
                    .map_err(|_| AgentError::Smoke("invalid output control"))?;
                match message["type"].as_str() {
                    Some("playback-start") => {
                        playback_id = message["context_id"].as_str().map(str::to_owned)
                    }
                    Some("playback-ready") => {
                        if message["context_id"].as_str() != playback_id.as_deref() || audio != PCM
                        {
                            return Err(AgentError::Smoke(
                                "synthesis completed before the expected PCM",
                            ));
                        }
                        // Only this synthetic test client simulates playback.
                        match mode {
                            CompletionMode::Acknowledge | CompletionMode::Introduction => client
                                .sender()
                                .send(wire::message(
                                    json!({"type":"playback-complete","context_id":playback_id}),
                                )?)
                                .await?,
                            CompletionMode::EndUnplayed => {
                                client
                                    .sender()
                                    .send(wire::message(json!({"type":"end"}))?)
                                    .await?;
                                break;
                            }
                            CompletionMode::Interrupt => {
                                client
                                    .sender()
                                    .send(wire::message(json!({"type":"interrupt"}))?)
                                    .await?;
                                client.sender().send(wire::message(json!({"type":"playback-complete","context_id":playback_id}))?).await?;
                                client
                                    .sender()
                                    .send(wire::message(json!({"type":"end"}))?)
                                    .await?;
                                break;
                            }
                            CompletionMode::Disconnect => break,
                        }
                    }
                    Some("playback-acknowledged") => {
                        acked = true;
                        client
                            .sender()
                            .send(wire::message(json!({"type":"end"}))?)
                            .await?;
                        break;
                    }
                    _ => {}
                }
            }
            Some(Kind::Interruption(_)) => {
                audio.clear();
                playback_id = None;
            }
            _ => {}
        }
    }
    if acked
        != matches!(
            mode,
            CompletionMode::Acknowledge | CompletionMode::Introduction
        )
    {
        return Err(AgentError::Smoke(
            "playback acknowledgement differs from client mode",
        ));
    }
    let client = if matches!(mode, CompletionMode::Disconnect) {
        let _ = client.cancel().await;
        None
    } else {
        Some(client)
    };
    let report = (&mut server.0).await??;
    if let Some(client) = client {
        let _ = client.finish().await;
    }
    (&mut dg_task.0).await??;
    (&mut llm_task.0).await??;
    (&mut tts_task.0).await??;
    let messages = &report.context.messages;
    let expected_messages = if acked { 2 } else { 1 };
    let (first_role, first_text) = if matches!(mode, CompletionMode::Introduction) {
        (Role::Developer, "Please introduce yourself to the user.")
    } else {
        (Role::User, USER)
    };
    if messages.len() != expected_messages
        || messages[0].role != first_role
        || messages[0].content != Some(Content::Text(first_text.into()))
    {
        return Err(AgentError::Smoke(
            "conversation context differs from confirmed user turn",
        ));
    }
    if acked
        && (messages[1].role != Role::Assistant
            || messages[1].content != Some(Content::Text(ASSISTANT.into())))
    {
        return Err(AgentError::Smoke(
            "conversation context differs from confirmed assistant turn",
        ));
    }
    if report.pipeline.total_errors > 0 {
        return Err(AgentError::Smoke("pipeline reported errors"));
    }
    Ok(SmokeReport {
        audio_bytes: audio.len(),
    })
}
async fn deepgram(listener: TcpListener, expect_audio: bool) -> Result<(), AgentError> {
    let (stream, _) = listener.accept().await?;
    let mut session = WebSocketSession::accept(stream, SessionConfig::default()).await?;
    let mut transcribed = false;
    while let Some(packet) = session.recv().await {
        match packet {
            Packet::Binary(audio) => {
                if transcribed || audio != vec![0; INPUT_BYTES] {
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
    if transcribed != expect_audio {
        return Err(AgentError::Smoke("STT never received audio"));
    }
    Ok(())
}
async fn openai(listener: TcpListener, introduction: bool) -> Result<(), AgentError> {
    let (mut socket, _) = listener.accept().await?;
    let body = read_http_json(&mut socket).await?;
    let (role, text) = if introduction {
        ("developer", "Please introduce yourself to the user.")
    } else {
        ("user", USER)
    };
    let message_present = body["messages"].as_array().is_some_and(|messages| {
        messages
            .iter()
            .any(|m| m["role"] == role && m["content"] == text)
    });
    if body["stream"] != true || !message_present {
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
async fn cartesia(listener: TcpListener) -> Result<(), AgentError> {
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
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn full_native_voice_pipeline_uses_real_loopback_protocols() {
        let report = run().await.expect("native loopback smoke");
        assert_eq!(report.audio_bytes, 8);
    }
    #[tokio::test]
    async fn introduction_reaches_first_http_request_without_user_audio() {
        tokio::time::timeout(
            Duration::from_secs(10),
            run_inner(CompletionMode::Introduction),
        )
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn socket_provider_done_without_playback_ack_does_not_commit_speech() {
        tokio::time::timeout(
            Duration::from_secs(10),
            run_inner(CompletionMode::EndUnplayed),
        )
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn socket_late_ack_after_explicit_interruption_is_ignored() {
        tokio::time::timeout(
            Duration::from_secs(10),
            run_inner(CompletionMode::Interrupt),
        )
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn socket_disconnect_cancels_pipeline_without_committing_unplayed_speech() {
        tokio::time::timeout(
            Duration::from_secs(10),
            run_inner(CompletionMode::Disconnect),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
