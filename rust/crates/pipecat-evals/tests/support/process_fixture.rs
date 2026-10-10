// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Opt-in process ownership fixture. Never a provider or production bot.
use pipecat_evals::serializer::{decode_control, EvalControl};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::json;
use std::{io::Write, path::PathBuf, time::Duration};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mode = &args[0];
    let port: u16 = args[1].parse()?;
    let dir = PathBuf::from(&args[2]);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("pid.tmp"), std::process::id().to_string())?;
    std::fs::rename(dir.join("pid.tmp"), dir.join("pid"))?;
    println!("fixture started");
    eprintln!("fixture diagnostic");
    if mode == "flood" {
        let bytes = vec![b'x'; 512 * 1024];
        loop {
            std::io::stdout().write_all(&bytes)?;
        }
    }
    if mode == "exit-start" {
        std::process::exit(17);
    }
    if mode == "overflow-start" {
        std::io::stdout().write_all(&vec![b'x'; 128 * 1024])?;
        std::io::stdout().flush()?;
    }
    if mode == "hang-start" || mode == "overflow-start" {
        tokio::time::sleep(Duration::from_secs(60)).await;
        return Ok(());
    }
    if mode == "inspect" {
        println!(
            "{}",
            json!({"cwd":std::env::current_dir()?,"args":args,
            "inherited_path":std::env::var("PATH").ok(),
            "allowed":std::env::var("EVAL_ALLOWED").ok()})
        );
        if let Some(path) = args.get(3) {
            let bytes = std::fs::read(path)?;
            println!("body={}", String::from_utf8(bytes)?);
            std::fs::write(dir.join("body-path"), path)?;
        }
    }
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    std::fs::write(dir.join("listening"), b"yes")?;
    let (stream, _) = listener.accept().await?;
    let config = SessionConfig {
        close_timeout: Duration::from_millis(50),
        write_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let mut socket = WebSocketSession::accept(stream, config).await?;
    std::fs::write(dir.join("connected"), b"yes")?;
    while let Some(packet) = socket.recv().await {
        let bytes = match packet {
            Packet::Text(s) => s.into_bytes(),
            Packet::Binary(b) => b.to_vec(),
        };
        match decode_control(&bytes, 1024 * 1024)? {
            EvalControl::Cancel => {
                std::fs::write(dir.join("cleanup"), b"yes")?;
                if mode == "exit-cleanup" {
                    std::process::exit(19);
                }
                break;
            }
            EvalControl::Rtvi(message) => {
                let replies = match message.message_type.as_str() {
                    "client-ready" => vec![("bot-ready", json!({}))],
                    "send-text" => {
                        std::fs::write(dir.join("run"), b"yes")?;
                        if mode == "exit-run" {
                            std::process::exit(18);
                        }
                        if mode == "hang-run" {
                            continue;
                        }
                        if mode == "slow-reply" {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        let text = if mode == "large-reply" {
                            "hello ".repeat(4000)
                        } else {
                            message
                                .data
                                .as_ref()
                                .and_then(|d| d.get("content"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_owned()
                        };
                        vec![
                            ("bot-llm-started", json!({})),
                            ("bot-llm-text", json!({"text":text})),
                            ("bot-llm-stopped", json!({})),
                        ]
                    }
                    _ => vec![],
                };
                for (kind, data) in replies {
                    socket
                        .sender()
                        .send(Packet::Text(
                            json!({"label":"rtvi-ai","type":kind,"data":data}).to_string(),
                        ))
                        .await?;
                }
            }
            _ => {}
        }
    }
    let _ = socket.finish().await;
    if mode == "ignore-stop" {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    Ok(())
}
