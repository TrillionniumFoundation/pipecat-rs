//! JSONL media endpoint. Hepta remains the model/session/tool owner.
use pipecat_hepta::{
    local_speech::*, HeptaOutputScope, HmiSession, OutputIdentity, PresentationState,
};
use serde::Deserialize;
use serde_json::json;
use std::{future::Future, io::Read, pin::Pin, time::Duration};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Speak {
        scope: HeptaOutputScope,
        text: String,
    },
    Interrupt,
    Turn {
        scope: HeptaOutputScope,
    },
    Shutdown,
}
type Render<'a> = Pin<
    Box<dyn Future<Output = Result<pipecat_local_audio::SubmissionReceipt, LocalSpeechError>> + 'a>,
>;
struct Active<'a> {
    identity: OutputIdentity,
    render: Render<'a>,
}

// Cancellation-safe: partial reads live in the caller's bounded buffer.
async fn line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    pending: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if pending.is_empty() {
                Ok(None)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete JSONL command",
                ))
            };
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if count > 65_536 - pending.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSONL command exceeds 64 KiB",
            ));
        }
        pending.extend_from_slice(&available[..count]);
        reader.consume(count);
        if pending.last() == Some(&b'\n') {
            return Ok(Some(std::mem::take(pending)));
        }
    }
}
fn emit(tx: &tokio::sync::mpsc::Sender<Vec<u8>>, value: serde_json::Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(&value).map_err(|_| "event encoding failed")?;
    if bytes.len() > 65_535 {
        return Err("event exceeds 64 KiB".into());
    }
    bytes.push(b'\n');
    tx.try_send(bytes)
        .map_err(|_| "media event receiver closed or overloaded".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Tokio stdin uses a non-cancellable OS read. Native owners have already
    // joined before this bounded runtime teardown; no model work is detached.
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 1 {
        return Err("usage: local_speech CONFIG_JSON; stdin/stdout use bounded JSONL".into());
    }
    let mut config_bytes = Vec::new();
    std::fs::File::open(&args[0])?
        .take(128 * 1024 + 1)
        .read_to_end(&mut config_bytes)?;
    if config_bytes.len() > 128 * 1024 {
        return Err("configuration exceeds 128 KiB".into());
    }
    let config: LocalSpeechConfig = serde_json::from_slice(&config_bytes)?;
    let mut session = HmiSession::with_epoch(
        config.stream.clone(),
        config.turn_id.clone(),
        config.output_epoch,
        128,
    )?;
    let mut speech = LocalSpeech::load(config).await?;
    speech.presentation().open_output()?;
    let mut input = speech.start_input().await?;
    let media = speech.presentation();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    let mut writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(bytes) = rx.recv().await {
            match tokio::time::timeout(Duration::from_secs(2), stdout.write_all(&bytes)).await {
                Ok(Ok(())) => {}
                _ => break,
            }
        }
    });
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut partial = Vec::new();
    let mut active: Option<Active<'_>> = None;
    let mut writer_finished = false;
    let result: Result<(), String> = loop {
        tokio::select! {
            _ = &mut writer => {
                writer_finished = true;
                break Err("media event receiver closed or stalled".into());
            }
            incoming = line(&mut reader, &mut partial) => {
                let bytes = match incoming { Ok(Some(bytes)) => bytes, Ok(None) => break Ok(()), Err(error) => break Err(error.to_string()) };
                let command = match serde_json::from_slice::<Command>(&bytes) { Ok(command) => command, Err(_) => break Err("invalid JSONL command".into()) };
                match command {
                    Command::Shutdown => break Ok(()),
                    Command::Interrupt => {
                        let outcome = match session.barge_in(media) { Ok(value) => value, Err(error) => break Err(error.to_string()) };
                        active.take();
                        if let Err(error) = emit(&tx, json!({"type":"turn_interrupt", "request":outcome.request, "local_error":outcome.local_error})) { break Err(error); }
                    }
                    Command::Turn { scope } => {
                        let identity = match OutputIdentity::try_from(scope) { Ok(value) => value, Err(error) => break Err(error.to_string()) };
                        if identity.stream != *session.identity() { break Err("foreign Hepta turn scope".into()); }
                        let same_turn = session.check_current(&identity).is_ok();
                        let local_error = match session.attach_turn(identity.turn_id.clone(), identity.output_epoch, media) { Ok(value) => value, Err(error) => break Err(error.to_string()) };
                        if !same_turn { active.take(); }
                        if let Err(error) = media.admit_turn(&identity.turn_id, identity.output_epoch) { break Err(error.to_string()); }
                        if let Err(error) = emit(&tx, json!({"type":"turn_attached", "identity":identity, "local_error":local_error})) { break Err(error); }
                    }
                    Command::Speak { scope, text } => {
                        let identity = match OutputIdentity::try_from(scope) { Ok(value) => value, Err(error) => break Err(error.to_string()) };
                        if active.is_some() || text.len() > 4096 || text.trim().is_empty() { break Err("speech request is invalid or output is busy".into()); }
                        if let Err(error) = session.generated(identity.clone()).and_then(|()| session.transition(&identity, PresentationState::Queued)) { break Err(error.to_string()); }
                        let scope = identity.clone();
                        active = Some(Active { identity, render: Box::pin(async move { let audio = media.synthesize(scope, &text).await?; media.submit(audio).await }) });
                    }
                }
            }
            event = input.recv() => {
                match event {
                    Some(RecognitionEvent::SpeechStarted) => {
                        let outcome = match session.barge_in(media) { Ok(value) => value, Err(error) => break Err(error.to_string()) };
                        active.take();
                        if let Err(error) = emit(&tx, json!({"type":"turn_interrupt", "request":outcome.request, "local_error":outcome.local_error})) { break Err(error); }
                    }
                    Some(RecognitionEvent::FinalTranscript(text)) => {
                        // Caller must bind a retained audio asset/span before Hepta admission.
                        if let Err(error) = emit(&tx, json!({"type":"unbound_transcript", "stream":session.identity(), "transcript":text})) { break Err(error); }
                    }
                    Some(RecognitionEvent::Failure(_)) => break Err("recognition pipeline reported failure".into()),
                    None => break Err("recognition pipeline closed".into()),
                }
            }
            rendered = async { match &mut active { Some(active) => active.render.as_mut().await, None => std::future::pending().await } } => {
                let current = active.take().expect("selected active render");
                let state = if rendered.is_ok() { PresentationState::Submitted } else { PresentationState::Unknown };
                if let Err(error) = session.transition(&current.identity, state) { break Err(error.to_string()); }
                if let Err(error) = emit(&tx, json!({"type":"presentation", "identity":&current.identity, "state":state, "error":rendered.err().map(|error|error.to_string())})) { break Err(error); }
                if let Err(error) = session.release_terminal(&current.identity.utterance_id) { break Err(error.to_string()); }
            }
        }
    };
    // Every malformed/oversized/closed command path revokes before native cleanup.
    let retired = session.barge_in(media);
    drop(active);
    let terminal = input.shutdown().await;
    let cleanup = speech.shutdown().await;
    drop(tx);
    if !writer_finished {
        writer.abort();
        let _ = writer.await;
    }
    result.map_err(std::io::Error::other)?;
    if let Some(error) = retired?.local_error {
        return Err(error.into());
    }
    if let Some(error) = terminal.error {
        return Err(error.into());
    }
    if terminal.total_errors != 0 {
        return Err("input reported errors before shutdown".into());
    }
    cleanup?;
    Ok(())
}
