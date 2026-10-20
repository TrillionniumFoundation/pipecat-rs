//! Actual compiled recorded-PCM executable, cleared environment, synthetic key,
//! literal-loopback peers and joined child processes. No live provider or playback.
#![cfg(target_os = "linux")]
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{sleep, timeout},
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

const WAIT: Duration = Duration::from_secs(10);
const KEY: &str = "synthetic-cli-key-never-live";
const TRANSCRIPT: &str = "private-generated-transcript";
type Ws = WebSocketStream<TcpStream>;
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "grok-cli-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn input(&self, bytes: &[u8]) -> PathBuf {
        let path = self.path("input.pcm");
        fs::write(&path, bytes).unwrap();
        path
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct ChildOwner(Option<Child>);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn command(address: Option<SocketAddr>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grok_pcm"));
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(address) = address {
        command
            .env("GROK_PCM_API_KEY", KEY)
            .env("GROK_PCM_LOOPBACK_ADDRESS", address.to_string());
    }
    command
}
fn active(command: &mut Command, input: &Path, output: &Path) {
    command
        .arg("--enable")
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(output)
        .args(["--responses", "1", "--timeout-ms", "3000"]);
}
fn spawn(command: &mut Command) -> ChildOwner {
    ChildOwner(Some(command.spawn().unwrap()))
}
async fn collect(mut child: ChildOwner) -> Output {
    timeout(WAIT, async {
        loop {
            if child.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compiled Grok process exceeded bounded fixture window");
    child.0.take().unwrap().wait_with_output().unwrap()
}
fn report(output: &Output) -> Value {
    let lines: Vec<_> = output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(lines.len(), 1, "unexpected binary diagnostic shape");
    let report: Value = serde_json::from_slice(lines[0]).unwrap();
    for field in [
        "first_failure_stage",
        "mapping_primary",
        "provider_primary",
        "provider_cleanup",
        "pipeline_failure",
        "pipeline_cleanup",
    ] {
        let value = report
            .get(field)
            .expect("bounded terminal classification missing");
        assert!(value.is_null() || value.as_str().is_some_and(|text| text.len() <= 256));
    }
    assert!(report["output_cleanup_failed"].is_boolean());
    assert!(report["forced_cleanup"].is_boolean());
    report
}
fn sanitized(output: &Output) {
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        KEY,
        TRANSCRIPT,
        "private-tool-text",
        "private-provider-error",
    ] {
        assert!(
            !diagnostic.contains(secret),
            "binary leaked private fixture content"
        );
    }
    assert!(diagnostic.len() < 20_000);
}
async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
async fn no_connection(listener: &TcpListener) {
    assert!(timeout(Duration::from_millis(60), listener.accept())
        .await
        .is_err());
}
async fn release(listener: TcpListener) {
    no_connection(&listener).await;
    let address = listener.local_addr().unwrap();
    drop(listener);
    let rebound = TcpListener::bind(address)
        .await
        .expect("fixture listener not released");
    drop(rebound);
}
async fn wire(ws: &mut Ws, mut value: Value) {
    static EVENT: AtomicU64 = AtomicU64::new(0);
    value["event_id"] = json!(format!("cli-{}", EVENT.fetch_add(1, Ordering::Relaxed)));
    timeout(WAIT, ws.send(Message::Text(value.to_string().into())))
        .await
        .unwrap()
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
    assert!(message.is_text(), "unexpected non-text client frame");
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
#[allow(clippy::result_large_err)] // The tungstenite handshake callback fixes this error type.
async fn accept(listener: &TcpListener) -> Ws {
    let (stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let address = listener.local_addr().unwrap();
    timeout(
        WAIT,
        tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                  response| {
                assert_eq!(request.headers()["authorization"], format!("Bearer {KEY}"));
                assert_eq!(request.uri().path(), "/v1/realtime");
                assert_eq!(request.uri().query(), Some("model=grok-voice-latest"));
                assert_eq!(request.headers()["host"], address.to_string());
                Ok(response)
            },
        ),
    )
    .await
    .unwrap()
    .unwrap()
}
async fn ready(listener: &TcpListener, echo: bool) -> Ws {
    let mut ws = accept(listener).await;
    wire(
        &mut ws,
        json!({"type":"conversation.created","conversation":{"id":"cli-conversation"}}),
    )
    .await;
    let expected_tools = if echo {
        json!([{
            "type":"function", "name":"echo",
            "description":"Return the supplied text unchanged; no external effects.",
            "parameters":{
                "type":"object",
                "properties":{"text":{"type":"string","maxLength":1024}},
                "required":["text"], "additionalProperties":false
            }
        }])
    } else {
        json!([])
    };
    assert_eq!(
        read(&mut ws).await,
        json!({
            "type":"session.update", "event_id":"pc_evt_1",
            "session":{
                "voice":"eve", "turn_detection":{"type":"server_vad"},
                "audio":{
                    "input":{"format":{"type":"audio/pcm","rate":24000},"transport":"json"},
                    "output":{"format":{"type":"audio/pcm","rate":24000},"transport":"json"}
                },
                "tools":expected_tools
            }
        })
    );
    wire(
        &mut ws,
        json!({"type":"session.updated","session":{"id":"cli-session"}}),
    )
    .await;
    // The real app admits one empty, run_llm=false context seed after readiness.
    // The provider's first context binding always emits a session patch. With
    // the CLI's empty seed and connection-owned tool schema, that patch is
    // exactly empty: no duplicate tools/settings, instructions or seed items.
    // Consume exactly this one source-backed update, never skip arbitrary wire
    // messages. pcm() still rejects any further update or premature inference.
    assert_eq!(
        read(&mut ws).await,
        json!({
            "type":"session.update", "event_id":"pc_evt_2", "session":{}
        })
    );
    ws
}
async fn pcm(ws: &mut Ws, expected: &[u8], explicit_run: bool) {
    let mut received = vec![];
    while received.len() < expected.len() {
        let append = read(ws).await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        let chunk = STANDARD.decode(append["audio"].as_str().unwrap()).unwrap();
        assert!(!chunk.is_empty() && chunk.len() <= 4800 && chunk.len().is_multiple_of(2));
        received.extend(chunk);
    }
    assert_eq!(received, expected);
    if explicit_run {
        assert_eq!(read(ws).await["type"], "response.create");
    }
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    wire(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
async fn audio(ws: &mut Ws, response: &str, bytes: &[u8]) {
    wire(ws, json!({"type":"response.output_audio.delta","response_id":response,"item_id":"speech-item","output_index":0,"content_index":0,"delta":STANDARD.encode(bytes)})).await;
}
async fn speech(ws: &mut Ws, id: &str) {
    response(ws, id, false).await;
    audio(ws, id, &[1]).await;
    audio(ws, id, &[2, 3]).await;
    audio(ws, id, &[4]).await;
    wire(ws, json!({"type":"response.output_audio_transcript.delta","response_id":id,"item_id":"speech-item","delta":TRANSCRIPT})).await;
    wire(ws, json!({"type":"response.output_audio.done","response_id":id,"item_id":"speech-item","output_index":0,"content_index":0})).await;
    response(ws, id, true).await;
}
async fn closed(ws: &mut Ws, clean: bool) {
    match timeout(WAIT, ws.next())
        .await
        .expect("peer did not close within fixture window")
    {
        Some(Ok(Message::Close(_))) => {
            let _ = ws.flush().await;
        }
        None | Some(Err(_)) if !clean => {}
        other => panic!("unexpected terminal socket frame: {other:?}"),
    }
}
fn complete(output: &Output, path: &Path) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    sanitized(output);
    let report = report(output);
    assert_eq!(report["status"], "complete");
    assert_eq!(report["stage"], "joined");
    assert_eq!(report["recording_only"], true);
    assert_eq!(report["terminal_report_received"], true);
    assert_eq!(report["application_successful"], true);
    assert_eq!(report["output_finalized"], true);
    assert_eq!(report["output_path_matches"], true);
    assert_eq!(report["retained_file_flushed"], true);
    assert_eq!(report["owners_joined"], true);
    assert_eq!(report["recorded_bytes"], 4);
    assert_eq!(report["buffered_bytes"], 4);
    assert!(report["buffer_capacity"].as_u64().unwrap() <= 16 * 1024 * 1024);
    assert_eq!(fs::read(path).unwrap(), [1, 2, 3, 4]);
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = fs::metadata(path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(report["output_inode"], metadata.ino());
    assert_eq!(report["output_device"], metadata.dev());
}

#[tokio::test]
async fn default_help_and_check_are_credential_free_and_effect_free() {
    use std::os::unix::ffi::OsStringExt;
    let directory = Directory::new();
    let listener = listener().await;
    for args in [vec![], vec!["--help"], vec!["--check"]] {
        let mut command = command(Some(listener.local_addr().unwrap()));
        command
            .current_dir(&directory.0)
            .args(args)
            .env(
                "GROK_PCM_API_KEY",
                std::ffi::OsString::from_vec(vec![0xff; 5000]),
            )
            .env("XAI_API_KEY", "ambient-key-must-not-be-used");
        let result = collect(spawn(&mut command)).await;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
    release(listener).await;
}
#[tokio::test]
async fn invalid_arguments_and_explicit_configuration_have_no_effects() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("absent.pcm");
    let listener = listener().await;
    for (name, value) in [
        ("GROK_PCM_API_KEY", ""),
        ("GROK_PCM_API_KEY", "invalid\nheader"),
        ("GROK_PCM_LOOPBACK_ADDRESS", "localhost:7"),
        ("GROK_PCM_LOOPBACK_ADDRESS", "192.0.2.1:7"),
        ("GROK_PCM_LOOPBACK_ADDRESS", "127.0.0.1:0"),
        ("GROK_PCM_CONNECT_ADDRESS", "127.0.0.1:443"),
    ] {
        let mut command = command(Some(listener.local_addr().unwrap()));
        command.env(name, value);
        active(&mut command, &input, &output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        sanitized(&result);
        assert!(!output.exists());
    }
    let mut no_key = command(Some(listener.local_addr().unwrap()));
    no_key
        .env_remove("GROK_PCM_API_KEY")
        .env("XAI_API_KEY", KEY);
    active(&mut no_key, &input, &output);
    assert!(!collect(spawn(&mut no_key)).await.status.success());
    for args in [
        vec!["--enable"],
        vec!["--help", "extra"],
        vec!["--check", "extra"],
        vec!["--enable", "--responses", "0"],
        vec!["--enable", "--responses", "65"],
        vec!["--enable", "--responses", "1", "--responses", "1"],
        vec!["--enable", "--max-output-bytes", "3"],
        vec!["--enable", "--timeout-ms", "0"],
        vec!["--unknown"],
        vec!["--enable", "--echo-tool", "--echo-tool"],
    ] {
        let mut command = command(Some(listener.local_addr().unwrap()));
        command.args(args);
        assert!(!collect(spawn(&mut command)).await.status.success());
    }
    assert!(!output.exists());
    release(listener).await;
}
#[tokio::test]
async fn aligned_regular_bounded_input_and_create_new_paths_preflight_before_connection() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("absent.pcm");
    let listener = listener().await;
    for bytes in [vec![], vec![1], vec![0; 1024 * 1024 + 2]] {
        fs::write(&input, bytes).unwrap();
        let mut command = command(Some(listener.local_addr().unwrap()));
        active(&mut command, &input, &output);
        assert!(!collect(spawn(&mut command)).await.status.success());
        assert!(!output.exists());
    }
    fs::write(&input, [0, 0]).unwrap();
    let existing = directory.path("existing.pcm");
    fs::write(&existing, b"keep").unwrap();
    let input_link = directory.path("input-link");
    symlink(&input, &input_link).unwrap();
    let output_link = directory.path("output-link");
    symlink(&existing, &output_link).unwrap();
    let hard = directory.path("hard");
    fs::hard_link(&input, &hard).unwrap();
    fs::create_dir(directory.path("real")).unwrap();
    symlink(directory.path("real"), directory.path("directory-link")).unwrap();
    for (source, target) in [
        (input.clone(), input.clone()),
        (input.clone(), existing.clone()),
        (input_link, output.clone()),
        (input.clone(), output_link),
        (input.clone(), hard),
        (directory.0.clone(), output.clone()),
        (input.clone(), directory.path("directory-link/new")),
        (input.clone(), directory.path("real/../new")),
    ] {
        let mut command = command(Some(listener.local_addr().unwrap()));
        active(&mut command, &source, &target);
        assert!(!collect(spawn(&mut command)).await.status.success());
    }
    assert_eq!(fs::read(&input).unwrap(), [0, 0]);
    assert_eq!(fs::read(&existing).unwrap(), b"keep");
    assert!(!output.exists());
    release(listener).await;
}
#[tokio::test]
async fn actual_binary_explicit_run_records_fragmented_pcm_and_joins() {
    let directory = Directory::new();
    let bytes: Vec<_> = (0..9602).map(|n| n as u8).collect();
    let input = directory.input(&bytes);
    let output = directory.path("recording.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &bytes, true).await;
        speech(&mut ws, "speech").await;
        closed(&mut ws, true).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    command.arg("--run");
    let result = collect(spawn(&mut command)).await;
    complete(&result, &output);
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_server_vad_does_not_invent_explicit_run_or_playback() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("recording.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &[0, 0], false).await;
        wire(
            &mut ws,
            json!({"type":"input_audio_buffer.speech_started","item_id":"user","audio_start_ms":0}),
        )
        .await;
        // Speech start traverses the real user aggregator and interrupts the
        // provider. Admit exactly that cancellation before the new server turn;
        // an invented response.create, clear or truncate fails this assertion.
        assert_eq!(
            read(&mut ws).await,
            json!({"type":"response.cancel","event_id":"pc_evt_4"})
        );
        wire(
            &mut ws,
            json!({"type":"input_audio_buffer.speech_stopped","item_id":"user","audio_end_ms":20}),
        )
        .await;
        wire(&mut ws, json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user","transcript":"private-user-transcript"})).await;
        speech(&mut ws, "vad-speech").await;
        closed(&mut ws, true).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    complete(&result, &output);
    assert!(!String::from_utf8_lossy(&result.stdout).contains("private-user-transcript"));
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_server_vad_rapid_burst_preserves_speech_and_joins() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("recording.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &[0, 0], false).await;
        // Deliberately do not wait for the pipeline's interruption round trip:
        // a rapid server turn must not be lost to an old-scope cancellation.
        wire(
            &mut ws,
            json!({"type":"input_audio_buffer.speech_started","item_id":"user","audio_start_ms":0}),
        )
        .await;
        wire(
            &mut ws,
            json!({"type":"input_audio_buffer.speech_stopped","item_id":"user","audio_end_ms":20}),
        )
        .await;
        wire(&mut ws, json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user","transcript":"private-user-transcript"})).await;
        speech(&mut ws, "vad-burst-speech").await;
        assert_eq!(
            read(&mut ws).await,
            json!({"type":"response.cancel","event_id":"pc_evt_4"})
        );
        // Keep the peer alive through the real clean close, and reject every
        // extra frame, including duplicate cancel, create, clear or truncate.
        closed(&mut ws, true).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    complete(&result, &output);
    assert!(!String::from_utf8_lossy(&result.stdout).contains("private-user-transcript"));
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_final_echo_requires_assistant_continuation_and_generated_speech() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("recording.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let (tool_done, observed) = oneshot::channel();
    let (release_speech, released) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, true).await;
        pcm(&mut ws, &[0, 0], true).await;
        response(&mut ws, "tool", false).await;
        wire(&mut ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"tool-item","call_id":"call","name":"echo","arguments":""}})).await;
        wire(&mut ws, json!({"type":"response.function_call_arguments.done","call_id":"call","name":"echo","arguments":"{\"text\":\"private-tool-text\"}"})).await;
        let result = read(&mut ws).await;
        assert_eq!(result["type"], "conversation.item.create");
        assert_eq!(result["item"]["type"], "function_call_output");
        assert_eq!(result["item"]["call_id"], "call");
        let echoed: Value =
            serde_json::from_str(result["item"]["output"].as_str().unwrap()).unwrap();
        assert_eq!(echoed, json!({"text":"private-tool-text"}));
        assert!(timeout(Duration::from_millis(60), ws.next()).await.is_err());
        response(&mut ws, "tool", true).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        tool_done.send(()).unwrap();
        released.await.unwrap();
        speech(&mut ws, "continuation").await;
        closed(&mut ws, true).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    command.args(["--run", "--echo-tool"]);
    let mut child = spawn(&mut command);
    timeout(WAIT, observed).await.unwrap().unwrap();
    assert!(
        child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
        "tool-only response falsely satisfied speech condition"
    );
    assert!(fs::read(&output).unwrap().is_empty());
    release_speech.send(()).unwrap();
    let result = collect(child).await;
    complete(&result, &output);
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_absent_required_speech_times_out_cancels_and_retains_partial() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("partial.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &[0, 0], true).await;
        response(&mut ws, "empty", false).await;
        response(&mut ws, "empty", true).await;
        closed(&mut ws, false).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    command
        .arg("--enable")
        .arg("--input")
        .arg(&input)
        .arg("--output")
        .arg(&output)
        .args(["--responses", "1", "--timeout-ms", "500", "--run"]);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    sanitized(&result);
    assert_eq!(report(&result)["status"], "partial");
    assert!(fs::read(&output).unwrap().is_empty());
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_readiness_provider_and_protocol_failures_are_sanitized_partial() {
    for mode in 0..4 {
        let directory = Directory::new();
        let input = directory.input(&[0, 0]);
        let output = directory.path("partial.pcm");
        let listener = listener().await;
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let mut ws = if mode == 0 {
                accept(&listener).await
            } else {
                ready(&listener, false).await
            };
            if mode != 0 {
                pcm(&mut ws, &[0, 0], true).await;
            }
            match mode {
                0 | 1 => wire(&mut ws, json!({"type":"error","error":{"type":"server_error","code":"fixture","message":"private-provider-error"}})).await,
                2 => { ws.send(Message::Text("{malformed-private-provider-error".into())).await.unwrap(); },
                _ => {
                    response(&mut ws, "odd", false).await; audio(&mut ws, "odd", &[1]).await;
                    response(&mut ws, "odd", true).await;
                }
            }
            closed(&mut ws, false).await;
            drop(ws);
            release(listener).await;
        });
        let mut command = command(Some(address));
        active(&mut command, &input, &output);
        command.arg("--run");
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        sanitized(&result);
        assert_eq!(report(&result)["status"], "partial");
        assert!(output.exists());
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn actual_binary_failure_after_accepted_pcm_persists_exact_partial_only_after_join() {
    use std::os::unix::fs::MetadataExt;
    for malformed in [false, true] {
        let directory = Directory::new();
        let input = directory.input(&[0, 0]);
        let output = directory.path("partial.pcm");
        let listener = listener().await;
        let address = listener.local_addr().unwrap();
        let (accepted, observed) = oneshot::channel();
        let (fail, failed) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut ws = ready(&listener, true).await;
            pcm(&mut ws, &[0, 0], true).await;
            response(&mut ws, "accepted-partial", false).await;
            audio(&mut ws, "accepted-partial", &[1]).await;
            audio(&mut ws, "accepted-partial", &[2, 3, 4]).await;
            wire(&mut ws, json!({"type":"response.output_audio_transcript.delta","response_id":"accepted-partial","item_id":"speech-item","delta":TRANSCRIPT})).await;
            wire(&mut ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"partial-tool-item","call_id":"partial-call","name":"echo","arguments":""}})).await;
            wire(&mut ws, json!({"type":"response.function_call_arguments.done","call_id":"partial-call","name":"echo","arguments":"{\"text\":\"private-tool-text\"}"})).await;
            let result = read(&mut ws).await;
            assert_eq!(result["type"], "conversation.item.create");
            assert_eq!(result["item"]["type"], "function_call_output");
            assert_eq!(result["item"]["call_id"], "partial-call");
            assert_eq!(
                serde_json::from_str::<Value>(result["item"]["output"].as_str().unwrap()).unwrap(),
                json!({"text":"private-tool-text"})
            );
            response(&mut ws, "accepted-partial", true).await;
            // This genuine assistant continuation is causally after the real
            // result/response boundary through output, not an empty-queue or
            // quiet-time assumption. The preceding PCM has been accepted by
            // the same recording stage used by the production executable.
            assert_eq!(read(&mut ws).await["type"], "response.create");
            accepted.send(()).unwrap();
            failed.await.unwrap();
            if malformed {
                ws.send(Message::Text("{malformed-private-provider-error".into()))
                    .await
                    .unwrap();
            } else {
                wire(&mut ws, json!({"type":"error","error":{"type":"server_error","code":"fixture","message":"private-provider-error"}})).await;
            }
            closed(&mut ws, false).await;
            drop(ws);
            release(listener).await;
        });
        let mut command = command(Some(address));
        command
            .arg("--enable")
            .arg("--input")
            .arg(&input)
            .arg("--output")
            .arg(&output)
            .args([
                "--responses",
                "2",
                "--timeout-ms",
                "3000",
                "--run",
                "--echo-tool",
            ]);
        let mut child = spawn(&mut command);
        timeout(WAIT, observed).await.unwrap().unwrap();
        assert!(child.0.as_mut().unwrap().try_wait().unwrap().is_none());
        let original = fs::metadata(&output).unwrap();
        assert_eq!(
            original.len(),
            0,
            "PCM was persisted while application owners were live"
        );
        assert!(fs::read(&output).unwrap().is_empty());
        fail.send(()).unwrap();
        let result = collect(child).await;
        assert!(!result.status.success());
        sanitized(&result);
        let report = report(&result);
        assert_eq!(report["status"], "partial");
        assert_eq!(report["terminal_report_received"], true);
        assert_eq!(report["owners_joined"], true);
        assert_eq!(report["application_successful"], false);
        assert_eq!(report["output_finalized"], true);
        assert_eq!(report["retained_file_flushed"], true);
        assert_eq!(report["output_path_matches"], true);
        assert_eq!(report["recorded_bytes"], 4);
        assert_eq!(report["buffered_bytes"], 4);
        assert_eq!(report["required_speech_responses"], 2);
        assert_eq!(report["output_device"], original.dev());
        assert_eq!(report["output_inode"], original.ino());
        assert_eq!(fs::read(&output).unwrap(), [1, 2, 3, 4]);
        assert_eq!(fs::metadata(&output).unwrap().ino(), original.ino());
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn actual_binary_output_exhaustion_retains_bounded_partial_bytes() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("partial.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &[0, 0], true).await;
        response(&mut ws, "overflow", false).await;
        audio(&mut ws, "overflow", &[1, 2]).await;
        audio(&mut ws, "overflow", &[3, 4]).await;
        closed(&mut ws, false).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    command.args(["--run", "--max-output-bytes", "2"]);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    sanitized(&result);
    assert_eq!(report(&result)["status"], "partial");
    let partial = fs::read(&output).unwrap();
    assert!(partial.is_empty() || partial == [1, 2]);
    assert_eq!(report(&result)["recorded_bytes"], partial.len());
    assert_eq!(report(&result)["buffered_bytes"], partial.len());
    assert_eq!(report(&result)["buffer_capacity"], 2);
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_output_path_swap_does_not_write_replacement() {
    let directory = Directory::new();
    let input = directory.input(&[0, 0]);
    let output = directory.path("recording.pcm");
    let retained = directory.path("retained.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let (connected, observed) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut ws = ready(&listener, false).await;
        pcm(&mut ws, &[0, 0], true).await;
        connected.send(()).unwrap();
        resumed.await.unwrap();
        speech(&mut ws, "swapped").await;
        closed(&mut ws, false).await;
        drop(ws);
        release(listener).await;
    });
    let mut command = command(Some(address));
    active(&mut command, &input, &output);
    command.arg("--run");
    let child = spawn(&mut command);
    timeout(WAIT, observed).await.unwrap().unwrap();
    fs::rename(&output, &retained).unwrap();
    fs::write(&output, b"replacement remains untouched").unwrap();
    resume.send(()).unwrap();
    let result = collect(child).await;
    assert!(!result.status.success());
    sanitized(&result);
    assert_eq!(report(&result)["status"], "partial");
    assert_eq!(report(&result)["owners_joined"], true);
    assert_eq!(report(&result)["application_successful"], true);
    assert_eq!(report(&result)["output_finalized"], false);
    assert_eq!(report(&result)["stage"], "persistence");
    assert_eq!(fs::read(&output).unwrap(), b"replacement remains untouched");
    assert_eq!(fs::read(&retained).unwrap(), [1, 2, 3, 4]);
    assert_eq!(report(&result)["recorded_bytes"], 4);
    assert_eq!(report(&result)["buffered_bytes"], 4);
    assert_eq!(report(&result)["output_path_matches"], false);
    assert_eq!(report(&result)["retained_file_flushed"], true);
    use std::os::unix::fs::MetadataExt;
    assert_eq!(
        report(&result)["output_inode"],
        fs::metadata(&retained).unwrap().ino()
    );
    peer.await.unwrap();
}
