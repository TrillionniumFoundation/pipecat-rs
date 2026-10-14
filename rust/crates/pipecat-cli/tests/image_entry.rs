//! Real binary configuration and public provider entry, not browser rendering.
#[path = "support/image_fixture.rs"]
mod image_fixture;
use fixture::*;
use image_fixture::chunk as fixture;
use pipecat_cli::{config::Options, receipt_output::PlaybackProtocol};
use pipecat_protocol::protobuf::frame::Kind;
use pipecat_services::{Content, Role};
use serde_json::{json, Value};
use std::{process::Command, time::Duration};

fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pipecat-voice-agent"));
    command.env_clear();
    command
}
fn options() -> Options {
    Options {
        playback_protocol: PlaybackProtocol::ChunkV2,
        introduce: false,
        turn_timeout: Duration::from_millis(20),
        ..Default::default()
    }
}
fn require(ok: bool, message: impl Into<String>) -> TestResult<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn acceptance(offer: &Value) -> Value {
    json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],"chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true})
}
async fn complete_audio(f: &mut Fixture, offer: &Value) -> TestResult<()> {
    start_user(f.client()?).await?;
    let mut bytes = vec![];
    let mut expected = pcm();
    expected.resize(CHUNK_BYTES * 3, 0);
    let mut previous_id = 0;
    loop {
        match next(f.client()?).await? {
            Kind::Audio(audio) => {
                let meta: Value = serde_json::from_str(&audio.name).map_err(|e| e.to_string())?;
                require(
                    meta["session_id"] == offer["session_id"] && meta["chunk_id"] == audio.id,
                    "PCM identity",
                )?;
                require(
                    audio.sample_rate == 24000 && audio.num_channels == 1,
                    "PCM format",
                )?;
                require(
                    bytes.len() + audio.audio.len() <= expected.len(),
                    "extra PCM",
                )?;
                require(
                    audio.audio.len() == CHUNK_BYTES
                        && audio.audio == expected[bytes.len()..bytes.len() + CHUNK_BYTES],
                    "exact transformed chunk and zero padding",
                )?;
                require(audio.id > previous_id, "chunk IDs must increase")?;
                previous_id = audio.id;
                bytes.extend_from_slice(&audio.audio);
                send(f.client()?, json!({"type":"chunk-complete","version":2,"session_id":meta["session_id"],"epoch":meta["epoch"],"chunk_id":meta["chunk_id"],"receipt_nonce":meta["receipt_nonce"]})).await?;
            }
            Kind::Message(message) => {
                let value: Value =
                    serde_json::from_str(&message.data).map_err(|e| e.to_string())?;
                if value["type"] == "playback-v2-context-committed" {
                    require(
                        bytes == expected && value["committed_contexts"] == 1,
                        "exact PCM or history commit differs",
                    )?;
                    return Ok(());
                }
            }
            other => return Err(format!("unexpected audio event {other:?}")),
        }
    }
}
#[tokio::test]
async fn default_provider_entry_keeps_exact_audio_history_and_joined_end() {
    let mut f = Fixture::start(options()).await.unwrap();
    let observed = async {
        let offer = control(next(f.client()?).await?)?;
        require(
            offer["type"] == "playback-v2-offer" && offer.get("image_channel").is_none(),
            "default image capability changed",
        )?;
        send(f.client()?, acceptance(&offer)).await?;
        complete_audio(&mut f, &offer).await?;
        send(f.client()?, json!({"type":"end"})).await?;
        f.wait_closed().await?;
        Ok::<_, String>(())
    }
    .await;
    let closed = f.close().await;
    eprintln!(
        "entry baseline observation={observed:?}; owner={:?}; cleanup={:?}",
        closed.server.as_ref().err(),
        closed.cleanup_errors
    );
    assert!(observed.is_ok(), "{observed:?}");
    assert!(
        closed.cleanup_errors.is_empty(),
        "{:?}",
        closed.cleanup_errors
    );
    let report = closed.server.unwrap();
    assert_eq!(report.pipeline.total_errors, 0);
    assert_eq!(closed.contacts, 3);
    let assistant: Vec<_> = report
        .context
        .messages
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .collect();
    assert_eq!(assistant.len(), 1);
    assert_eq!(assistant[0].content, Some(Content::Text(ASSISTANT.into())));
}
#[test]
fn desired_actual_binary_image_option_reaches_credential_validation() {
    let output = binary()
        .args(["--playback-protocol", "chunk-v2", "--image-channel"])
        .output()
        .unwrap();
    let error = String::from_utf8(output.stderr).unwrap();
    eprintln!(
        "actual child exit={:?}; stderr={error:?}; no image I/O or physical cancellation claimed",
        output.status.code()
    );
    assert!(!output.status.success());
    assert!(
        error.contains("DEEPGRAM_API_KEY is required"),
        "image option is not reachable from the actual binary: {error}"
    );
}

use pipecat_cli::config::{Command as ParsedCommand, StartupImage};
use pipecat_cli::receipt_output::ImageChannelConfig;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, OutputImageRawFrame,
    ProcessorContext, ProcessorError, StartFrame,
};
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::net::TcpStream;

struct Asset(PathBuf);
impl Asset {
    fn new(bytes: &[u8]) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pipecat-image-entry-{}-{}.rgb",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        Self(path)
    }
    fn args(&self, size: &str) -> Vec<String> {
        [
            "--playback-protocol",
            "chunk-v2",
            "--image-channel",
            "--no-intro",
            "--turn-timeout-ms",
            "20",
            "--initial-image-rgb",
            self.0.to_str().unwrap(),
            "--initial-image-size",
            size,
        ]
        .map(str::to_owned)
        .to_vec()
    }
}
impl Drop for Asset {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn parsed(args: Vec<String>) -> Options {
    let ParsedCommand::Serve(options) = ParsedCommand::parse(args).unwrap() else {
        panic!("not serve")
    };
    *options
}
fn image_options() -> Options {
    parsed(
        [
            "--playback-protocol",
            "chunk-v2",
            "--image-channel",
            "--no-intro",
            "--turn-timeout-ms",
            "20",
        ]
        .map(str::to_owned)
        .to_vec(),
    )
}
async fn image_offer(f: &mut Fixture) -> TestResult<Value> {
    let offer = control(next(f.client()?).await?)?;
    require(offer["type"] == "playback-v2-offer", "wrong offer")?;
    require(
        offer["image_channel"]["session_id"] == offer["session_id"]
            && offer["image_channel"]["version"] == 1,
        "image session/version",
    )?;
    require(
        f.provider_contacts.load(Ordering::SeqCst) == 0,
        "provider contacted before accept",
    )?;
    let mut accepted = acceptance(&offer);
    accepted["independent_images"] = json!(true);
    send(f.client()?, accepted).await?;
    Ok(offer)
}
async fn listener_closed(offer: &Value) -> TestResult<()> {
    let address = offer["image_channel"]["address"]
        .as_str()
        .ok_or("missing listener")?;
    bounded(async {
        require(
            TcpStream::connect(address).await.is_err(),
            "image listener survived owner",
        )
    })
    .await
}
fn joined(observed: TestResult<()>, closed: Closed, success: bool) {
    eprintln!(
        "entry observation={observed:?}; owner={:?}; cleanup={:?}; providers={:?}",
        closed.server.as_ref().err(),
        closed.cleanup_errors,
        closed.provider_exits
    );
    assert!(observed.is_ok(), "{observed:?}");
    assert!(
        closed.cleanup_errors.is_empty(),
        "{:?}",
        closed.cleanup_errors
    );
    if success {
        let report = closed.server.unwrap();
        assert_eq!(
            report.pipeline.total_errors, 0,
            "{:?}",
            report.pipeline.errors
        );
        assert_eq!(closed.contacts, 3);
        let assistant: Vec<_> = report
            .context
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .collect();
        assert_eq!(assistant.len(), 1);
        assert_eq!(assistant[0].content, Some(Content::Text(ASSISTANT.into())));
    } else {
        let error = closed.server.err().expect("expected closed owner error");
        assert!(
            !error.contains("unverified") && !error.contains("disposal deadline"),
            "{error}"
        );
    }
}
async fn verify_image(
    peer: &mut CompleteImagePeer,
    offer: &Value,
    size: (u32, u32),
    pixels: &[u8],
) -> TestResult<()> {
    let (header, got) = peer.image().await?;
    require(
        header["session_id"] == offer["session_id"]
            && header["incarnation"] == offer["image_channel"]["incarnation"]
            && header["request_id"].as_u64().is_some_and(|id| id > 0),
        "image identity",
    )?;
    require(
        header["width"] == size.0 && header["height"] == size.1 && header["format"] == "RGB",
        format!("image dimensions/format {header}"),
    )?;
    require(got == pixels, "exact image pixels")
}
#[tokio::test]
async fn parsed_startup_asset_is_loaded_once_and_sent_once_per_provider_session() {
    let pixels: Vec<u8> = (0..(512 * 512 * 3)).map(|i| (i % 251) as u8).collect();
    let asset = Asset::new(&pixels);
    let config = parsed(asset.args("512x512"));
    drop(asset); // Subsequent accepted sessions must not reload the disk path.
    let mut identities = vec![];
    for _ in 0..2 {
        let mut f = Fixture::start(config.clone()).await.unwrap();
        let mut offer = Value::Null;
        let observed = async {
            offer = image_offer(&mut f).await?;
            let mut peer = CompleteImagePeer::pair(&offer["image_channel"]).await?;
            verify_image(&mut peer, &offer, (512, 512), &pixels).await?;
            complete_audio(&mut f, &offer).await?;
            send(f.client()?, json!({"type":"end"})).await?;
            f.wait_closed().await?;
            peer.closed().await?; // No extra complete image/application frame.
            Ok(())
        }
        .await;
        let closed = f.close().await;
        let listener = listener_closed(&offer).await;
        identities.push(offer["session_id"].clone());
        joined(observed.and(listener), closed, true);
    }
    assert_ne!(identities[0], identities[1]);
}
struct Feed(Mutex<Option<tokio::sync::oneshot::Sender<ProcessorContext>>>);
#[async_trait]
impl FrameProcessor for Feed {
    fn name(&self) -> &str {
        "entry-native-image-feed"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(sender) = self.0.lock().unwrap().take() {
            let _ = sender.send(ctx.clone());
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn opted_in_native_processor_sends_large_synchronous_image_without_startup_asset() {
    let (send_ctx, recv_ctx) = tokio::sync::oneshot::channel();
    let mut f = Fixture::start_with_processors(
        image_options(),
        vec![Arc::new(Feed(Mutex::new(Some(send_ctx))))],
    )
    .await
    .unwrap();
    let mut offer = Value::Null;
    let observed = async {
        offer = image_offer(&mut f).await?;
        let ctx = bounded(async { recv_ctx.await.map_err(|e| e.to_string()) }).await?;
        let mut peer = CompleteImagePeer::pair(&offer["image_channel"]).await?;
        let pixels = vec![29; 512 * 512 * 3];
        let frame = Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
            image: pipecat_core::ImageRawFrame {
                image: pixels.clone(),
                size: (512, 512),
                format: Some("RGB".into()),
            },
            sync_with_audio: true,
        }));
        bounded(async {
            ctx.push(frame, Direction::Downstream)
                .await
                .map_err(|e| e.to_string())
        })
        .await?;
        verify_image(&mut peer, &offer, (512, 512), &pixels).await?;
        complete_audio(&mut f, &offer).await?;
        send(f.client()?, json!({"type":"end"})).await?;
        f.wait_closed().await?;
        peer.closed().await?; // No extra complete image/application frame.
        Ok(())
    }
    .await;
    let closed = f.close().await;
    let listener = listener_closed(&offer).await;
    joined(observed.and(listener), closed, true);
}
#[test]
fn actual_binary_rejects_incompatible_and_malformed_image_options_before_credentials() {
    let asset = Asset::new(&[7; 12]);
    let cases: Vec<Vec<String>> = vec![
        vec!["--image-channel".into()],
        vec!["--smoke".into(), "--image-channel".into()],
        vec![
            "--initial-image-rgb".into(),
            asset.0.to_str().unwrap().into(),
        ],
        vec!["--initial-image-size".into(), "2x2".into()],
        asset.args("0x2"),
        asset.args("4097x1"),
        asset.args("2x3"),
        asset.args("2x2x2"),
        asset.args("4294967295x4294967295"),
    ];
    for args in cases {
        let output = binary().args(&args).output().unwrap();
        let text = String::from_utf8(output.stderr).unwrap();
        assert!(!output.status.success(), "{args:?}");
        assert!(text.starts_with("configuration:"), "{args:?}: {text}");
        assert!(
            !text.contains("DEEPGRAM_API_KEY"),
            "invalid image options reached credentials: {text}"
        );
    }
    let output = binary().args(asset.args("2x2")).output().unwrap();
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("DEEPGRAM_API_KEY is required"));
    let help = binary()
        .args(["--initial-image-rgb", "/definitely/not/a/file", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8(help.stdout)
        .unwrap()
        .contains("--image-channel"));
}
#[test]
fn raw_rgb_loader_rejects_wrong_files_and_debug_does_not_dump_pixels() {
    for pixels in [vec![7; 11], vec![7; 13]] {
        let asset = Asset::new(&pixels);
        assert!(StartupImage::load_rgb(&asset.0, (2, 2)).is_err());
    }
    assert!(StartupImage::load_rgb(std::env::temp_dir(), (2, 2)).is_err());
    let valid = Asset::new(&[177; 12]);
    let image = StartupImage::load_rgb(&valid.0, (2, 2)).unwrap();
    assert!(!format!("{image:?}").contains("177"));
    #[cfg(unix)]
    {
        let link = valid.0.with_extension("link");
        std::os::unix::fs::symlink(&valid.0, &link).unwrap();
        let rejected = StartupImage::load_rgb(&link, (2, 2)).is_err();
        std::fs::remove_file(link).unwrap();
        assert!(rejected);
    }
}
#[test]
fn native_options_reject_legacy_missing_channel_and_smaller_image_budgets() {
    let image = StartupImage::from_rgb(vec![7; 12], (2, 2)).unwrap();
    let mut config = Options {
        image_channel: Some(ImageChannelConfig::default()),
        initial_image: Some(image),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    config.playback_protocol = PlaybackProtocol::ChunkV2;
    assert!(config.validate().is_ok());
    config.image_channel.as_mut().unwrap().max_pixel_bytes = 11;
    assert!(config.validate().is_err());
    config.image_channel.as_mut().unwrap().max_pixel_bytes = 12;
    config.image_channel.as_mut().unwrap().max_dimension = 1;
    assert!(config.validate().is_err());
    config.image_channel = None;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn cancel_unpaired_startup_does_not_block_audio_or_leave_listener() {
    let asset = Asset::new(&[31; 12]);
    let mut f = Fixture::start(parsed(asset.args("2x2"))).await.unwrap();
    let mut offer = Value::Null;
    let observed = async {
        offer = image_offer(&mut f).await?;
        complete_audio(&mut f, &offer).await?; // Real provider audio progresses without an image peer.
        send(f.client()?, json!({"type":"cancel"})).await?;
        f.wait_closed().await?;
        Ok(())
    }
    .await;
    let closed = f.close_after_abort().await;
    let listener = listener_closed(&offer).await;
    assert!(closed
        .server
        .as_ref()
        .is_err_and(|e| e.contains("v2 client cancelled")));
    joined(observed.and(listener), closed, false);
}
async fn close_paired_entry(disconnect: bool) {
    let asset = Asset::new(&[32; 12]);
    let mut f = Fixture::start(parsed(asset.args("2x2"))).await.unwrap();
    let mut offer = Value::Null;
    let mut peer = None;
    let observed = async {
        offer = image_offer(&mut f).await?;
        let mut connected = CompleteImagePeer::pair(&offer["image_channel"]).await?;
        verify_image(&mut connected, &offer, (2, 2), &[32; 12]).await?;
        peer = Some(connected);
        if !disconnect {
            send(f.client()?, json!({"type":"cancel"})).await?;
            f.wait_closed().await?;
        }
        Ok(())
    }
    .await;
    let closed = f.close_after_abort().await;
    let physical = async {
        let p = peer.as_mut().ok_or("no paired peer")?;
        p.closed().await?; // No extra complete image/application frame.
        listener_closed(&offer).await
    }
    .await;
    let expected = if disconnect {
        "v2 client disconnected"
    } else {
        "v2 client cancelled"
    };
    assert!(
        closed.server.as_ref().is_err_and(|e| e.contains(expected)),
        "{:?}",
        closed.server.as_ref().err()
    );
    joined(observed.and(physical), closed, false);
}
#[tokio::test]
async fn paired_entry_cancel_closes_actual_image_stream_and_listener() {
    close_paired_entry(false).await;
}
#[tokio::test]
async fn paired_entry_disconnect_closes_actual_image_stream_and_listener() {
    close_paired_entry(true).await;
}
#[tokio::test]
async fn disconnect_during_image_http_upgrade_disposes_owned_attempt() {
    use tokio::io::AsyncWriteExt;
    let mut f = Fixture::start(image_options()).await.unwrap();
    let mut offer = Value::Null;
    let mut peer = None;
    let observed = async {
        offer = image_offer(&mut f).await?;
        let mut stream = TcpStream::connect(
            offer["image_channel"]["address"]
                .as_str()
                .ok_or("address")?,
        )
        .await
        .map_err(|e| e.to_string())?;
        stream
            .write_all(b"GET / HTTP/1.1\r\n")
            .await
            .map_err(|e| e.to_string())?;
        peer = Some(stream);
        Ok(())
    }
    .await;
    let closed = f.close_after_abort().await;
    let physical = async {
        require(
            image_fixture::drain(peer.as_mut().ok_or("missing peer")?)
                .await?
                .is_empty(),
            "unexpected HTTP output",
        )?;
        listener_closed(&offer).await
    }
    .await;
    joined(observed.and(physical), closed, false);
}
#[tokio::test]
async fn incompatible_acceptance_starts_no_provider_and_retires_image_listener() {
    let mut f = Fixture::start(image_options()).await.unwrap();
    let mut offer = Value::Null;
    let observed = async {
        offer = control(next(f.client()?).await?)?;
        require(
            offer.get("image_channel").is_some(),
            "missing image capability",
        )?;
        send(f.client()?, acceptance(&offer)).await?;
        f.wait_closed().await?;
        Ok(())
    }
    .await;
    let closed = f.close_after_abort().await;
    let listener = listener_closed(&offer).await;
    assert_eq!(closed.contacts, 0);
    assert!(closed
        .server
        .as_ref()
        .is_err_and(|e| e.contains("exact playback and cutoff negotiation")));
    joined(observed.and(listener), closed, false);
}
#[tokio::test]
async fn provider_preparation_failure_after_image_negotiation_joins_and_closes_listener() {
    let mut f = Fixture::start_invalid_provider(image_options())
        .await
        .unwrap();
    let mut offer = Value::Null;
    let observed = async {
        offer = image_offer(&mut f).await?;
        f.wait_closed().await?;
        Ok(())
    }
    .await;
    let closed = f.close_after_abort().await;
    let listener = listener_closed(&offer).await;
    assert_eq!(closed.contacts, 0);
    assert!(
        closed
            .server
            .as_ref()
            .is_err_and(|e| e.contains("provider initialization failed")),
        "{:?}",
        closed.server.as_ref().err()
    );
    joined(observed.and(listener), closed, false);
}
#[tokio::test]
async fn direct_native_invalid_options_fail_before_websocket_handshake() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) = tokio::join!(listener.accept(), TcpStream::connect(addr));
    let stream = accepted.unwrap().0;
    let mut peer = connected.unwrap();
    let providers = pipecat_cli::config::ProviderConfig::from_lookup(|key| {
        key.ends_with("KEY").then(|| "synthetic-only".into())
    })
    .unwrap();
    let invalid = Options {
        image_channel: Some(ImageChannelConfig::default()),
        ..Default::default()
    };
    let result = pipecat_cli::run_session(stream, &invalid, &providers).await;
    assert!(result.as_ref().is_err_and(|e| e
        .to_string()
        .contains("requires --playback-protocol chunk-v2")));
    let n = tokio::time::timeout(IO_BUDGET, peer.read(&mut [0; 8]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);
}
#[test]
fn raw_rgb_retained_capacity_is_part_of_the_budget() {
    let mut pixels = Vec::with_capacity(pipecat_cli::receipt_output::IMAGE_MAX_PIXELS_BYTES + 1);
    pixels.extend([7; 12]);
    assert!(StartupImage::from_rgb(pixels, (2, 2)).is_err());
    let mut pixels = Vec::with_capacity(100);
    pixels.extend([7; 12]);
    let image = StartupImage::from_rgb(pixels, (2, 2)).unwrap();
    let mut config = image_options();
    config.initial_image = Some(image);
    config.image_channel.as_mut().unwrap().max_pixel_bytes = 12;
    assert!(config.validate().is_err());
}

struct BinaryOwner {
    child: std::process::Child,
    stdout: Option<std::thread::JoinHandle<std::io::Result<String>>>,
    stderr: Option<std::thread::JoinHandle<std::io::Result<String>>>,
}
impl BinaryOwner {
    fn spawn() -> (Self, std::sync::mpsc::Receiver<String>) {
        use std::io::{BufRead, Read};
        let mut command = binary();
        command
            .args([
                "--playback-protocol",
                "chunk-v2",
                "--image-channel",
                "--bind",
                "127.0.0.1:0",
                "--once",
                "--no-intro",
            ])
            .env("DEEPGRAM_API_KEY", "synthetic-only")
            .env("OPENAI_API_KEY", "synthetic-only")
            .env("CARTESIA_API_KEY", "synthetic-only")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let stdout = std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stdout);
            let mut first = String::new();
            reader.read_line(&mut first)?;
            let _ = send.send(first.clone());
            let mut rest = String::new();
            reader.take(16384).read_to_string(&mut rest)?;
            first.push_str(&rest);
            Ok(first)
        });
        let stderr = std::thread::spawn(move || {
            let mut text = String::new();
            stderr.take(16384).read_to_string(&mut text)?;
            Ok(text)
        });
        (
            Self {
                child,
                stdout: Some(stdout),
                stderr: Some(stderr),
            },
            receive,
        )
    }
    async fn finish(&mut self) -> TestResult<(std::process::ExitStatus, String, String)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if tokio::time::Instant::now() >= deadline {
                self.child.kill().map_err(|e| e.to_string())?;
                self.child.wait().map_err(|e| e.to_string())?;
                return Err("actual binary required kill; joined but qualification failed".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let stdout = self
            .stdout
            .take()
            .ok_or("stdout already joined")?
            .join()
            .map_err(|_| "stdout task panicked")?
            .map_err(|e| e.to_string())?;
        let stderr = self
            .stderr
            .take()
            .ok_or("stderr already joined")?
            .join()
            .map_err(|_| "stderr task panicked")?
            .map_err(|e| e.to_string())?;
        Ok((status, stdout, stderr))
    }
}
impl Drop for BinaryOwner {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(task) = self.stdout.take() {
            let _ = task.join();
        }
        if let Some(task) = self.stderr.take() {
            let _ = task.join();
        }
    }
}
#[tokio::test]
async fn actual_binary_advertises_images_then_rejects_incompatible_acceptance_before_providers() {
    let (mut owner, ready) = BinaryOwner::spawn();
    let mut client = None;
    let mut offer = Value::Null;
    let observed: TestResult<()> = async {
        let line = ready
            .recv_timeout(Duration::from_secs(3))
            .map_err(|e| e.to_string())?;
        let url = line
            .trim()
            .strip_prefix("Native voice agent listening on ")
            .ok_or("actual binary readiness line")?;
        let socket = pipecat_transports::WebSocketSession::connect(url, config())
            .await
            .map_err(|e| e.to_string())?;
        client = Some(socket);
        offer = control(next(client.as_mut().unwrap()).await?)?;
        require(
            offer["image_channel"]["session_id"] == offer["session_id"]
                && offer["image_channel"]["version"] == 1,
            "binary image offer",
        )?;
        // Deliberately omit independent_images. Providers are constructed only
        // after this handshake, so no synthetic key reaches any remote provider.
        send(client.as_ref().unwrap(), acceptance(&offer)).await?;
        bounded(async {
            require(
                client.as_mut().unwrap().recv().await.is_none(),
                "unexpected post-rejection frame",
            )
        })
        .await?;
        Ok(())
    }
    .await;
    let client_cleanup = if let Some(client) = client.take() {
        match client.cancel().await {
            Ok(()) => Ok(()),
            Err(e) => {
                let raw = fixture::peer_exit(e);
                eprintln!("actual binary client joined: {raw}");
                require(raw.starts_with("expected peer shutdown: "), raw)
            }
        }
    } else {
        Ok(())
    };
    let exited = owner.finish().await;
    let listener = listener_closed(&offer).await;
    eprintln!("binary observation={observed:?}; client={client_cleanup:?}; exit={exited:?}; listener={listener:?}");
    assert!(observed.is_ok() && client_cleanup.is_ok() && listener.is_ok());
    let (status, _, stderr) = exited.unwrap();
    assert!(!status.success());
    assert!(
        stderr.contains("exact playback and cutoff negotiation"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("provider initialization") && !stderr.contains("unverified"),
        "{stderr}"
    );
}

/// Keep the codec after pairing: startup pixels may already be in its read buffer.
/// The raw TCP fixture is only suitable when writes are deliberately triggered
/// after its pairing handoff, as in the unchanged physical-Pending witnesses.
struct CompleteImagePeer(tokio_tungstenite::WebSocketStream<TcpStream>);
impl CompleteImagePeer {
    async fn pair(offer: &Value) -> TestResult<Self> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::{protocol::WebSocketConfig, Message};
        bounded(async {
            let address = offer["address"].as_str().ok_or("missing image address")?;
            let stream = TcpStream::connect(address)
                .await
                .map_err(|e| e.to_string())?;
            let limit = pipecat_cli::receipt_output::IMAGE_MAX_PIXELS_BYTES
                + pipecat_cli::receipt_output::IMAGE_HEADER_BYTES
                + 12;
            let config = WebSocketConfig::default()
                .read_buffer_size(4096)
                .max_frame_size(Some(limit))
                .max_message_size(Some(limit));
            let (mut socket, _) = tokio_tungstenite::client_async_with_config(
                format!("ws://{address}"),
                stream,
                Some(config),
            )
            .await
            .map_err(|e| e.to_string())?;
            socket
                .send(Message::Text(
                    image_fixture::pair_message(offer).to_string().into(),
                ))
                .await
                .map_err(|e| e.to_string())?;
            let message = socket
                .next()
                .await
                .ok_or("no pairing reply")?
                .map_err(|e| e.to_string())?;
            let Message::Text(text) = message else {
                return Err("non-text pairing reply".into());
            };
            let value: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            require(
                value["type"] == "image-channel-paired"
                    && value["incarnation"] == offer["incarnation"],
                "wrong image pair confirmation",
            )?;
            Ok(Self(socket))
        })
        .await
    }
    async fn image(&mut self) -> TestResult<(Value, Vec<u8>)> {
        use futures_util::StreamExt;
        bounded(async {
            let message = self
                .0
                .next()
                .await
                .ok_or("image peer closed")?
                .map_err(|e| e.to_string())?;
            let tokio_tungstenite::tungstenite::Message::Binary(bytes) = message else {
                return Err("non-binary image".into());
            };
            image_fixture::decode(&bytes)
        })
        .await
    }
    // This codec-level check excludes extra complete application frames, not
    // trailing partial TCP bytes. Unchanged native tests retain raw prefixes.
    async fn closed(&mut self) -> TestResult<()> {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::{error::ProtocolError, Error, Message};
        bounded(async {
            match self.0.next().await {
                None | Some(Ok(Message::Close(_))) => Ok(()),
                Some(Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {eprintln!("complete image peer observed owned socket disposal without close handshake");Ok(())},
                Some(Err(Error::Io(error))) if error.kind()==std::io::ErrorKind::ConnectionReset => {eprintln!("complete image peer observed ConnectionReset after owner disposal");Ok(())},
                other => Err(format!("unexpected trailing image event: {other:?}")),
            }
        }).await
    }
}
