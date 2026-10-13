//! Old public-backend capability witness; no physical display is inferred.
use fixture::{bounded, config, control, next, send, TestResult, CHUNK_BYTES};
use native::chunk as fixture;
use pipecat_cli::receipt_output::{
    ReceiptBackend, ReceiptConfig, ReceiptOutput, ReceiptRunConfig, ReceiptRunReport,
    ReceiptSession,
};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_protocol::protobuf::frame::Kind;
use pipecat_transports::WebSocketSession;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

struct Owner {
    client: WebSocketSession,
    backend: Arc<ReceiptBackend>,
    task: JoinHandle<TestResult<ReceiptRunReport>>,
}
impl Owner {
    async fn start(initial_frames: Vec<Frame>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let url = format!("ws://{}", listener.local_addr().map_err(|e| e.to_string())?);
        let (ready, waiting) = oneshot::channel();
        let task = tokio::spawn(async move {
            let stream = listener.accept().await.map_err(|e| e.to_string())?.0;
            let socket = WebSocketSession::accept(stream, config())
                .await
                .map_err(|e| e.to_string())?;
            let owner = ReceiptSession::negotiate(
                socket,
                ReceiptConfig {
                    handshake_timeout: Duration::from_secs(1),
                    chunk_timeout: Duration::from_secs(1),
                    cutoff_timeout: Duration::from_secs(1),
                    enqueue_timeout: Duration::from_secs(1),
                    drain_timeout: Duration::from_secs(2),
                },
                24000,
            )
            .await
            .map_err(|e| e.to_string())?;
            let backend = owner.backend();
            let output = ReceiptOutput::new(
                "real-image-baseline-output",
                OutputTransportConfig {
                    sample_rate: Some(24000),
                    channels: 1,
                    chunk_duration: Duration::from_millis(40),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    ..Default::default()
                },
                backend.clone(),
                None,
            )
            .map_err(|e| e.to_string())?;
            let output_control = output.control();
            let _ = ready.send((backend, output_control));
            owner
                .run(
                    Pipeline::new(vec![Arc::new(output)]),
                    PipelineConfig {
                        shutdown_timeout: Duration::from_secs(2),
                        ..Default::default()
                    },
                    ReceiptRunConfig {
                        input_rate: 16000,
                        idle_timeout: Duration::from_secs(5),
                        history: None,
                        initial_frames,
                    },
                )
                .await
                .map_err(|e| e.to_string())
        });
        let connected = WebSocketSession::connect(&url, config()).await;
        let mut client = match connected {
            Ok(client) => client,
            Err(error) => {
                task.abort();
                let _ = task.await;
                return Err(error.to_string());
            }
        };
        let setup:TestResult<Arc<ReceiptBackend>>=async {
            let offer=control(next(&mut client).await?)?;
            if offer["type"]!="playback-v2-offer" {return Err("missing real v2 offer".into());}
            send(&client,json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],"chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true})).await?;
            let (backend,output)=bounded(async{waiting.await.map_err(|e|e.to_string())}).await?;
            bounded(async{while !output.is_ready(){tokio::task::yield_now().await;}Ok(())}).await?;
            Ok(backend)
        }.await;
        match setup {
            Ok(backend) => Ok(Self {
                client,
                backend,
                task,
            }),
            Err(error) => {
                let close = client.cancel().await;
                task.abort();
                let joined = task.await;
                Err(format!(
                    "{error}; setup cleanup socket={close:?}, owner={:?}",
                    joined.err()
                ))
            }
        }
    }
    async fn end(&mut self) -> TestResult<()> {
        send(&self.client, json!({"type":"end"})).await?;
        bounded(async {
            let mut controls = 0;
            while let Some(packet) = self.client.recv().await {
                let pipecat_transports::Packet::Binary(bytes) = packet else {
                    return Err("non-protobuf terminal packet".into());
                };
                let kind = pipecat_protocol::protobuf::Frame::parse(&bytes)
                    .map_err(|e| e.to_string())?
                    .frame
                    .ok_or("empty terminal packet")?;
                let mut value = control(kind)?;
                if value["type"] != "playback-cutoff" {
                    return Err(format!("unexpected terminal control {value}"));
                }
                value["type"] = json!("cutoff-complete");
                send(&self.client, value).await?;
                controls += 1;
                if controls > 8 {
                    return Err("unbounded terminal controls".into());
                }
            }
            Ok(())
        })
        .await
    }
    async fn close(mut self) -> (TestResult<ReceiptRunReport>, Vec<String>) {
        let mut errors = vec![];
        if let Err(e) = self.client.cancel().await {
            errors.push(format!("client join: {e}"));
        }
        let result = match tokio::time::timeout(Duration::from_secs(4), &mut self.task).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                errors.push("owner cleanup abort required and joined".into());
                Err("owner cleanup deadline".into())
            }
        };
        if result.as_ref().is_err_and(|e| e.contains("unverified")) {
            errors.push("unverified ownership completion".into());
        }
        eprintln!(
            "baseline owner error={:?}; cleanup={errors:?}",
            result.as_ref().err()
        );
        (result, errors)
    }
}
fn pixels() -> Frame {
    Frame::new(FrameData::OutputImage(ImageRawFrame {
        image: vec![255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255],
        size: (2, 2),
        format: Some("RGB".into()),
    }))
}
#[tokio::test]
async fn existing_chunk_audio_remains_exact_and_default_images_are_explicitly_unsupported() {
    let frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![7; CHUNK_BYTES], 24000, 1).unwrap(),
    ));
    let mut owner = Owner::start(vec![frame]).await.expect("real owner");
    let observation:TestResult<()>=async{
        if owner.backend.supports_independent_video(){return Err("old backend unexpectedly advertises independent images".into());}
        if owner.backend.write_image(&pixels()).await.is_ok(){return Err("old backend silently accepted an unsupported image".into());}
        let Kind::Audio(audio)=next(&mut owner.client).await? else{return Err("no actual PCM".into());};
        if audio.audio!=vec![7;CHUNK_BYTES] || audio.sample_rate!=24000 || audio.num_channels!=1 {return Err("actual PCM differs".into());}
        let meta:serde_json::Value=serde_json::from_str(&audio.name).map_err(|e|e.to_string())?;
        send(&owner.client,json!({"type":"chunk-complete","version":2,"session_id":meta["session_id"],"epoch":meta["epoch"],"chunk_id":meta["chunk_id"],"receipt_nonce":meta["receipt_nonce"]})).await?;
        owner.end().await
    }.await;
    let (report, errors) = owner.close().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(observation.is_ok(), "{observation:?}");
    let report = report.expect("joined ordinary success");
    assert_eq!(
        report.pipeline.total_errors, 0,
        "{:?}",
        report.pipeline.errors
    );
}
// The original negative whole-case witness is retained in the evidence packet.
// Candidate qualification uses the explicit new native constructor below.
#[path = "support/image_fixture.rs"]
mod native;

#[tokio::test]
async fn explicit_native_constructor_sends_all_four_real_image_variants() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation: native::TestResult<()> = async {
        if !owner.backend.supports_independent_video() {
            return Err("explicit backend lacks capability".into());
        }
        let mut peer = owner.pair().await?;
        let mut previous_id = 0;
        for i in 0..4u8 {
            let raw = ImageRawFrame {
                image: vec![11 + i; if i == 0 { 12 } else { 16 }],
                size: (2, 2),
                format: Some(if i == 0 { "RGB" } else { "RGBA" }.into()),
            };
            let detailed = OutputImageRawFrame {
                image: raw.clone(),
                sync_with_audio: false,
            };
            let frame = Frame::new(match i {
                0 => FrameData::OutputImage(raw),
                1 => FrameData::OutputImageDetailed(detailed),
                2 => FrameData::UrlImage(UrlImageRawFrame {
                    image: detailed,
                    url: Some("https://invalid.example/not-fetched".into()),
                }),
                _ => FrameData::AssistantImage(AssistantImageRawFrame {
                    image: detailed,
                    original_data: Some(vec![99]),
                    original_mime_type: Some("ignored/native".into()),
                }),
            });
            let (written, received) = tokio::join!(
                owner.backend.write_image(&frame),
                native::read_message(&mut peer)
            );
            written.map_err(native::err)?;
            let (header, bytes) = received?;
            let id = header["request_id"].as_u64().ok_or("request ID missing")?;
            if id <= previous_id {
                return Err("completed image request IDs were reused or regressed".into());
            }
            previous_id = id;

            native::exact(
                &header,
                &bytes,
                &owner.offer,
                11 + i,
                if i == 0 { 12 } else { 16 },
            )?;
            if header["width"] != 2
                || header["height"] != 2
                || header["format"] != if i == 0 { "RGB" } else { "RGBA" }
            {
                return Err("image dimensions or format differ".into());
            }
        }
        owner.end().await?;
        if !native::drain(&mut peer).await?.is_empty() {
            return Err("late bytes after complete image writes".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn invalid_pixels_and_destinations_do_not_consume_pairing_or_request_budget() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation: native::TestResult<()> = async {
        let original = owner.status();
        let mut oversized = owner.offer.clone();
        oversized["pairing_nonce"] = json!("x".repeat(2048));
        if native::pair(&oversized).await.is_ok() {
            return Err("oversized pairing frame admitted".into());
        }

        let mut bad = vec![Frame::new(FrameData::Sprite(SpriteFrame {
            images: vec![],
        }))];
        for (bytes, size, format) in [
            (11, (2, 2), "RGB"),
            (12, (0, 2), "RGB"),
            (12, (5000, 2), "RGB"),
            (12, (2, 2), "JPEG"),
        ] {
            bad.push(Frame::new(FrameData::OutputImage(ImageRawFrame {
                image: vec![1; bytes],
                size,
                format: Some(format.into()),
            })));
        }
        let mut destination = native::image(1, false);
        destination.transport_destination = Some("other".into());
        bad.push(destination);
        for frame in bad {
            if owner.backend.write_image(&frame).await.is_ok() {
                return Err("invalid image accepted".into());
            }
        }
        if owner.status().request_id.is_some() || owner.status().incarnation != original.incarnation
        {
            return Err("invalid image changed admission".into());
        }
        let mut peer = owner.pair().await?;
        let frame = native::image(23, false);
        let (written, received) = tokio::join!(
            owner.backend.write_image(&frame),
            native::read_message(&mut peer)
        );
        written.map_err(native::err)?;
        let (header, bytes) = received?;
        native::exact(&header, &bytes, &owner.offer, 23, 12)?;
        owner.end().await?;
        native::drain(&mut peer).await?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn actual_pending_callback_drop_disposes_old_prefix_and_fresh_incarnation_only() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation:native::TestResult<()>=async{
        let mut peer=owner.pair().await?;let old_offer=owner.offer.clone();
        let backend=owner.backend.clone();let frame=native::image(31,true);let mut write=Box::pin(backend.write_image(&frame));
        tokio::select!{
            result=&mut write=>return Err(format!("large physical write completed before Pending proof: {result:?}")),
            result=owner.wait_status(|s|s.leased&&s.tcp_write_pending>0)=>{let status=result?;eprintln!("actual Pending before direct drop: {status:?}");}
        }
        let mut bytes=native::prefix(&mut peer).await?;
        drop(write); // There is deliberately no further poll/flush on the owned socket.
        bytes.extend(native::drain(&mut peer).await?);native::partial(&bytes,"direct-drop",31)?;
        owner.replacement().await?;
        if native::pair(&old_offer).await.is_ok(){return Err("stale one-use token paired".into());}
        let mut replacement=owner.pair().await?;let frame=native::image(32,false);
        let (written,received)=tokio::join!(backend.write_image(&frame),native::read_message(&mut replacement));
        written.map_err(native::err)?;let (header,pixels)=received?;native::exact(&header,&pixels,&owner.offer,32,12)?;
        owner.end().await?;if !native::drain(&mut replacement).await?.is_empty(){return Err("unexpected replacement tail".into());}Ok(())
    }.await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn blocked_physical_image_allows_exact_audio_receipt_and_cutoff_control() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation: native::TestResult<()> = async {
        let mut peer = owner.pair().await?;
        owner.push(native::image(41, true)).await?;
        let status = owner
            .wait_status(|s| s.leased && s.tcp_write_pending > 0)
            .await?;
        // A second physical connection may enter the bounded OS listen backlog,
        // but cannot upgrade or replace the callback-owned stream.
        use tokio::io::AsyncWriteExt;
        let mut duplicate=tokio::net::TcpStream::connect(owner.offer["address"].as_str().unwrap()).await.map_err(native::err)?;
        duplicate.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.map_err(native::err)?;
        let mut first=[0;1];
        if tokio::time::timeout(Duration::from_millis(60),duplicate.peek(&mut first)).await.is_ok(){return Err("duplicate pairing was polled while stream leased".into());}
        if !owner.status().leased||owner.status().open_streams!=1{return Err("duplicate replaced leased stream".into());}
        drop(duplicate);
        let mut bytes = native::prefix(&mut peer).await?;
        owner.push(native::pcm()).await?;
        let pcm = owner.pcm().await?;
        owner.ack(&pcm).await?;
        // A real cutoff round trip runs on the audio socket while the same Arc's
        // unrelated image remains physically blocked.
        owner.push(native::pcm()).await?;
        let _held = owner.pcm().await?;
        let backend = owner.backend.clone();
        let (interrupt, cutoff) = tokio::join!(backend.interrupt(None), owner.cutoff());
        interrupt.map_err(native::err)?;
        cutoff?;
        if owner.status().request_id != status.request_id || !owner.status().leased {
            return Err("ordinary backend interrupt invalidated unrelated image".into());
        }
        // Shared transport interruption owns the interruptible image cancellation.
        owner.push(native::image(99, false)).await?; // queued old-generation image must also be revoked
        owner.push(Frame::interruption()).await?;
        owner.wait_status(|s| !s.leased).await?;
        bytes.extend(native::drain(&mut peer).await?);
        native::partial(&bytes, "shared-interruption", 41)?;
        owner.replacement().await?;
        let mut replacement = owner.pair().await?;
        owner.push(native::image(42, false)).await?;
        let (header, pixels) = native::read_message(&mut replacement).await?;
        native::exact(&header, &pixels, &owner.offer, 42, 12)?;
        owner.end().await?;
        native::drain(&mut replacement).await?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn held_audio_receipt_does_not_block_live_image_but_sync_image_keeps_order() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation: native::TestResult<()> = async {
        let mut peer = owner.pair().await?;
        owner.push(native::pcm()).await?;
        let pcm = owner.pcm().await?;
        owner.push(native::image(51, false)).await?;
        let (header, pixels) = native::read_message(&mut peer).await?;
        native::exact(&header, &pixels, &owner.offer, 51, 12)?;
        owner
            .push(Frame::new(FrameData::OutputImageDetailed(
                OutputImageRawFrame {
                    image: ImageRawFrame {
                        image: vec![52; 12],
                        size: (2, 2),
                        format: Some("RGB".into()),
                    },
                    sync_with_audio: true,
                },
            )))
            .await?;
        let mut byte = [0; 1];
        if tokio::time::timeout(Duration::from_millis(80), peer.peek(&mut byte))
            .await
            .is_ok()
        {
            return Err("sync image overtook held PCM receipt".into());
        }
        owner.ack(&pcm).await?;
        let (header, pixels) = native::read_message(&mut peer).await?;
        native::exact(&header, &pixels, &owner.offer, 52, 12)?;
        owner.end().await?;
        native::drain(&mut peer).await?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn uninterruptible_shared_image_survives_interruption_and_end_drains_real_pixels() {
    let mut owner = native::Native::start(native::images_config())
        .await
        .unwrap();
    let observation: native::TestResult<()> = async {
        let mut peer = owner.pair().await?;
        let mut frame = native::image(61, true);
        frame.interruptible = false;
        owner.push(frame).await?;
        let status = owner
            .wait_status(|s| s.leased && s.tcp_write_pending > 0)
            .await?;
        owner.push(native::pcm()).await?;
        let _held = owner.pcm().await?;
        owner.push(Frame::interruption()).await?;
        owner.cutoff().await?;
        if owner.status().request_id != status.request_id || !owner.status().leased {
            return Err("uninterruptible image was revoked".into());
        }
        let (received, ended) = tokio::join!(native::read_message(&mut peer), owner.end());
        let (header, pixels) = received?;
        native::exact(
            &header,
            &pixels,
            &owner.offer,
            61,
            pipecat_cli::receipt_output::IMAGE_MAX_PIXELS_BYTES,
        )?;
        if header["interruptible"] != false {
            return Err("lost uninterruptible identity".into());
        }
        ended?;
        if !native::drain(&mut peer).await?.is_empty() {
            return Err("late image tail".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}
