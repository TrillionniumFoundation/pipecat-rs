#[path = "support/image_fixture.rs"]
mod fixture;
use fixture::*;
use pipecat_core::*;
use pipecat_media_transport::OutputBackend;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn waiting_callback_drop_is_local_and_concurrent_callback_fails_fast() {
    let mut owner = Native::start(images_config()).await.unwrap();
    let observation:TestResult<()>=async{
        let backend=owner.backend.clone();let frame=image(71,false);let mut write=Box::pin(backend.write_image(&frame));
        tokio::select!{result=&mut write=>return Err(format!("unpaired callback completed: {result:?}")),result=owner.wait_status(|s|s.request_id.is_some()&&!s.leased)=>{result?;}}
        let second=backend.write_image(&frame).await;
        if !second.is_err_and(|e|e.to_string().contains("callback budget exhausted")){return Err("concurrent callback did not fail fast".into());}
        drop(write);owner.wait_status(|s|s.request_id.is_none()).await?;
        if owner.status().incarnation!=1{return Err("waiting cancellation rotated usable pairing identity".into());}
        let mut peer=owner.pair().await?;
        let (written,received)=tokio::join!(backend.write_image(&frame),read_message(&mut peer));
        written.map_err(err)?;let(h,b)=received?;exact(&h,&b,&owner.offer,71,12)?;
        owner.end().await?;drain(&mut peer).await?;Ok(())
    }.await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn actual_pending_write_timeout_disposes_prefix_without_replay() {
    let mut config = images_config();
    config.operation_timeout = Duration::from_millis(180);
    let mut owner = Native::start(config).await.unwrap();
    let observation:TestResult<()>=async{
        let mut peer=owner.pair().await?;let backend=owner.backend.clone();let frame=image(72,true);
        let start=tokio::time::Instant::now();let mut write=Box::pin(backend.write_image(&frame));
        tokio::select!{result=&mut write=>return Err(format!("no physical Pending: {result:?}")),result=owner.wait_status(|s|s.leased&&s.tcp_write_pending>0)=>{result?;}}
        let mut bytes=prefix(&mut peer).await?;
        let result=write.await;
        if !result.is_err_and(|e|e.to_string().contains("physical write timed out"))||start.elapsed()>Duration::from_secs(1){return Err("wrong fixed physical timeout".into());}
        bytes.extend(drain(&mut peer).await?);partial(&bytes,"physical-timeout",72)?;
        owner.replacement().await?;owner.end().await?;Ok(())
    }.await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

async fn cancelled_physical_image(disconnect: bool) -> TestResult<()> {
    let mut owner = Native::start(images_config()).await?;
    let observation: TestResult<()> = async {
        let mut peer = owner.pair().await?;
        owner.push(image(73, true)).await?;
        owner
            .wait_status(|s| s.leased && s.tcp_write_pending > 0)
            .await?;
        let mut bytes = prefix(&mut peer).await?;
        if disconnect {
            let result = owner.client.take().unwrap().cancel().await;
            result.map_err(err)?;
        } else {
            send(owner.client.as_ref().unwrap(), json!({"type":"cancel"})).await?;
        }
        owner
            .wait_status(|s| s.closed && !s.leased && !s.owner_alive)
            .await?;
        bytes.extend(drain(&mut peer).await?);
        partial(
            &bytes,
            if disconnect {
                "physical-disconnect"
            } else {
                "physical-cancel"
            },
            73,
        )?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(true).await;
    observation.map_err(|e| format!("{e}; cleanup={cleanup:?}"))?;
    cleanup
}
#[tokio::test]
async fn cancel_disposes_real_shared_image_write_and_joins() {
    cancelled_physical_image(false).await.unwrap();
}
#[tokio::test]
async fn disconnect_disposes_real_shared_image_write_and_joins() {
    cancelled_physical_image(true).await.unwrap();
}

#[tokio::test]
async fn stop_reports_retained_unpolled_lease_then_release_proves_disposal_and_idempotence() {
    let mut owner = Native::start(images_config()).await.unwrap();
    let observation:TestResult<()>=async{
        let mut peer=owner.pair().await?;let backend=owner.backend.clone();let frame=image(74,true);
        let mut write=Box::pin(backend.write_image(&frame));
        tokio::select!{result=&mut write=>return Err(format!("no physical Pending: {result:?}")),result=owner.wait_status(|s|s.leased&&s.tcp_write_pending>0)=>{result?;}}
        let mut bytes=prefix(&mut peer).await?;
        let started=tokio::time::Instant::now();let stop=backend.stop().await;
        if !stop.is_err_and(|e|e.to_string().contains("image disposal deadline exceeded; join is unverified"))||started.elapsed()>Duration::from_secs(1){return Err("retained lease incorrectly declared joined".into());}
        if !owner.status().leased||owner.status().open_streams!=1{return Err("unpolled caller ownership was hidden".into());}
        drop(write);
        bytes.extend(drain(&mut peer).await?);partial(&bytes,"retained-stop",74)?;
        backend.stop().await.map_err(err)?;backend.stop().await.map_err(err)?;
        let fresh=backend.write_image(&frame); // Fresh unpolled future owns no resource.
        if fresh.await.is_ok(){return Err("post-stop fresh callback admitted".into());}
        owner.end().await?;Ok(())
    }.await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn malformed_pair_attempts_do_not_extend_waiting_deadline_or_rotate_offer() {
    let mut config = images_config();
    config.operation_timeout = Duration::from_millis(150);
    let mut owner = Native::start(config).await.unwrap();
    let observation: TestResult<()> = async {
        let backend = owner.backend.clone();
        let frame = image(75, false);
        let started = tokio::time::Instant::now();
        let mut wrong = owner.offer.clone();
        wrong["pairing_nonce"] = json!("0".repeat(32));
        let traffic = async {
            let mut attempts = 0;
            while started.elapsed() < Duration::from_millis(300) {
                if pair(&wrong).await.is_ok() {
                    return Err("malformed pair accepted".to_owned());
                }
                attempts += 1;
                tokio::task::yield_now().await;
            }
            Ok(attempts)
        };
        let write = async {
            let result = backend.write_image(&frame).await;
            (result, started.elapsed())
        };
        let ((written, elapsed), traffic) = tokio::join!(write, traffic);
        if !written.is_err_and(|e| e.to_string().contains("timed out waiting for pairing"))
            || elapsed >= Duration::from_millis(300)
        {
            return Err(format!("pair traffic extended deadline: {elapsed:?}"));
        }
        let attempts = traffic?;
        if attempts < 2 {
            return Err("insufficient malformed pairing pressure".into());
        }
        eprintln!("malformed pairing attempts={attempts}, callback elapsed={elapsed:?}");
        if owner.status().incarnation != 1 || owner.status().request_id.is_some() {
            return Err("malformed peer rotated token or leaked request".into());
        }
        let mut peer = owner.pair().await?;
        let (written, received) =
            tokio::join!(backend.write_image(&frame), read_message(&mut peer));
        written.map_err(err)?;
        let (h, b) = received?;
        exact(&h, &b, &owner.offer, 75, 12)?;
        owner.end().await?;
        drain(&mut peer).await?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn cancel_while_waiting_for_pairing_releases_request_and_listener() {
    let owner = Native::start(images_config()).await.unwrap();
    let observation: TestResult<()> = async {
        owner.push(image(76, false)).await?;
        owner
            .wait_status(|s| s.request_id.is_some() && !s.leased)
            .await?;
        send(owner.client.as_ref().unwrap(), json!({"type":"cancel"})).await?;
        owner
            .wait_status(|s| s.closed && !s.owner_alive && s.request_id.is_none())
            .await?;
        if tokio::net::TcpStream::connect(owner.offer["address"].as_str().unwrap())
            .await
            .is_ok()
        {
            return Err("closed image listener still accepts".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(true).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn partial_pipeline_setup_failure_closes_image_owner_without_disposal_wait_cycle() {
    let started = tokio::time::Instant::now();
    let mut config = images_config();
    config.disposal_timeout = Duration::from_secs(2);
    let owner = Native::start_partial_failure(config).await.unwrap();
    let observation: TestResult<()> = async {
        owner
            .wait_status(|s| s.closed && !s.owner_alive && s.open_streams == 0)
            .await?;
        if started.elapsed() > Duration::from_secs(1) {
            return Err("partial start required disposal timeout".into());
        }
        if tokio::net::TcpStream::connect(owner.offer["address"].as_str().unwrap())
            .await
            .is_ok()
        {
            return Err("partial startup leaked listener".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(true).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn cancel_disposes_partial_http_upgrade_and_cannot_install_late_pair() {
    use tokio::io::AsyncWriteExt;
    let owner = Native::start(images_config()).await.unwrap();
    let observation: TestResult<()> = async {
        let mut peer = tokio::net::TcpStream::connect(owner.offer["address"].as_str().unwrap())
            .await
            .map_err(err)?;
        peer.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .map_err(err)?;
        owner
            .wait_status(|s| s.pairing && s.open_streams == 1)
            .await?;
        send(owner.client.as_ref().unwrap(), json!({"type":"cancel"})).await?;
        owner
            .wait_status(|s| s.closed && !s.owner_alive && s.open_streams == 0)
            .await?;
        if !drain(&mut peer).await?.is_empty() {
            return Err("partial HTTP upgrade emitted late data".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(true).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn shared_stop_drains_image_and_repeated_backend_stop_is_joined() {
    let owner = Native::start(images_config()).await.unwrap();
    let observation: TestResult<()> = async {
        let mut peer = owner.pair().await?;
        let mut frame = image(77, true);
        frame.interruptible = false;
        owner.push(frame).await?;
        owner
            .wait_status(|s| s.leased && s.tcp_write_pending > 0)
            .await?;
        owner.push(Frame::new(FrameData::Stop)).await?;
        let (h, b) = read_message(&mut peer).await?;
        exact(
            &h,
            &b,
            &owner.offer,
            77,
            pipecat_cli::receipt_output::IMAGE_MAX_PIXELS_BYTES,
        )?;
        owner
            .wait_status(|s| s.closed && !s.owner_alive && s.request_id.is_none())
            .await?;
        if !drain(&mut peer).await?.is_empty() {
            return Err("late stop bytes".into());
        }
        owner.backend.stop().await.map_err(err)?;
        owner.backend.stop().await.map_err(err)?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn image_enabled_negotiation_requires_explicit_accept_and_closes_partial_owner() {
    use pipecat_cli::receipt_output::{ReceiptConfig, ReceiptSession};
    use pipecat_transports::{SessionConfig, WebSocketSession};
    use tokio::net::{TcpListener, TcpStream};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let images = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = images.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let stream = listener.accept().await.map_err(err)?.0;
        let session = WebSocketSession::accept(stream, SessionConfig::default())
            .await
            .map_err(err)?;
        match ReceiptSession::negotiate_with_images(
            session,
            ReceiptConfig::default(),
            24000,
            images,
            images_config(),
        )
        .await
        {
            Ok(owner) => {
                owner.close().await.map_err(err)?;
                Err("image opt-in unexpectedly accepted".into())
            }
            Err(error) => Ok::<_, String>(error.to_string()),
        }
    });
    let mut client = WebSocketSession::connect(&url, SessionConfig::default())
        .await
        .unwrap();
    let observation: TestResult<()> = async {
        let offer = control(next(&mut client).await?)?;
        if offer["image_channel"].is_null() {
            return Err("missing explicit image negotiation".into());
        }
        send(
            &client,
            json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],
            "chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true}),
        )
        .await?;
        bounded(async {
            if client.recv().await.is_some() {
                return Err("rejected negotiation emitted media".into());
            }
            Ok(())
        })
        .await
    }
    .await;
    let client_close = client.cancel().await;
    let joined = bounded(async { task.await.map_err(err)? }).await;
    assert!(
        observation.is_ok(),
        "{observation:?}; client={client_close:?}; owner={joined:?}"
    );
    assert!(client_close.is_ok(), "{client_close:?}");
    assert!(joined
        .unwrap()
        .contains("requires exact playback and cutoff negotiation"));
    assert!(
        TcpStream::connect(address).await.is_err(),
        "negotiation failure leaked image listener"
    );
}

#[tokio::test]
async fn strict_pair_identity_rejections_leave_current_one_use_offer_usable() {
    let mut owner = Native::start(images_config()).await.unwrap();
    let observation: TestResult<()> = async {
        for (field, value) in [
            ("session_id", json!("f".repeat(32))),
            ("version", json!(2)),
            ("incarnation", json!(2)),
            ("destination", json!("other")),
            ("pairing_nonce", json!("e".repeat(32))),
            ("extra_field", json!(true)),
            ("type", json!("image-channel-paired")),
        ] {
            let mut request = pair_message(&owner.offer);
            request[field] = value;
            if pair_request(&owner.offer, request).await.is_ok() {
                return Err(format!("invalid {field} paired"));
            }
            let status = owner.wait_status(|s| s.open_streams == 0).await?;
            if status.incarnation != 1
                || status.request_id.is_some()
                || status.idle
                || status.closed
            {
                return Err(format!("invalid {field} consumed live state: {status:?}"));
            }
        }
        let mut peer = owner.pair().await?;
        let frame = image(78, false);
        let (written, received) =
            tokio::join!(owner.backend.write_image(&frame), read_message(&mut peer));
        written.map_err(err)?;
        let (h, b) = received?;
        exact(&h, &b, &owner.offer, 78, 12)?;
        owner.end().await?;
        drain(&mut peer).await?;
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}

#[tokio::test]
async fn shared_interruption_revokes_waiting_and_queued_images_without_rotating_unused_offer() {
    let mut owner = Native::start(images_config()).await.unwrap();
    let observation: TestResult<()> = async {
        owner.push(image(79, false)).await?;
        owner
            .wait_status(|s| s.request_id.is_some() && !s.leased)
            .await?;
        owner.push(image(80, false)).await?;
        owner.push(Frame::interruption()).await?;
        owner.wait_status(|s| s.request_id.is_none()).await?;
        if owner.status().incarnation != 1 {
            return Err("unconsumed pair offer rotated on waiting interruption".into());
        }
        let mut peer = owner.pair().await?;
        owner.push(image(81, false)).await?;
        let (h, b) = read_message(&mut peer).await?;
        exact(&h, &b, &owner.offer, 81, 12)?;
        owner.end().await?;
        if !drain(&mut peer).await?.is_empty() {
            return Err("cancelled waiting/queued images replayed".into());
        }
        Ok(())
    }
    .await;
    let cleanup = owner.close(false).await;
    assert!(observation.is_ok(), "{observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_ok(), "{cleanup:?}");
}
