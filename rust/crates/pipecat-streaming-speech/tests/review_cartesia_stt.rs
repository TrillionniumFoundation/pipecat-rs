//! Independent Cartesia live STT qualification against a real loopback peer.
//! Expected protocol details were derived from the pinned original static source.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

type Socket = WebSocketStream<TcpStream>;
fn config(listener: &TcpListener) -> CartesiaSttConfig {
    let mut config = CartesiaSttConfig::new("review-secret");
    config.endpoint = format!("ws://{}/stt/websocket", listener.local_addr().unwrap());
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.write_timeout = Duration::from_millis(400);
    config.limits.response_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(50);
    config
}
async fn read(socket: &mut Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn write(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn pcm(byte: u8, len: usize) -> AudioRawFrame {
    AudioRawFrame::new(vec![byte; len], 16000, 1).unwrap()
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let marker = Frame::text("review barrier");
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    let mut frames = vec![];
    loop {
        let frame = next(worker).await;
        let done = frame.frame.id == id;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
fn delta(language: &str) -> SttSettings {
    SttSettings {
        language: Setting::Value(language.into()),
        ..Default::default()
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Required tungstenite handshake callback type.
async fn independent_wire_encoding_keyterm_limits_and_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(&listener);
    assert_eq!(config.settings.model, "ink-whisper");
    assert_eq!(config.settings.language, "en");
    assert_eq!(config.keepalive_timeout, Duration::from_secs(120));
    assert_eq!(config.keepalive_interval, Duration::from_secs(30));
    assert!(!format!("{config:?}").contains("review-secret"));
    config.settings.model = "ink-preview-custom".into();
    config.settings.keyterm = vec!["\u{1c} Ink 2\u{1f}".into(), "café/~-_*+".into(), "".into()];
    let server = tokio::spawn(async move {
        let mut socket = accept_hdr_async(listener.accept().await.unwrap().0, |r: &Request, response: Response| {
            assert_eq!(r.uri().path_and_query().unwrap().as_str(), "/stt/websocket?model=ink-preview-custom&language=en&encoding=pcm_s16le&sample_rate=16000&keyterm=Ink%202&keyterm=caf%C3%A9%2F~-_%2A%2B");
            assert_eq!(r.headers()["x-api-key"], "review-secret");
            assert_eq!(r.headers()["cartesia-version"], "2026-03-01");
            assert!(r.headers().get("authorization").is_none());
            Ok(response)
        }).await.unwrap();
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "done");
        socket.close(None).await.unwrap();
    });
    let mut session = CartesiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().finish().await.unwrap();
    assert!(session.next().await.is_none());
    server.await.unwrap();
}

#[test]
fn independent_keyterm_normalization_and_atomic_settings() {
    let mut s = CartesiaSttSettings {
        model: "ink-2".into(),
        keyterm: vec!["汉".repeat(700), "é".repeat(500), "tail".into()],
        ..Default::default()
    };
    let prepared = s.prepared_keyterms();
    assert_eq!(prepared.terms, s.keyterm[..2]);
    assert_eq!(prepared.dropped, 1);
    s.keyterm = vec!["a".repeat(1201), "short".into()];
    assert!(s.prepared_keyterms().terms.is_empty());
    assert_eq!(s.prepared_keyterms().dropped, 2);
    s.keyterm = (0..101).map(|_| "same".into()).collect();
    assert_eq!(s.prepared_keyterms().terms.len(), 100);
    s.model = "ink-whisper".into();
    assert!(s.prepared_keyterms().ignored_model);
    assert!(s.prepared_keyterms().terms.is_empty());
    let original = s.clone();
    let invalid = SttSettings {
        language: Setting::Value("fr".into()),
        base: ServiceSettings {
            extra: [("unknown".into(), json!(true))].into(),
            ..Default::default()
        },
    };
    assert!(s.merge(&invalid).is_err());
    assert_eq!(s, original);
    assert!(s
        .merge(&SttSettings {
            language: Setting::Null,
            ..Default::default()
        })
        .is_err());
    let clear = SttSettings {
        base: ServiceSettings {
            extra: [("keyterm".into(), Value::Null)].into(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(s.merge(&clear).unwrap().keyterm.is_empty());
}

#[tokio::test]
async fn independent_binary_finalize_interim_final_and_trailing_usage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut socket).await.into_data().to_vec(), vec![1; 320]);
        write(
            &mut socket,
            json!({"type":"transcript","text":"part","is_final":false,"language":"en"}),
        )
        .await;
        write(
            &mut socket,
            json!({"type":"transcript","text":"","is_final":true}),
        )
        .await;
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "finalize");
        write(
            &mut socket,
            json!({"type":"transcript","text":"final","is_final":true,"language":"en","extra":17}),
        )
        .await;
        assert_eq!(read(&mut socket).await.into_data().to_vec(), vec![2; 640]);
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "done");
        socket.close(None).await.unwrap();
    });
    let mut session = CartesiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.send_audio(pcm(1, 320)).await.unwrap();
    let Some(Ok(CartesiaSttEvent::Transcript(part))) = session.next().await else {
        panic!("missing interim")
    };
    assert_eq!(part.text, "part");
    assert!(!part.is_final);
    assert_eq!(part.audio_seconds, 0.0);
    sender.finalize().await.unwrap();
    let Some(Ok(CartesiaSttEvent::Transcript(final_))) = session.next().await else {
        panic!("missing final")
    };
    assert_eq!(final_.text, "final");
    assert!(final_.is_final);
    assert_eq!(final_.result["extra"], 17);
    assert_eq!(final_.audio_seconds, 0.01);
    sender.send_audio(pcm(2, 640)).await.unwrap();
    sender.finish().await.unwrap();
    assert_eq!(
        session.next().await,
        Some(Ok(CartesiaSttEvent::Usage {
            audio_seconds: 0.02
        }))
    );
    assert!(session.next().await.is_none());
    assert!(sender.try_send_audio(pcm(3, 2)).is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn independent_active_rejection_preserves_audio_and_usage_precedes_final() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut socket).await.into_data().to_vec(), vec![3; 320]);
        write(
            &mut socket,
            json!({"type":"transcript","text":"hello","is_final":true,"language":"en"}),
        )
        .await;
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "done");
        socket.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let mut pipeline = PipelineConfig::default();
    pipeline.start.enable_usage_metrics = true;
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(pipeline)
        .await
        .unwrap();
    assert!(processor.update_settings(&delta("fr")).is_err());
    assert_eq!(processor.config_snapshot().unwrap().settings.language, "en");
    assert!(processor.update_settings(&delta("en")).is_ok());
    let ineffective = SttSettings {
        base: ServiceSettings {
            extra: [("keyterm".into(), json!(["ignored"]))].into(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(processor.update_settings(&ineffective).is_err());
    send(&worker, FrameData::InputAudio(pcm(3, 320))).await;
    let mut outputs = vec![];
    loop {
        let f = next(&mut worker).await;
        let done = matches!(f.frame.data, FrameData::Transcription(_));
        outputs.push(f);
        if done {
            break;
        }
    }
    let relevant: Vec<_> = outputs
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::Metrics(v) if v.iter().any(|m| matches!(m, Metric::SttUsage { .. })) => {
                Some("usage")
            }
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "hello");
                assert!(!t.finalized);
                assert_eq!(t.language.as_deref(), Some("en"));
                Some("final")
            }
            _ => None,
        })
        .collect();
    assert_eq!(relevant, ["usage", "final"]);
    assert!(worker.processor_health(0).unwrap().is_usable());
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

struct Capture {
    recognizer: Option<Arc<CartesiaSttProcessor>>,
    context: Arc<Mutex<Option<ProcessorContext>>>,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "CartesiaReviewCapture"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.recognizer.as_ref().and_then(|p| p.health())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        if let Some(p) = &self.recognizer {
            p.setup(start, ctx).await?;
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(t) if t.text == "capture invocation") {
            *self.context.lock().unwrap() = Some(ctx.clone());
        }
        if let Some(p) = &self.recognizer {
            p.process(frame, direction, ctx).await
        } else {
            ctx.push(frame, direction).await
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(p) = &self.recognizer {
            p.prepare_end(ctx).await?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(p) = &self.recognizer {
            p.cleanup().await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn independent_restart_rejects_foreign_context_before_retirement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut socket).await.into_data().to_vec(), vec![4; 320]);
        write(
            &mut socket,
            json!({"type":"transcript","text":"still original","is_final":true}),
        )
        .await;
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "done");
        socket.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let foreign = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let mut other = Pipeline::new(vec![Arc::new(Capture {
        recognizer: None,
        context: foreign.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let ctx = foreign.lock().unwrap().clone().unwrap();
    assert!(processor
        .restart_with_settings(&delta("fr"), &ctx)
        .await
        .is_err());
    assert_eq!(processor.config_snapshot().unwrap().settings.language, "en");
    send(&worker, FrameData::InputAudio(pcm(4, 320))).await;
    loop {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            assert_eq!(t.text, "still original");
            break;
        }
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let old = owner.lock().unwrap().clone().unwrap();
    assert!(processor
        .restart_with_settings(&delta("fr"), &old)
        .await
        .is_err());
    other.cancel("review cleanup").await.unwrap();
    while other.recv().await.is_some() {}
    other.join().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn independent_keepalive_is_raw_silence_and_excluded_from_usage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(&listener);
    config.keepalive_interval = Duration::from_millis(10);
    config.keepalive_timeout = Duration::from_millis(20);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let message = read(&mut socket).await;
        assert!(message.is_binary());
        assert_eq!(message.into_data().to_vec(), vec![0; 3200]);
        write(
            &mut socket,
            json!({"type":"transcript","text":"silence response","is_final":true}),
        )
        .await;
        loop {
            if read(&mut socket).await.to_text().ok() == Some("done") {
                break;
            }
        }
        socket.close(None).await.unwrap();
    });
    let mut session = CartesiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let Some(Ok(CartesiaSttEvent::Transcript(t))) = session.next().await else {
        panic!("missing response")
    };
    assert_eq!(t.audio_seconds, 0.0);
    session.sender().finish().await.unwrap();
    assert!(session.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Required tungstenite handshake callback type.
async fn independent_restart_discards_old_pcm_transcripts_and_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let (old_read_tx, old_read_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut old).await.into_data().to_vec(), vec![5; 320]);
        old_read_tx.send(()).unwrap();
        let mut fresh = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |r: &Request, response: Response| {
                assert!(r.uri().query().unwrap().contains("language=fr"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        let _ = old
            .send(Message::Text(
                json!({"type":"transcript","text":"STALE","is_final":true})
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = old
            .send(Message::Text(
                json!({"type":"error","status_code":401}).to_string().into(),
            ))
            .await;
        assert_eq!(read(&mut fresh).await.into_data().to_vec(), vec![6; 320]);
        write(
            &mut fresh,
            json!({"type":"transcript","text":"fresh","is_final":true}),
        )
        .await;
        assert_eq!(read(&mut fresh).await.to_text().unwrap(), "done");
        fresh.close(None).await.unwrap();
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, FrameData::InputAudio(pcm(5, 320))).await;
    tokio::time::timeout(Duration::from_secs(2), old_read_rx)
        .await
        .unwrap()
        .unwrap();
    let ctx = owner.lock().unwrap().clone().unwrap();
    processor
        .restart_with_settings(&delta("fr"), &ctx)
        .await
        .unwrap();
    send(&worker, FrameData::InputAudio(pcm(6, 320))).await;
    let mut outputs = vec![];
    loop {
        let f = next(&mut worker).await;
        let done = matches!(f.frame.data, FrameData::Transcription(_));
        outputs.push(f);
        if done {
            break;
        }
    }
    assert!(worker.processor_health(0).unwrap().is_usable());
    worker.end().await.unwrap();
    let (tail, report) = worker.collect().await.unwrap();
    outputs.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let texts: Vec<_> = outputs
        .iter()
        .filter_map(|f| {
            if let FrameData::Transcription(t) = &f.frame.data {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(texts, ["fresh"]);
    server.await.unwrap();
}

#[tokio::test]
async fn independent_failed_restart_returns_error_and_committed_candidate_is_honest() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (mut denied, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 8192];
        let count = denied.read(&mut request).await.unwrap();
        assert!(String::from_utf8_lossy(&request[..count]).contains("language=fr"));
        denied
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        assert!(matches!(read(&mut old).await, Message::Close(_)));
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let ctx = owner.lock().unwrap().clone().unwrap();
    assert!(
        processor
            .restart_with_settings(&delta("fr"), &ctx)
            .await
            .is_err(),
        "failed destructive replacement must not return success"
    );
    assert_eq!(processor.config_snapshot().unwrap().settings.language, "fr");
    assert!(
        worker.processor_health(0).unwrap().is_usable(),
        "503 is transient"
    );
    worker.cancel("review cleanup").await.unwrap();
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn independent_full_output_cancel_and_drop_close_peer() {
    for drop_consumer in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(&listener);
        config.limits.queue_capacity = 1;
        let (sent_tx, sent_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            for index in 0..4 {
                write(
                    &mut socket,
                    json!({"type":"transcript","text":format!("queued {index}")}),
                )
                .await;
            }
            sent_tx.send(()).unwrap();
            assert!(matches!(read(&mut socket).await, Message::Close(_)));
        });
        let mut session = CartesiaSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let sender = session.sender();
        sent_rx.await.unwrap();
        if drop_consumer {
            drop(session);
        } else {
            session.cancel();
            assert_eq!(session.next().await, Some(Err(StreamingError::Cancelled)));
            assert!(session.next().await.is_none());
        }
        assert!(sender.try_send_audio(pcm(7, 320)).is_err());
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn independent_end_and_cancel_abort_pending_restart_without_false_success() {
    use tokio::io::AsyncReadExt;
    for cancel in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(&listener);
        config.limits.connect_timeout = Duration::from_secs(2);
        let (pending_tx, pending_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut old = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let (mut pending, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            assert!(pending.read(&mut request).await.unwrap() > 0);
            pending_tx.send(()).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_millis(750), pending.read(&mut request))
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "terminal must close a pending handshake"
            );
            assert!(matches!(read(&mut old).await, Message::Close(_)));
        });
        let processor = Arc::new(CartesiaSttProcessor::new(
            CartesiaSttClient::new(config).unwrap(),
        ));
        let owner = Arc::new(Mutex::new(None));
        let worker = Pipeline::new(vec![Arc::new(Capture {
            recognizer: Some(processor.clone()),
            context: owner.clone(),
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let ctx = owner.lock().unwrap().clone().unwrap();
        let restart =
            tokio::spawn(async move { processor.restart_with_settings(&delta("fr"), &ctx).await });
        tokio::time::timeout(Duration::from_secs(2), pending_rx)
            .await
            .unwrap()
            .unwrap();
        if cancel {
            worker.cancel("review cancel").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_millis(750), restart)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err(), "retired replacement cannot report success");
        let _ = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn independent_muted_restart_is_rejected_without_changing_configuration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(read(&mut old).await, Message::Close(_)));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, FrameData::SttMute(true)).await;
    barrier(&mut worker).await;
    let ctx = owner.lock().unwrap().clone().unwrap();
    assert!(processor
        .restart_with_settings(&delta("fr"), &ctx)
        .await
        .is_err());
    assert_eq!(processor.config_snapshot().unwrap().settings.language, "en");
    worker.end().await.unwrap();
    let _ = worker.collect().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn independent_invocation_becoming_stale_during_restart_cannot_commit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let (pending_tx, pending_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (pending, _) = listener.accept().await.unwrap();
        pending_tx.send(()).unwrap();
        release_rx.await.unwrap();
        if let Ok(mut fresh) = accept_async(pending).await {
            assert!(matches!(read(&mut fresh).await, Message::Close(_)));
        }
        assert!(matches!(read(&mut old).await, Message::Close(_)));
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(
        &worker,
        FrameData::Text(TextFrame::new("capture invocation")),
    )
    .await;
    barrier(&mut worker).await;
    let ctx = owner.lock().unwrap().clone().unwrap();
    let operation_ctx = ctx.clone();
    let operation_processor = processor.clone();
    let restart = tokio::spawn(async move {
        operation_processor
            .restart_with_settings(&delta("fr"), &operation_ctx)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), pending_rx)
        .await
        .unwrap()
        .unwrap();
    send(&worker, FrameData::Interruption).await;
    loop {
        if matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {
            break;
        }
    }
    assert!(!ctx.is_current());
    release_tx.send(()).unwrap();
    assert!(restart.await.unwrap().is_err());
    assert!(
        processor.update_settings(&delta("de")).is_ok(),
        "failed replacement must leave a dormant configuration"
    );
    assert_eq!(processor.config_snapshot().unwrap().settings.language, "de");
    worker.end().await.unwrap();
    let _ = worker.collect().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn independent_surviving_task_cannot_launder_a_stale_restart_invocation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut socket).await.into_data().to_vec(), vec![8; 320]);
        write(
            &mut socket,
            json!({"type":"transcript","text":"original survives","is_final":true}),
        )
        .await;
        assert_eq!(read(&mut socket).await.to_text().unwrap(), "done");
        socket.close(None).await.unwrap();
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let owner = Arc::new(Mutex::new(None));
    let mut worker = Pipeline::new(vec![Arc::new(Capture {
        recognizer: Some(processor.clone()),
        context: owner.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let setup = owner.lock().unwrap().clone().unwrap();
    send(
        &worker,
        FrameData::Text(TextFrame::new("capture invocation")),
    )
    .await;
    barrier(&mut worker).await;
    let stale = owner.lock().unwrap().clone().unwrap();
    send(&worker, FrameData::Interruption).await;
    loop {
        if matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {
            break;
        }
    }
    assert!(!stale.invocation_fence().unwrap().is_current());
    let (result_tx, result_rx) = oneshot::channel();
    let operation_processor = processor.clone();
    setup
        .spawn("review-retained-stale", false, async move {
            assert!(
                stale.is_current(),
                "the task-local survival override is the adversarial precondition"
            );
            let rejected = operation_processor
                .restart_with_settings(&delta("fr"), &stale)
                .await
                .is_err();
            result_tx.send(rejected).unwrap();
            Ok(())
        })
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), result_rx)
        .await
        .unwrap()
        .unwrap());
    assert_eq!(
        processor.config_snapshot().unwrap().settings.language,
        "en",
        "stale owner cannot mutate before rejection"
    );
    send(&worker, FrameData::InputAudio(pcm(8, 320))).await;
    loop {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            assert_eq!(t.text, "original survives");
            break;
        }
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[tokio::test]
async fn independent_idle_true_to_true_restoration_closes_the_old_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(read(&mut socket).await, Message::Close(_)));
        closed_tx.send(()).unwrap();
    });
    let processor = Arc::new(CartesiaSttProcessor::new(
        CartesiaSttClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    let before = health.generation();
    assert!(!health.set_usable(true));
    assert_ne!(health.generation(), before);
    tokio::time::timeout(Duration::from_secs(2), closed_rx)
        .await
        .unwrap()
        .unwrap();
    processor.update_settings(&delta("de")).unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}
