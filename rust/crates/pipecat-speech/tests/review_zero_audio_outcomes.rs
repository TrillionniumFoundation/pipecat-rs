//! Audio evidence survives later recoverable failure without becoming playout.
#[allow(dead_code)]
mod support;

use pipecat_core::*;
use pipecat_speech::{cartesia::*, elevenlabs::*, *};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}

async fn pipeline(
    bodies: Vec<&'static [u8]>,
    cutoff: usize,
) -> (PipelineWorker, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("fixture-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(1);
    let server = tokio::spawn(async move {
        for body in bodies {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    let processor = HttpTtsProcessor::new(
        HttpTtsBackend::ElevenLabs {
            client: ElevenLabsHttpClient::new(config).unwrap(),
            request: ElevenLabsSynthesisRequest::new("", "voice"),
        },
        4,
    )
    .unwrap()
    .with_max_consecutive_zero_audio_contexts(cutoff);
    let worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    (worker, server)
}

async fn utterance(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("Hello."))),
        end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut frames = vec![];
    loop {
        let frame = bounded(worker.recv()).await.unwrap();
        let done = frame.frame.id == end_id;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}

fn errors(frames: &[DirectedFrame]) -> Vec<&ErrorFrame> {
    frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Error(error) => Some(error),
            _ => None,
        })
        .collect()
}

fn assert_no_audio(frames: &[DirectedFrame]) {
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::BotStartedSpeaking
            | FrameData::BotStoppedSpeaking
    )));
}

#[tokio::test]
async fn explicit_empty_audio_resets_streak_even_when_a_later_line_fails() {
    let (mut worker, server) =
        pipeline(vec![b"", b"{\"audio_base64\":\"\"}\nnot json\n", b""], 2).await;
    let health = worker.processor_health(0).unwrap();
    for index in 0..3 {
        let frames = utterance(&mut worker).await;
        assert_no_audio(&frames);
        let errors = errors(&frames);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].error.contains("no audio"), index != 1);
        assert!(health.is_usable());
    }
    worker.end().await.unwrap();
    assert_eq!(bounded(worker.collect()).await.unwrap().1.total_errors, 3);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn an_empty_audio_key_cannot_turn_a_held_partial_sample_into_audio() {
    let (mut worker, server) = pipeline(
        vec![b"{\"audio_base64\":\"AQ==\"}\n{\"audio_base64\":\"\"}\n"],
        1,
    )
    .await;
    let frames = utterance(&mut worker).await;
    assert_no_audio(&frames);
    let errors = errors(&frames);
    assert_eq!(errors.len(), 2);
    assert_eq!(
        errors
            .iter()
            .filter(|error| error.error.contains("no audio"))
            .count(),
        1
    );
    assert!(!worker.processor_health(0).unwrap().is_usable());
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn cartesia_non200_empty_response_is_not_source_audio_evidence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("fixture-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let processor = HttpTtsProcessor::new(
        HttpTtsBackend::Cartesia {
            client: CartesiaHttpClient::new(config).unwrap(),
            request: CartesiaSynthesisRequest::new("", "voice"),
        },
        4,
    )
    .unwrap()
    .with_max_consecutive_zero_audio_contexts(1);
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let frames = utterance(&mut worker).await;
    assert_no_audio(&frames);
    let errors = errors(&frames);
    // Cartesia now rejects non-200 status before decoding; the independent
    // zero-audio watchdog must still observe no source audio and trip once.
    assert_eq!(errors.len(), 2);
    assert_eq!(
        errors
            .iter()
            .filter(|error| error.error == "HTTP 204")
            .count(),
        1
    );
    assert_eq!(
        errors
            .iter()
            .filter(|error| error.error.contains("no audio"))
            .count(),
        1
    );
    assert!(!worker.processor_health(0).unwrap().is_usable());
    worker.end().await.unwrap();
    assert_eq!(bounded(worker.collect()).await.unwrap().1.total_errors, 2);
    bounded(server).await.unwrap();
}
