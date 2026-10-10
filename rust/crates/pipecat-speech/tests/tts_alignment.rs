//! Source mapping: services/tts_service.py::_synthesize_text separates display,
//! original and prepared text; serialized completion follows its audio context.
//! Native HTTP completion is deliberately Unreported, never a playback receipt.
//! All provider requests use real loopback HTTP and synthetic PCM, not live APIs.
#[allow(dead_code)]
mod support;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_speech::{cartesia::*, elevenlabs::*, *};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

fn processor(config: SpeechConfig) -> Arc<HttpTtsProcessor> {
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            },
            16,
        )
        .unwrap(),
    )
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn finish(worker: PipelineWorker) -> (Vec<Frame>, WorkerReport) {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap();
    (
        frames
            .into_iter()
            .filter(|f| f.direction == Direction::Downstream)
            .map(|f| f.frame)
            .collect(),
        report,
    )
}
async fn reply(socket: &mut TcpStream) {
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\n\x01\x00\x02\x00").await.unwrap();
}
fn assert_no_spoken(frames: &[Frame]) {
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
    assert!(!frames
        .iter()
        .any(|f| f.metadata.get("spoken") == Some(&json!(true))));
}

#[tokio::test]
async fn prepared_channels_retain_delimiters_anchor_identity_and_success_evidence() {
    let (config, request, server) = support::server(
        "200 OK",
        "application/octet-stream",
        vec![vec![1, 0, 2, 0]],
        false,
        Duration::ZERO,
    )
    .await;
    let worker = Pipeline::new(vec![processor(config)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    let mut input = TtsTextInputFrame::new("Pay $5 中文");
    input.synthesis_text = "Pay five dollars 中文".into();
    input.original_text = "**Pay <b>$5</b> 中文**".into();
    input.aggregated_by = "paragraph".into();
    let frame = Frame::new(FrameData::TtsTextInput(input));
    let source_id = frame.id;
    send(&worker, frame).await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        request.await.unwrap().json()["transcript"],
        "Pay five dollars 中文"
    );
    let anchor = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    let FrameData::AggregatedText(aggregate) = &anchor.data else {
        unreachable!()
    };
    assert_eq!(aggregate.text.text, "Pay $5 中文");
    assert_eq!(
        aggregate.raw_text.as_deref(),
        Some("**Pay <b>$5</b> 中文**")
    );
    assert_eq!(aggregate.aggregated_by, "paragraph");
    assert!(!aggregate.text.append_to_context);
    assert!(aggregate.text.includes_inter_frame_spaces);
    assert!(aggregate.will_be_spoken);
    assert_ne!(anchor.id, source_id);
    assert_eq!(anchor.metadata["source_frame_id"], source_id);
    let diagnostic = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::TtsAlignmentDiagnostic(_)))
        .unwrap();
    let FrameData::TtsAlignmentDiagnostic(d) = &diagnostic.data else {
        unreachable!()
    };
    assert_eq!(d.segment_id, Some(anchor.id));
    assert_eq!(d.context_id, aggregate.context_id.clone().unwrap());
    assert_eq!(d.evidence, TtsAlignmentEvidence::Unreported);
    assert_eq!(d.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
    assert_eq!(d.original_text.as_deref(), Some("**Pay <b>$5</b> 中文**"));
    assert_eq!(
        d.source_span,
        Some(TtsSourceSpan {
            start: 0,
            end: "**Pay <b>$5</b> 中文**".chars().count()
        })
    );
    assert!(anchor.has_pipeline_pts() && diagnostic.has_pipeline_pts());
    let ai = frames.iter().position(|f| f.id == anchor.id).unwrap();
    let audio = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsAudio(_)))
        .unwrap();
    let di = frames.iter().position(|f| f.id == diagnostic.id).unwrap();
    let stopped = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsStopped(_)))
        .unwrap();
    let end = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert!(ai < audio && audio < di && di < stopped && stopped < end);
    assert!(frames[stopped].pts > diagnostic.pts);
    assert_eq!(frames[end].pts, frames[stopped].pts);
    assert_no_spoken(&frames);
    server.await.unwrap();
}

#[tokio::test]
async fn aggregate_raw_text_and_policies_are_immutable_across_serial_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("test");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for expected in ["first", "second"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                support::read_request(&mut socket).await.json()["transcript"],
                expected
            );
            reply(&mut socket).await;
        }
    });
    let worker = Pipeline::new(vec![processor(config)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for (word, destination, append, spaces) in [
        ("first", "left", false, true),
        ("second", "right", true, false),
    ] {
        let mut text = TextFrame::new(word);
        text.append_to_context = append;
        text.includes_inter_frame_spaces = spaces;
        let mut frame = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
            text,
            aggregated_by: "paragraph".into(),
            context_id: None,
            raw_text: Some(format!("**{word}**")),
            will_be_spoken: false,
        }));
        frame.transport_destination = Some(destination.into());
        send(&worker, frame).await;
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let mut contexts = vec![];
    let mut active = None;
    let mut last_stop = 0;
    for frame in &frames {
        match &frame.data {
            FrameData::TtsStarted(start) => {
                contexts.push(start.context_id.clone());
                active = frame.transport_destination.clone();
                assert_eq!(start.append_to_context, active.as_deref() == Some("right"));
            }
            FrameData::AggregatedText(text) => {
                assert_eq!(frame.transport_destination, active);
                assert_eq!(
                    text.text.includes_inter_frame_spaces,
                    active.as_deref() == Some("left")
                );
                assert_eq!(text.raw_text, Some(format!("**{}**", text.text.text)));
                assert_eq!(text.context_id, *contexts.last().unwrap());
                assert!(frame.pts.unwrap() >= last_stop);
            }
            FrameData::TtsAlignmentDiagnostic(d) => {
                assert_eq!(frame.transport_destination, active);
                assert_eq!(Some(d.context_id.clone()), *contexts.last().unwrap());
            }
            FrameData::TtsAudio(audio) => {
                assert_eq!(frame.transport_destination, active);
                assert_eq!(audio.context_id, *contexts.last().unwrap());
            }
            FrameData::TtsStopped(stop) => {
                assert_eq!(frame.transport_destination, active);
                assert_eq!(stop.context_id, *contexts.last().unwrap());
                last_stop = frame.pts.unwrap();
            }
            _ => {}
        }
    }
    assert_eq!(contexts.len(), 2);
    assert_ne!(contexts[0], contexts[1]);
    assert_no_spoken(&frames);
    server.await.unwrap();
}

#[tokio::test]
async fn skipped_inputs_are_serialized_without_network_or_speech_completion() {
    let (config, request, server) = support::server(
        "200 OK",
        "application/octet-stream",
        vec![vec![1, 0]],
        false,
        Duration::from_millis(25),
    )
    .await;
    let worker = Pipeline::new(vec![processor(config)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("spoken"))),
    )
    .await;
    let mut skip = TtsTextInputFrame::new("code");
    skip.synthesis_text = "must not send".into();
    skip.original_text = "```code```".into();
    skip.skip_tts = true;
    skip.append_to_context = false;
    skip.includes_inter_frame_spaces = false;
    skip.aggregated_by = "code".into();
    let mut skipped = Frame::new(FrameData::TtsTextInput(skip));
    skipped.transport_destination = Some("screen".into());
    send(&worker, skipped).await;
    let mut text = TextFrame::new("existing skip");
    text.skip_tts = Some(true);
    let aggregate = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
        text,
        raw_text: Some("<skip>existing skip</skip>".into()),
        context_id: None,
        aggregated_by: "skip".into(),
        will_be_spoken: false,
    }));
    let aggregate_id = aggregate.id;
    send(&worker, aggregate).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(request.await.unwrap().json()["transcript"], "spoken");
    let stopped = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsStopped(_)))
        .unwrap();
    let skipped = frames
        .iter()
        .position(|f| matches!(&f.data, FrameData::AggregatedText(t) if t.text.text == "code"))
        .unwrap();
    assert!(stopped < skipped);
    assert!(
        matches!(&frames[skipped].data, FrameData::AggregatedText(t) if t.raw_text.as_deref() == Some("```code```") && !t.will_be_spoken && !t.text.append_to_context && !t.text.includes_inter_frame_spaces && t.text.skip_tts == Some(true))
    );
    assert_eq!(
        frames[skipped].transport_destination.as_deref(),
        Some("screen")
    );
    assert!(frames.iter().any(|f| f.id == aggregate_id));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert_no_spoken(&frames);
    server.await.unwrap();
}

#[tokio::test]
async fn partial_pcm_failure_never_emits_success_diagnostics() {
    let (config, _, server) = support::server(
        "200 OK",
        "application/octet-stream",
        vec![vec![1, 0]],
        true,
        Duration::ZERO,
    )
    .await;
    let worker = Pipeline::new(vec![processor(config)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    send(
        &worker,
        Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(
            "unfinished",
        ))),
    )
    .await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    assert!(!report.errors.is_empty());
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsAudio(_))));
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsAlignmentDiagnostic(_))));
    for frame in frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsStopped(_) | FrameData::LlmResponseEnd))
    {
        assert_eq!(frame.metadata.get("failed"), Some(&json!(true)));
    }
    assert_no_spoken(&frames);
    server.await.unwrap();
}

#[tokio::test]
async fn elevenlabs_raw_character_reports_do_not_become_playback_claims() {
    let body = json!({"audio_base64":"AQACAA==", "alignment": {
        "characters":["H","i"], "character_start_times_seconds":[0.0,0.1], "character_end_times_seconds":[0.1,0.2]
    }}).to_string().into_bytes();
    let (config, request, server) = support::server(
        "200 OK",
        "application/json",
        vec![body],
        false,
        Duration::ZERO,
    )
    .await;
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::ElevenLabs {
                client: ElevenLabsHttpClient::new(config).unwrap(),
                request: ElevenLabsSynthesisRequest::new("", "voice"),
            },
            4,
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("Hi"))),
    )
    .await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request.await.unwrap().json()["text"], "Hi");
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::TtsAlignmentDiagnostic(d) if d.synthesis_outcome == TtsSynthesisOutcome::Succeeded && d.evidence == TtsAlignmentEvidence::Unreported)));
    assert_no_spoken(&frames);
    server.await.unwrap();
}

// Record actual OutputTransport submissions and explicitly decline playback
// acknowledgement. No synthetic Played receipt enters the history path.
#[derive(Default)]
struct RecordingDiscard(Mutex<Vec<Frame>>);
#[async_trait]
impl OutputBackend for RecordingDiscard {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.0.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct NeverCommit;
impl SpokenHistorySink for NeverCommit {
    fn commit(&self, _: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        panic!("HTTP completion is not evidence of spoken text")
    }
}

#[tokio::test]
async fn real_output_path_preserves_diagnostics_but_never_commits_spoken_history() {
    let pcm = [1, 0].repeat(480);
    let (config, request, server) = support::server(
        "200 OK",
        "application/octet-stream",
        vec![pcm.clone()],
        false,
        Duration::ZERO,
    )
    .await;
    let recording = Arc::new(RecordingDiscard::default());
    let history =
        SpokenHistory::new(SpokenHistoryConfig::default(), Arc::new(NeverCommit)).unwrap();
    let source = Arc::new(history.source_boundary("HTTP source").unwrap());
    let output = Arc::new(
        OutputTransport::new(
            "HTTP output",
            OutputTransportConfig {
                auto_silence: false,
                chunk_duration: Duration::from_millis(20),
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            recording.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let worker = Pipeline::new(vec![source, processor(config), output])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let start = Frame::new(FrameData::LlmResponseStart);
    let start_id = start.id;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        start,
        Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(
            "unreported",
        ))),
        end,
    ] {
        send(&worker, frame).await;
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request.await.unwrap().json()["transcript"], "unreported");
    let sent: Vec<u8> = recording
        .0
        .lock()
        .unwrap()
        .iter()
        .flat_map(|f| match &f.data {
            FrameData::TtsAudio(a) | FrameData::OutputAudio(a) => a.audio.clone(),
            _ => panic!("not PCM"),
        })
        .collect();
    assert_eq!(sent, pcm);
    let anchor = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::TtsAlignmentDiagnostic(d) if d.segment_id == Some(anchor.id) && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded && d.evidence == TtsAlignmentEvidence::Unreported)));
    for id in [start_id, end_id] {
        assert_eq!(frames.iter().filter(|f| f.id == id).count(), 1);
    }
    assert_no_spoken(&frames);
    assert_eq!(history.status().committed_contexts, 0);
    server.await.unwrap();
}

#[tokio::test]
async fn provenance_bounds_fail_before_provider_admission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("test");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let worker = Pipeline::new(vec![processor(config)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut input = TtsTextInputFrame::new("safe");
    input.aggregated_by = "x".repeat(129);
    send(&worker, Frame::new(FrameData::TtsTextInput(input))).await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    assert!(report
        .errors
        .iter()
        .any(|e| e.category == ErrorCategory::Application));
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsStarted(_) | FrameData::TtsAlignmentDiagnostic(_)
    )));
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::LlmResponseEnd)
            && f.metadata.get("failed") == Some(&json!(true))));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}
