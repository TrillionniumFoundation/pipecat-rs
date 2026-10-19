//! Shared real-loopback fixtures for the opt-in initial-history contract.
//! An observed peer close is never substituted for the actor join observer.
#![allow(dead_code)]

#[path = "paired_history_peer.rs"]
mod peer;

pub use peer::{Peer, WireTrace, WAIT};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

pub struct Harness {
    pub processor: Arc<GeminiLiveProcessor>,
    pub worker: PipelineWorker,
    pub output: Vec<DirectedFrame>,
    marker: u64,
}

pub fn policy(inference_on_initialization: bool) -> InitialHistoryPolicy {
    InitialHistoryPolicy {
        inference_on_initialization,
    }
}

pub fn context(messages: Vec<ChatMessage>) -> LlmContextFrame {
    LlmContextFrame {
        messages,
        ..Default::default()
    }
}

pub fn text(role: ChatRole, text: &str) -> ChatMessage {
    ChatMessage::new(role, text)
}

pub fn audio() -> FrameData {
    FrameData::InputAudio(AudioRawFrame::new(vec![3, 0, 4, 0], 16_000, 1).unwrap())
}

pub fn video() -> FrameData {
    FrameData::InputImage(ImageRawFrame {
        image: vec![255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255],
        size: (2, 2),
        format: Some("RGB".into()),
    })
}

pub fn turn(role: &str, text: &str) -> Value {
    json!({"role":role,"parts":[{"text":text}]})
}

pub fn seed_wire(turns: Vec<Value>, complete: bool) -> Value {
    json!({"clientContent":{"turns":turns,"turnComplete":complete}})
}

pub fn nudge() -> Value {
    json!({"realtimeInput":{"text":" "}})
}

impl Harness {
    pub async fn start(client: GeminiClient) -> Self {
        Self::reuse(Arc::new(GeminiLiveProcessor::new(client))).await
    }

    pub async fn reuse(processor: Arc<GeminiLiveProcessor>) -> Self {
        let worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                startup_timeout: Duration::from_secs(4),
                shutdown_timeout: Duration::from_secs(7),
                start: StartFrame {
                    enable_usage_metrics: true,
                    audio_out_sample_rate: 24_000,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();
        Self {
            processor,
            worker,
            output: Vec::new(),
            marker: 1000,
        }
    }

    pub async fn next(&mut self) -> DirectedFrame {
        let frame = timeout(WAIT, self.worker.recv())
            .await
            .expect("pipeline observation deadline")
            .expect("pipeline closed before observation");
        self.output.push(frame.clone());
        frame
    }

    /// Observe the original frame after the real processor, or its real error.
    /// This is local processor admission evidence, not a remote acknowledgment.
    pub async fn frame(&mut self, data: FrameData, reject: bool) -> u64 {
        let mut frame = Frame::new(data);
        frame.name = "selected-initial-history-fixture".into();
        frame.metadata.insert("fixture".into(), json!("original"));
        frame
            .metadata
            .insert("context_id".into(), json!("startup-origin"));
        frame.set_pipeline_pts(3456);
        let expected = frame.clone();
        self.worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let observed = self.next().await;
            if matches!(
                observed.frame.data,
                FrameData::Error(_) | FrameData::ErrorDetailed(_)
            ) {
                assert!(
                    reject,
                    "unexpected real processor error: {:?}",
                    observed.frame
                );
                return expected.id;
            }
            if observed.frame.id == expected.id {
                assert!(!reject, "invalid frame was forwarded without rejection");
                assert_eq!(observed.frame, expected);
                assert_eq!(observed.direction, Direction::Downstream);
                return expected.id;
            }
        }
    }

    pub async fn seed(&mut self, seed: LlmContextFrame) -> u64 {
        self.frame(FrameData::LlmContext(seed), false).await
    }

    /// A real mapped metric proves the actor consumed the preceding completion.
    pub async fn settle_turn(&mut self, peer: &Peer) {
        self.marker += 1;
        peer.send(json!({
            "serverContent":{"turnComplete":true},
            "usageMetadata":{"promptTokenCount":1,"responseTokenCount":1,
                             "totalTokenCount":self.marker}
        }))
        .await;
        loop {
            let output = self.next().await;
            if matches!(&output.frame.data, FrameData::Metrics(metrics)
                if metrics.iter().any(|metric| matches!(metric,
                    Metric::LlmUsage { total_tokens, .. } if *total_tokens == self.marker)))
            {
                return;
            }
            assert!(
                !matches!(
                    output.frame.data,
                    FrameData::Error(_) | FrameData::ErrorDetailed(_)
                ),
                "{:?}",
                output.frame
            );
        }
    }

    pub async fn terminal(mut self, peer: Peer, kind: WorkerState) -> (WorkerReport, WireTrace) {
        let observer = self
            .processor
            .selected_join_observer()
            .expect("created actor observer");
        assert!(
            !observer.joined(),
            "actor unexpectedly settled before terminal request"
        );
        match kind {
            WorkerState::Ended => self.worker.end().await.unwrap(),
            WorkerState::Stopped => self
                .worker
                .sender()
                .send(Frame::new(FrameData::Stop), Direction::Downstream)
                .await
                .unwrap(),
            WorkerState::Cancelled => self
                .worker
                .cancel("selected fixture complete")
                .await
                .unwrap(),
            other => panic!("unsupported fixture terminal {other:?}"),
        }
        while timeout(WAIT, self.worker.recv()).await.unwrap().is_some() {}
        let report = timeout(WAIT, self.worker.join()).await.unwrap().unwrap();
        assert_eq!(report.state, kind, "{:?}", report.errors);
        assert!(
            observer.joined(),
            "the actual socket actor JoinHandle was not awaited"
        );
        let trace = peer.join().await;
        (report, trace)
    }
}
