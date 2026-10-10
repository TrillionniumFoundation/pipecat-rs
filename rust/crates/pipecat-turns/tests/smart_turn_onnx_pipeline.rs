//! The local ONNX model drives real pipeline aggregation; only provider STT/LLM
//! calls are synthetic so this test never needs a network service or Python.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use pipecat_vad::{RuntimeOptions, SmartTurnAnalyzer, SmartTurnModel, SmartTurnParams};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

struct SyntheticStt;
#[async_trait]
impl FrameProcessor for SyntheticStt {
    fn name(&self) -> &str {
        "SyntheticStt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let transcribe =
            matches!(frame.data, FrameData::InputAudio(_)) && direction == Direction::Downstream;
        ctx.push(frame, direction).await?;
        if transcribe {
            ctx.push(
                Frame::new(FrameData::TranscriptionDetailed(
                    DetailedTranscriptionFrame {
                        text: TextFrame::new("A native turn is ready"),
                        user_id: "speaker".into(),
                        timestamp: String::new(),
                        language: Some("en".into()),
                        result: None,
                        finalized: true,
                    },
                )),
                direction,
            )
            .await?;
        }
        Ok(())
    }
}
struct SyntheticLlm(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for SyntheticLlm {
    fn name(&self) -> &str {
        "SyntheticLlm"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::LlmContext(context) = &frame.data {
            if direction == Direction::Downstream {
                assert_eq!(
                    context.messages.last().unwrap().content,
                    "A native turn is ready"
                );
                self.0.fetch_add(1, Ordering::SeqCst);
                ctx.push(Frame::new(FrameData::LlmResponseStart), direction)
                    .await?;
                ctx.push(
                    Frame::new(FrameData::LlmText(TextFrame::new("Native response"))),
                    direction,
                )
                .await?;
                return ctx
                    .push(Frame::new(FrameData::LlmResponseEnd), direction)
                    .await;
            }
        }
        ctx.push(frame, direction).await
    }
}
async fn recv_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let output = worker.recv().await.expect("pipeline output");
            if let FrameData::Error(error) = &output.frame.data {
                panic!("pipeline error: {error:?}");
            }
            if predicate(&output.frame.data) {
                return output;
            }
        }
    })
    .await
    .expect("model and pipeline must finish")
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_smart_turn_onnx_gates_synthetic_stt_llm_and_paired_context() {
    run_real_model(None).await;
}
#[tokio::test]
async fn actual_smart_turn_onnx_drives_composable_analyzer_policy() {
    run_real_model(Some(
        UserTurnStopStrategies::new(vec![UserTurnStopStrategy::turn_analyzer(true)]).unwrap(),
    ))
    .await;
}
async fn run_real_model(stop_strategies: Option<UserTurnStopStrategies>) {
    let runtime = RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
    };
    let model_path = std::env::var_os("PIPECAT_SMART_TURN_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx")
        });
    let model = SmartTurnModel::load(&runtime, model_path)
        .expect("explicit local ONNX runtime and tracked Smart Turn v3.2 model");
    let analyzer = SmartTurnAnalyzer::new(model, SmartTurnParams::default()).unwrap();
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                stt_timeout: Duration::from_secs(10),
                stop_strategies,
                enable_interruptions: false,
                ..Default::default()
            },
            ..Default::default()
        },
        AssistantAggregatorConfig::default(),
    );
    let user = Arc::new(SmartTurnUserAggregator::with_aggregator(pair.user(), analyzer).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut worker = Pipeline::new(vec![
        Arc::new(SyntheticStt),
        user.clone(),
        Arc::new(SyntheticLlm(calls.clone())),
        pair.assistant(),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    recv_until(&mut worker, |data| data.is_start()).await;
    send(
        &worker,
        FrameData::VadUserStartedSpeakingDetailed(VadUserStartedSpeakingFrame {
            start_secs: 0.2,
            timestamp: 0.0,
        }),
    )
    .await;
    send(
        &worker,
        FrameData::InputAudio(
            AudioRawFrame::new(
                include_bytes!("../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16").to_vec(),
                16_000,
                1,
            )
            .unwrap(),
        ),
    )
    .await;
    send(
        &worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("stt-completed".into()),
        }),
    )
    .await;
    recv_until(&mut worker, |data| matches!(data, FrameData::Flush(_))).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "final STT text must not bypass the model gate"
    );
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    send(
        &worker,
        FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
            stop_secs: 0.2,
            timestamp: 0.0,
        }),
    )
    .await;
    let metrics = recv_until(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
    let probability = metrics.frame.metadata["smart_turn"]["probability"]
        .as_f64()
        .unwrap();
    let expected: f64 =
        include_str!("../../pipecat-vad/tests/fixtures/smart_turn_speech.probability.txt")
            .trim()
            .parse()
            .unwrap();
    assert!((probability - expected).abs() < 1e-5);
    assert!(metrics.frame.metadata["smart_turn"]["is_complete"]
        .as_bool()
        .unwrap());
    recv_until(&mut worker, |data| {
        matches!(data, FrameData::LlmContext(context) if context.messages.last().is_some_and(|message| message.content == "Native response"))
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let context = pair.context().snapshot().unwrap();
    assert_eq!(context.messages.len(), 2);
    assert_eq!(context.messages[0].role, Role::User);
    assert_eq!(
        context.messages[0].content,
        Some(Content::Text("A native turn is ready".into()))
    );
    assert_eq!(context.messages[1].role, Role::Assistant);
    assert_eq!(
        context.messages[1].content,
        Some(Content::Text("Native response".into()))
    );
    assert_eq!(user.buffered_samples().await, 0);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
