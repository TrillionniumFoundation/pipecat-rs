use pipecat_vad::{
    EndOfTurnState, RuntimeOptions, SmartTurnAnalyzer, SmartTurnCompletionReason, SmartTurnModel,
    SmartTurnParams, VadError,
};
use std::{path::PathBuf, time::Duration};

fn real_model() -> SmartTurnModel {
    let options = RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
    };
    let path = std::env::var_os("PIPECAT_SMART_TURN_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx")
        });
    SmartTurnModel::load(&options, path)
        .expect("real local ONNX runtime and tracked Smart Turn model are required")
}
fn pcm(bytes: &[u8]) -> Vec<i16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
        .collect()
}

#[tokio::test]
async fn smart_turn_real_model_controller_reanalysis_complete_reset_and_reuse() {
    let mut analyzer = SmartTurnAnalyzer::new(real_model(), SmartTurnParams::default()).unwrap();
    let tone = pcm(include_bytes!("fixtures/smart_turn_tone.pcm16"));
    let expected_tone: f32 = include_str!("fixtures/smart_turn_tone.probability.txt")
        .trim()
        .parse()
        .unwrap();
    analyzer.append_pcm16(&tone, true).unwrap();
    for _ in 0..2 {
        let result = analyzer.analyze_end_of_turn().await.unwrap();
        assert_eq!(result.state, EndOfTurnState::Incomplete);
        assert_eq!(result.reason, SmartTurnCompletionReason::Model);
        let metrics = result.metrics.unwrap();
        assert!((metrics.probability - expected_tone).abs() < 1e-5);
        assert!(metrics.e2e_processing_time_ms > 0.0);
        assert_eq!(analyzer.buffer().analysis_samples(), tone);
        assert!(analyzer.buffer().speech_triggered());
    }
    analyzer.cleanup().await.unwrap();
    analyzer.clear();
    let speech = pcm(include_bytes!("fixtures/smart_turn_speech.pcm16"));
    analyzer.append_pcm16(&speech, true).unwrap();
    let result = analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Complete);
    let expected: f32 = include_str!("fixtures/smart_turn_speech.probability.txt")
        .trim()
        .parse()
        .unwrap();
    assert!((result.metrics.unwrap().probability - expected).abs() < 1e-5);
    assert_eq!(analyzer.buffer().buffered_samples(), 0);
    assert!(!analyzer.buffer().speech_triggered());
    let empty = analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(empty.state, EndOfTurnState::Incomplete);
    assert_eq!(empty.reason, SmartTurnCompletionReason::Empty);
    assert!(empty.metrics.is_none());
    analyzer.cleanup().await.unwrap();
}

#[tokio::test]
async fn smart_turn_real_model_cancellation_terminates_onnx_and_preserves_reuse() {
    let mut analyzer = SmartTurnAnalyzer::new(real_model(), SmartTurnParams::default()).unwrap();
    let tone = pcm(include_bytes!("fixtures/smart_turn_tone.pcm16"));
    analyzer.append_pcm16(&tone, true).unwrap();
    let pending = analyzer.begin_analysis().unwrap();
    let cancellation = pending.cancellation();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !cancellation.is_inference_running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual ONNX run must start");
    cancellation.cancel();
    assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
    analyzer.cleanup().await.unwrap();
    assert!(!cancellation.is_inference_running());
    assert!(analyzer.buffer().speech_triggered());
    let result = analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Incomplete);
    assert!(result.metrics.is_some());
    analyzer.cleanup().await.unwrap();
}

#[tokio::test]
async fn smart_turn_real_model_silence_timeout_does_not_fabricate_metrics() {
    let mut analyzer = SmartTurnAnalyzer::new(
        real_model(),
        SmartTurnParams {
            stop_secs: 0.02,
            ..Default::default()
        },
    )
    .unwrap();
    analyzer.append_pcm16(&[123; 320], true).unwrap();
    assert_eq!(
        analyzer.append_pcm16(&[0; 320], false).unwrap(),
        EndOfTurnState::Complete
    );
    let result = analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Incomplete);
    assert!(result.metrics.is_none());
    assert_eq!(result.reason, SmartTurnCompletionReason::Empty);
    analyzer.cleanup().await.unwrap();
}
