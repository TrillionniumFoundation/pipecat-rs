use pipecat_cli::config::{Command, VadPaths};
use std::path::PathBuf;
fn parse(args: &[&str]) -> Result<Command, pipecat_cli::AgentError> {
    Command::parse(args.iter().map(|s| s.to_string()))
}
#[test]
fn native_vad_requires_explicit_runtime_and_model_paths() {
    let Command::Serve(options) = parse(&[
        "--vad-model",
        "/models/silero.onnx",
        "--onnx-runtime",
        "/lib/ort.so",
    ])
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        options.vad,
        Some(VadPaths {
            model: PathBuf::from("/models/silero.onnx"),
            runtime: PathBuf::from("/lib/ort.so")
        })
    );
    assert!(parse(&["--vad-model", "model.onnx"]).is_err());
    assert!(parse(&["--onnx-runtime", "runtime.so"]).is_err());
    assert!(parse(&[
        "--smoke",
        "--vad-model",
        "model.onnx",
        "--onnx-runtime",
        "runtime.so"
    ])
    .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_silero_processor_integrates_with_native_voice_pipeline() {
    let model = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx");
    let runtime = std::env::var_os("ORT_DYLIB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0"));
    let result = pipecat_cli::smoke::run_with_vad(VadPaths { model, runtime })
        .await
        .unwrap();
    assert_eq!(result.audio_bytes, 8);
}
