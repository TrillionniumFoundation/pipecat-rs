//! Whole-upload HTTP STT is available through the default facade, independently
//! of the optional ElevenLabs websocket service feature.
use pipecat::{
    core::FrameProcessor,
    speech::{
        elevenlabs::{ElevenLabsHttpClient, ElevenLabsTranscriptionRequest},
        HttpSttBackend, HttpSttOptions, HttpSttProcessor, OpenAiTranscriptionOptions, SpeechClient,
        SpeechConfig, Transcript, TranscriptionRequest,
    },
};

#[test]
fn http_stt_exports_preserve_legacy_clients_and_both_typed_backends() {
    // Existing callers can still construct these public structs directly.
    let request = TranscriptionRequest {
        model: "gpt-transcribe".into(),
        language: Some("en".into()),
        prompt: None,
    };
    let result = Transcript {
        text: "text".into(),
    };
    assert_eq!(result.text, "text");
    let openai = HttpSttProcessor::new(
        HttpSttBackend::OpenAi {
            client: SpeechClient::new(SpeechConfig::openai("synthetic-key")).unwrap(),
            request,
        },
        HttpSttOptions::default(),
    )
    .unwrap();
    assert_eq!(openai.name(), "OpenAIHttpSTTService");
    let elevenlabs = HttpSttProcessor::new(
        HttpSttBackend::ElevenLabs {
            client: ElevenLabsHttpClient::new(SpeechConfig::elevenlabs("synthetic-key")).unwrap(),
            request: ElevenLabsTranscriptionRequest::default(),
        },
        HttpSttOptions::default(),
    )
    .unwrap();
    assert_eq!(elevenlabs.name(), "ElevenLabsHttpSTTService");
    assert!(!OpenAiTranscriptionOptions::default().include_prob_metrics);
}
