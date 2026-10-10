#![cfg(feature = "speechmatics")]
#[test]
fn facade_exposes_native_stt_and_http_tts_without_loading_credentials() {
    use pipecat::{
        speech::{HttpTtsBackend, HttpTtsProcessor, SpeechConfig},
        speechmatics_tts::{SpeechmaticsHttpClient, SpeechmaticsSynthesisRequest},
    };
    let request = SpeechmaticsSynthesisRequest::new("");
    assert_eq!((request.sample_rate, request.max_retries), (16000, 5));
    let processor = HttpTtsProcessor::new(
        HttpTtsBackend::Speechmatics {
            client: SpeechmaticsHttpClient::new(SpeechConfig::speechmatics("synthetic")).unwrap(),
            request,
        },
        4,
    )
    .unwrap();
    assert_eq!(
        pipecat::core::FrameProcessor::name(&processor),
        "HttpTtsService"
    );
    fn assert_processor<T: pipecat::core::FrameProcessor>() {}
    assert_processor::<pipecat::speechmatics::SpeechmaticsSttProcessor>();
}
