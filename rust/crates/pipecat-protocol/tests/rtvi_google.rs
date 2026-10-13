use pipecat_core::{LlmSearchOrigin, LlmSearchResponseFrame, LlmSearchResult};
use pipecat_protocol::{
    rtvi::{Envelope, MESSAGE_LABEL},
    rtvi_google::{
        GoogleSearchOrigin, GoogleSearchResponseData, GoogleSearchResult,
        GOOGLE_SEARCH_RESPONSE_TYPE,
    },
    ProtocolError, DEFAULT_MAX_MESSAGE_BYTES,
};
use serde_json::json;

fn source() -> LlmSearchResponseFrame {
    LlmSearchResponseFrame {
        search_result: Some("answer".into()),
        rendered_content: None,
        origins: vec![LlmSearchOrigin {
            site_uri: Some("https://example.test/source".into()),
            site_title: None,
            results: vec![LlmSearchResult {
                text: Some("segment".into()),
                confidence: Some(vec![0.8, 0.6]),
            }],
        }],
    }
}

fn envelope(data: serde_json::Value) -> Envelope {
    Envelope {
        label: MESSAGE_LABEL.into(),
        message_type: GOOGLE_SEARCH_RESPONSE_TYPE.into(),
        id: None,
        data: Some(data),
    }
}

#[test]
fn google_search_native_projection_preserves_complete_vectors_and_optional_omission() {
    let input = source();
    let before = input.clone();
    let data = GoogleSearchResponseData::try_from(&input).unwrap();
    let wire = data.envelope().unwrap();
    assert_eq!(
        serde_json::to_value(&wire).unwrap(),
        json!({"label":"rtvi-ai","type":"bot-llm-search-response","data":{
            "search_result":"answer","origins":[{"site_uri":"https://example.test/source",
                "results":[{"text":"segment","confidence":[0.8,0.6]}]}]
        }})
    );
    let parsed = Envelope::parse(wire.encode().unwrap().as_bytes()).unwrap();
    assert_eq!(
        GoogleSearchResponseData::from_envelope(&parsed).unwrap(),
        data
    );
    assert_eq!(input, before);
    assert!(
        wire.server_event().is_err(),
        "the closed base event enum stays unchanged"
    );
}

#[test]
fn google_search_decoder_keeps_empty_values_and_defaults_only_missing_confidence() {
    let wire = envelope(json!({"search_result":"","rendered_content":"","origins":[{
        "site_uri":"","site_title":null,"results":[{"text":""}]
    },{}]}));
    let data = GoogleSearchResponseData::from_envelope(&wire).unwrap();
    assert_eq!(data.origins[0].results[0].confidence, Vec::<f64>::new());
    assert_eq!(
        data.envelope().unwrap().data.unwrap(),
        json!({"search_result":"","rendered_content":"","origins":[{
            "site_uri":"","results":[{"text":"","confidence":[]}]
        },{"results":[]}]})
    );
    for invalid in [
        json!({"origins":[{"results":[{"text":null,"confidence":[]}]}]}),
        json!({"origins":[{"results":[{"confidence":[]}]}]}),
        json!({"origins":[{"results":[{"text":"","confidence":null}]}]}),
        json!({}),
    ] {
        assert!(GoogleSearchResponseData::from_envelope(&envelope(invalid)).is_err());
    }
}

#[test]
fn google_search_native_null_and_nonfinite_fields_reject_without_fallback() {
    for text_absent in [false, true] {
        let mut input = source();
        if text_absent {
            input.origins[0].results[0].text = None;
        } else {
            input.origins[0].results[0].confidence = None;
        }
        assert!(matches!(
            GoogleSearchResponseData::try_from(&input),
            Err(ProtocolError::MissingField(_))
        ));
    }
    for score in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut input = source();
        input.origins[0].results[0].confidence = Some(vec![score]);
        assert!(matches!(
            GoogleSearchResponseData::try_from(&input),
            Err(ProtocolError::InvalidField(_))
        ));
        let data = GoogleSearchResponseData {
            origins: vec![GoogleSearchOrigin {
                results: vec![GoogleSearchResult {
                    text: "segment".into(),
                    confidence: vec![score],
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            data.envelope(),
            Err(ProtocolError::InvalidField(_))
        ));
        assert!(
            serde_json::to_value(data).is_err(),
            "scores must not silently become JSON null"
        );
    }
}

#[test]
fn google_search_decoder_validates_label_type_and_required_data() {
    let mut wire = envelope(json!({"origins":[]}));
    wire.label = "foreign".into();
    assert!(matches!(
        GoogleSearchResponseData::from_envelope(&wire),
        Err(ProtocolError::InvalidField(_))
    ));
    wire.label = MESSAGE_LABEL.into();
    wire.message_type = "bot-llm-text".into();
    assert!(matches!(
        GoogleSearchResponseData::from_envelope(&wire),
        Err(ProtocolError::InvalidField(_))
    ));
    wire.message_type = GOOGLE_SEARCH_RESPONSE_TYPE.into();
    wire.data = None;
    assert!(matches!(
        GoogleSearchResponseData::from_envelope(&wire),
        Err(ProtocolError::MissingField(_))
    ));
    wire.data = Some(serde_json::Value::Null);
    assert!(matches!(
        GoogleSearchResponseData::from_envelope(&wire),
        Err(ProtocolError::InvalidField(_))
    ));
}

#[test]
fn google_search_byte_limit_counts_the_complete_envelope_before_owned_conversion() {
    let mut data = GoogleSearchResponseData {
        search_result: Some(String::new()),
        ..Default::default()
    };
    let overhead = data.envelope().unwrap().encode().unwrap().len();
    data.search_result = Some("x".repeat(DEFAULT_MAX_MESSAGE_BYTES - overhead));
    let wire = data.envelope().unwrap();
    assert_eq!(wire.encode().unwrap().len(), DEFAULT_MAX_MESSAGE_BYTES);
    assert_eq!(
        GoogleSearchResponseData::from_envelope(&wire).unwrap(),
        data
    );
    let mut input = LlmSearchResponseFrame {
        search_result: data.search_result.clone(),
        ..Default::default()
    };
    GoogleSearchResponseData::try_from(&input).unwrap();
    input.search_result.as_mut().unwrap().push('x');
    assert!(matches!(
        GoogleSearchResponseData::try_from(&input),
        Err(ProtocolError::MessageTooLarge { .. })
    ));
    data.search_result.as_mut().unwrap().push('x');
    assert!(matches!(
        data.envelope(),
        Err(ProtocolError::MessageTooLarge { .. })
    ));
    let mut oversized = envelope(json!({"origins":[]}));
    oversized.id = Some("x".repeat(DEFAULT_MAX_MESSAGE_BYTES));
    assert!(matches!(
        GoogleSearchResponseData::from_envelope(&oversized),
        Err(ProtocolError::MessageTooLarge { .. })
    ));
}
