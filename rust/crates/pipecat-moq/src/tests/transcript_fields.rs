use super::*;

fn record(raw: &[u8]) -> Result<transcript::Record> {
    let mut compressor = moq_flate::Encoder::new();
    transcript::Subscriber::new(Default::default()).decode(&compressor.frame(raw))
}
#[test]
fn literal_serde_marker_objects_are_preserved_in_moq_transcripts() {
    for key in [
        "$serde_json::private::RawValue",
        "$serde_json::private::Number",
    ] {
        let value =
            json!({"seq":0,"epoch":"test","nested":[{key:"42"}],"object":{key:"{\"seq\":123}"}});
        let mut expected = value.clone();
        expected.as_object_mut().unwrap().remove("seq");
        expected.as_object_mut().unwrap().remove("epoch");
        assert_eq!(
            record(&serde_json::to_vec(&value).unwrap()).unwrap(),
            transcript::Record::Message(expected)
        );
    }
}
#[test]
fn duplicate_decoded_keys_are_last_wins_and_nested_seq_is_not_transport_metadata() {
    let mut compressor = moq_flate::Encoder::new();
    let mut subscriber = transcript::Subscriber::new(Default::default());
    let raw = br#"{"seq":999,"\u0073eq":0,"epoch":"old","\u0065poch":"new","nested":{"seq":99999,"epoch":"decoy"}}"#;
    assert_eq!(
        subscriber.decode(&compressor.frame(raw)).unwrap(),
        transcript::Record::Message(json!({"nested":{"seq":99999,"epoch":"decoy"}}))
    );
    assert_eq!(
        subscriber
            .decode(&compressor.frame(br#"{"seq":1,"epoch":"new","n":2}"#))
            .unwrap(),
        transcript::Record::Message(json!({"n":2}))
    );
    assert_eq!(
        subscriber
            .decode(&compressor.frame(br#"{"seq":0,"epoch":"new"}"#))
            .unwrap(),
        transcript::Record::Replay
    );
}
#[test]
fn negative_zero_float_exponent_and_quoted_sequences_retain_classification() {
    assert_eq!(
        record(br#"{"seq":-0,"epoch":null,"n":1}"#).unwrap(),
        transcript::Record::Message(json!({"n":1}))
    );
    for raw in [
        br#"{"seq":0e0,"epoch":17}"#.as_slice(),
        br#"{"seq":0.0,"epoch":17}"#,
        br#"{"seq":"999999999999999999999","epoch":17}"#,
        br#"{"seq":true,"epoch":17}"#,
    ] {
        assert_eq!(
            record(raw).unwrap(),
            transcript::Record::Message(serde_json::from_slice(raw).unwrap())
        );
    }
}
#[test]
fn huge_integer_sequence_is_exact_and_bounded_without_float_conversion() {
    let number = "9".repeat(4096);
    let raw = format!("{{\"seq\":{number},\"epoch\":\"big\",\"n\":1}}");
    let mut compressor = moq_flate::Encoder::new();
    let mut subscriber = transcript::Subscriber::new(Default::default());
    assert_eq!(
        subscriber
            .decode(&compressor.frame(raw.as_bytes()))
            .unwrap(),
        transcript::Record::Message(json!({"n":1}))
    );
    assert_eq!(
        subscriber
            .decode(&compressor.frame(raw.as_bytes()))
            .unwrap(),
        transcript::Record::Replay
    );
    let too_big = format!("{{\"seq\":{}}}", "1".repeat(65536));
    assert!(matches!(record(too_big.as_bytes()), Err(Error::Limit(_))));
}
#[test]
fn malformed_record_syntax_escapes_and_deep_materialized_values_are_rejected() {
    for raw in [
        br#"{"seq":1,}"#.as_slice(),
        br#"{"seq":01}"#,
        br#"{"seq":+1}"#,
        br#"{"seq":1} trailing"#,
        br#"{"seq":1 "n":2}"#,
        br#"{"seq":1,"x":"\uZZZZ"}"#,
        br#"{"seq":1,"x":"\q"}"#,
        br#"[1,2]"#,
        br#"null"#,
    ] {
        assert!(
            record(raw).is_err(),
            "unexpected acceptance: {}",
            String::from_utf8_lossy(raw)
        );
    }
    let deep = format!(
        "{{\"seq\":0,\"nested\":{}null{}}}",
        "[".repeat(300),
        "]".repeat(300)
    );
    assert!(record(deep.as_bytes()).is_err());
}
