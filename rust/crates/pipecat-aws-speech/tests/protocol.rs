use pipecat_aws_speech::*;
fn credentials() -> AwsCredentials {
    AwsCredentials::new(
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        Some("TOKEN/+=".into()),
    )
    .unwrap()
}
#[test]
fn presign_is_fresh_sorted_and_uses_double_encoded_equals() {
    let c = TranscribeConfig::new(credentials(), "us-east-1");
    let signed = c.presigned_url_at("20241010T010203Z").unwrap();
    assert!(signed.as_str().ends_with(
        "X-Amz-Signature=ad036f1f5912b19d7109033e8ec61c45200527e0c03473cd0d9f50a491456e98"
    ));
    assert!(signed
        .as_str()
        .contains("X-Amz-Security-Token=TOKEN%2F%2B%253D"));
    assert_ne!(
        signed.as_str(),
        c.presigned_url_at("20241010T010204Z").unwrap().as_str()
    );
    assert!(!format!("{signed:?}").contains("TOKEN"));
}
#[test]
fn source_languages_and_full_unknown_fallback() {
    for (input, expected) in [
        ("ar", "ar-SA"),
        ("et-EE", "et-ET"),
        ("no", "no-NO"),
        ("cy-GB", "cy-WL"),
        ("yue", "zh-HK"),
        ("lg", "lg-IN"),
        ("fil-PH", "tl-PH"),
        ("ckb-IQ", "ckb-IQ"),
        ("ckb-IR", "ckb-IR"),
        ("en-AB", "en-AB"),
        ("en-WL", "en-WL"),
        ("fa-AF", "fa-AF"),
        ("kab", "kab-DZ"),
        ("kab-DZ", "kab-DZ"),
        ("sw-BI", "sw-BI"),
        ("sw-RW", "sw-RW"),
        ("sw-UG", "sw-UG"),
        ("zz-ZZ", "zz-ZZ"),
    ] {
        assert_eq!(transcribe_language(input), expected);
    }
    for (input, expected) in [
        ("ar", "arb"),
        ("zh", "cmn-CN"),
        ("yue", "yue-CN"),
        ("nb", "nb-NO"),
        ("cy", "cy-GB"),
        ("en", "en-US"),
        ("zz-ZZ", "zz-ZZ"),
    ] {
        assert_eq!(polly_language(input), expected);
    }
}
#[test]
fn ssml_escapes_text_and_attributes_and_limits_pitch_to_standard() {
    let mut settings = PollySettings {
        engine: Some("standard".into()),
        pitch: Some("high' on='bad".into()),
        rate: Some("80%".into()),
        volume: Some("+3dB".into()),
        ..Default::default()
    };
    let ssml = settings.ssml("<speak> & hello >").unwrap();
    assert!(ssml.contains("&lt;speak&gt; &amp; hello &gt;"));
    assert!(ssml.contains("pitch='high&apos; on=&apos;bad'"));
    settings.engine = Some("neural".into());
    assert!(!settings.ssml("hi").unwrap().contains("pitch="));
    settings.lexicon_names = Some(vec!["Names".into()]);
    let body = settings.request_body("hi").unwrap();
    assert_eq!(body["SampleRate"], "16000");
    assert_eq!(body["OutputFormat"], "pcm");
    assert_eq!(body["LexiconNames"][0], "Names");
    assert!(settings.ssml("hi\u{1}").is_err());
    assert!(settings.ssml(&"x".repeat(3001)).is_err());
}
#[test]
fn aws_routes_prevent_credential_exfiltration() {
    let mut c = TranscribeConfig::new(credentials(), "us-east-1");
    for endpoint in [
        "wss://evil.example/stream-transcription-websocket",
        "ws://192.168.0.1/stream-transcription-websocket",
        "ws://127.0.0.1/other",
        "ws://user:pass@127.0.0.1/stream-transcription-websocket",
        "ws://127.0.0.1/stream-transcription-websocket?bad=1",
    ] {
        c.endpoint = endpoint.into();
        assert!(TranscribeClient::new(c.clone()).is_err());
    }
    let mut c = PollyConfig::new(credentials(), "us-east-1");
    c.endpoint = "https://evil.example/v1/speech".into();
    assert!(PollyClient::new(c).is_err());
    let c = TranscribeConfig::new(credentials(), "cn-north-1");
    assert!(c
        .presigned_url_at("20241010T010203Z")
        .unwrap()
        .as_str()
        .starts_with("wss://transcribestreaming.cn-north-1.amazonaws.com.cn:8443/"));
}
#[test]
fn eventstream_validates_checksums_lengths_types_and_duplicates() {
    let encoded = eventstream::audio(&[1, 2, 3, 4], 1024).unwrap();
    let e = eventstream::decode(&encoded, 1024).unwrap();
    assert_eq!(e.payload, [1, 2, 3, 4]);
    assert_eq!(e.header(":event-type"), Some("AudioEvent"));
    for end in 0..encoded.len() {
        assert!(eventstream::decode(&encoded[..end], 1024).is_err());
    }
    for i in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[i] ^= 1;
        assert!(eventstream::decode(&corrupt, 1024).is_err());
    }
    assert!(eventstream::decode(&encoded, encoded.len() - 1).is_err());
    assert!(eventstream::encode(&[("x", "one"), ("x", "two")], b"", 1024).is_err());
    // Repair CRCs after structural damage so parsing, not just CRC rejection, is tested.
    let mut unknown = encoded.clone();
    unknown[26] = 99;
    let crc = crc32fast::hash(&unknown[..unknown.len() - 4]);
    let n = unknown.len();
    unknown[n - 4..].copy_from_slice(&crc.to_be_bytes());
    assert!(eventstream::decode(&unknown, 1024).is_err());
    let mut oversized_headers = encoded.clone();
    oversized_headers[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
    let crc = crc32fast::hash(&oversized_headers[..8]);
    oversized_headers[8..12].copy_from_slice(&crc.to_be_bytes());
    assert!(eventstream::decode(&oversized_headers, 1024).is_err());
}
#[test]
fn terminal_event_is_binary_audio_event_with_empty_payload() {
    let bytes = eventstream::audio(&[], 1024).unwrap();
    let event = eventstream::decode(&bytes, 1024).unwrap();
    assert!(event.payload.is_empty());
    assert_eq!(event.header(":message-type"), Some("event"));
    assert_eq!(event.header(":event-type"), Some("AudioEvent"));
}
