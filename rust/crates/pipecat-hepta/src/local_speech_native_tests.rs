//! Explicit device qualification, not a synthetic playback acknowledgement.
use super::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires explicit trusted local speech config and authorized physical output"]
async fn long_chinese_output_survives_device_deadline_and_interrupt_fences_remaining_pcm() {
    let path = std::env::var_os("PIPECAT_LOCAL_SPEECH_CONFIG").expect("explicit config required");
    let config: LocalSpeechConfig = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let mut identity = OutputIdentity {
        stream: config.stream.clone(),
        turn_id: config.turn_id.clone(),
        utterance_id: "native-qualification".into(),
        sequence: 1,
        output_epoch: config.output_epoch,
    };
    let speech = LocalSpeech::load(config).await.unwrap();
    let media = speech.presentation();
    media.open_output().unwrap();
    let text = "这是中文语音设备测试。长回复应该连续输出，不能因为五秒超时而丢失。".to_owned();
    let audio = media.synthesize(identity.clone(), &text).await.unwrap();
    let frames = audio.audio.audio.len() / 2;
    let rate = audio.audio.sample_rate;
    assert!(
        frames > rate as usize * 5,
        "fixture must exceed the old write deadline"
    );
    let receipt = media.submit(audio).await.unwrap();
    assert_eq!(receipt.frames, frames);
    assert_eq!(receipt.sample_rate, rate);

    let audio = media.synthesize(identity.clone(), &text).await.unwrap();
    let mut submission = Box::pin(media.submit(audio));
    tokio::select! {
        result = &mut submission => panic!("long output unexpectedly finished before interruption: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    let next_epoch = identity.output_epoch.checked_add(1).unwrap();
    media.interrupt(next_epoch).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), submission)
        .await
        .unwrap()
        .is_err());
    assert!(matches!(
        media.synthesize(identity.clone(), "旧输出").await,
        Err(LocalSpeechError::Stale)
    ));

    identity.turn_id = "native-qualification-next".into();
    identity.output_epoch = next_epoch;
    identity.sequence += 1;
    media.admit_turn(&identity.turn_id, next_epoch).unwrap();
    // interrupt already reopened the native stream under the new fence.
    let audio = media.synthesize(identity, "测试完成。").await.unwrap();
    assert!(media.submit(audio).await.unwrap().frames > 0);
    speech.shutdown().await.unwrap();
    // Receipts establish callback submission only, never audibility or listening.
}

#[tokio::test]
#[ignore = "requires explicit hashed multilingual Whisper and Chinese Piper assets"]
async fn chinese_tts_to_stt_records_clean_and_noisy_model_results() {
    let path = std::env::var_os("PIPECAT_LOCAL_SPEECH_CONFIG").expect("explicit config required");
    let config: LocalSpeechConfig = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let identity = OutputIdentity {
        stream: config.stream.clone(),
        turn_id: config.turn_id.clone(),
        utterance_id: "offline-chinese-qualification".into(),
        sequence: 1,
        output_epoch: config.output_epoch,
    };
    let settings = config.whisper_settings.clone();
    let speech = LocalSpeech::load(config).await.unwrap();
    let mut times = Vec::new();
    let store = speech
        .media_store()
        .expect("explicit private qualification media root required");
    let mut sequence = 0;
    for text in [
        "你好，欢迎使用本地语音助手。",
        "请停止播放，保留当前会话。",
        "今天我们测试中文语音识别。",
        "设备重新连接以后继续工作。",
    ] {
        let audio = speech
            .presentation()
            .synthesize(identity.clone(), text)
            .await
            .unwrap();
        let pcm =
            pipecat_audio::resample_pcm16(&audio.audio.audio, audio.audio.sample_rate, 16000, 1)
                .unwrap();
        for noisy in [false, true] {
            let mut pcm = pcm.clone();
            if noisy {
                // Reproducible low-amplitude white noise; not acoustic echo.
                let mut seed = 17u32;
                for sample in pcm.as_chunks_mut::<2>().0 {
                    seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                    let noise = ((seed >> 16) as i32 % 601) - 300;
                    let value = (i32::from(i16::from_le_bytes([sample[0], sample[1]])) + noise)
                        .clamp(-32768, 32767) as i16;
                    sample.copy_from_slice(&value.to_le_bytes());
                }
            }
            let captured_bytes = pcm.len();
            let captured_digest = pipecat_piper::sha256_bytes(&pcm);
            pcm.resize(captured_bytes + 32000, 0); // decoder padding is not capture
            let input = AudioRawFrame::new(pcm, 16000, 1).unwrap();
            sequence += 1;
            let started = std::time::Instant::now();
            let transcript = speech
                .whisper
                .as_ref()
                .unwrap()
                .submit_with_source(
                    &input,
                    settings.clone(),
                    captured_bytes / 2,
                    sequence,
                    store.clone(),
                )
                .unwrap()
                .result()
                .await
                .unwrap();
            let elapsed = started.elapsed().as_micros();
            let source: crate::CapturedAudioRef = serde_json::from_value(
                transcript
                    .audio_source
                    .clone()
                    .expect("successful final must retain its source"),
            )
            .unwrap();
            assert_eq!(source.byte_length, captured_bytes as u64);
            assert_eq!(source.sha256, captured_digest);
            store.verify(&source).unwrap();
            // Explicit release by this isolated qualification consumer; no
            // production observation/admission receipt is asserted here.
            store.release(&source).unwrap();
            times.push(elapsed);
            let rss = std::fs::read_to_string("/proc/self/status").unwrap();
            let rss_kib = rss
                .lines()
                .find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|value| value.split_whitespace().next())
                        .and_then(|value| value.parse::<u64>().ok())
                })
                .unwrap();
            println!(
                "{}",
                serde_json::json!({"scope":"offline_synthetic_chinese_tts_to_stt", "expected":text,"recognized":transcript.text,"noisy":noisy,"stt_us":elapsed,"rss_kib":rss_kib})
            );
            assert!(
                !transcript.text.trim().is_empty(),
                "actual recognizer returned no speech"
            );
        }
    }
    times.sort_unstable();
    println!(
        "{}",
        serde_json::json!({"scope":"offline_stt_and_retention_not_mic_to_speaker","n":times.len(),"p50_us":times[3],"p95_us":times[7],"p99_us":times[7]})
    );
    speech.shutdown().await.unwrap();
}
