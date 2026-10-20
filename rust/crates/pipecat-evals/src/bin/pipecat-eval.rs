//! Native scripted evaluation CLI; exit 0 for pass, 1 for failed runs, 2 for invalid input.
#[path = "support/wav_output.rs"]
mod wav_output;

use pipecat_evals::{
    process::ProcessOptions,
    release::{run_release, ReleaseManifest},
    BotAudioLimits, Cancellation, EvalSession, RecordedScenarioFile, ScenarioFile, SessionOptions,
    TimelineRecordingLimits,
};
#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = run(&args).await;
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
async fn run(args: &[String]) -> Result<bool, pipecat_evals::EvalError> {
    let suite = args.len() >= 2 && args[0] == "suite";
    let single = (3..=11).contains(&args.len()) && args[0] == "run";
    let mut stop_bot = false;
    let mut recorded = false;
    let mut bot_audio_wav = None;
    let mut recording_wav = None;
    let mut bot_audio_response = None;
    let mut user_speech = None;
    let mut user_images = false;
    if suite {
        let mut flags = args[2..].iter();
        while let Some(flag) = flags.next() {
            match flag.as_str() {
                "--user-speech" if user_speech.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--user-speech needs a runtime configuration file path".into(),
                            )
                        })?;
                    user_speech = Some(path.as_str());
                }
                "--recorded-audio" if !recorded => recorded = true,
                "--bot-audio-response" if bot_audio_response.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--bot-audio-response needs a configuration file path".into(),
                            )
                        })?;
                    bot_audio_response = Some(path.as_str());
                }
                _ => {
                    return Err(pipecat_evals::EvalError::Scenario(
                        "unknown or duplicate suite flag".into(),
                    ))
                }
            }
        }
    }
    if single {
        let mut flags = args[3..].iter();
        while let Some(flag) = flags.next() {
            match flag.as_str() {
                "--user-images" if !user_images => user_images = true,
                "--user-speech" if user_speech.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--user-speech needs a runtime configuration file path".into(),
                            )
                        })?;
                    user_speech = Some(path.as_str());
                }
                "--stop-bot" if !stop_bot => stop_bot = true,
                "--recorded-audio" if !recorded => recorded = true,
                "--bot-audio-wav" if bot_audio_wav.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--bot-audio-wav needs a file path".into(),
                            )
                        })?;
                    bot_audio_wav = Some(path.as_str());
                }
                "--recording-wav" if recording_wav.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--recording-wav needs a file path".into(),
                            )
                        })?;
                    recording_wav = Some(path.as_str());
                }
                "--bot-audio-response" if bot_audio_response.is_none() => {
                    let path = flags
                        .next()
                        .filter(|path| !path.starts_with("--") && !path.is_empty())
                        .ok_or_else(|| {
                            pipecat_evals::EvalError::Scenario(
                                "--bot-audio-response needs a configuration file path".into(),
                            )
                        })?;
                    bot_audio_response = Some(path.as_str());
                }
                _ => {
                    return Err(pipecat_evals::EvalError::Scenario(
                        "unknown or duplicate run flag".into(),
                    ))
                }
            }
        }
    }

    if !suite && !single {
        return Err(pipecat_evals::EvalError::Scenario(
            "Usage: pipecat-eval run SCENARIOS.yaml ws://HOST:PORT[/PATH] [--stop-bot] [--recorded-audio] [--bot-audio-wav NEW.wav | --bot-audio-response CONFIG.json | --recording-wav NEW.wav] [--user-speech RUNTIME.json | --user-images]\n       pipecat-eval suite NATIVE-MANIFEST.yaml [--recorded-audio] [--bot-audio-response CONFIG.json] [--user-speech RUNTIME.json]\n       --recording-wav requires --recorded-audio or --user-speech and exactly one direct scenario".into()));
    }
    // Reject unsupported recording compositions before loading any scenario,
    // runtime configuration or model, or creating an output temporary.
    if recording_wav.is_some() {
        if bot_audio_response.is_some() {
            return Err(pipecat_evals::EvalError::Unsupported(
                "--recording-wav cannot combine with --bot-audio-response".into(),
            ));
        }
        if bot_audio_wav.is_some() {
            return Err(pipecat_evals::EvalError::Scenario(
                "--recording-wav and --bot-audio-wav are mutually exclusive".into(),
            ));
        }
        if user_images {
            return Err(pipecat_evals::EvalError::Unsupported(
                "--recording-wav cannot combine with --user-images".into(),
            ));
        }
        if !recorded && user_speech.is_none() {
            return Err(pipecat_evals::EvalError::Unsupported(
                "--recording-wav requires --recorded-audio or --user-speech".into(),
            ));
        }
    }
    if user_images
        && (user_speech.is_some()
            || recorded
            || bot_audio_response.is_some()
            || bot_audio_wav.is_some())
    {
        return Err(pipecat_evals::EvalError::Unsupported(
            "--user-images cannot combine with audio modes".into(),
        ));
    }
    #[cfg(not(feature = "user-images"))]
    if user_images {
        return Err(pipecat_evals::EvalError::Unsupported(
            "--user-images requires the user-images build feature".into(),
        ));
    }
    if user_speech.is_some() && (recorded || bot_audio_wav.is_some()) {
        return Err(pipecat_evals::EvalError::Scenario(
            "--user-speech cannot combine with --recorded-audio or --bot-audio-wav".into(),
        ));
    }
    #[cfg(not(feature = "user-speech"))]
    if user_speech.is_some() {
        return Err(pipecat_evals::EvalError::Unsupported(
            "--user-speech requires the user-speech build feature".into(),
        ));
    }
    if bot_audio_response.is_some() && bot_audio_wav.is_some() {
        return Err(pipecat_evals::EvalError::Scenario(
            "--bot-audio-response and --bot-audio-wav are mutually exclusive".into(),
        ));
    }
    #[cfg(not(feature = "bot-audio-transcription"))]
    if bot_audio_response.is_some() {
        return Err(pipecat_evals::EvalError::Unsupported(
            "--bot-audio-response requires the bot-audio-transcription build feature".into(),
        ));
    }
    let cancel = Cancellation::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancel.cancel();
        }
    });
    let result = async {
        // Selection is exclusive: the joint AudioResponse plan is constructed
        // once, before either standalone branch, and can never be overwritten.
        // The signal owner is already installed for owned model preparation.
        let manifest = if suite {
            Some(if let (Some(runtime), Some(response)) = (user_speech, bot_audio_response) {
                #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
                {
                    ReleaseManifest::load_audio_to_audio(
                        &args[1],
                        pipecat_evals::CartesiaUserSpeechSettings::load(runtime)?,
                        pipecat_evals::BotTranscriptionSettings::load(response)?,
                    )?
                }
                #[cfg(not(all(feature = "user-speech", feature = "bot-audio-transcription")))]
                {
                    let _ = (runtime, response);
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "audio-to-audio requires user-speech and bot-audio-transcription build features".into(),
                    ));
                }
            } else if let Some(runtime) = user_speech {
                #[cfg(feature = "user-speech")]
                {
                    ReleaseManifest::load_user_speech(
                        &args[1], pipecat_evals::CartesiaUserSpeechSettings::load(runtime)?,
                    )?
                }
                #[cfg(not(feature = "user-speech"))]
                {
                    let _ = runtime;
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "--user-speech requires the user-speech build feature".into(),
                    ));
                }
            } else if let Some(response) = bot_audio_response {
                #[cfg(feature = "bot-audio-transcription")]
                {
                    ReleaseManifest::load_audio_response(
                        &args[1], pipecat_evals::BotTranscriptionSettings::load(response)?, recorded,
                    )?
                }
                #[cfg(not(feature = "bot-audio-transcription"))]
                {
                    let _ = response;
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "--bot-audio-response requires the bot-audio-transcription build feature".into(),
                    ));
                }
            } else if recorded {
                ReleaseManifest::load_recorded(&args[1])?
            } else {
                ReleaseManifest::load(&args[1])?
            })
        } else {
            None
        };
        let sessions = if single {
            let options = SessionOptions {
                stop_bot,
                ..Default::default()
            };
            if let (Some(runtime), Some(response)) = (user_speech, bot_audio_response) {
                #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
                {
                    let speech = pipecat_evals::CartesiaUserSpeechSettings::load(runtime)?;
                    let transcription = pipecat_evals::BotTranscriptionSettings::load(response)?;
                    pipecat_evals::AudioToAudioScenarioFile::load(&args[1])?
                        .into_sessions_with_cancellation(
                            &args[2], options, speech, transcription, cancel.clone(),
                        )
                        .await?
                }
                #[cfg(not(all(feature = "user-speech", feature = "bot-audio-transcription")))]
                {
                    let _ = (runtime, response);
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "audio-to-audio requires user-speech and bot-audio-transcription build features".into(),
                    ));
                }
            } else if let Some(response) = bot_audio_response {
                #[cfg(feature = "bot-audio-transcription")]
                {
                    pipecat_evals::BotTranscriptionSettings::load(response)?
                        .into_sessions_with_cancellation(
                            &args[1], &args[2], options, recorded, cancel.clone(),
                        )
                        .await?
                }
                #[cfg(not(feature = "bot-audio-transcription"))]
                {
                    let _ = response;
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "--bot-audio-response requires the bot-audio-transcription build feature".into(),
                    ));
                }
            } else if let Some(runtime) = user_speech {
                #[cfg(feature = "user-speech")]
                {
                    let file = pipecat_evals::UserSpeechScenarioFile::load(&args[1])?;
                    if recording_wav.is_some() && file.len() != 1 {
                        return Err(pipecat_evals::EvalError::Scenario(
                            "--recording-wav requires exactly one scenario".into(),
                        ));
                    }
                    if options.stop_bot && file.len() > 1 {
                        return Err(pipecat_evals::EvalError::Scenario("--stop-bot with several scenarios needs independent bot endpoints; use separate runs".into()));
                    }
                    file.into_sessions(
                        &args[2], options,
                        pipecat_evals::CartesiaUserSpeechSettings::load(runtime)?,
                    )?
                }
                #[cfg(not(feature = "user-speech"))]
                {
                    let _ = runtime;
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "--user-speech requires the user-speech build feature".into(),
                    ));
                }
            } else if user_images {
                #[cfg(feature = "user-images")]
                {
                    let file = pipecat_evals::UserImageScenarioFile::load(&args[1])?;
                    if options.stop_bot && file.len() > 1 {
                        return Err(pipecat_evals::EvalError::Scenario(
                            "--stop-bot with several scenarios needs independent endpoints".into(),
                        ));
                    }
                    file.into_sessions(&args[2], options)?
                }
                #[cfg(not(feature = "user-images"))]
                {
                    return Err(pipecat_evals::EvalError::Unsupported(
                        "--user-images requires the user-images build feature".into(),
                    ));
                }
            } else if recorded {
                let file = RecordedScenarioFile::load(&args[1])?;
                if recording_wav.is_some() && file.len() != 1 {
                    return Err(pipecat_evals::EvalError::Scenario(
                        "--recording-wav requires exactly one scenario".into(),
                    ));
                }
                if options.stop_bot && file.len() > 1 {
                    return Err(pipecat_evals::EvalError::Scenario("--stop-bot with several scenarios needs independent bot endpoints; use separate runs".into()));
                }
                file.into_sessions(&args[2], options)?
            } else {
                let file = ScenarioFile::load(&args[1])?;
                if options.stop_bot && file.scenarios.len() > 1 {
                    return Err(pipecat_evals::EvalError::Scenario("--stop-bot with several scenarios needs independent bot endpoints; use run_suite".into()));
                }
                file.scenarios
                    .into_iter()
                    .map(|scenario| EvalSession::new(scenario, &args[2], options.clone()))
                    .collect::<Result<Vec<_>, _>>()?
            }
        } else {
            Vec::new()
        };
        if bot_audio_wav.is_some() && sessions.len() != 1 {
            return Err(pipecat_evals::EvalError::Scenario(
                "--bot-audio-wav requires exactly one scenario".into(),
            ));
        }
        if recording_wav.is_some() {
            if sessions.len() != 1 {
                return Err(pipecat_evals::EvalError::Scenario(
                    "--recording-wav requires exactly one scenario".into(),
                ));
            }
            sessions[0].validate_timeline_recording(TimelineRecordingLimits::default())?;
        }
        let mut wav_output = bot_audio_wav.map(wav_output::Output::prepare).transpose()?;
        let mut recording_output = recording_wav
            .map(wav_output::Output::prepare_recording)
            .transpose()?;
        if let Some(manifest) = manifest {
            let result = run_release(
                manifest,
                SessionOptions::default(),
                ProcessOptions::default(),
                cancel,
            )
            .await?;
            println!("{}", serde_json::to_string(&result)?);
            Ok(result.passed)
        } else {
            let mut passed = true;
            for session in sessions {
                if let Some(output) = recording_output.take() {
                    let run = session
                        .run_with_timeline_recording(cancel.clone(), TimelineRecordingLimits::default())
                        .await?;
                    passed &= run.result.passed;
                    // Rendering and publication can fail independently of the run.
                    println!("{}", serde_json::to_string(&run.result)?);
                    match output.publish_recording(&run) {
                        Ok(Some(bytes)) => eprintln!("timeline recording WAV saved ({bytes} bytes); admitted user source and simulated bot playout, possibly partial"),
                        Ok(None) => eprintln!("no WAV written: empty timeline recording"),
                        Err(error) => return Err(pipecat_evals::EvalError::Scenario(format!("timeline recording WAV artifact: {error}"))),
                    }
                } else if let Some(output) = wav_output.take() {
                    let run = session.run_with_bot_audio(cancel.clone(), BotAudioLimits::default()).await?;
                    passed &= run.result.passed;
                    // Preserve the evaluation result even when artifact publication fails.
                    println!("{}", serde_json::to_string(&run.result)?);
                    match output.publish(&run) {
                        Ok(Some(bytes)) => eprintln!("bot audio WAV saved ({bytes} bytes); received chunks only, possibly partial"),
                        Ok(None) => eprintln!("no WAV written: empty bot audio capture"),
                        Err(error) => return Err(pipecat_evals::EvalError::Scenario(format!("bot audio WAV artifact: {error}"))),
                    }
                } else {
                    let result = session.run(cancel.clone()).await;
                    passed &= result.passed;
                    println!("{}", serde_json::to_string(&result)?);
                }
            }
            Ok(passed)
        }
    }
    .await;
    signal.abort();
    let _ = signal.await;
    result
}
