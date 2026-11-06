//! Joined-room media qualifications; no physical capture or playback is involved.
//!
//! Every fallible scenario returns before asserting, so failure also awaits the
//! established participants' owners and stops/reaps the fixture server. The only
//! application event consumer for each receiver is the scenario below.
use super::*;
use pipecat_core::{ImageRawFrame, OutputImageRawFrame};
use pipecat_livekit::{VideoCaptureOutcome, VideoInputConfig, VideoOutputConfig, VideoSource};
use serde_json::{json, Value};
use std::{
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::time::{Instant, MissedTickBehavior};

type Check<T = ()> = std::result::Result<T, String>;
const VIDEO_SIZE: (u32, u32) = (320, 240);
const DRAIN: Duration = Duration::from_millis(500);
const QUIET: Duration = Duration::from_secs(1);
const COLORS: [[u8; 3]; 3] = [[224, 32, 32], [32, 224, 32], [32, 32, 224]];

fn require(condition: bool, message: impl Into<String>) -> Check {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

struct Evidence {
    file: File,
    started: Instant,
}
impl Evidence {
    fn new(server: &Server) -> Self {
        Self {
            file: File::create(server.directory.join("media-events.jsonl"))
                .expect("open fixture evidence before joining any participant"),
            started: Instant::now(),
        }
    }
    fn record(&mut self, stage: &str, details: Value) -> Check {
        writeln!(
            self.file,
            "{}",
            json!({"elapsed_ms": self.started.elapsed().as_millis(), "stage": stage, "details": details})
        )
        .and_then(|()| self.file.flush())
        .map_err(|error| format!("write {stage} evidence: {error}"))
    }
}

fn config() -> LiveKitConfig {
    LiveKitConfig {
        input_format: PcmFormat::new(16000, 1).expect("fixed valid input format"),
        output_format: None,
        queue_capacity: 2048,
        connect_timeout: DEADLINE,
        operation_timeout: DEADLINE,
        close_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}

async fn join(
    server: &Server,
    room: &str,
    identity: &str,
    config: LiveKitConfig,
    peers: &mut Vec<LiveKitConnection>,
    evidence: &mut Evidence,
) -> Check {
    let peer = LiveKitConnection::connect(
        &server.url(),
        &token(identity, room),
        config,
        CancellationToken::new(),
    )
    .await
    .map_err(|error| format!("join {identity}: {error}"))?;
    // Store ownership before checking any post-join property.
    peers.push(peer);
    let peer = &peers[peers.len() - 1];
    require(
        peer.state() == ConnectionState::Connected
            && peer.identity() == identity
            && peer.room_name() == room,
        format!("joined participant properties for {identity}"),
    )?;
    evidence.record("joined", json!({"identity": identity, "room": room}))
}

fn checked_event(event: Option<LiveKitEvent>, receiver: &str) -> Check<LiveKitEvent> {
    match event {
        None => Err(format!("{receiver} event stream ended")),
        Some(LiveKitEvent::Error(error)) => Err(format!("{receiver} adapter error: {error}")),
        Some(LiveKitEvent::Disconnected { reason }) => {
            Err(format!("{receiver} disconnected: {reason}"))
        }
        Some(LiveKitEvent::Reconnecting | LiveKitEvent::Reconnected) => Err(format!(
            "unexpected {receiver} reconnection during media qualification"
        )),
        Some(event) => Ok(event),
    }
}

/// Always run, including after timeout or failed content checks. Keep `server`
/// alive until the final assertion so its Drop retains logs during unwinding.
async fn finish(
    server: &mut Server,
    client: &loopback_ws::LoopbackClient,
    peers: &mut Vec<LiveKitConnection>,
    evidence: &mut Evidence,
    result: Check,
) {
    let mut failures: Vec<String> = result.err().into_iter().collect();
    for peer in peers.iter() {
        let identity = peer.identity();
        let result = match timeout(DEADLINE, peer.close()).await {
            Ok(result) => result.map_err(|error| format!("close {identity}: {error}")),
            Err(_) => Err(format!("close {identity} exceeded {DEADLINE:?}")),
        };
        if let Err(error) = result {
            failures.push(error);
        }
        if let Err(error) = evidence.record(
            "participant_close",
            json!({"identity": identity, "state": format!("{:?}", peer.state())}),
        ) {
            failures.push(error);
        }
    }
    peers.clear();
    if timeout(Duration::from_secs(3), async {
        while client.live_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_err()
    {
        failures.push(format!(
            "{} signaling socket references remain after close",
            client.live_connections()
        ));
    }
    if let Err(error) = server.child.kill() {
        failures.push(format!("stop owned SFU: {error}"));
    }
    if let Err(error) = server.child.wait() {
        failures.push(format!("reap owned SFU: {error}"));
    }
    match timeout(
        Duration::from_secs(1),
        tokio::net::TcpStream::connect(("127.0.0.1", server.port)),
    )
    .await
    {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => failures.push("owned SFU signaling port remains open".into()),
        Err(_) => failures.push("owned SFU signaling-port closure check timed out".into()),
    }
    if let Err(error) = evidence.record("finished", json!({"failures": failures})) {
        failures.push(error);
    }
    assert!(
        failures.is_empty(),
        "{}\noriginal SFU and media evidence: {}",
        failures.join("\n"),
        server.directory.display()
    );
}

fn pattern(rotation: usize) -> OutputImageRawFrame {
    let (width, height) = VIDEO_SIZE;
    let mut image = Vec::with_capacity((width * height * 3) as usize);
    for _ in 0..height {
        for x in 0..width {
            image.extend_from_slice(&COLORS[((x * 3 / width) as usize + rotation) % 3]);
        }
    }
    OutputImageRawFrame {
        image: ImageRawFrame {
            image,
            size: VIDEO_SIZE,
            format: Some("RGB".into()),
        },
        sync_with_audio: false,
    }
}

/// Interior patches avoid I420 chroma boundaries. With this small single-layer
/// fixture we require full 320x240 decode. Mean error <=30 per channel and >=95%
/// dominant-color pixels tolerate lossy VP8, while rejecting black/gray, channel
/// swaps and either stale layout. No pixel-exact or device-quality claim is made.
fn color_stats(image: &ImageRawFrame, rotation: usize) -> Check<(bool, Value)> {
    require(
        image.size == VIDEO_SIZE,
        format!("decoded size {:?}", image.size),
    )?;
    require(image.format.as_deref() == Some("RGB"), "decoded RGB format")?;
    let (width, height) = image.size;
    require(
        image.image.len() == (width * height * 3) as usize,
        "decoded RGB payload length",
    )?;
    let mut matches = true;
    let mut regions = Vec::new();
    for band in 0..3 {
        let dominant = (band + rotation) % 3;
        let expected = COLORS[dominant];
        let x0 = band as u32 * width / 3 + width / 12;
        let x1 = (band as u32 + 1) * width / 3 - width / 12;
        let mut sums = [0u64; 3];
        let mut pixels = 0u64;
        let mut dominant_pixels = 0u64;
        for y in height / 4..height * 3 / 4 {
            for x in x0..x1 {
                let offset = ((y * width + x) * 3) as usize;
                let pixel = &image.image[offset..offset + 3];
                for (sum, value) in sums.iter_mut().zip(pixel) {
                    *sum += u64::from(*value);
                }
                if pixel[dominant] >= 160
                    && (0..3)
                        .filter(|index| *index != dominant)
                        .all(|index| u16::from(pixel[dominant]) >= u16::from(pixel[index]) + 80)
                {
                    dominant_pixels += 1;
                }
                pixels += 1;
            }
        }
        let mean = sums.map(|sum| sum as f64 / pixels as f64);
        let fraction = dominant_pixels as f64 / pixels as f64;
        matches &= mean
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (*actual - f64::from(expected)).abs() <= 30.0)
            && fraction >= 0.95;
        regions.push(json!({"mean_rgb": mean, "dominant_fraction": fraction}));
    }
    Ok((
        matches,
        json!({"rotation": rotation, "size": image.size, "regions": regions}),
    ))
}

async fn video_phase(
    sender: &LiveKitConnection,
    receiver: &LiveKitConnection,
    rotation: usize,
    track_sid: &mut Option<String>,
    evidence: &mut Evidence,
) -> Check {
    let output = pattern(rotation);
    let identity = sender.identity();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = tokio::time::sleep(DEADLINE);
    tokio::pin!(deadline);
    let mut forwarded = 0u64;
    let mut adaptation_dropped = 0u64;
    let mut decoded = 0u64;
    let mut matching = 0u64;
    let mut last_colors = Value::Null;
    evidence.record(
        "video_pattern_started",
        json!({"rotation": rotation, "size": VIDEO_SIZE}),
    )?;
    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => {
                let details = json!({"rotation": rotation, "forwarded": forwarded,
                    "adaptation_dropped": adaptation_dropped, "decoded": decoded,
                    "matching": matching, "last_colors": last_colors,
                    "input_stats": format!("{:?}", receiver.video_input_stats())});
                evidence.record("video_deadline", details.clone())?;
                return Err(format!("remote decoded video deadline: {details}"));
            }
            _ = tick.tick() => {
                match sender.publish_video(&output).map_err(|error| format!("capture video: {error}"))? {
                    VideoCaptureOutcome::ForwardedToSource => forwarded += 1,
                    VideoCaptureOutcome::AdaptationDropped => adaptation_dropped += 1,
                }
            }
            event = receiver.recv() => {
                let event = checked_event(event, "video receiver")?;
                match &event {
                    LiveKitEvent::VideoSubscribed { identity: from, track_sid: sid, source } => {
                        require(from == &identity && *source == VideoSource::Camera && !sid.is_empty(),
                            "video subscription identity/source/SID")?;
                        if let Some(previous) = track_sid.as_ref() {
                            require(previous == sid, "video publication unexpectedly replaced")?;
                        }
                        *track_sid = Some(sid.clone());
                        evidence.record("video_subscribed", json!({"identity": from, "track_sid": sid, "source": source.as_str()}))?;
                    }
                    LiveKitEvent::VideoUnsubscribed { .. } => return Err("video unsubscribed during pattern delivery".into()),
                    LiveKitEvent::Video { identity: from, track_sid: sid, source, image } => {
                        require(from == &identity && *source == VideoSource::Camera && !sid.is_empty(),
                            "decoded video identity/source/SID")?;
                        // Control and pixel lanes have no total order. A pixel
                        // before VideoSubscribed is observed but cannot qualify.
                        let subscribed = track_sid.as_deref() == Some(sid.as_str());
                        if track_sid.is_some() {
                            require(subscribed, "decoded video SID differs from subscription")?;
                        }
                        decoded += 1;
                        let (correct, colors) = color_stats(image, rotation)?;
                        last_colors = colors;
                        let sid = sid.clone();
                        let frame = event.into_frame().map_err(|error| error.to_string())?
                            .ok_or("decoded video did not project to a frame")?;
                        require(frame.transport_source.as_deref() == Some("camera")
                            && frame.metadata.get("livekit.track_sid").and_then(Value::as_str) == Some(sid.as_str()),
                            "decoded video pipeline source/track metadata")?;
                        require(matches!(&frame.data, FrameData::UserImage(image)
                            if image.user_id == identity && image.image.size == VIDEO_SIZE
                                && image.text.is_none() && image.request.is_none()
                                && image.append_to_context.is_none() && image.run_llm.is_none()),
                            "decoded video UserImage identity and plain-image fields")?;
                        if correct && subscribed {
                            matching += 1;
                            if matching == 3 {
                                require(forwarded > 0, "no pattern forwarded to native source")?;
                                evidence.record("remote_pattern_decoded", json!({"identity": identity,
                                    "track_sid": sid, "forwarded": forwarded, "adaptation_dropped": adaptation_dropped,
                                    "decoded": decoded, "matching": matching, "colors": last_colors}))?;
                                return Ok(());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned LiveKit server, native codecs and permitted local interface enumeration; run --test-threads=1"]
async fn native_remote_video_decodes_two_rgb_patterns_with_identity_and_source() {
    let mut server = Server::start().await;
    let client = Arc::new(loopback_ws::LoopbackClient::default());
    livekit_net::set_ws_client(client.clone());
    let mut evidence = Evidence::new(&server);
    let mut peers = Vec::new();
    let result = timeout(Duration::from_secs(60), async {
        let room = format!("pipecat-video-{}", now());
        let mut sender = config();
        sender.video_output = Some(VideoOutputConfig {
            width: VIDEO_SIZE.0,
            height: VIDEO_SIZE.1,
            ..Default::default()
        });
        join(
            &server,
            &room,
            "native-video-source",
            sender,
            &mut peers,
            &mut evidence,
        )
        .await?;
        let mut receiver = config();
        receiver.video_input = Some(VideoInputConfig::default());
        receiver.participant_filter = vec!["native-video-source".into()];
        join(
            &server,
            &room,
            "native-video-receiver",
            receiver,
            &mut peers,
            &mut evidence,
        )
        .await?;
        let mut sid = None;
        video_phase(&peers[0], &peers[1], 0, &mut sid, &mut evidence).await?;
        // Rotation one is never captured before the first pattern qualifies.
        // Three newly decoded matches cannot be satisfied by replaying pattern zero.
        video_phase(&peers[0], &peers[1], 1, &mut sid, &mut evidence).await
    })
    .await
    .map_err(|_| "complete video scenario exceeded 60s".to_owned())
    .and_then(|result| result);
    finish(&mut server, &client, &mut peers, &mut evidence, result).await;
}

#[derive(Default)]
struct AudioProbe {
    sid: Option<String>,
    subscriptions: u64,
    unsubscriptions: u64,
    subscribed: bool,
    frames: u64,
    nonzero: u64,
    samples: u64,
    last_nonzero: Option<Instant>,
}
impl AudioProbe {
    fn counts(&self) -> Value {
        json!({"track_sid": self.sid, "subscriptions": self.subscriptions,
            "unsubscriptions": self.unsubscriptions, "subscribed": self.subscribed,
            "frames": self.frames, "nonzero_frames": self.nonzero, "samples": self.samples})
    }
    fn observe(&mut self, event: LiveKitEvent, label: &str, evidence: &mut Evidence) -> Check {
        match event {
            LiveKitEvent::AudioSubscribed {
                identity,
                track_sid,
            } => {
                require(
                    identity == "native-tone-source" && !track_sid.is_empty(),
                    "audio subscription identity/SID",
                )?;
                require(
                    self.sid.as_ref().is_none_or(|sid| sid == &track_sid),
                    "resubscription must use the same current publication SID",
                )?;
                self.sid = Some(track_sid);
                self.subscribed = true;
                self.subscriptions += 1;
                evidence.record(
                    "audio_subscribed",
                    json!({"receiver": label, "counts": self.counts()}),
                )?;
            }
            LiveKitEvent::AudioUnsubscribed {
                identity,
                track_sid,
            } => {
                require(
                    identity == "native-tone-source"
                        && self.sid.as_deref() == Some(track_sid.as_str()),
                    "audio unsubscribe identity/SID",
                )?;
                self.subscribed = false;
                self.unsubscriptions += 1;
                evidence.record(
                    "audio_unsubscribed",
                    json!({"receiver": label, "counts": self.counts()}),
                )?;
            }
            LiveKitEvent::Audio {
                identity,
                track_sid,
                audio,
            } => {
                require(
                    identity == "native-tone-source"
                        && self.sid.as_deref() == Some(track_sid.as_str()),
                    "received PCM identity/SID",
                )?;
                audio.validate().map_err(|error| error.to_string())?;
                require(
                    audio.sample_rate == 16000
                        && audio.num_channels == 1
                        && !audio.audio.is_empty(),
                    "received 16kHz mono PCM",
                )?;
                let samples = audio.audio.as_chunks::<2>().0;
                let nonzero = samples
                    .iter()
                    .any(|sample| i16::from_le_bytes(*sample).unsigned_abs() > 100);
                self.frames += 1;
                self.samples += samples.len() as u64;
                if nonzero {
                    self.nonzero += 1;
                    self.last_nonzero = Some(Instant::now());
                }
                let expected_sid = track_sid.clone();
                let frame = LiveKitEvent::Audio {
                    identity,
                    track_sid,
                    audio,
                }
                .into_frame()
                .map_err(|error| error.to_string())?
                .ok_or("PCM did not project to a frame")?;
                require(
                    frame.transport_source.as_deref() == Some("native-tone-source")
                        && frame
                            .metadata
                            .get("livekit.track_sid")
                            .and_then(Value::as_str)
                            == Some(expected_sid.as_str())
                        && matches!(&frame.data, FrameData::UserAudio(audio) if audio.user_id == "native-tone-source"),
                    "PCM pipeline identity/track metadata",
                )?;
            }
            _ => {}
        }
        Ok(())
    }
}

async fn observe_audio(
    receiver: &LiveKitConnection,
    witness: &LiveKitConnection,
    probes: &mut [AudioProbe; 2],
    evidence: &mut Evidence,
) -> Check {
    tokio::select! {
        event = receiver.recv() => probes[0].observe(checked_event(event, "controlled receiver")?, "controlled", evidence),
        event = witness.recv() => probes[1].observe(checked_event(event, "witness receiver")?, "witness", evidence),
    }
}

async fn audio_until(
    receiver: &LiveKitConnection,
    witness: &LiveKitConnection,
    probes: &mut [AudioProbe; 2],
    evidence: &mut Evidence,
    stage: &str,
    complete: impl Fn(&[AudioProbe; 2]) -> bool,
) -> Check {
    timeout(DEADLINE, async {
        while !complete(probes) {
            observe_audio(receiver, witness, probes, evidence).await?;
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| {
        format!(
            "{stage} deadline; controlled={}, witness={}",
            probes[0].counts(),
            probes[1].counts()
        )
    })??;
    evidence.record(
        stage,
        json!({"controlled": probes[0].counts(), "witness": probes[1].counts()}),
    )
}

async fn audio_window(
    receiver: &LiveKitConnection,
    witness: &LiveKitConnection,
    probes: &mut [AudioProbe; 2],
    evidence: &mut Evidence,
    duration: Duration,
    stage: &str,
) -> Check {
    let before = [probes[0].counts(), probes[1].counts()];
    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => break,
            result = observe_audio(receiver, witness, probes, evidence) => result?,
        }
    }
    evidence.record(
        stage,
        json!({"duration_ms": duration.as_millis(), "before": before,
        "after": [probes[0].counts(), probes[1].counts()]}),
    )
}

async fn set_audio_subscription(
    receiver: &LiveKitConnection,
    witness: &LiveKitConnection,
    probes: &mut [AudioProbe; 2],
    evidence: &mut Evidence,
    subscribed: bool,
) -> Check {
    evidence.record("owner_command_started", json!({"subscribed": subscribed}))?;
    let command = async {
        if subscribed {
            receiver.unmute_participant("native-tone-source").await
        } else {
            receiver.mute_participant("native-tone-source").await
        }
    };
    tokio::pin!(command);
    let deadline = tokio::time::sleep(DEADLINE);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            result = &mut command => {
                result.map_err(|error| format!("owner subscription setter ({subscribed}): {error}"))?;
                return evidence.record("owner_command_completed", json!({"subscribed": subscribed,
                    "controlled": probes[0].counts(), "witness": probes[1].counts(),
                    "meaning": "local setter completion only"}));
            }
            result = observe_audio(receiver, witness, probes, evidence) => result?,
            _ = &mut deadline => return Err(format!("owner subscription setter ({subscribed}) deadline")),
        }
    }
}

async fn tone(sender: &LiveKitConnection, accepted: &AtomicU64) -> Check {
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut offset = 0u64;
    loop {
        tick.tick().await;
        let samples = (offset..offset + 240)
            .flat_map(|sample| {
                let value = (8_000.0
                    * (sample as f64 * 440.0 * std::f64::consts::TAU / 24_000.0).sin())
                    as i16;
                value.to_le_bytes()
            })
            .collect::<Vec<_>>();
        let frame = AudioRawFrame::new(samples, 24000, 1).map_err(|error| error.to_string())?;
        let receipt = timeout(DEADLINE, sender.publish_audio(&frame))
            .await
            .map_err(|_| "continuous tone source admission deadline".to_owned())?
            .map_err(|error| format!("continuous tone source admission: {error}"))?;
        require(
            receipt.samples_per_channel == 240,
            "10ms PCM source admission sample count",
        )?;
        accepted.fetch_add(1, Ordering::Relaxed);
        offset += 240;
    }
}

async fn participant_controls(
    receiver: &LiveKitConnection,
    witness: &LiveKitConnection,
    accepted: &AtomicU64,
    evidence: &mut Evidence,
) -> Check {
    let mut probes = [AudioProbe::default(), AudioProbe::default()];
    audio_until(
        receiver,
        witness,
        &mut probes,
        evidence,
        "baseline_pcm_received",
        |probes| {
            probes
                .iter()
                .all(|probe| probe.subscribed && probe.nonzero >= 10)
        },
    )
    .await?;
    require(
        probes
            .iter()
            .all(|probe| probe.subscriptions == 1 && probe.unsubscriptions == 0),
        "one initial audio subscription per receiver",
    )?;
    set_audio_subscription(receiver, witness, &mut probes, evidence, false).await?;
    audio_until(
        receiver,
        witness,
        &mut probes,
        evidence,
        "unsubscribe_observed",
        |probes| probes[0].unsubscriptions == 1 && !probes[0].subscribed,
    )
    .await?;
    // A fixed allowance for already queued PCM, not a retry-until-silent loop.
    audio_window(
        receiver,
        witness,
        &mut probes,
        evidence,
        DRAIN,
        "post_unsubscribe_drain",
    )
    .await?;
    let controlled_before = probes[0].frames;
    let witness_before = probes[1].nonzero;
    let accepted_before = accepted.load(Ordering::Relaxed);
    let quiet_started = Instant::now();
    audio_window(
        receiver,
        witness,
        &mut probes,
        evidence,
        QUIET,
        "muted_observation_window",
    )
    .await?;
    let accepted_during = accepted.load(Ordering::Relaxed) - accepted_before;
    evidence.record("muted_media_check", json!({"controlled_frames": probes[0].frames - controlled_before,
        "witness_nonzero_frames": probes[1].nonzero - witness_before, "source_accepted_chunks": accepted_during}))?;
    require(probes[0].frames == controlled_before && !probes[0].subscribed && probes[0].subscriptions == 1,
        "controlled receiver must receive zero PCM frames during the fixed 1s muted window after its 500ms drain")?;
    require(
        probes[1].nonzero - witness_before >= 20
            && accepted_during >= 20
            && probes[1]
                .last_nonzero
                .is_some_and(|last| last >= quiet_started + Duration::from_millis(500)),
        "witness must keep decoding tone while source keeps accepting PCM during the muted window",
    )?;
    let resume_before = probes[0].nonzero;
    set_audio_subscription(receiver, witness, &mut probes, evidence, true).await?;
    audio_until(
        receiver,
        witness,
        &mut probes,
        evidence,
        "resubscribe_and_fresh_pcm_observed",
        |probes| {
            probes[0].subscriptions == 2
                && probes[0].subscribed
                && probes[0].nonzero >= resume_before + 10
        },
    )
    .await?;
    require(
        probes[0].unsubscriptions == 1
            && probes[1].subscriptions == 1
            && probes[1].unsubscriptions == 0,
        "one controlled unsubscribe/resubscribe without changing the witness subscription",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned LiveKit server, native codecs and permitted local interface enumeration; run --test-threads=1"]
async fn native_participant_mute_unmute_observes_subscription_and_remote_pcm() {
    let mut server = Server::start().await;
    let client = Arc::new(loopback_ws::LoopbackClient::default());
    livekit_net::set_ws_client(client.clone());
    let mut evidence = Evidence::new(&server);
    let mut peers = Vec::new();
    let result = timeout(Duration::from_secs(60), async {
        let room = format!("pipecat-controls-{}", now());
        let mut sender = config();
        sender.output_format = Some(PcmFormat::new(24000, 1).map_err(|error| error.to_string())?);
        join(
            &server,
            &room,
            "native-tone-source",
            sender,
            &mut peers,
            &mut evidence,
        )
        .await?;
        for identity in ["native-controlled-receiver", "native-witness-receiver"] {
            let mut receiver = config();
            receiver.participant_filter = vec!["native-tone-source".into()];
            join(
                &server,
                &room,
                identity,
                receiver,
                &mut peers,
                &mut evidence,
            )
            .await?;
        }
        let accepted = AtomicU64::new(0);
        // Scoped futures: completion/failure cancels the pump, then finish()
        // clears native source buffers and joins the established room owners.
        tokio::select! {
            result = tone(&peers[0], &accepted) => result,
            result = participant_controls(&peers[1], &peers[2], &accepted, &mut evidence) => result,
        }
    })
    .await
    .map_err(|_| "complete audio-control scenario exceeded 60s".to_owned())
    .and_then(|result| result);
    finish(&mut server, &client, &mut peers, &mut evidence, result).await;
}
