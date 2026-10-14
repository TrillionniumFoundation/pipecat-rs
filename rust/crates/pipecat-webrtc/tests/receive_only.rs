//! Real receive-only RTP direction and capability, with cleanup before assertions.
use pipecat_core::{AudioRawFrame, Frame, FrameData};
use pipecat_webrtc::{AudioCodec, Error, SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent};
use serde_json::{json, Value};
use std::{
    net::{SocketAddr, UdpSocket},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
type Outcome<T = ()> = Result<T, String>;
macro_rules! get {
    ($v:expr) => {
        $v.map_err(|e| format!("{e:?}"))?
    };
}
macro_rules! check {
    ($v:expr,$m:expr) => {
        if !$v {
            return Err($m.into());
        }
    };
}
fn config(codec: Option<AudioCodec>) -> SmallWebRtcConfig {
    SmallWebRtcConfig {
        audio_codec: codec,
        localhost_only: true,
        ..Default::default()
    }
}
fn pcm(codec: AudioCodec) -> AudioRawFrame {
    let rate = codec.sample_rate();
    let bytes = (0..rate / 50)
        .flat_map(|i| {
            ((i as f64 * std::f64::consts::TAU * 440. / rate as f64)
                .sin()
                .mul_add(9000., 0.) as i16)
                .to_le_bytes()
        })
        .collect();
    AudioRawFrame::new(bytes, rate, 1).unwrap()
}
fn audio_direction(sdp: &str) -> Outcome<String> {
    let mut audio = vec![];
    let mut section = None;
    for line in sdp.lines().map(str::trim) {
        if line.starts_with("m=") {
            if let Some(lines) = section.take() {
                audio.push(lines);
            }
            section = line.starts_with("m=audio ").then(Vec::<String>::new);
        } else if let Some(lines) = section.as_mut() {
            lines.push(line.to_owned());
        }
    }
    if let Some(lines) = section {
        audio.push(lines);
    }
    check!(audio.len() == 1, "expected exactly one audio media section");
    let directions = audio[0]
        .iter()
        .filter(|line| {
            matches!(
                line.as_str(),
                "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    check!(
        directions.len() == 1,
        "audio m-line needs one explicit direction"
    );
    if directions[0] == "a=recvonly" {
        check!(
            !audio[0].iter().any(|line| line.starts_with("a=msid:")
                || line.starts_with("a=ssrc:")
                || line.starts_with("a=ssrc-group:")),
            "receive-only audio describes a local sending track"
        );
    }
    Ok(directions[0].clone())
}
fn addresses(sdp: &str) -> Outcome<Vec<SocketAddr>> {
    sdp.lines()
        .filter(|line| line.starts_with("a=candidate:"))
        .map(|line| {
            let parts = line.split_whitespace().collect::<Vec<_>>();
            if parts.len() < 6 {
                return Err("invalid candidate".into());
            }
            format!("{}:{}", parts[4], parts[5])
                .parse()
                .map_err(|e| format!("{e}"))
        })
        .collect()
}
struct Pair {
    receive: SmallWebRtcConnection,
    send: SmallWebRtcConnection,
    addresses: Vec<SocketAddr>,
}
impl Pair {
    async fn new(codec: AudioCodec) -> Outcome<Self> {
        let receive = get!(SmallWebRtcConnection::new_receive_only(config(Some(codec))).await);
        let send = match SmallWebRtcConnection::new(config(Some(codec))).await {
            Ok(peer) => peer,
            Err(error) => {
                let cleanup = receive.close().await;
                return Err(format!(
                    "second constructor: {error:?}; first cleanup={cleanup:?}"
                ));
            }
        };
        Ok(Self {
            receive,
            send,
            addresses: vec![],
        })
    }
    async fn connect(&mut self) -> Outcome {
        let offer = get!(self.receive.create_offer().await);
        check!(
            audio_direction(&offer.sdp)? == "a=recvonly",
            "receive audio offer direction"
        );
        self.addresses.extend(addresses(&offer.sdp)?);
        let answer = get!(self.send.answer_offer(offer.sdp).await);
        check!(
            audio_direction(&answer.sdp)? == "a=sendonly",
            "sender audio answer direction"
        );
        self.addresses.extend(addresses(&answer.sdp)?);
        get!(self.receive.accept_answer(answer).await);
        let (a, b) = tokio::join!(
            self.receive.wait_data_channel(Duration::from_secs(5)),
            self.send.wait_data_channel(Duration::from_secs(5))
        );
        get!(a);
        get!(b);
        check!(!self.addresses.is_empty(), "real UDP candidates missing");
        Ok(())
    }
    async fn rejected(&self, codec: AudioCodec) -> Outcome {
        check!(
            matches!(
                self.receive.send_audio(pcm(codec)).await,
                Err(Error::AudioDisabled)
            ),
            "async audio admission was not disabled"
        );
        check!(
            matches!(
                self.receive.try_send_audio(pcm(codec)),
                Err(Error::AudioDisabled)
            ),
            "try audio admission was not disabled"
        );
        for data in [
            FrameData::OutputAudio(pcm(codec)),
            FrameData::TtsAudio(pcm(codec)),
            FrameData::SpeechOutputAudio(pcm(codec)),
        ] {
            check!(
                matches!(
                    self.receive.send_frame(&Frame::new(data)).await,
                    Err(Error::AudioDisabled)
                ),
                "frame audio admission was not disabled"
            );
        }
        get!(self.receive.flush(Duration::from_secs(1)).await);
        Ok(())
    }
    async fn exchange(&self, codec: AudioCodec) -> Outcome {
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let (seen_tx, mut seen_rx) = mpsc::channel::<Value>(2);
        let observer = async {
            loop {
                tokio::select! { biased;
                    event=self.send.recv_event()=>match event {
                        Some(TransportEvent::Audio(_))=>return Err("opposite peer received forbidden audio RTP/media".to_string()),
                        Some(TransportEvent::Error(error))=>return Err(error),
                        Some(TransportEvent::AppMessage(value))=>{
                            get!(self.send.send_app_message(&json!({"echo":value})).await);
                            get!(seen_tx.send(value).await);
                        }
                        None=>return Err("opposite peer closed during observation".into()),
                        _=>{}
                    },
                    _=&mut stop_rx=>return Ok::<_,String>(()),
                }
            }
        };
        let producer = async {
            let result=tokio::time::timeout(Duration::from_secs(15),async {
                self.rejected(codec).await?;
                let request=json!({"text":"你好，receive-only 🦀","stage":1});
                get!(self.receive.send_app_message(&request).await);
                check!(seen_rx.recv().await==Some(request.clone()),"upstream application request absent");
                for _ in 0..3 {get!(self.send.send_audio(pcm(codec)).await);}
                get!(self.send.flush(Duration::from_secs(3)).await);
                let(mut echoed,mut packets)=(false,0);
                while !echoed||packets<3 {
                    match self.receive.recv_event().await {
                        Some(TransportEvent::AppMessage(value))=>{check!(value==json!({"echo":request}),"downlink echo differs");echoed=true;}
                        Some(TransportEvent::Audio(audio))=>{
                            check!(audio.codec==codec&&audio.frame.sample_rate==codec.sample_rate(),"wrong encoded downlink codec/rate");
                            check!(audio.frame.audio.as_chunks::<2>().0.iter().any(|s|i16::from_le_bytes(*s).abs()>100),"silent downlink");packets+=1;
                        }
                        Some(TransportEvent::Error(error))=>return Err(error),
                        None=>return Err("receive-only peer closed during media".into()),
                        _=>{}
                    }
                }
                get!(self.send.send_app_message(&json!({"type":"signalling","message":{"type":"trackStatus","receiver_index":0,"enabled":true}})).await);
                loop {
                    match self.receive.recv_event().await {
                        Some(TransportEvent::TrackStatus{receiver_index:0,enabled:true})=>break,
                        Some(TransportEvent::Error(error))=>return Err(error),
                        None=>return Err("closed before trackStatus".into()),
                        _=>{}
                    }
                }
                self.rejected(codec).await?;
                let after=json!({"text":"still no audio uplink","stage":2});get!(self.receive.send_app_message(&after).await);
                check!(seen_rx.recv().await==Some(after.clone()),"application control after enable missing");
                loop {
                    match self.receive.recv_event().await {
                        Some(TransportEvent::AppMessage(value))=>{check!(value==json!({"echo":after}),"second echo differs");break;}
                        Some(TransportEvent::Error(error))=>return Err(error),
                        None=>return Err("closed before second echo".into()),
                        _=>{}
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                let a=self.receive.stats();let b=self.send.stats();eprintln!("receive-only codec={codec:?}; client transport counters={a:?}; opposite transport counters={b:?}");
                check!(a.audio_packets_sent==0&&b.audio_packets_received==0,"audio uplink counters nonzero");
                check!(a.audio_packets_received>=3&&b.audio_packets_sent>=3&&a.app_messages_sent>=2&&b.app_messages_received>=2,"positive traffic counters missing");
                get!(self.receive.send_frame(&Frame::interruption()).await);
                get!(self.receive.flush(Duration::from_secs(1)).await);Ok::<(),String>(())
            }).await;
            let _ = stop_tx.send(());
            result.map_err(|_| "receive-only exchange deadline".to_string())?
        };
        let (a, b) = tokio::join!(producer, observer);
        eprintln!("receive-only producer={a:?}; opposite observation={b:?}");
        a?;
        b?;
        Ok(())
    }
    async fn close(self, receive_first: bool) -> Vec<String> {
        let mut errors = vec![];
        let first = if receive_first {
            &self.receive
        } else {
            &self.send
        };
        let second = if receive_first {
            &self.send
        } else {
            &self.receive
        };
        for (label, peer) in [("first", first), ("second", second)] {
            let closed = tokio::time::timeout(Duration::from_secs(8), peer.close()).await;
            eprintln!("receive-only {label} close={closed:?}");
            if !matches!(closed, Ok(Ok(()))) {
                errors.push(format!("{label} close unverified: {closed:?}"));
            }
        }
        if !matches!(
            self.receive.send_audio(pcm(AudioCodec::Pcmu)).await,
            Err(Error::Closed)
        ) {
            errors.push("closed send not rejected".into());
        }
        let released = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.addresses.iter().all(|a| UdpSocket::bind(a).is_ok()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if released.is_err() {
            errors.push("native candidate sockets not released".into());
        }
        errors
    }
}
async fn run(codec: AudioCodec, receive_first: bool) {
    let mut pair = Pair::new(codec).await.expect("owned peer constructors");
    let observation = tokio::time::timeout(Duration::from_secs(25), async {
        pair.connect().await?;
        pair.exchange(codec).await?;
        if receive_first {
            check!(
                get!(pair.receive.send_frame(&Frame::end()).await),
                "normal End was not handled"
            );
        }
        Ok::<(), String>(())
    })
    .await;
    let cleanup = pair.close(receive_first).await;
    eprintln!("receive-only observation={observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_empty(), "cleanup failed: {cleanup:?}");
    assert!(
        matches!(observation, Ok(Ok(()))),
        "observation failed: {observation:?}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pcmu_receive_only_has_no_audio_uplink_and_peer_first_close() {
    run(AudioCodec::Pcmu, false).await;
}
#[cfg(feature = "opus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opus_receive_only_has_no_audio_uplink_and_client_first_close() {
    run(AudioCodec::Opus, true).await;
}
#[tokio::test]
async fn receive_only_requires_an_audio_codec_before_peer_setup() {
    let result = SmallWebRtcConnection::new_receive_only(config(None)).await;
    let (valid, cleanup) = match result {
        Err(Error::InvalidConfig("receive-only audio requires an audio codec")) => (true, None),
        Err(error) => {
            eprintln!("wrong constructor error: {error:?}");
            (false, None)
        }
        Ok(peer) => (false, Some(peer.close().await)),
    };
    eprintln!("receive-only invalid configuration exact={valid}; cleanup={cleanup:?}");
    assert!(valid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_only_end_and_concurrent_close_join_while_wrapper_is_retained() {
    let mut pair = Pair::new(AudioCodec::Pcmu)
        .await
        .expect("owned constructors");
    let observation = tokio::time::timeout(Duration::from_secs(15), async {
        pair.connect().await?;
        let end_frame = Frame::end();
        let (end, a, b) = tokio::join!(
            pair.receive.send_frame(&end_frame),
            pair.receive.close(),
            pair.receive.close()
        );
        eprintln!("receive-only concurrent End={end:?}; closes={a:?}/{b:?}");
        // End may lose admission to a concurrent close; both close callers must join.
        check!(
            matches!(end, Ok(true) | Err(Error::Closed)),
            "unexpected concurrent End error"
        );
        get!(a);
        get!(b);
        check!(
            pair.receive.state() == pipecat_webrtc::ConnectionState::Closed,
            "retained wrapper not Closed"
        );
        check!(
            matches!(pair.receive.create_offer().await, Err(Error::Closed)),
            "closed negotiation accepted"
        );
        Ok::<(), String>(())
    })
    .await;
    let cleanup = pair.close(true).await;
    eprintln!("receive-only concurrent observation={observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_empty(), "{cleanup:?}");
    assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
}
