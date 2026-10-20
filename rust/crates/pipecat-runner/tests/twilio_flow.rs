//! Actual Twilio HTTP/WebSocket application tests with synthetic, literal-loopback
//! peers only. No environment lookup, public network, real account, or phone call.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use pipecat_runner::{
    twilio::{CallSummary, HangupPolicy, Termination, TwilioApp, TwilioConfig, TwilioError},
    ProviderConfig,
};
use pipecat_services::{Content, Role};
use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession};
use serde_json::{json, Value};
use sha1::Sha1;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, oneshot},
    task::JoinSet,
};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest, handshake::client::Request, Error as WsError,
};

const ACCOUNT: &str = "AC00000000000000000000000000000001";
const SECRET: &str = "synthetic-twilio-secret-not-an-account";
const PROVIDER_KEY: &str = "synthetic-only";
const VOICE_ONE: &str = "first caller voice";
const VOICE_TWO: &str = "second caller voice";
const REPLY_ONE: &str = "First synthetic reply.";
const REPLY_DTMF: &str = "Synthetic keypad reply.";
const REPLY_TWO: &str = "Fresh second reply.";
const IO_BOUND: Duration = Duration::from_secs(3);

fn sid(prefix: &str, index: usize) -> String {
    format!("{prefix}{index:032x}")
}

async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn session_config() -> SessionConfig {
    SessionConfig {
        connect_timeout: IO_BOUND,
        write_timeout: IO_BOUND,
        close_timeout: IO_BOUND,
        ..Default::default()
    }
}

struct Peers {
    stt: TcpListener,
    llm: TcpListener,
    tts: TcpListener,
    hangup: TcpListener,
}
impl Peers {
    async fn new() -> Self {
        Self {
            stt: listener().await,
            llm: listener().await,
            tts: listener().await,
            hangup: listener().await,
        }
    }

    fn providers(&self) -> ProviderConfig {
        ProviderConfig {
            deepgram_key: PROVIDER_KEY.into(),
            openai_key: PROVIDER_KEY.into(),
            cartesia_key: PROVIDER_KEY.into(),
            deepgram_endpoint: format!("ws://{}/v1/listen", self.stt.local_addr().unwrap()),
            openai_endpoint: format!(
                "http://{}/v1/chat/completions",
                self.llm.local_addr().unwrap()
            ),
            cartesia_endpoint: format!("ws://{}/tts/websocket", self.tts.local_addr().unwrap()),
            model: "synthetic-model".into(),
            voice_id: "synthetic-voice".into(),
            system_prompt: "Synthetic calls only.".into(),
            input_rate: 16_000,
            output_rate: 24_000,
        }
    }

    async fn app(&self, configure: impl FnOnce(&mut TwilioConfig)) -> Host {
        let host = listener().await;
        let address = host.local_addr().unwrap();
        let webhook = format!("http://{address}/twilio/voice");
        let media = format!("ws://{address}/twilio/media");
        let mut config = TwilioConfig::new(
            ACCOUNT.into(),
            SECRET.into(),
            webhook.clone(),
            media.clone(),
        );
        config.introduce = false;
        config.api_base = format!("http://{}/2010-04-01", self.hangup.local_addr().unwrap());
        config.connect_timeout = IO_BOUND;
        config.io_timeout = IO_BOUND;
        config.idle_timeout = Duration::from_secs(10);
        config.session_timeout = Duration::from_secs(20);
        config.turn_timeout = Duration::from_millis(25);
        configure(&mut config);
        let app = TwilioApp::new(host, config, self.providers()).unwrap();
        let reports = app.subscribe();
        Host {
            app,
            reports,
            webhook,
            media,
            address,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(IO_BOUND)
                .build()
                .unwrap(),
        }
    }

    async fn assert_untouched(&self) {
        for endpoint in [&self.stt, &self.llm, &self.tts, &self.hangup] {
            assert_no_connection(endpoint).await;
        }
    }
}

struct Host {
    app: TwilioApp,
    reports: broadcast::Receiver<CallSummary>,
    webhook: String,
    media: String,
    address: SocketAddr,
    http: reqwest::Client,
}
impl Host {
    fn post(&self, fields: &BTreeMap<String, String>) -> reqwest::RequestBuilder {
        self.http
            .post(&self.webhook)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form(fields))
    }

    async fn admit(&self, call: &str) -> String {
        let fields = fields(call);
        let response = self
            .post(&fields)
            .header("X-Twilio-Signature", signature(&self.webhook, &fields))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/xml"));
        let twiml = response.text().await.unwrap();
        // The real returned capability is used below, not a guessed route/token.
        let url = twiml
            .split_once("<Connect><Stream url=\"")
            .expect("TwiML must connect a media stream")
            .1
            .split_once('"')
            .unwrap()
            .0
            .to_owned();
        assert!(twiml.ends_with(" /></Connect></Response>"));
        assert_eq!(url.rsplit_once('/').unwrap().0, self.media);
        assert_eq!(url.rsplit_once('/').unwrap().1.len(), 64);
        assert_eq!(url::Url::parse(&url).unwrap().host_str(), Some("127.0.0.1"));
        url
    }

    async fn report(&mut self) -> CallSummary {
        tokio::time::timeout(IO_BOUND * 2, self.reports.recv())
            .await
            .expect("application did not publish joined call summary")
            .unwrap()
    }

    async fn shutdown(&self) {
        let report = tokio::time::timeout(IO_BOUND * 2, self.app.shutdown())
            .await
            .expect("application shutdown must join owned work")
            .unwrap();
        assert_eq!(report.task_errors, 0);
        assert_eq!(self.app.active_sessions(), 0);
        assert_eq!(self.app.pending_sessions(), 0);
        assert!(TcpStream::connect(self.address).await.is_err());
    }
}

fn fields(call: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("AccountSid".into(), ACCOUNT.into()),
        ("CallSid".into(), call.into()),
        // Unknown fields and exact Unicode/space/plus values must be signed too.
        ("FutureField".into(), " + synthetic 雪 & value ".into()),
    ])
}

fn form(fields: &BTreeMap<String, String>) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(fields)
        .finish()
}

fn signature(url: &str, fields: &BTreeMap<String, String>) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(url.as_bytes());
    for (name, value) in fields {
        mac.update(name.as_bytes());
        mac.update(value.as_bytes());
    }
    STANDARD.encode(mac.finalize().into_bytes())
}

fn signed_upgrade(url: &str) -> Request {
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "X-Twilio-Signature",
        signature(url, &BTreeMap::new()).parse().unwrap(),
    );
    request
}

async fn rejected_upgrade(request: Request) {
    match WebSocketSession::connect_with_request(request, session_config()).await {
        Err(TransportError::WebSocket(WsError::Http(response))) => {
            assert_eq!(response.status().as_u16(), 403);
        }
        Err(error) => panic!("expected HTTP 403, not {error}"),
        Ok(mut session) => {
            let _ = session.cancel_and_join().await;
            panic!("unauthorized media upgrade succeeded");
        }
    }
}

fn start(call: &str, stream: &str) -> Value {
    json!({"event":"start","sequenceNumber":"1","streamSid":stream,
        "start":{"accountSid":ACCOUNT,"callSid":call,"streamSid":stream,
        "tracks":["inbound"],"mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1}}})
}

async fn send(session: &WebSocketSession, value: Value) {
    session
        .sender()
        .send(Packet::Text(value.to_string()))
        .await
        .unwrap();
}

struct Phone {
    session: WebSocketSession,
    call: String,
    stream: String,
    sequence: usize,
}
impl Phone {
    async fn connect(url: &str, call: String, stream: String) -> Self {
        let session = WebSocketSession::connect_with_request(signed_upgrade(url), session_config())
            .await
            .unwrap();
        send(
            &session,
            json!({"event":"connected","protocol":"Call","version":"1.0.0"}),
        )
        .await;
        send(&session, start(&call, &stream)).await;
        Self {
            session,
            call,
            stream,
            sequence: 2,
        }
    }

    async fn event(&mut self, mut value: Value) {
        value["streamSid"] = json!(self.stream);
        value["sequenceNumber"] = json!(self.sequence.to_string());
        self.sequence += 1;
        send(&self.session, value).await;
    }

    async fn audio(&mut self) {
        // 40 ms of nonzero constant 8 kHz mu-law. 0xfe decodes to PCM16 +8.
        self.event(json!({"event":"media","media":{"track":"inbound",
            "chunk":"1","timestamp":"0","payload":STANDARD.encode(vec![0xfe;320])}}))
            .await;
    }

    async fn keypad(&mut self) {
        for digit in ["0", "0", "1", "#"] {
            self.event(json!({"event":"dtmf","dtmf":{"track":"inbound_track","digit":digit}}))
                .await;
        }
    }

    async fn audio_reply(&mut self) -> usize {
        let mut audio = Vec::new();
        let mut clears = 0;
        while audio.len() < 160 {
            let packet = tokio::time::timeout(IO_BOUND, self.session.recv())
                .await
                .expect("phone received no generated audio")
                .expect("phone closed before generated audio");
            let Packet::Text(text) = packet else {
                panic!("phone received binary/browser wire data")
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["streamSid"], self.stream);
            match value["event"].as_str() {
                Some("media") => {
                    assert_eq!(value.as_object().unwrap().len(), 3);
                    assert_eq!(value["media"].as_object().unwrap().len(), 1);
                    let bytes = STANDARD
                        .decode(value["media"]["payload"].as_str().unwrap())
                        .unwrap();
                    assert!(
                        !bytes.starts_with(b"RIFF"),
                        "telephone output is headerless mu-law, not WAV"
                    );
                    audio.extend(bytes);
                }
                Some("clear") => clears += 1,
                other => panic!("non-telephone output leaked onto phone: {other:?}"),
            }
        }
        // 480 mono samples at 24 kHz become exactly 160 mu-law samples at 8 kHz,
        // including the normal TTS-stop resampler tail. No playback ack is implied.
        assert_eq!(audio, vec![pipecat_audio::encode_ulaw(1000); 160]);
        assert_eq!(pipecat_audio::ulaw_to_pcm(&audio).len(), 320);
        self.event(json!({"event":"mark","mark":{"name":"not-proof-of-audible-playback"}}))
            .await;
        clears
    }

    async fn stop(&mut self) {
        self.event(json!({"event":"stop","stop":{"accountSid":ACCOUNT,"callSid":self.call}}))
            .await;
        self.closed(false).await;
    }

    async fn closed(&mut self, abrupt: bool) {
        // No RTVI, text transcript, WAV, or extra unaccounted audio may follow.
        assert!(tokio::time::timeout(IO_BOUND, self.session.recv())
            .await
            .unwrap()
            .is_none());
        join_phone(&mut self.session, abrupt).await;
    }
}

async fn join_phone(session: &mut WebSocketSession, abrupt: bool) {
    match session.finish_and_join().await {
        Ok(()) => {}
        Err(TransportError::WebSocket(WsError::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ))) if abrupt => {}
        Err(TransportError::WebSocket(WsError::Io(error)))
            if abrupt
                && matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::BrokenPipe
                ) => {}
        Err(error) => panic!("phone socket did not join cleanly: {error}"),
    }
}

async fn assert_no_connection(listener: &TcpListener) {
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err(),
        "an unauthorized or duplicate external request reached a loopback peer"
    );
}

async fn accept_stt(listener: &TcpListener) -> WebSocketSession {
    let (stream, peer) = listener.accept().await.unwrap();
    assert_eq!(peer.ip().to_string(), "127.0.0.1");
    WebSocketSession::accept_with_request_policy(stream, session_config(), |request| {
        assert_eq!(request.uri().path(), "/v1/listen");
        let query: BTreeMap<_, _> =
            url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
                .into_owned()
                .collect();
        assert_eq!(query["encoding"], "linear16");
        assert_eq!(query["sample_rate"], "16000");
        assert_eq!(query["channels"], "1");
        assert_eq!(request.headers()["authorization"], "Token synthetic-only");
        true
    })
    .await
    .unwrap()
}

async fn stt_call(
    listener: &TcpListener,
    transcript: Option<&str>,
    ready: Option<oneshot::Sender<()>>,
) -> bool {
    let mut session = accept_stt(listener).await;
    if let Some(ready) = ready {
        ready.send(()).unwrap();
    }
    let mut pcm = Vec::new();
    let mut transcribed = false;
    let mut graceful = false;
    while let Some(packet) = session.recv().await {
        match packet {
            Packet::Binary(audio) => {
                assert!(!audio.is_empty());
                assert!(audio.len().is_multiple_of(2));
                assert!(
                    audio
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .all(|sample| *sample == [8, 0]),
                    "8 kHz mu-law was not decoded/resampled to the requested PCM16"
                );
                pcm.extend_from_slice(&audio);
                if !transcribed {
                    let transcript =
                        transcript.expect("unexpected STT audio in silent lifecycle case");
                    send(&session, json!({"type":"Results","is_final":true,"speech_final":true,
                        "start":0.0,"duration":0.04,"channel":{"alternatives":[{"transcript":transcript,"confidence":1.0,"words":[]}]}})).await;
                    transcribed = true;
                }
            }
            Packet::Text(text) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                match value["type"].as_str() {
                    Some("Finalize") => send(&session, json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}})).await,
                    Some("CloseStream") => { graceful = true; break; }
                    Some("KeepAlive") => {}
                    other => panic!("unexpected Deepgram command {other:?}"),
                }
            }
        }
    }
    // CloseStream is the provider's normal close request. Otherwise recv(None)
    // witnesses the runner closing its actual socket during cancel/disconnect.
    session.finish_and_join().await.unwrap();
    if transcript.is_some() {
        assert!(graceful);
        assert!(transcribed);
        assert_eq!(
            pcm.len(),
            1280,
            "graceful stop lost the 8-to-16 kHz input tail"
        );
    } else {
        assert!(pcm.is_empty());
    }
    graceful
}

struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
async fn read_http(socket: &mut TcpStream) -> HttpRequest {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let end = loop {
        let size = socket.read(&mut buffer).await.unwrap();
        assert!(size > 0, "truncated fixture HTTP request");
        bytes.extend_from_slice(&buffer[..size]);
        assert!(bytes.len() < 1024 * 1024);
        if let Some(index) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text = std::str::from_utf8(&bytes[..end]).unwrap();
    let mut lines = header_text.lines();
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().to_owned();
    let path = first.next().unwrap().to_owned();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length < 1024 * 1024);
    while bytes.len() < end + length {
        let size = socket.read(&mut buffer).await.unwrap();
        assert!(size > 0);
        bytes.extend_from_slice(&buffer[..size]);
    }
    HttpRequest {
        method,
        path,
        headers,
        body: bytes[end..end + length].to_vec(),
    }
}

async fn llm_turn(listener: &TcpListener, expected: &[(&str, &str)], reply: &str) {
    let (mut socket, peer) = listener.accept().await.unwrap();
    assert_eq!(peer.ip().to_string(), "127.0.0.1");
    let request = read_http(&mut socket).await;
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/chat/completions");
    assert_eq!(request.headers["authorization"], "Bearer synthetic-only");
    let value: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(value["model"], "synthetic-model");
    assert_eq!(value["stream"], true);
    let messages = value["messages"].as_array().unwrap();
    let conversation: Vec<_> = messages
        .iter()
        .filter(|message| message["role"] != "system")
        .map(|message| {
            (
                message["role"].as_str().unwrap(),
                message["content"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        conversation, expected,
        "LLM history must be exact and isolated per verified call"
    );
    let split = reply.find(' ').unwrap() + 1;
    let chunks = [
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{"content":&reply[..split]}}]})
        ),
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{"content":&reply[split..]}}]})
        ),
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
        ),
        "data: [DONE]\n\n".into(),
    ];
    let length: usize = chunks.iter().map(String::len).sum();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    for chunk in chunks {
        socket.write_all(chunk.as_bytes()).await.unwrap();
        socket.flush().await.unwrap();
        tokio::task::yield_now().await;
    }
    socket.shutdown().await.unwrap();
    assert_eq!(
        socket.read(&mut [0; 1]).await.unwrap(),
        0,
        "LLM HTTP peer remained owned after response"
    );
}

async fn tts_turn(listener: &TcpListener, expected: &str) {
    let (stream, peer) = listener.accept().await.unwrap();
    assert_eq!(peer.ip().to_string(), "127.0.0.1");
    let mut session =
        WebSocketSession::accept_with_request_policy(stream, session_config(), |request| {
            assert_eq!(request.uri().path(), "/tts/websocket");
            assert_eq!(request.headers()["x-api-key"], PROVIDER_KEY);
            true
        })
        .await
        .unwrap();
    let mut transcript = String::new();
    let mut context = None;
    loop {
        let Packet::Text(text) = session.recv().await.expect("TTS closed before text finish")
        else {
            panic!("TTS expects JSON")
        };
        let value: Value = serde_json::from_str(&text).unwrap();
        let id = value["context_id"].as_str().unwrap();
        if let Some(old) = &context {
            assert_eq!(old, id);
        }
        context = Some(id.to_owned());
        transcript.push_str(value["transcript"].as_str().unwrap());
        assert_eq!(value["output_format"]["sample_rate"], 24000);
        assert_eq!(value["output_format"]["encoding"], "pcm_s16le");
        assert_eq!(value["output_format"]["container"], "raw");
        if value["continue"] == false {
            assert_eq!(
                transcript, expected,
                "Cartesia must receive full generated LLM text"
            );
            let pcm: Vec<u8> = (0..480).flat_map(|_| 1000_i16.to_le_bytes()).collect();
            send(
                &session,
                json!({"type":"chunk","context_id":id,"data":STANDARD.encode(pcm)}),
            )
            .await;
            send(&session, json!({"type":"done","context_id":id})).await;
            break;
        }
    }
    assert!(
        session.recv().await.is_none(),
        "Cartesia client did not close after done"
    );
    session.finish_and_join().await.unwrap();
}

async fn join_fixtures(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.join_next().await {
        result.expect("loopback fixture task panicked");
    }
}

fn assert_context(summary: &CallSummary, expected: &[(Role, &str)]) {
    assert!(summary.verified_start);
    let actual: Vec<_> = summary
        .generated_context
        .messages
        .iter()
        .map(|message| {
            let Some(Content::Text(text)) = &message.content else {
                panic!("expected generated text only")
            };
            assert!(message.tool_calls.is_empty());
            (message.role, text.as_str())
        })
        .collect();
    assert_eq!(actual, expected);
    // These are generated-context assertions only. Neither media reception nor
    // the incoming mark proves audible playback or authorizes spoken-history claims.
}

#[tokio::test]
async fn signed_twiml_phone_loop_has_resampled_audio_dtmf_history_and_call_isolation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let peers = Peers::new().await;
        let mut host = peers
            .app(|config| config.hangup_policy = HangupPolicy::OnLocalTermination)
            .await;
        let Peers {
            stt,
            llm,
            tts,
            hangup,
        } = peers;
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            stt_call(&stt, Some(VOICE_ONE), None).await;
            stt_call(&stt, Some(VOICE_TWO), None).await;
            assert_no_connection(&stt).await;
        });
        tasks.spawn(async move {
            llm_turn(&llm, &[("user", VOICE_ONE)], REPLY_ONE).await;
            llm_turn(
                &llm,
                &[
                    ("user", VOICE_ONE),
                    ("assistant", REPLY_ONE),
                    ("user", "DTMF: 001#"),
                ],
                REPLY_DTMF,
            )
            .await;
            llm_turn(&llm, &[("user", VOICE_TWO)], REPLY_TWO).await;
            assert_no_connection(&llm).await;
        });
        tasks.spawn(async move {
            for reply in [REPLY_ONE, REPLY_DTMF, REPLY_TWO] {
                tts_turn(&tts, reply).await;
            }
            assert_no_connection(&tts).await;
        });
        for index in 1..=2 {
            let call = sid("CA", index);
            let url = host.admit(&call).await;
            assert_eq!(host.app.pending_sessions(), 1);
            let mut phone = Phone::connect(&url, call.clone(), sid("MZ", index)).await;
            phone.audio().await;
            phone.audio_reply().await;
            if index == 1 {
                phone.keypad().await;
                assert!(
                    phone.audio_reply().await >= 1,
                    "DTMF interruption did not clear telephone playback"
                );
            }
            phone.stop().await;
            let summary = host.report().await;
            assert_eq!(summary.call_sid, call);
            assert!(summary.successful(), "{summary:?}");
            let expected = if index == 1 {
                vec![
                    (Role::User, VOICE_ONE),
                    (Role::Assistant, REPLY_ONE),
                    (Role::User, "DTMF: 001#"),
                    (Role::Assistant, REPLY_DTMF),
                ]
            } else {
                vec![(Role::User, VOICE_TWO), (Role::Assistant, REPLY_TWO)]
            };
            assert_context(&summary, &expected);
            assert_eq!(host.app.active_sessions(), 0);
            assert_eq!(host.app.pending_sessions(), 0);
        }
        host.shutdown().await;
        join_fixtures(&mut tasks).await;
        // Remote stop must not produce a redundant REST completion.
        assert_no_connection(&hangup).await;
    })
    .await
    .expect("bounded synthetic phone round trip and cleanup");
}

#[tokio::test]
async fn signatures_capabilities_and_canonical_urls_gate_provider_admission() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let peers = Peers::new().await;
        let mut host = peers
            .app(|config| {
                config.connect_timeout = Duration::from_secs(1);
                config.hangup_policy = HangupPolicy::OnLocalTermination;
            })
            .await;
        let call = sid("CA", 10);
        let original = fields(&call);
        let valid = signature(&host.webhook, &original);
        assert_eq!(
            host.http
                .post(&host.webhook)
                .json(&json!({"CallSid":call}))
                .send()
                .await
                .unwrap()
                .status(),
            415
        );
        assert_eq!(
            host.post(&original)
                .header("content-type", "application/x-www-form-urlencoded")
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            415
        );
        let mut malformed_call = original.clone();
        malformed_call.insert("CallSid".into(), "CAinvalid".into());
        assert_eq!(
            host.post(&malformed_call)
                .header(
                    "X-Twilio-Signature",
                    signature(&host.webhook, &malformed_call)
                )
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        for headers in [
            vec![],
            vec!["AAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned()],
            vec![valid.clone(), valid.clone()],
        ] {
            let mut request = host.post(&original);
            for value in headers {
                request = request.header("X-Twilio-Signature", value);
            }
            assert_eq!(request.send().await.unwrap().status(), 403);
        }
        let mut tampered = original.clone();
        tampered.insert("FutureField".into(), "changed unknown signed field".into());
        assert_eq!(
            host.post(&tampered)
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        let mut other_account = original.clone();
        other_account.insert("AccountSid".into(), sid("AC", 2));
        assert_eq!(
            host.post(&other_account)
                .header(
                    "X-Twilio-Signature",
                    signature(&host.webhook, &other_account)
                )
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        // Headers claiming a different host/proto cannot replace trusted canonical config.
        let substitute = "https://127.0.0.1:1/twilio/voice";
        assert_eq!(
            host.post(&original)
                .header("X-Twilio-Signature", signature(substitute, &original))
                .header("Host", "127.0.0.1:1")
                .header("Forwarded", "host=127.0.0.1:1;proto=https")
                .header("X-Forwarded-Host", "127.0.0.1:1")
                .header("X-Forwarded-Proto", "https")
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        let body = format!("{}&%43allSid={call}", form(&original));
        assert_eq!(
            host.post(&original)
                .body(body)
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(
            host.post(&original)
                .body("Broken=%GG")
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(
            host.post(&original)
                .body("x".repeat(20_000))
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            413
        );
        assert_eq!(host.app.pending_sessions(), 0);
        assert_eq!(host.app.active_sessions(), 0);

        let url = host.admit(&call).await;
        // A signed GET that is not a WebSocket upgrade must fail without
        // spending its otherwise valid pending capability.
        assert_eq!(
            host.http
                .get(url.replacen("ws://", "http://", 1))
                .header("X-Twilio-Signature", signature(&url, &BTreeMap::new()))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(host.app.pending_sessions(), 1);
        assert_eq!(
            host.post(&original)
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
        let mut missing = signed_upgrade(&url);
        missing.headers_mut().remove("X-Twilio-Signature");
        rejected_upgrade(missing).await;
        let mut wrong = signed_upgrade(&url);
        wrong.headers_mut().insert(
            "X-Twilio-Signature",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAA=".parse().unwrap(),
        );
        rejected_upgrade(wrong).await;
        let mut duplicate = signed_upgrade(&url);
        duplicate.headers_mut().append(
            "X-Twilio-Signature",
            signature(&url, &BTreeMap::new()).parse().unwrap(),
        );
        rejected_upgrade(duplicate).await;
        let mut forwarded = signed_upgrade(&url);
        forwarded.headers_mut().insert(
            "X-Twilio-Signature",
            signature(&url.replacen("ws://", "wss://", 1), &BTreeMap::new())
                .parse()
                .unwrap(),
        );
        forwarded
            .headers_mut()
            .insert("X-Forwarded-Proto", "wss".parse().unwrap());
        rejected_upgrade(forwarded).await;
        assert_eq!(
            host.app.pending_sessions(),
            1,
            "invalid upgrades must not consume a capability"
        );
        let mut accepted =
            WebSocketSession::connect_with_request(signed_upgrade(&url), session_config())
                .await
                .unwrap();
        rejected_upgrade(signed_upgrade(&url)).await;
        accepted.finish_and_join().await.unwrap();
        let summary = host.report().await;
        assert!(!summary.verified_start);
        assert!(summary.generated_context.messages.is_empty());
        assert!(summary.hangup_error.is_none());
        assert_eq!(host.app.active_sessions(), 0);
        assert_eq!(
            host.post(&original)
                .header("X-Twilio-Signature", &valid)
                .send()
                .await
                .unwrap()
                .status(),
            409
        );

        let expired = host.admit(&sid("CA", 11)).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(host.app.pending_sessions(), 0);
        rejected_upgrade(signed_upgrade(&expired)).await;
        host.shutdown().await;
        peers.assert_untouched().await;
    })
    .await
    .expect("admission negatives and cleanup must remain bounded");
}

#[tokio::test]
async fn invalid_or_missing_start_never_creates_providers_or_hangs_up() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let peers = Peers::new().await;
        let mut host = peers.app(|config| {
            config.connect_timeout = Duration::from_millis(150);
            config.hangup_policy = HangupPolicy::OnLocalTermination;
        }).await;
        assert!(!host.app.terminate(&sid("CA", 999), Termination::End));
        for case in 0..9 {
            let call = sid("CA", 20 + case);
            let stream = sid("MZ", 20 + case);
            let url = host.admit(&call).await;
            let mut phone = WebSocketSession::connect_with_request(signed_upgrade(&url), session_config()).await.unwrap();
            if case != 8 {
                if case != 0 { send(&phone, json!({"event":"connected","protocol":"Call","version":"1.0.0"})).await; }
                let mut value = start(&call, &stream);
                match case {
                    0 | 1 => value = json!({"event":"media","sequenceNumber":"1","streamSid":stream,"media":{"track":"inbound","payload":"/w=="}}),
                    2 => value["start"]["accountSid"] = json!(sid("AC", 9)),
                    3 => value["start"]["callSid"] = json!(sid("CA", 999)),
                    4 => { value["streamSid"] = json!("MZbad"); value["start"]["streamSid"] = json!("MZbad"); }
                    5 => value["streamSid"] = json!(sid("MZ", 999)),
                    6 => value["start"]["mediaFormat"]["sampleRate"] = json!(16000),
                    7 => value["sequenceNumber"] = json!("2"),
                    _ => unreachable!(),
                }
                send(&phone, value).await;
            }
            assert!(tokio::time::timeout(IO_BOUND, phone.recv()).await.unwrap().is_none());
            join_phone(&mut phone, true).await;
            let summary = host.report().await;
            assert_eq!(summary.call_sid, call);
            assert!(!summary.verified_start);
            assert!(matches!(summary.error, Some(TwilioError::Protocol(_)) | Some(TwilioError::Deadline)));
            assert!(summary.generated_context.messages.is_empty());
            assert!(summary.hangup_error.is_none());
            assert_eq!(host.app.active_sessions(), 0);
        }
        host.shutdown().await;
        peers.assert_untouched().await;
    }).await.expect("rejected starts must close and release all admission state");
}

#[derive(Clone, Copy, Debug)]
enum Exit {
    End,
    Cancel,
    Disconnect,
    ShutdownRace,
}

async fn hangup_request(
    listener: &TcpListener,
    call: &str,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) {
    let (mut socket, peer) = listener.accept().await.unwrap();
    assert_eq!(peer.ip().to_string(), "127.0.0.1");
    let request = read_http(&mut socket).await;
    assert_eq!(request.method, "POST");
    assert_eq!(
        request.path,
        format!("/2010-04-01/Accounts/{ACCOUNT}/Calls/{call}.json")
    );
    assert_eq!(
        request.headers["authorization"],
        format!("Basic {}", STANDARD.encode(format!("{ACCOUNT}:{SECRET}")))
    );
    assert_eq!(
        request.headers["content-type"],
        "application/x-www-form-urlencoded"
    );
    assert_eq!(request.body, b"Status=completed");
    entered.send(()).unwrap();
    release.await.unwrap();
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
    socket.shutdown().await.unwrap();
    assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
    assert_no_connection(listener).await;
}

#[tokio::test]
async fn verified_local_end_cancel_disconnect_and_shutdown_own_one_hangup() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for (index, mode) in [
            Exit::End,
            Exit::Cancel,
            Exit::Disconnect,
            Exit::ShutdownRace,
        ]
        .into_iter()
        .enumerate()
        {
            let peers = Peers::new().await;
            let mut host = peers
                .app(|config| config.hangup_policy = HangupPolicy::OnLocalTermination)
                .await;
            let call = sid("CA", 40 + index);
            let url = host.admit(&call).await;
            let (ready, started) = oneshot::channel();
            let (hangup_entered, hangup_seen) = oneshot::channel();
            let (release_hangup, hangup_released) = oneshot::channel();
            let Peers {
                stt,
                llm,
                tts,
                hangup,
            } = peers;
            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                let graceful = stt_call(&stt, None, Some(ready)).await;
                if matches!(mode, Exit::Cancel) {
                    assert!(
                        !graceful,
                        "End must not downgrade an already requested Cancel into graceful drain"
                    );
                }
                assert_no_connection(&stt).await;
            });
            let hanging_up = call.clone();
            let hangup = Arc::new(hangup);
            let hangup_peer = hangup.clone();
            tasks.spawn(async move {
                hangup_request(&hangup_peer, &hanging_up, hangup_entered, hangup_released).await;
            });
            let mut phone = Phone::connect(&url, call.clone(), sid("MZ", 40 + index)).await;
            started.await.unwrap();
            match mode {
                Exit::End => assert!(host.app.terminate(&call, Termination::End)),
                Exit::Cancel => {
                    assert!(host.app.terminate(&call, Termination::Cancel));
                    assert!(host.app.terminate(&call, Termination::End));
                }
                Exit::Disconnect => {
                    phone.session.finish_and_join().await.unwrap();
                }
                Exit::ShutdownRace => {
                    assert!(host.app.terminate(&call, Termination::End));
                    let _ = host.app.terminate(&call, Termination::Cancel);
                }
            }
            if matches!(mode, Exit::ShutdownRace) {
                let app = host.app.clone();
                let waiter = tokio::spawn(async move { app.shutdown().await });
                hangup_seen.await.unwrap();
                // The fixture holds the response, making this cancellation real
                // rather than racing an already completed shutdown result.
                assert!(!waiter.is_finished());
                waiter.abort();
                assert!(waiter.await.unwrap_err().is_cancelled());
                release_hangup.send(()).unwrap();
                let (first, second) = tokio::join!(host.app.shutdown(), host.app.shutdown());
                let first = first.unwrap();
                let second = second.unwrap();
                assert_eq!(first.joined_connections, second.joined_connections);
                assert_eq!(first.task_errors, 0);
                assert!(!first.failed_cleanup());
            } else {
                hangup_seen.await.unwrap();
                release_hangup.send(()).unwrap();
            }
            if !matches!(mode, Exit::Disconnect) {
                phone
                    .closed(matches!(mode, Exit::Cancel | Exit::ShutdownRace))
                    .await;
            }
            let summary = host.report().await;
            assert!(summary.verified_start, "{mode:?}: {summary:?}");
            assert!(summary.generated_context.messages.is_empty());
            assert!(summary.hangup_error.is_none(), "{mode:?}: {summary:?}");
            if matches!(mode, Exit::Disconnect) {
                assert_eq!(summary.error, Some(TwilioError::Transport));
            } else {
                assert!(summary.error.is_none(), "{mode:?}: {summary:?}");
            }
            assert!(
                summary.media_cleanup_error.is_none(),
                "{mode:?}: {summary:?}"
            );
            assert!(!host.app.terminate(&call, Termination::Cancel));
            host.shutdown().await;
            join_fixtures(&mut tasks).await;
            assert_no_connection(&hangup).await;
            assert_no_connection(&llm).await;
            assert_no_connection(&tts).await;
        }
    })
    .await
    .expect("local termination and every owned peer must be bounded");
}

#[tokio::test]
async fn pending_and_active_capacity_is_released_by_expiry_cancel_and_shutdown() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peers = Peers::new().await;
        let mut host = peers
            .app(|config| {
                config.max_calls = 1;
                config.connect_timeout = Duration::from_millis(400);
            })
            .await;
        let first = host.admit(&sid("CA", 60)).await;
        let second_call = sid("CA", 61);
        let second = fields(&second_call);
        assert_eq!(
            host.post(&second)
                .header("X-Twilio-Signature", signature(&host.webhook, &second))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
        assert_eq!(host.app.pending_sessions(), 1);
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(host.app.pending_sessions(), 0);
        rejected_upgrade(signed_upgrade(&first)).await;
        let second_url = host.admit(&second_call).await;
        let mut phone =
            WebSocketSession::connect_with_request(signed_upgrade(&second_url), session_config())
                .await
                .unwrap();
        assert_eq!(host.app.active_sessions(), 1);
        assert_eq!(host.app.pending_sessions(), 0);
        let third = fields(&sid("CA", 62));
        assert_eq!(
            host.post(&third)
                .header("X-Twilio-Signature", signature(&host.webhook, &third))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
        assert!(host.app.terminate(&second_call, Termination::Cancel));
        assert!(phone.recv().await.is_none());
        join_phone(&mut phone, true).await;
        let summary = host.report().await;
        assert!(!summary.verified_start);
        assert_eq!(host.app.active_sessions(), 0);
        host.admit(&sid("CA", 63)).await;
        assert_eq!(host.app.pending_sessions(), 1);
        host.shutdown().await;
        peers.assert_untouched().await;
    })
    .await
    .expect("bounded admission slots must be reusable and joined");
}

#[tokio::test]
async fn established_calls_reject_duplicate_start_wrong_stream_and_invalid_controls() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for case in 0..6 {
            let peers = Peers::new().await;
            let mut host = peers.app(|_| {}).await;
            let call = sid("CA", 70 + case);
            let stream = sid("MZ", 70 + case);
            let url = host.admit(&call).await;
            let (ready, accepted) = oneshot::channel();
            let Peers { stt, llm, tts, hangup } = peers;
            let mut tasks = JoinSet::new();
            tasks.spawn(async move { stt_call(&stt, None, Some(ready)).await; });
            let mut phone = Phone::connect(&url, call.clone(), stream.clone()).await;
            accepted.await.unwrap();
            let mut event = json!({"event":"media","streamSid":stream,"sequenceNumber":"2","media":{"track":"inbound","payload":"/w=="}});
            match case {
                0 => { event = start(&call, &stream); event["sequenceNumber"] = json!("2"); }
                1 => event["streamSid"] = json!(sid("MZ", 999)),
                2 => event["sequenceNumber"] = json!("1"),
                3 => event["media"]["track"] = json!("outbound"),
                4 => event = json!({"event":"dtmf","streamSid":stream,"sequenceNumber":"2","dtmf":{"track":"outbound_track","digit":"1"}}),
                5 => event = json!({"event":"stop","streamSid":stream,"sequenceNumber":"2","stop":{"accountSid":ACCOUNT,"callSid":sid("CA", 999)}}),
                _ => unreachable!(),
            }
            send(&phone.session, event).await;
            phone.closed(true).await;
            let summary = host.report().await;
            assert!(summary.verified_start);
            assert!(matches!(summary.error, Some(TwilioError::Protocol(_))), "{summary:?}");
            assert!(summary.generated_context.messages.is_empty());
            assert!(summary.hangup_error.is_none());
            host.shutdown().await;
            join_fixtures(&mut tasks).await;
            for unused in [&llm, &tts, &hangup] { assert_no_connection(unused).await; }
        }
    }).await.expect("established invalid protocol must close all owned providers");
}

async fn read_stt_upgrade(socket: &mut TcpStream) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let size = socket.read(&mut buffer).await.unwrap();
        assert!(size > 0);
        bytes.extend_from_slice(&buffer[..size]);
        assert!(bytes.len() < 16 * 1024);
        if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.starts_with("GET /v1/listen?"));
    assert!(text
        .to_ascii_lowercase()
        .contains("authorization: token synthetic-only"));
}

#[tokio::test]
async fn setup_failure_and_shutdown_under_phone_queue_pressure_join_the_pending_owner() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for pressure in [false, true] {
            let peers = Peers::new().await;
            let mut host = peers.app(|config| config.queue_capacity = 2).await;
            let call = sid("CA", if pressure { 81 } else { 80 });
            let url = host.admit(&call).await;
            let (accepted, accepting) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let Peers {
                stt,
                llm,
                tts,
                hangup,
            } = peers;
            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                let (mut socket, peer) = stt.accept().await.unwrap();
                assert_eq!(peer.ip().to_string(), "127.0.0.1");
                read_stt_upgrade(&mut socket).await;
                accepted.send(()).unwrap();
                released.await.unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 503 Synthetic\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                socket.shutdown().await.unwrap();
                assert_eq!(
                    socket.read(&mut [0; 1]).await.unwrap(),
                    0,
                    "startup failure did not close provider TCP peer"
                );
                assert_no_connection(&stt).await;
            });
            let mut phone = Phone::connect(&url, call.clone(), sid("MZ", 80)).await;
            accepting.await.unwrap();
            if pressure {
                // The real app is waiting in provider setup and cannot drain its
                // capacity-two inbound queue. Four media packets exceed it without
                // depending on enormous buffers or unreliable TCP congestion.
                for _ in 0..4 {
                    phone.audio().await;
                }
                assert!(host.app.terminate(&call, Termination::Cancel));
                assert!(host.app.terminate(&call, Termination::End));
                let app = host.app.clone();
                let mut waiter = tokio::spawn(async move { app.shutdown().await });
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), &mut waiter)
                        .await
                        .is_err(),
                    "shutdown abandoned an outstanding provider setup"
                );
                assert_eq!(host.app.active_sessions(), 1);
                release.send(()).unwrap();
                let shutdown = waiter.await.unwrap().unwrap();
                assert_eq!(shutdown.task_errors, 0);
            } else {
                release.send(()).unwrap();
            }
            phone.closed(true).await;
            let summary = host.report().await;
            assert!(summary.verified_start);
            assert_eq!(summary.error, Some(TwilioError::Pipeline));
            assert!(summary.generated_context.messages.is_empty());
            assert!(summary.hangup_error.is_none());
            host.shutdown().await;
            join_fixtures(&mut tasks).await;
            for unused in [&llm, &tts, &hangup] {
                assert_no_connection(unused).await;
            }
        }
    })
    .await
    .expect("startup failure and pressured shutdown must retain and join ownership");
}
