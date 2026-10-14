//! Real native Pipeline and bounded loopback Anthropic HTTP/SSE policy witnesses.
use futures_util::FutureExt;
use pipecat_core::{self as core, *};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{
    panic::{resume_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Semaphore},
    task::JoinHandle,
};
const WAIT: Duration = Duration::from_secs(5);

async fn read_request(socket: &mut TcpStream) -> Result<Value, String> {
    const MAX: usize = 64 * 1024;
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("header closed".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("request exceeded limit".into());
        }
        if let Some(i) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let length = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse::<usize>())
        })
        .ok_or("missing length")?
        .map_err(|e| e.to_string())?;
    if length > MAX - end {
        return Err("body exceeded limit".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("body closed".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("request exceeded limit".into());
        }
    }
    serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())
}
async fn reply(socket: &mut TcpStream) -> Result<(), String> {
    let events = [
        json!({"type":"message_start","message":{"usage":{"input_tokens":2,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"answer"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ];
    let body = events
        .iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect::<String>();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.map_err(|e|e.to_string())?;
    socket.shutdown().await.map_err(|e| e.to_string())
}
#[derive(Default)]
struct Captured {
    bodies: Vec<Value>,
    peer_closed: usize,
}
struct Fixture {
    processor: Arc<LlmProcessor>,
    worker: Option<PipelineWorker>,
    captured: Arc<Mutex<Captured>>,
    release: Arc<Semaphore>,
    stop: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<Result<(), String>>>,
}
impl Fixture {
    async fn new(provider: Provider, hold_first: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::new(provider, "synthetic");
        config.endpoint = format!("http://{}/messages", listener.local_addr().unwrap());
        let mut request = CompletionRequest::new(
            "fixture",
            Context::new(vec![Message::text(Role::User, "ask")]),
        );
        request.system_instruction = Some("shared prefix".into());
        if provider == Provider::Anthropic {
            request = request.with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
                enabled: false,
                system_ttl: Some(AnthropicCacheTtl::OneHour),
            });
        }
        let processor = Arc::new(LlmProcessor::new(LlmClient::new(config).unwrap(), request));
        let worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_millis(500),
                ..Default::default()
            })
            .await
            .unwrap();
        let captured = Arc::new(Mutex::new(Captured::default()));
        let release = Arc::new(Semaphore::new(0));
        let (stop, mut stopped) = oneshot::channel();
        let record = captured.clone();
        let gate = release.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {biased;_=&mut stopped=>return Ok(()),r=listener.accept()=>r.map_err(|e|e.to_string())?};
                tokio::select! {biased;_=&mut stopped=>return Ok(()),r=async {
                    let body=read_request(&mut socket).await?;
                    let index={let mut r=record.lock().unwrap();r.bodies.push(body);r.bodies.len()-1};
                    if hold_first && index==0 {
                        let mut probe=[0;1];
                        tokio::select!{
                            permit=gate.acquire()=>permit.map_err(|e|e.to_string())?.forget(),
                            n=socket.read(&mut probe)=>{
                                if n.map_err(|e|e.to_string())?!=0{return Err("unexpected pipelined data".into());}
                                record.lock().unwrap().peer_closed+=1;return Ok(());
                            }
                        }
                    }
                    reply(&mut socket).await
                }=>r?};
            }
        });
        Self {
            processor,
            worker: Some(worker),
            captured,
            release,
            stop: Some(stop),
            server: Some(server),
        }
    }
    fn worker(&mut self) -> &mut PipelineWorker {
        self.worker.as_mut().unwrap()
    }
    async fn send(&mut self, data: FrameData, direction: Direction) {
        self.worker()
            .sender()
            .send(Frame::new(data), direction)
            .await
            .unwrap();
    }
    async fn recv(&mut self) -> DirectedFrame {
        tokio::time::timeout(WAIT, self.worker().recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn barrier(&mut self) -> Vec<DirectedFrame> {
        let marker = Frame::new(FrameData::Data);
        let id = marker.id;
        self.worker()
            .sender()
            .send(marker, Direction::Downstream)
            .await
            .unwrap();
        let mut frames = vec![];
        loop {
            let f = self.recv().await;
            if f.frame.id == id {
                return frames;
            }
            frames.push(f);
        }
    }
    async fn captures(&self, n: usize) -> Vec<Value> {
        tokio::time::timeout(WAIT, async {
            loop {
                let bodies = self.captured.lock().unwrap().bodies.clone();
                if bodies.len() >= n {
                    return bodies;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn end(&mut self) {
        self.worker().end().await.unwrap();
        tokio::time::timeout(WAIT, async {
            while self.worker().recv().await.is_some() {}
            self.worker().join().await.unwrap();
        })
        .await
        .unwrap();
    }
    async fn cleanup(&mut self) {
        let mut errors = vec![];
        if let Some(worker) = self.worker.as_ref() {
            if !worker.has_finished() {
                let result = tokio::time::timeout(WAIT, worker.cancel("fixture cleanup")).await;
                if !matches!(result, Ok(Ok(())) | Ok(Err(PipelineError::Closed))) {
                    errors.push(format!("cancel failed: {result:?}"));
                }
            }
        }
        if let Some(worker) = self.worker.as_mut() {
            let result = match tokio::time::timeout(WAIT, async {
                while worker.recv().await.is_some() {}
                worker.join().await
            })
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    errors.push("worker collection deadline".into());
                    let cancelled =
                        tokio::time::timeout(WAIT, worker.cancel("collection deadline")).await;
                    if !matches!(cancelled, Ok(Ok(())) | Ok(Err(PipelineError::Closed))) {
                        errors.push(format!("fallback cancel failed: {cancelled:?}"));
                    }
                    // Borrowed join retains ownership; core's configured shutdown
                    // deadline bounds owned-task cleanup after cancellation.
                    worker.join().await
                }
            };
            if !matches!(result, Ok(_) | Err(PipelineError::AlreadyJoined)) {
                errors.push(format!("worker join failed: {result:?}"));
            }
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut server) = self.server.take() {
            match tokio::time::timeout(WAIT, &mut server).await {
                Ok(Ok(Ok(()))) => {}
                Ok(other) => errors.push(format!("server failed: {other:?}")),
                Err(_) => {
                    server.abort();
                    let joined = server.await;
                    errors.push(format!("server deadline; aborted and joined: {joined:?}"));
                }
            }
        }
        assert!(errors.is_empty(), "all owners cleaned: {errors:?}");
    }
}
macro_rules! fixture_test {
    ($name:ident,$provider:expr,$hold:expr,|$fixture:ident|$body:block) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            let mut fixture = Fixture::new($provider, $hold).await;
            let result = AssertUnwindSafe(async {
                let $fixture = &mut fixture;
                $body
            })
            .catch_unwind()
            .await;
            fixture.cleanup().await;
            if let Err(error) = result {
                resume_unwind(error);
            }
        }
    };
}
fn cache(body: &Value) -> bool {
    body["messages"][0]["content"][0]
        .get("cache_control")
        .is_some()
}
fn update(entries: &[(&str, Value)]) -> FrameData {
    FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
        settings: entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
        ..Default::default()
    }))
}
fixture_test!(
    active_and_queued_requests_keep_exact_policy_snapshots,
    Provider::Anthropic,
    true,
    |f| {
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        let first = f.captures(1).await;
        assert!(!cache(&first[0]));
        f.send(
            FrameData::LlmEnablePromptCaching { enable: true },
            Direction::Upstream,
        )
        .await;
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        f.send(
            FrameData::LlmEnablePromptCaching { enable: false },
            Direction::Downstream,
        )
        .await;
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        let output = f.barrier().await;
        assert!(!output.iter().any(|x| matches!(
            x.frame.data,
            FrameData::LlmEnablePromptCaching { .. } | FrameData::Error(_)
        )));
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 1);
        f.release.add_permits(1);
        let bodies = f.captures(3).await;
        assert!(!cache(&bodies[0]));
        assert!(cache(&bodies[1]));
        assert!(!cache(&bodies[2]));
        assert_eq!(bodies[1]["system"][0]["cache_control"]["ttl"], "1h");
        for b in &bodies {
            assert!(b.get("enable_prompt_caching").is_none());
            assert!(b.get("system_prompt_cache_ttl").is_none());
        }
        f.end().await;
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 3);
    }
);
fixture_test!(
    toggle_and_rejected_proposals_make_no_request_and_preserve_health,
    Provider::Anthropic,
    false,
    |f| {
        let health = f.processor.health().unwrap();
        let before = health.snapshot();
        f.send(
            FrameData::LlmEnablePromptCaching { enable: true },
            Direction::Downstream,
        )
        .await;
        f.send(
            update(&[
                ("enable_prompt_caching", json!(false)),
                ("system_prompt_cache_ttl", json!("invalid")),
            ]),
            Direction::Downstream,
        )
        .await;
        let out = f.barrier().await;
        assert_eq!(
            out.iter()
                .filter(|x| matches!(x.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        assert!(health.is_usable());
        assert_eq!(health.snapshot(), before);
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 0);
        let snapshot = f.processor.request_snapshot().unwrap();
        assert_eq!(snapshot.settings.base.extra["enable_prompt_caching"], true);
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        let bodies = f.captures(1).await;
        assert!(cache(&bodies[0]));
        assert_eq!(bodies[0]["system"][0]["cache_control"]["ttl"], "1h");
        f.end().await;
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 1);
    }
);
fixture_test!(
    unsupported_provider_toggle_is_a_recoverable_proposal,
    Provider::OpenAi,
    false,
    |f| {
        let before = serde_json::to_value(f.processor.request_snapshot().unwrap()).unwrap();
        let health = f.processor.health().unwrap();
        let status = health.snapshot();
        f.send(
            FrameData::LlmEnablePromptCaching { enable: false },
            Direction::Downstream,
        )
        .await;
        let out = f.barrier().await;
        assert_eq!(
            out.iter()
                .filter(|x| matches!(x.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        assert!(health.is_usable());
        assert_eq!(health.snapshot(), status);
        assert_eq!(
            serde_json::to_value(f.processor.request_snapshot().unwrap()).unwrap(),
            before
        );
        f.end().await;
        assert!(f.captured.lock().unwrap().bodies.is_empty());
    }
);
fixture_test!(
    cancellation_closes_peer_without_replaying_admitted_cache_policy,
    Provider::Anthropic,
    true,
    |f| {
        f.send(
            FrameData::LlmEnablePromptCaching { enable: true },
            Direction::Downstream,
        )
        .await;
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        assert!(cache(&f.captures(1).await[0]));
        f.send(FrameData::Interruption, Direction::Downstream).await;
        f.send(
            FrameData::LlmEnablePromptCaching { enable: false },
            Direction::Downstream,
        )
        .await;
        f.barrier().await;
        tokio::time::timeout(WAIT, async {
            loop {
                if f.captured.lock().unwrap().peer_closed == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        let bodies = f.captures(2).await;
        assert!(cache(&bodies[0]));
        assert!(!cache(&bodies[1]));
        f.end().await;
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 2);
    }
);

fixture_test!(
    completed_sequential_ordinary_sessions_retain_policy_until_updated,
    Provider::Anthropic,
    false,
    |f| {
        f.send(
            FrameData::LlmEnablePromptCaching { enable: true },
            Direction::Downstream,
        )
        .await;
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        assert!(cache(&f.captures(1).await[0]));
        f.end().await;
        let worker = Pipeline::new(vec![f.processor.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_millis(500),
                ..Default::default()
            })
            .await
            .unwrap();
        f.worker = Some(worker);
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        assert!(cache(&f.captures(2).await[1]));
        f.send(
            FrameData::LlmEnablePromptCaching { enable: false },
            Direction::Downstream,
        )
        .await;
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        assert!(!cache(&f.captures(3).await[2]));
        f.end().await;
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 3);
    }
);

fixture_test!(
    per_call_override_does_not_change_stored_or_later_pipeline_policy,
    Provider::Anthropic,
    false,
    |f| {
        f.send(
            FrameData::LlmEnablePromptCaching { enable: true },
            Direction::Downstream,
        )
        .await;
        f.barrier().await;
        let before = serde_json::to_value(f.processor.request_snapshot().unwrap()).unwrap();
        let context = core::LlmContextData {
            messages: vec![core::ChatMessage::new(core::ChatRole::User, "per-call").into()],
            tools: core::ToolSet::Functions(vec![]),
            tool_choice: None,
        };
        let mut options = InferenceOptions::default();
        options
            .settings
            .base
            .extra
            .insert("enable_prompt_caching".into(), json!(false));
        let answer = tokio::time::timeout(WAIT, f.processor.run_inference(context, options))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer.as_deref(), Some("answer"));
        assert!(!cache(&f.captures(1).await[0]));
        assert_eq!(
            serde_json::to_value(f.processor.request_snapshot().unwrap()).unwrap(),
            before
        );
        f.send(FrameData::LlmRun, Direction::Downstream).await;
        assert!(cache(&f.captures(2).await[1]));
        f.end().await;
        assert_eq!(f.captured.lock().unwrap().bodies.len(), 2);
    }
);
