//! Real Gemini HTTP -> services -> standard assistant consumption receipts.
//! Existing regressions are included verbatim in one dependency graph.
#[path = "../../pipecat-services/tests/review_thought_boundaries.rs"]
mod anthropic_regressions;
#[path = "../../pipecat-rtvi/tests/gemini_first_progress.rs"]
mod first_progress_regressions;
#[path = "gemini_history.rs"]
mod history_regressions;
#[path = "gemini_history_runtime.rs"]
mod history_runtime_regressions;
#[path = "../../pipecat-services/tests/nvidia_reasoning.rs"]
mod nvidia_regressions;
#[path = "../../pipecat-services/tests/gemini_thought_boundaries.rs"]
mod raw;
#[path = "review_gemini_terminal.rs"]
mod terminal_regressions;

use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{LlmClient, LlmProcessor};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use raw::{
    candidate, chunk, closed, config, eof, header, join_peer, read_request, request, stopped,
    Check, WAIT,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Receipt {
    Start,
    Thought(String),
    End,
    Text(String),
    Checkpoint,
    ResponseEnd,
    Interruption,
    Ttfb,
    Error(String),
}

// The receipt is sent only after the real assistant's process future succeeds.
// A pipeline observer's Process entry, Pong, or an arbitrary delay cannot open
// the peer gate. This wrapper does not manufacture thought/history boundaries.
struct AssistantReceipt {
    inner: LLMAssistantAggregator,
    receipts: mpsc::UnboundedSender<Receipt>,
    hold_old_text: Mutex<Option<oneshot::Receiver<()>>>,
    release_old_text: Mutex<Option<oneshot::Sender<()>>>,
}
#[async_trait]
impl FrameProcessor for AssistantReceipt {
    fn name(&self) -> &str {
        "GeminiAssistantReceipt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let receipt = if direction == Direction::Downstream {
            match &frame.data {
                FrameData::LlmThoughtStart => Some(Receipt::Start),
                FrameData::LlmThoughtText(text) => {
                    if text.skip_tts != Some(true) || text.append_to_context {
                        return Err(ProcessorError::new("thought is not display-only"));
                    }
                    Some(Receipt::Thought(text.text.clone()))
                }
                FrameData::LlmThoughtEnd => {
                    if frame
                        .native_llm_continuation()
                        .map_err(|e| ProcessorError::new(e.to_string()))?
                        .is_some()
                    {
                        return Err(ProcessorError::new(
                            "ThoughtEnd must not commit a signature",
                        ));
                    }
                    Some(Receipt::End)
                }
                FrameData::LlmText(text) => Some(Receipt::Text(text.text.clone())),
                FrameData::LlmContinuationCheckpoint => {
                    if frame
                        .native_llm_continuation()
                        .map_err(|e| ProcessorError::new(e.to_string()))?
                        .is_none()
                    {
                        return Err(ProcessorError::new("missing native checkpoint"));
                    }
                    Some(Receipt::Checkpoint)
                }
                FrameData::LlmResponseEnd => Some(Receipt::ResponseEnd),
                FrameData::Interruption => Some(Receipt::Interruption),
                FrameData::Metrics(metrics)
                    if metrics
                        .iter()
                        .any(|metric| matches!(metric, Metric::Ttfb { .. })) =>
                {
                    Some(Receipt::Ttfb)
                }
                FrameData::Error(error) => Some(Receipt::Error(error.error.clone())),
                _ => None,
            }
        } else {
            None
        };
        self.inner.process(frame, direction, ctx).await?;
        if let Some(receipt) = receipt {
            if receipt == Receipt::Interruption {
                if let Some(release) = self.release_old_text.lock().unwrap().take() {
                    let _ = release.send(());
                }
            }
            let hold = if receipt == Receipt::Thought("old-private".into()) {
                self.hold_old_text.lock().unwrap().take()
            } else {
                None
            };
            self.receipts
                .send(receipt)
                .map_err(|_| ProcessorError::new("fixture receipt owner closed"))?;
            if let Some(hold) = hold {
                let _ = hold.await;
            }
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

fn echo() -> Arc<ToolExecutor> {
    let mut tools = ToolExecutor::new(1, 16, 4096).unwrap();
    tools
        .register(
            "echo",
            RegisteredTool::new(Arc::new(|call| Box::pin(async move { Ok(call.arguments) }))),
        )
        .unwrap();
    Arc::new(tools)
}
async fn start(
    config: pipecat_services::ClientConfig,
    hold: bool,
) -> Check<(
    PipelineWorker,
    SharedContext,
    Arc<AssistantReceipt>,
    mpsc::UnboundedReceiver<Receipt>,
)> {
    let request = request();
    let shared = SharedContext::new(request.context.clone());
    let (receipts, receiver) = mpsc::unbounded_channel();
    let (release, held) = oneshot::channel();
    let assistant = Arc::new(AssistantReceipt {
        inner: LLMAssistantAggregator::new(shared.clone(), Default::default()),
        receipts,
        hold_old_text: Mutex::new(hold.then_some(held)),
        release_old_text: Mutex::new(hold.then_some(release)),
    });
    let worker = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(
            LlmClient::new(config).map_err(|e| e.to_string())?,
            request,
        )),
        echo(),
        assistant.clone(),
    ])
    .start(PipelineConfig {
        start: StartFrame {
            enable_metrics: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok((worker, shared, assistant, receiver))
}
async fn send(worker: &PipelineWorker, data: FrameData) -> Check<()> {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}
async fn next(
    receiver: &mut mpsc::UnboundedReceiver<Receipt>,
    trace: &mut Vec<Receipt>,
) -> Check<Receipt> {
    let event = receiver.recv().await.ok_or("assistant receipts closed")?;
    trace.push(event.clone());
    if let Receipt::Error(error) = &event {
        return Err(error.clone());
    }
    Ok(event)
}
// Every outcome, including a failed assertion precondition or timeout, reaches
// owner drain+join and peer join before assertions are evaluated.
async fn finish(
    worker: &mut PipelineWorker,
    assistant: &AssistantReceipt,
    cancel: bool,
) -> Check<()> {
    if let Some(release) = assistant.release_old_text.lock().unwrap().take() {
        let _ = release.send(());
    }
    let control = if cancel {
        worker.cancel("Gemini thought fixture cleanup").await
    } else {
        worker.end().await
    };
    let mut errors = vec![];
    if let Err(error) = control {
        errors.push(error.to_string());
    }
    if timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = frame.frame.data {
                errors.push(error.error);
            }
        }
    })
    .await
    .is_err()
    {
        errors.push("owner drain timeout".into());
        let _ = worker.cancel("Gemini thought drain timeout").await;
    }
    match timeout(WAIT, worker.join()).await {
        Ok(Ok(report))
            if report.state
                == if cancel {
                    WorkerState::Cancelled
                } else {
                    WorkerState::Ended
                } => {}
        Ok(other) => errors.push(format!("owner join: {other:?}")),
        Err(_) => {
            errors.push("owner join timeout".into());
            let _ = worker.cancel("Gemini thought join timeout").await;
            match timeout(WAIT, worker.join()).await {
                Ok(Ok(_)) => {}
                other => errors.push(format!("owner retry join: {other:?}")),
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
fn presentation(trace: &[Receipt]) -> Vec<Receipt> {
    trace
        .iter()
        .filter(|e| {
            matches!(
                e,
                Receipt::Start
                    | Receipt::Thought(_)
                    | Receipt::End
                    | Receipt::Text(_)
                    | Receipt::Interruption
            )
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn actual_thought_ends_unlock_http_tail_and_preserve_signed_part_order() {
    for same_record in [false, true] {
        for tool_first in [false, true] {
            let thoughts = json!([
                {"text":"first-private","thought":true,"thoughtSignature":"first+/=="},
                {"text":"","thought":true,"thoughtSignature":"empty+/=="},
                {"text":" \t ","thought":true,"thoughtSignature":"second+/=="}
            ]);
            let first_records = if same_record {
                vec![candidate(thoughts.clone())]
            } else {
                vec![
                    candidate(json!([thoughts[0], thoughts[1]])),
                    candidate(json!([thoughts[2]])),
                ]
            };
            let text = json!({"text":"answer","thoughtSignature":"answer+/=="});
            let tool = json!({"functionCall":{"id":"call","name":"echo","args":{"n":1}},"thoughtSignature":"tool+/=="});
            let tail = if tool_first {
                json!([tool, text])
            } else {
                json!([text, tool])
            };
            let mut all_parts = thoughts.as_array().unwrap().clone();
            all_parts.extend(tail.as_array().unwrap().clone());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = config(&listener);
            let (gate, gate_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
                read_request(&mut socket).await?;
                header(&mut socket).await?;
                chunk(&mut socket, &first_records).await?;
                gate_rx
                    .await
                    .map_err(|_| "assistant did not consume both ThoughtEnd boundaries")?;
                chunk(&mut socket, &[candidate(tail), stopped()]).await?;
                eof(&mut socket).await?;
                drop(socket);
                let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
                let continuation = read_request(&mut socket).await?;
                header(&mut socket).await?;
                chunk(
                    &mut socket,
                    &[candidate(json!([{"text":"done"}])), stopped()],
                )
                .await?;
                eof(&mut socket).await?;
                Ok(continuation)
            });
            let started = start(config, false).await;
            let (mut worker, shared, assistant, mut receipts) = match started {
                Ok(value) => value,
                Err(error) => {
                    drop(gate);
                    let peer = join_peer(peer).await;
                    panic!("start={error}; peer={peer:?}");
                }
            };
            let mut gate = Some(gate);
            let mut trace = vec![];
            let outcome = timeout(WAIT, async {
                send(&worker, FrameData::LlmRun).await?;
                let mut ends = 0;
                let mut responses = 0;
                loop {
                    match next(&mut receipts, &mut trace).await? {
                        Receipt::End => {
                            ends += 1;
                            if ends == 2 {
                                gate.take()
                                    .ok_or("duplicate gate")?
                                    .send(())
                                    .map_err(|_| "peer lost End gate")?;
                            }
                        }
                        Receipt::ResponseEnd => {
                            responses += 1;
                            if responses == 2 {
                                return Ok::<_, String>(());
                            }
                        }
                        _ => {}
                    }
                }
            })
            .await;
            drop(gate);
            let owner = finish(&mut worker, &assistant, !matches!(outcome, Ok(Ok(())))).await;
            let peer = join_peer(peer).await;
            assert!(matches!(outcome, Ok(Ok(()))), "scenario=({same_record},{tool_first}); outcome={outcome:?}; owner={owner:?}; peer={peer:?}; trace={trace:?}");
            owner.unwrap();
            let continuation = peer.unwrap();
            assert_eq!(
                presentation(&trace),
                vec![
                    Receipt::Start,
                    Receipt::Thought("first-private".into()),
                    Receipt::End,
                    Receipt::Start,
                    Receipt::Thought(" \t ".into()),
                    Receipt::End,
                    Receipt::Text("answer".into()),
                    Receipt::Text("done".into())
                ]
            );
            let display: Vec<_> = trace
                .iter()
                .filter(|e| !matches!(e, Receipt::Ttfb | Receipt::ResponseEnd))
                .cloned()
                .collect();
            let mut expected = vec![
                Receipt::Start,
                Receipt::Thought("first-private".into()),
                Receipt::End,
            ];
            if !same_record {
                expected.push(Receipt::Checkpoint);
            }
            expected.extend([
                Receipt::Start,
                Receipt::Thought(" \t ".into()),
                Receipt::End,
                Receipt::Checkpoint,
                Receipt::Text("answer".into()),
                Receipt::Checkpoint,
                Receipt::Text("done".into()),
            ]);
            assert_eq!(
                display, expected,
                "checkpoint is record-final, separate from each display End"
            );
            assert_eq!(
                continuation["contents"],
                json!([
                    {"role":"user","parts":[{"text":"question"}]},
                    {"role":"model","parts":all_parts},
                    {"role":"user","parts":[{"functionResponse":{"id":"call","name":"echo","response":{"n":1}}}]}
                ])
            );
            assert_eq!(
                trace.iter().filter(|e| matches!(e, Receipt::Ttfb)).count(),
                2
            );
            let snapshot = shared.snapshot().unwrap();
            let wire = pipecat_services::adapters::request_body(
                pipecat_services::Provider::Gemini,
                &pipecat_services::CompletionRequest::new("gemini-3-fixture", snapshot),
            )
            .unwrap();
            assert_eq!(
                wire["contents"][1]["parts"],
                json!(all_parts),
                "display segmentation must not append duplicate unsigned thoughts"
            );
        }
    }
}

#[tokio::test]
async fn interruption_after_consumed_thought_text_discards_old_display_and_signature_tail() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let (closed_tx, closed_rx) = oneshot::channel();
    let peer =
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            header(&mut socket).await?;
            chunk(
                &mut socket,
                &[candidate(json!([
                    {"text":"old-private","thought":true,"thoughtSignature":"old-signature+/=="},
                    {"text":"old-unheard","thoughtSignature":"old-text+/=="}
                ]))],
            )
            .await?;
            closed(&mut socket).await?;
            closed_tx
                .send(())
                .map_err(|_| "socket receipt owner closed")?;
            let (mut next, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let captured = read_request(&mut next).await?;
            header(&mut next).await?;
            chunk(&mut next, &[candidate(json!([
            {"text":"new-private","thought":true,"thoughtSignature":"new-signature+/=="},
            {"text":"new-answer","thoughtSignature":"new-text+/=="}
        ])), stopped()]).await?;
            eof(&mut next).await?;
            Ok(captured)
        });
    let started = start(config, true).await;
    let (mut worker, shared, assistant, mut receipts) = match started {
        Ok(value) => value,
        Err(error) => {
            let peer = join_peer(peer).await;
            panic!("start={error}; peer={peer:?}");
        }
    };
    let mut trace = vec![];
    let outcome = timeout(WAIT, async {
        send(&worker, FrameData::LlmRun).await?;
        loop {
            if next(&mut receipts, &mut trace).await? == Receipt::Thought("old-private".into()) {
                break;
            }
        }
        // The wrapper is now held after real assistant consumption. Neither the
        // old End nor the signed record checkpoint can pass this consumer fence.
        send(&worker, FrameData::Interruption).await?;
        loop {
            if next(&mut receipts, &mut trace).await? == Receipt::Interruption {
                break;
            }
        }
        closed_rx
            .await
            .map_err(|_| "old HTTP owner did not close")?;
        send(&worker, shared.frame_data().map_err(|e| e.to_string())?).await?;
        loop {
            if next(&mut receipts, &mut trace).await? == Receipt::ResponseEnd {
                return Ok::<_, String>(());
            }
        }
    })
    .await;
    let owner = finish(&mut worker, &assistant, !matches!(outcome, Ok(Ok(())))).await;
    let peer = join_peer(peer).await;
    assert!(
        matches!(outcome, Ok(Ok(()))),
        "outcome={outcome:?}; owner={owner:?}; peer={peer:?}; trace={trace:?}"
    );
    owner.unwrap();
    let next_request = peer.unwrap();
    assert_eq!(
        next_request["contents"],
        json!([{"role":"user","parts":[{"text":"question"}]}])
    );
    let split = trace
        .iter()
        .position(|e| e == &Receipt::Interruption)
        .unwrap();
    assert_eq!(
        presentation(&trace[..split]),
        vec![Receipt::Start, Receipt::Thought("old-private".into())]
    );
    assert_eq!(
        presentation(&trace[split..]),
        vec![
            Receipt::Interruption,
            Receipt::Start,
            Receipt::Thought("new-private".into()),
            Receipt::End,
            Receipt::Text("new-answer".into())
        ]
    );
    assert!(!trace[..split].contains(&Receipt::Checkpoint));
    let snapshot = shared.snapshot().unwrap();
    let wire = pipecat_services::adapters::request_body(
        pipecat_services::Provider::Gemini,
        &pipecat_services::CompletionRequest::new("gemini-3-fixture", snapshot),
    )
    .unwrap();
    assert_eq!(
        wire["contents"],
        json!([
            {"role":"user","parts":[{"text":"question"}]},
            {"role":"model","parts":[{"text":"new-private","thought":true,"thoughtSignature":"new-signature+/=="},{"text":"new-answer","thoughtSignature":"new-text+/=="}]}
        ])
    );
}
