//! Default-facade acceptance with the production RTVI trait codec and real
//! pipelines. Its wire payloads are text JSON, not protobuf or a live transport.
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat::{
    core::*,
    processors::{
        AsyncGeneratorProcessor, AsyncGeneratorProcessorConfig, AsyncGeneratorReceiver,
        AsyncGeneratorSerializerFactory,
    },
    protocol::{
        adapters::{FrameSerializer, RtviClientFrameSerializer},
        ProtocolError, WireMessage,
    },
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);
type TestResult<T> = Result<T, String>;

#[derive(Default)]
struct CodecTrace {
    created: AtomicUsize,
    calls: Mutex<Vec<(usize, u64)>>,
}

// Instrument the real implementation rather than inventing a fixture protocol.
// Recording every invocation also detects accidental End/Cancel serialization,
// even though the production codec would return None for those frames.
struct TracedRtvi {
    inner: RtviClientFrameSerializer,
    generation: usize,
    trace: Arc<CodecTrace>,
}
impl FrameSerializer for TracedRtvi {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
        self.trace
            .calls
            .lock()
            .unwrap()
            .push((self.generation, frame.id));
        self.inner.serialize(frame)
    }

    fn deserialize(&mut self, message: &WireMessage) -> Result<Option<Frame>, ProtocolError> {
        self.inner.deserialize(message)
    }
}

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}

async fn collect_borrowed(
    worker: &mut PipelineWorker,
    observed: &mut Vec<DirectedFrame>,
) -> TestResult<WorkerReport> {
    while let Some(frame) = worker.recv().await {
        observed.push(frame);
    }
    worker.join().await.map_err(|error| error.to_string())
}

async fn finish(
    mut worker: PipelineWorker,
    terminal: Frame,
    observed: &mut Vec<DirectedFrame>,
) -> TestResult<WorkerReport> {
    let sender = worker.sender();
    let sent = bounded("terminal", sender.send(terminal, Direction::Downstream)).await;
    if sent.is_err() {
        let _ = bounded(
            "cancel failed scenario",
            sender.cancel("codec fixture failed"),
        )
        .await;
    }
    // The timeout borrows the worker. Never drop a spawned collect task and
    // detach its supervisor; retain ownership until cancellation and join finish.
    let collected = match timeout(WAIT, collect_borrowed(&mut worker, observed)).await {
        Ok(result) => result,
        Err(_) => {
            let _ = bounded(
                "cancel collect deadline",
                sender.cancel("codec collect deadline"),
            )
            .await;
            let joined = collect_borrowed(&mut worker, observed).await;
            Err(format!(
                "collection deadline; cancelled and joined: {joined:?}"
            ))
        }
    };
    sent.and(collected)
}

async fn start(
    processor: &Arc<AsyncGeneratorProcessor>,
) -> TestResult<(PipelineWorker, AsyncGeneratorReceiver)> {
    let config = PipelineConfig {
        startup_timeout: WAIT,
        shutdown_timeout: WAIT,
        ..Default::default()
    };
    let worker = Pipeline::new(vec![processor.clone()])
        .start(config)
        .await
        .map_err(|error| error.to_string())?;
    match processor.take_receiver() {
        Some(receiver) => Ok((worker, receiver)),
        None => {
            let joined = finish(
                worker,
                Frame::cancel("codec fixture missing receiver"),
                &mut Vec::new(),
            )
            .await;
            Err(format!("setup produced no receiver; joined: {joined:?}"))
        }
    }
}

fn inputs(generation: usize, tag: &LocalFrameTag) -> Vec<DirectedFrame> {
    let mut frames = Vec::new();
    for (index, direction) in [Direction::Downstream, Direction::Upstream]
        .into_iter()
        .enumerate()
    {
        let message = json!({
            "label": "rtvi-ai",
            "type": "bot-llm-text",
            "data": { "text": format!("run {generation}, direction {index}: 你好") }
        });
        let mut audio = AudioRawFrame::new(
            vec![0, generation as u8, 0xff, 0x7f, 0, 0x80, 0x34, 0x12],
            if index == 0 { 16_000 } else { 48_000 },
            if index == 0 { 1 } else { 2 },
        )
        .unwrap();
        audio.context_id = Some(format!("run-{generation}-audio-{index}"));
        for data in [
            FrameData::OutputTransportMessage(message),
            FrameData::Text(TextFrame::new("ignored by the RTVI codec")),
            FrameData::TtsAudio(audio),
        ] {
            let mut frame = Frame::new(data);
            frame.pts = Some(123_000 + frames.len() as u64);
            frame.metadata.insert(
                "fixture".into(),
                json!({"generation": generation, "direction": index, "nested": [1, "元"]}),
            );
            frame.transport_source = Some("native-input".into());
            frame.transport_destination = Some("native-output".into());
            frame.broadcast_sibling_id = Some(900 + frames.len() as u64);
            frame.interruptible = false;
            tag.mark(&mut frame).unwrap();
            frames.push(DirectedFrame::new(frame, direction));
        }
    }
    frames
}

async fn submit(
    worker: &mut PipelineWorker,
    receiver: &AsyncGeneratorReceiver,
    inputs: &[DirectedFrame],
    observed: &mut Vec<DirectedFrame>,
) -> TestResult<()> {
    for input in inputs {
        bounded(
            "frame admission",
            worker.sender().send(input.frame.clone(), input.direction),
        )
        .await?;
        timeout(WAIT, async {
            loop {
                let output = worker.recv().await.ok_or("pipeline closed before input")?;
                let found = output.frame.id == input.frame.id;
                observed.push(output);
                if found {
                    return Ok::<(), String>(());
                }
            }
        })
        .await
        .map_err(|_| "forwarded input deadline".to_owned())??;
    }
    // A boundary receipt can precede serialization. Wait for actual wire
    // admission before Cancel, so cancellation cannot race pending input work.
    timeout(WAIT, async {
        while receiver.snapshot().queued_messages != 4 {
            if receiver.is_closed() {
                return Err("wire stream closed before all four payloads".to_owned());
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await
    .map_err(|_| "serialized payload admission deadline".to_owned())?
}

async fn drain(receiver: &mut AsyncGeneratorReceiver) -> TestResult<Vec<WireMessage>> {
    timeout(WAIT, async {
        let mut wire = Vec::new();
        while let Some(message) = receiver.recv().await {
            wire.push(message);
        }
        wire
    })
    .await
    .map_err(|_| "wire EOF deadline".to_owned())
}

fn assert_output(
    inputs: &[DirectedFrame],
    observed: &[DirectedFrame],
    terminal: &Frame,
    tag: &LocalFrameTag,
) {
    assert_eq!(
        observed.len(),
        inputs.len() + 2,
        "inputs, Start and terminal"
    );
    for expected in inputs {
        let matches: Vec<_> = observed
            .iter()
            .filter(|output| output.frame.id == expected.frame.id)
            .collect();
        assert_eq!(matches.len(), 1, "each original frame forwards once");
        assert_eq!(matches[0], expected, "identity, metadata and direction");
        assert_eq!(
            matches[0].frame.observation_token(),
            expected.frame.observation_token(),
            "preserve process-local identity too"
        );
        assert!(tag.contains(&matches[0].frame));
    }
    let terminals: Vec<_> = observed
        .iter()
        .filter(|output| output.frame.id == terminal.id)
        .collect();
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].frame, *terminal);
    assert_eq!(terminals[0].direction, Direction::Downstream);
}

fn assert_wire(inputs: &[DirectedFrame], wire: &[WireMessage]) {
    let mut reference = RtviClientFrameSerializer::default();
    let expected: Vec<_> = inputs
        .iter()
        .filter_map(|input| reference.serialize(&input.frame).unwrap())
        .collect();
    assert_eq!(wire, expected, "exact production codec wire and ordering");
    assert_eq!(wire.len(), 4, "Text and lifecycle frames add no messages");

    // Independently inspect the JSON schema and base64 bytes; equality against
    // the same serializer alone would not establish the actual payload contract.
    let mut messages = wire.iter();
    let mut audio_id = 0;
    for input in inputs {
        if matches!(input.frame.data, FrameData::Text(_)) {
            continue;
        }
        let WireMessage::Text(text) = messages.next().unwrap() else {
            panic!("the RTVI trait codec emits text JSON");
        };
        let value: Value = serde_json::from_str(text).unwrap();
        assert_eq!(value["label"], "rtvi-ai");
        match &input.frame.data {
            FrameData::OutputTransportMessage(message) => assert_eq!(&value, message),
            FrameData::TtsAudio(audio) => {
                audio_id += 1;
                assert_eq!(value["type"], "raw-audio");
                assert_eq!(value["id"], audio_id.to_string());
                assert_eq!(value["data"]["sampleRate"], audio.sample_rate);
                assert_eq!(value["data"]["numChannels"], audio.num_channels);
                assert_eq!(
                    STANDARD
                        .decode(value["data"]["base64Audio"].as_str().unwrap())
                        .unwrap(),
                    audio.audio
                );
            }
            _ => panic!("unexpected fixture input"),
        }
    }
    assert!(messages.next().is_none());
    assert_eq!(audio_id, 2);
}

#[tokio::test]
async fn default_facade_streams_real_rtvi_and_isolates_reused_end_cancel_runs() {
    let trace = Arc::new(CodecTrace::default());
    let factory: AsyncGeneratorSerializerFactory = Arc::new({
        let trace = trace.clone();
        move || {
            let generation = trace.created.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Box::new(TracedRtvi {
                inner: RtviClientFrameSerializer::default(),
                generation,
                trace: trace.clone(),
            }))
        }
    });
    let processor = Arc::new(
        AsyncGeneratorProcessor::new(
            AsyncGeneratorProcessorConfig {
                max_queued_messages: 8,
                max_message_bytes: 4096,
                max_queued_bytes: 32_768,
            },
            factory,
        )
        .unwrap(),
    );
    assert!(processor.take_receiver().is_none());
    assert!(processor.current_snapshot().is_none());
    assert_eq!(trace.created.load(Ordering::SeqCst), 0);
    let tag = LocalFrameTag::new();
    let first_inputs = inputs(1, &tag);
    let second_inputs = inputs(2, &tag);
    let end = Frame::end();
    let cancel = Frame::cancel("RTVI codec reuse complete");

    let (mut first_worker, mut first_receiver) = start(&processor).await.unwrap();
    let first_claim_is_unique = processor.take_receiver().is_none();
    let mut first_observed = Vec::new();
    let first_scenario = submit(
        &mut first_worker,
        &first_receiver,
        &first_inputs,
        &mut first_observed,
    )
    .await;
    let first_report = finish(
        first_worker,
        if first_scenario.is_ok() {
            end.clone()
        } else {
            Frame::cancel("first codec scenario failed")
        },
        &mut first_observed,
    )
    .await;
    // No assertions while this run still owns a live worker.
    let first_report = first_report.expect("End and join first run");
    assert!(first_scenario.is_ok(), "{first_scenario:?}");
    assert!(first_claim_is_unique);
    assert_eq!(first_report.state, WorkerState::Ended);
    assert!(first_report.errors.is_empty(), "{:?}", first_report.errors);
    assert!(first_receiver.is_closed());
    assert!(first_receiver.error_reason().is_none());
    assert_eq!(first_receiver.snapshot().queued_messages, 4);
    assert_output(&first_inputs, &first_observed, &end, &tag);

    // Leave the first generation's closed queue unread until the second
    // generation is active. Its receiver must not acquire the new queue.
    let (mut second_worker, mut second_receiver) = start(&processor).await.unwrap();
    let second_claim_is_unique = processor.take_receiver().is_none();
    let mut second_observed = Vec::new();
    let second_scenario = submit(
        &mut second_worker,
        &second_receiver,
        &second_inputs,
        &mut second_observed,
    )
    .await;
    let first_wire = drain(&mut first_receiver).await;
    let old_eof_while_new_active = timeout(WAIT, first_receiver.recv()).await;
    let new_queue_before_cancel = second_receiver.snapshot();
    let current_before_cancel = processor.current_snapshot();
    let second_report = finish(second_worker, cancel.clone(), &mut second_observed).await;
    let second_wire = drain(&mut second_receiver).await;
    let second_report = second_report.expect("Cancel and join second run");
    assert!(second_scenario.is_ok(), "{second_scenario:?}");
    assert!(second_claim_is_unique);
    assert_eq!(second_report.state, WorkerState::Cancelled);
    assert!(
        second_report.errors.is_empty(),
        "{:?}",
        second_report.errors
    );
    assert!(matches!(old_eof_while_new_active, Ok(None)));
    assert_eq!(new_queue_before_cancel.queued_messages, 4);
    assert!(!new_queue_before_cancel.closed);
    assert!(new_queue_before_cancel.receiver_taken);
    assert_eq!(current_before_cancel, Some(new_queue_before_cancel));
    assert!(second_receiver.is_closed());
    assert!(second_receiver.error_reason().is_none());
    assert_eq!(first_receiver.snapshot().queued_messages, 0);
    assert_eq!(second_receiver.snapshot().queued_messages, 0);
    assert_eq!(first_receiver.snapshot().queued_bytes, 0);
    assert_eq!(second_receiver.snapshot().queued_bytes, 0);
    assert_eq!(
        processor.current_snapshot(),
        Some(second_receiver.snapshot())
    );
    assert_eq!(trace.created.load(Ordering::SeqCst), 2);
    assert_output(&second_inputs, &second_observed, &cancel, &tag);
    assert_wire(
        &first_inputs,
        &first_wire.expect("first receiver drains to EOF"),
    );
    assert_wire(
        &second_inputs,
        &second_wire.expect("second receiver drains to EOF"),
    );

    let calls = trace.calls.lock().unwrap();
    for (generation, inputs, observed) in [
        (1, &first_inputs, &first_observed),
        (2, &second_inputs, &second_observed),
    ] {
        let mut actual: Vec<_> = calls
            .iter()
            .filter_map(|(run, id)| (*run == generation).then_some(*id))
            .collect();
        let mut expected: Vec<_> = inputs.iter().map(|input| input.frame.id).collect();
        expected.extend(observed.iter().filter_map(|output| {
            matches!(output.frame.data, FrameData::Start(_)).then_some(output.frame.id)
        }));
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected, "Start and inputs serialize exactly once");
    }
    assert!(calls
        .iter()
        .all(|(_, id)| *id != end.id && *id != cancel.id));
}
