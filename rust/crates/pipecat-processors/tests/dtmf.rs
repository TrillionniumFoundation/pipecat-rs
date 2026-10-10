//! Native pipeline fixtures derived from the pinned DTMFAggregator contract.
use pipecat_core::*;
use pipecat_processors::{DtmfAggregator, DtmfAggregatorConfig};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

async fn start(config: DtmfAggregatorConfig) -> (Arc<DtmfAggregator>, PipelineWorker) {
    let aggregator = Arc::new(DtmfAggregator::new(config).unwrap());
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    (aggregator, worker)
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let mut marker = Frame::new(FrameData::Flush(FlushFrame {
        returning: false,
        origin: Some("dtmf-barrier".into()),
    }));
    marker.interruptible = false;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    let mut output = vec![];
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), worker.recv())
            .await
            .expect("barrier deadline")
            .expect("pipeline output");
        if matches!(next.frame.data, FrameData::Flush(_)) {
            return output;
        }
        output.push(next);
    }
}
async fn send(worker: &PipelineWorker, key: KeypadEntry) {
    worker
        .sender()
        .send(Frame::new(FrameData::InputDtmf(key)), Direction::Downstream)
        .await
        .unwrap();
}
fn transcripts(output: &[DirectedFrame]) -> Vec<&str> {
    output
        .iter()
        .filter_map(|item| match &item.frame.data {
            FrameData::TranscriptionDetailed(frame) => {
                assert_eq!(item.direction, Direction::Downstream);
                assert!(frame.finalized);
                assert!(frame.user_id.is_empty());
                assert!(frame.language.is_none());
                assert!(frame.result.is_none());
                assert_eq!(frame.timestamp.len(), 32);
                assert_eq!(&frame.timestamp[10..11], "T");
                assert!(frame.timestamp.ends_with("+00:00"));
                Some(frame.text.text.as_str())
            }
            _ => None,
        })
        .collect()
}
fn interruptions(output: &[DirectedFrame], direction: Direction) -> usize {
    output
        .iter()
        .filter(|item| {
            item.direction == direction && matches!(item.frame.data, FrameData::Interruption)
        })
        .count()
}
async fn end(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.1.state, WorkerState::Ended);
    result
}
#[test]
fn configuration_is_bounded_and_telephone_only() {
    assert!(DtmfAggregator::new(Default::default()).is_ok());
    for config in [
        DtmfAggregatorConfig {
            idle_timeout: Duration::ZERO,
            ..Default::default()
        },
        DtmfAggregatorConfig {
            idle_timeout: Duration::from_nanos(1),
            ..Default::default()
        },
        DtmfAggregatorConfig {
            idle_timeout: Duration::MAX,
            ..Default::default()
        },
        DtmfAggregatorConfig {
            max_sequence_duration: Duration::from_secs(1),
            ..Default::default()
        },
        DtmfAggregatorConfig {
            max_sequence_duration: Duration::MAX,
            ..Default::default()
        },
        DtmfAggregatorConfig {
            max_digits: 0,
            ..Default::default()
        },
        DtmfAggregatorConfig {
            max_digits: 4097,
            ..Default::default()
        },
        DtmfAggregatorConfig {
            prefix: "é".repeat(2049),
            ..Default::default()
        },
        DtmfAggregatorConfig {
            termination_digit: KeypadEntry::A,
            ..Default::default()
        },
    ] {
        assert!(DtmfAggregator::new(config).is_err());
    }
    let config = DtmfAggregatorConfig {
        prefix: "é".repeat(2048),
        max_digits: 4096,
        ..Default::default()
    };
    assert!(DtmfAggregator::new(config).is_ok());
}
#[tokio::test(start_paused = true)]
async fn source_fixtures_preserve_digits_order_direction_and_final_payload() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/dtmf.json")).unwrap();
    for case in cases {
        let (_, mut worker) = start(DtmfAggregatorConfig {
            prefix: case["prefix"].as_str().unwrap().into(),
            termination_digit: serde_json::from_value(case["terminator"].clone()).unwrap(),
            ..Default::default()
        })
        .await;
        for key in case["keys"].as_array().unwrap() {
            send(&worker, serde_json::from_value(key.clone()).unwrap()).await;
        }
        let output = barrier(&mut worker).await;
        assert_eq!(
            transcripts(&output),
            [case["expected"].as_str().unwrap()],
            "{}",
            case["name"]
        );
        assert_eq!(interruptions(&output, Direction::Downstream), 1);
        assert_eq!(interruptions(&output, Direction::Upstream), 1);
        assert!(matches!(output[0].frame.data, FrameData::InputDtmf(_)));
        let last_key = output
            .iter()
            .rposition(|item| matches!(item.frame.data, FrameData::InputDtmf(_)))
            .unwrap();
        let final_index = output
            .iter()
            .position(|item| matches!(item.frame.data, FrameData::TranscriptionDetailed(_)))
            .unwrap();
        assert!(last_key < final_index);
        assert!(end(worker).await.1.errors.is_empty());
    }
}
#[tokio::test(start_paused = true)]
async fn upstream_key_keeps_identity_but_transcription_is_downstream() {
    let (_, mut worker) = start(Default::default()).await;
    let frame = Frame::new(FrameData::InputDtmf(KeypadEntry::Pound));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Upstream)
        .await
        .unwrap();
    let output = barrier(&mut worker).await;
    assert_eq!(output[0].frame.id, id);
    assert_eq!(output[0].direction, Direction::Upstream);
    assert_eq!(transcripts(&output), ["DTMF: #"]);
    end(worker).await;
}
#[tokio::test(start_paused = true)]
async fn idle_timer_resets_and_fires_without_another_input() {
    let (aggregator, mut worker) = start(Default::default()).await;
    send(&worker, KeypadEntry::One).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    tokio::time::advance(Duration::from_millis(1500)).await;
    send(&worker, KeypadEntry::Two).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    tokio::time::advance(Duration::from_millis(1500)).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    assert_eq!(aggregator.buffered_len().await, 2);
    tokio::time::advance(Duration::from_millis(500)).await;
    let frame = worker.recv().await.unwrap();
    assert_eq!(transcripts(&[frame]), ["DTMF: 12"]);
    assert_eq!(aggregator.buffered_len().await, 0);
    tokio::time::advance(Duration::from_secs(100)).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    send(&worker, KeypadEntry::Three).await;
    let output = barrier(&mut worker).await;
    assert_eq!(interruptions(&output, Direction::Downstream), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(transcripts(&[worker.recv().await.unwrap()]), ["DTMF: 3"]);
    end(worker).await;
}
#[tokio::test(start_paused = true)]
async fn maximum_lifetime_cannot_be_extended_by_repeated_digits() {
    let (_, mut worker) = start(DtmfAggregatorConfig {
        max_sequence_duration: Duration::from_secs(3),
        ..Default::default()
    })
    .await;
    for _ in 0..3 {
        send(&worker, KeypadEntry::Zero).await;
        assert!(transcripts(&barrier(&mut worker).await).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(transcripts(&[worker.recv().await.unwrap()]), ["DTMF: 000"]);
    end(worker).await;
}
#[tokio::test(start_paused = true)]
async fn terminator_at_deadline_never_duplicates_previous_entry() {
    let (_, mut worker) = start(Default::default()).await;
    send(&worker, KeypadEntry::One).await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    send(&worker, KeypadEntry::Pound).await;
    let output = barrier(&mut worker).await;
    assert_eq!(transcripts(&output), ["DTMF: 1", "DTMF: #"]);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    end(worker).await;
}
#[tokio::test(start_paused = true)]
async fn repeated_terminators_and_many_entries_reuse_one_owned_task() {
    let (_, mut worker) = start(Default::default()).await;
    for _ in 0..200 {
        send(&worker, KeypadEntry::Pound).await;
    }
    let output = barrier(&mut worker).await;
    assert_eq!(transcripts(&output).len(), 200);
    assert_eq!(interruptions(&output, Direction::Downstream), 200);
    assert_eq!(interruptions(&output, Direction::Upstream), 200);
    assert!(end(worker).await.1.errors.is_empty());
}
#[tokio::test(start_paused = true)]
async fn capacity_and_invalid_key_discard_entire_entry_without_phantom_text() {
    let (aggregator, mut worker) = start(DtmfAggregatorConfig {
        max_digits: 2,
        ..Default::default()
    })
    .await;
    for key in [KeypadEntry::One, KeypadEntry::Two, KeypadEntry::Three] {
        send(&worker, key).await;
    }
    let output = barrier(&mut worker).await;
    assert!(transcripts(&output).is_empty());
    assert_eq!(aggregator.buffered_len().await, 0);
    send(&worker, KeypadEntry::One).await;
    send(&worker, KeypadEntry::A).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    send(&worker, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&barrier(&mut worker).await), ["DTMF: #"]);
    let (_, report) = end(worker).await;
    assert_eq!(report.errors.len(), 2);
    assert_eq!(aggregator.buffered_len().await, 0);
}
#[tokio::test(start_paused = true)]
async fn external_interruption_discards_pending_timer_and_old_queued_digits() {
    let (aggregator, mut worker) = start(Default::default()).await;
    send(&worker, KeypadEntry::One).await;
    barrier(&mut worker).await;
    // Both system frames have already been admitted when the interruption
    // fences the queue. The old key must not seed a replacement sequence.
    send(&worker, KeypadEntry::Two).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    assert_eq!(aggregator.buffered_len().await, 0);
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    send(&worker, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&barrier(&mut worker).await), ["DTMF: #"]);
    end(worker).await;
}
#[tokio::test(start_paused = true)]
async fn empty_and_end_detailed_flush_only_admitted_pending_text() {
    let (_, worker) = start(Default::default()).await;
    assert!(transcripts(&end(worker).await.0).is_empty());
    let (_, mut worker) = start(Default::default()).await;
    send(&worker, KeypadEntry::One).await;
    barrier(&mut worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::EndDetailed(ReasonFrame {
                reason: Some("done".into()),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (output, report) = worker.collect().await.unwrap();
    assert_eq!(transcripts(&output), ["DTMF: 1"]);
    assert!(matches!(
        output.last().unwrap().frame.data,
        FrameData::EndDetailed(_)
    ));
    assert_eq!(report.state, WorkerState::Ended);
}
#[tokio::test(start_paused = true)]
async fn cancel_stop_and_reuse_never_flush_or_retain_old_sequence() {
    let aggregator = Arc::new(DtmfAggregator::default());
    for terminal in [
        Frame::cancel("test"),
        Frame::new(FrameData::CancelDetailed(ReasonFrame {
            reason: Some("test".into()),
        })),
        Frame::new(FrameData::Stop),
    ] {
        let mut worker = Pipeline::new(vec![aggregator.clone()])
            .start(Default::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        send(&worker, KeypadEntry::One).await;
        barrier(&mut worker).await;
        worker
            .sender()
            .send(terminal, Direction::Downstream)
            .await
            .unwrap();
        let (output, report) = tokio::time::timeout(Duration::from_millis(100), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            report.state,
            WorkerState::Cancelled | WorkerState::Stopped
        ));
        assert!(transcripts(&output).is_empty());
        assert_eq!(aggregator.buffered_len().await, 0);
    }
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(transcripts(&barrier(&mut worker).await).is_empty());
    send(&worker, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&barrier(&mut worker).await), ["DTMF: #"]);
    end(worker).await;
}
#[tokio::test]
async fn real_clock_shutdown_and_drop_are_bounded_even_with_long_timer() {
    let (aggregator, mut worker) = start(DtmfAggregatorConfig {
        idle_timeout: Duration::from_secs(3600),
        max_sequence_duration: Duration::from_secs(3600),
        ..Default::default()
    })
    .await;
    send(&worker, KeypadEntry::One).await;
    barrier(&mut worker).await;
    let started = std::time::Instant::now();
    let (output, _) = end(worker).await;
    assert_eq!(transcripts(&output), ["DTMF: 1"]);
    assert!(started.elapsed() < Duration::from_secs(2));
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    send(&worker, KeypadEntry::Two).await;
    barrier(&mut worker).await;
    drop(worker);
    tokio::time::timeout(Duration::from_secs(2), async {
        while aggregator.buffered_len().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn duplicate_start_is_rejected_without_resetting_its_entry() {
    let (_, mut worker) = start(Default::default()).await;
    send(&worker, KeypadEntry::One).await;
    barrier(&mut worker).await;
    assert_eq!(
        worker
            .sender()
            .send(
                Frame::new(FrameData::Start(StartFrame::default())),
                Direction::Downstream
            )
            .await,
        Err(PipelineError::DuplicateStart)
    );
    barrier(&mut worker).await;
    send(&worker, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&barrier(&mut worker).await), ["DTMF: 1#"]);
    assert!(end(worker).await.1.errors.is_empty());
}
#[tokio::test]
async fn full_output_fails_explicitly_and_still_cleans_timer() {
    let aggregator = Arc::new(DtmfAggregator::default());
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(PipelineConfig {
            output_capacity: 1,
            shutdown_timeout: Duration::from_millis(250),
            ..Default::default()
        })
        .await
        .unwrap();
    worker.recv().await.unwrap();
    send(&worker, KeypadEntry::One).await;
    assert!(tokio::time::timeout(Duration::from_secs(2), worker.join())
        .await
        .unwrap()
        .is_err());
    assert_eq!(aggregator.buffered_len().await, 0);
}

#[tokio::test(start_paused = true)]
async fn text_and_output_dtmf_are_passthrough_not_keypad_input() {
    let (aggregator, mut worker) = start(Default::default()).await;
    let text = Frame::text("001#");
    let id = text.id;
    worker
        .sender()
        .send(text, Direction::Downstream)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputDtmf(KeypadEntry::One)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let output = barrier(&mut worker).await;
    assert!(output.iter().any(|frame| frame.frame.id == id));
    assert_eq!(aggregator.buffered_len().await, 0);
    assert!(transcripts(&output).is_empty());
    assert_eq!(interruptions(&output, Direction::Downstream), 0);
    end(worker).await;
}
