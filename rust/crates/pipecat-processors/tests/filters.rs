use pipecat_core::*;
use pipecat_processors::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
async fn run(
    processor: Arc<dyn FrameProcessor>,
    frames: Vec<(Frame, Direction)>,
) -> (Vec<DirectedFrame>, WorkerReport) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let worker = Pipeline::new(vec![processor])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        for (frame, direction) in frames {
            worker.sender().send(frame, direction).await.unwrap();
        }
        worker.end().await.unwrap();
        worker.collect().await.unwrap()
    })
    .await
    .unwrap()
}
fn texts(frames: &[DirectedFrame]) -> Vec<&str> {
    frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect()
}
fn downstream(words: &[&str]) -> Vec<(Frame, Direction)> {
    words
        .iter()
        .map(|word| (Frame::text(*word), Direction::Downstream))
        .collect()
}
#[tokio::test]
async fn identity_keeps_frames_directions_and_ids() {
    let frame = Frame::text("up");
    let id = frame.id;
    let (out, report) = run(Arc::new(IdentityFilter), vec![(frame, Direction::Upstream)]).await;
    let up = out.iter().find(|f| f.frame.id == id).unwrap();
    assert_eq!(up.direction, Direction::Upstream);
    assert!(report.errors.is_empty());
}
#[tokio::test]
async fn null_blocks_data_but_preserves_lifecycle() {
    let (out, report) = run(Arc::new(NullFilter), downstream(&["discard"])).await;
    assert!(texts(&out).is_empty());
    assert!(out
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Start(_))));
    assert!(out
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::End(_))));
    assert!(report.errors.is_empty());
}
#[tokio::test]
async fn frame_filter_uses_predicate() {
    let filter =
        FrameFilter::new(|frame| matches!(&frame.data,FrameData::Text(t) if t.text=="yes"));
    let (out, _) = run(Arc::new(filter), downstream(&["no", "yes", "no"])).await;
    assert_eq!(texts(&out), vec!["yes"]);
}
#[tokio::test]
async fn function_filter_still_invokes_predicate_on_bypassed_frames() {
    let calls = Arc::new(AtomicUsize::new(0));
    let copy = calls.clone();
    let filter = FunctionFilter::new(Arc::new(move |_| {
        copy.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(false) })
    }));
    let (out, _) = run(
        Arc::new(filter),
        vec![
            (Frame::text("up"), Direction::Upstream),
            (Frame::text("down"), Direction::Downstream),
        ],
    )
    .await;
    assert_eq!(texts(&out), vec!["up"]);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
fn gate(capacity: usize) -> GatedAggregator {
    GatedAggregator::new(
        Arc::new(|frame| matches!(&frame.data,FrameData::Text(t) if t.text=="open")),
        Arc::new(|frame| matches!(&frame.data,FrameData::Text(t) if t.text=="close")),
        false,
        Direction::Downstream,
        capacity,
    )
    .unwrap()
}
#[tokio::test]
async fn opening_marker_precedes_all_buffered_frames_and_close_is_buffered() {
    let (out, report) = run(
        Arc::new(gate(10)),
        downstream(&["first", "open", "second", "close", "third", "open"]),
    )
    .await;
    assert_eq!(
        texts(&out),
        vec!["open", "first", "second", "open", "close", "third"]
    );
    assert!(report.errors.is_empty());
}
#[tokio::test]
async fn end_flushes_closed_gate_before_terminal() {
    let (out, report) = run(Arc::new(gate(10)), downstream(&["retained"])).await;
    assert_eq!(texts(&out), vec!["retained"]);
    assert!(matches!(out.last().unwrap().frame.data, FrameData::End(_)));
    assert!(report.errors.is_empty());
}
#[tokio::test]
async fn overflow_is_explicit_and_storage_stays_bounded() {
    let gate = Arc::new(gate(1));
    let (out, report) = run(gate.clone(), downstream(&["one", "overflow"])).await;
    assert_eq!(texts(&out), vec!["one"]);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(gate.buffered_len().unwrap(), 0);
}
#[tokio::test]
async fn interruption_removes_interruptible_buffered_frames() {
    let gate = Arc::new(gate(10));
    let mut worker = Pipeline::new(vec![gate.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    let mut frame = Frame::text("old");
    frame.interruptible = true;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while gate.buffered_len().unwrap() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ));
    assert_eq!(gate.buffered_len().unwrap(), 0);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
