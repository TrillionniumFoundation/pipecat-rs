use super::*;
async fn failure_fixture(
    downstream: bool,
) -> (
    PipelineWorker,
    Arc<ImmediatePlayed>,
    Arc<HoldBoundary>,
    SpokenHistory,
    SharedContext,
) {
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        Default::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(history.source_boundary("failure-source").unwrap());
    let backend = Arc::new(ImmediatePlayed::default());
    let output = Arc::new(
        OutputTransport::new(
            "failure-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let hold = HoldBoundary::new(HoldKind::SourceStart);
    let mut nodes: Vec<Arc<dyn FrameProcessor>> = vec![source, output];
    if downstream {
        nodes.extend([hold.clone() as Arc<dyn FrameProcessor>, assistant]);
    }
    let worker = Pipeline::new(nodes)
        .start(Default::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (worker, backend, hold, history, shared)
}
#[tokio::test]
async fn missing_downstream_assistant_fails_before_audio_and_end_drains() {
    let (worker, backend, _, history, shared) = failure_fixture(false).await;
    response(&worker, "missing", None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(history.status().committed_contexts, 0);
    assert!(shared.snapshot().unwrap().messages.is_empty());
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !report.errors.is_empty(),
        "missing readiness must be visible"
    );
}
#[tokio::test]
async fn timed_out_start_permanently_denies_already_queued_pcm_even_after_release() {
    let (worker, backend, hold, history, shared) = failure_fixture(true).await;
    response(&worker, "late", None).await;
    hold.wait_entered().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    hold.release.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(history.status().committed_contexts, 0);
    assert!(shared.snapshot().unwrap().messages.is_empty());
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(!report.errors.is_empty());
}
#[tokio::test]
async fn interruption_retires_pending_readiness_without_submitting_audio() {
    let (worker, backend, hold, history, shared) = failure_fixture(true).await;
    response(&worker, "interrupted", None).await;
    hold.wait_entered().await;
    send(&worker, Frame::interruption()).await;
    hold.release.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(history.status().committed_contexts, 0);
    assert!(shared.snapshot().unwrap().messages.is_empty());
    // End still joins the existing sender; no readiness dispatcher exists.
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn cancel_aborts_readiness_instead_of_waiting_for_the_downstream_gate() {
    let (worker, backend, hold, history, shared) = failure_fixture(true).await;
    response(&worker, "cancelled", None).await;
    hold.wait_entered().await;
    send(&worker, Frame::new(FrameData::Cancel(Default::default()))).await;
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(history.status().committed_contexts, 0);
    assert!(shared.snapshot().unwrap().messages.is_empty());
}

#[tokio::test]
async fn stop_drains_a_missing_readiness_without_orphaning_its_sender() {
    let (worker, backend, _, history, shared) = failure_fixture(false).await;
    response(&worker, "stopped", None).await;
    send(&worker, Frame::new(FrameData::Stop)).await;
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(!report.errors.is_empty());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(history.status().committed_contexts, 0);
    assert!(shared.snapshot().unwrap().messages.is_empty());
}
