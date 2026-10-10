use super::*;

// The unguarded diagnostic is preserved separately. With the explicit adapter,
// accepted downstream readiness must precede backend submission and the single
// receipt must produce exactly one completion using that source timestamp.
#[cfg(feature = "assistant-history")]
#[tokio::test]
async fn receipt_cannot_turn_one_delayed_assistant_start_into_two_completions() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let mut records = assistant.subscribe_stopped(Default::default()).unwrap();
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(history.source_boundary("independent-source").unwrap());
    let backend = Arc::new(Receipts::new(true, None, None));
    let output = Arc::new(
        OutputTransport::new(
            "independent-output",
            config(),
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let hold = Arc::new(HoldFirstSourceStart {
        entered: Notify::new(),
        gate: Semaphore::new(0),
        first: std::sync::atomic::AtomicBool::new(true),
    });
    let mut worker = Pipeline::new(vec![source, output, hold.clone(), assistant])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before_source = chrono::Utc::now();
    begin(&worker, "one-source-response").await;
    tokio::time::timeout(Duration::from_secs(3), hold.entered.notified())
        .await
        .unwrap();
    complete(&worker, "one-source-response").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), backend.wait_calls(1))
            .await
            .is_err()
    );
    assert_eq!(history.status().committed_contexts, 0);
    assert!(contents(&shared).is_empty());
    assert!(records.try_recv().is_none());
    let mut completed = Vec::new();
    hold.gate.add_permits(1);
    backend.wait_calls(1).await;
    let before_receipt = chrono::Utc::now();
    // The real Start has been reduced before backend submission. Hold its
    // trusted receipt longer so source-start and history-commit times differ.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let receipt_release = chrono::Utc::now();
    backend.gate.as_ref().unwrap().add_permits(1);
    let feedback = feedback_fence(&mut worker).await;
    while let Some(record) = records.try_recv() {
        completed.push(record.completion().clone());
    }
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
    while let Some(record) = records.recv().await {
        completed.push(record.completion().clone());
    }
    assert_eq!(contents(&shared), ["Heard"]);
    assert_eq!(
        completed.len(),
        1,
        "one source response and one trusted receipt must not produce an extra empty turn"
    );
    assert_eq!(completed[0].text(), "Heard");
    let turn_timestamps: Vec<_> = feedback
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::LlmContextAssistantTurn(turn) => Some(turn.timestamp.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(turn_timestamps, [completed[0].started_at(); 2]);
    let commit_timestamps: Vec<_> = feedback
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::LlmContextAssistantTimestamp { timestamp } => Some(timestamp.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(commit_timestamps.len(), 1);
    let started = chrono::DateTime::parse_from_rfc3339(completed[0].started_at()).unwrap();
    let committed = chrono::DateTime::parse_from_rfc3339(commit_timestamps[0]).unwrap();
    assert!(
        started >= before_source && started <= before_receipt,
        "completion retains the downstream source-start time before playback receipt release"
    );
    assert!(
        committed >= receipt_release && committed > started,
        "history commit follows the independently gated receipt"
    );
}
