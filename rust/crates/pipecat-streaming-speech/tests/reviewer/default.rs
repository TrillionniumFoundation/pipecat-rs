mod review_default {
    use super::*;
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    async fn finish_review(
        mut harness: Harness,
        ws: &mut WsPeer,
        http: &mut HttpPeer,
    ) -> Vec<String> {
        async fn drain(worker: &mut PipelineWorker) -> Result<(), String> {
            while worker.recv().await.is_some() {}
            let report = worker.join().await.map_err(|error| error.to_string())?;
            if report.errors.is_empty() {
                Ok(())
            } else {
                Err(format!("worker errors: {:?}", report.errors))
            }
        }
        let mut failures = Vec::new();
        match tokio::time::timeout(Duration::from_secs(5), harness.worker.end()).await {
            Ok(Ok(())) => (),
            Ok(Err(error)) => failures.push(format!("End enqueue: {error}")),
            Err(_) => failures.push("End enqueue deadline".into()),
        }
        match tokio::time::timeout(Duration::from_secs(5), drain(&mut harness.worker)).await {
            Ok(Ok(())) => (),
            Ok(Err(error)) => failures.push(format!("worker join: {error}")),
            Err(_) => {
                failures.push("worker join deadline".into());
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    harness.worker.cancel("review default cleanup"),
                )
                .await;
                match tokio::time::timeout(Duration::from_secs(5), drain(&mut harness.worker)).await
                {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => failures.push(format!("cancelled worker join: {error}")),
                    Err(_) => failures.push("cancelled worker join deadline".into()),
                }
            }
        }
        if let Some(stop) = ws.stop.take() {
            let _ = stop.send(());
        }
        if let Some(stop) = http.stop.take() {
            let _ = stop.send(());
        }
        for (name, task) in [("WebSocket", &mut ws.task), ("HTTP", &mut http.task)] {
            match tokio::time::timeout(Duration::from_secs(5), &mut *task).await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("{name} join: {error}")),
                Err(_) => {
                    failures.push(format!("{name} child joins not established"));
                    task.abort();
                    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
                }
            }
        }
        failures
    }

    #[tokio::test(flavor = "current_thread")]
    async fn review_default_restart_keeps_interims_and_committed_history_without_capabilities() {
        for eager_consumer in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("Committed default answer."));
            let user_config = UserAggregatorConfig {
                eager: eager_consumer.then(EagerUserTurnConfig::default),
                ..Default::default()
            };
            let mut harness = Harness::new(
                &mut ws,
                &http,
                false,
                user_config,
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            let outcome = AssertUnwindSafe(async {
            let mut partial_counts = Vec::new();
            let mut requests_before_finals = Vec::new();
            for (index, text) in ["Default before restart", "Default after restart"].into_iter().enumerate() {
                if index != 0 {
                    let source = harness.source.processor.clone();
                    let context = harness.source.context.lock().unwrap().clone().unwrap();
                    let restart = tokio::spawn(async move {
                        let mut settings = SttSettings::default();
                        settings.base.extra.insert("keyterm".into(), json!(["review-default-restart"]));
                        source.restart_with_settings(&settings, &context).await
                    });
                    harness.connection = ws.connection().await;
                    within(restart).await.unwrap().unwrap();
                }
                harness.start().await;
                let partial = format!("partial-{index}");
                harness.connection.batch(vec![
                    json!({"type":"turn.eager_end","transcript":"never speculative"}),
                    json!({"type":"turn.resume"}),
                    json!({"type":"turn.update","transcript":partial}),
                ]).await;
                wait(|| harness.before.count(|data| matches!(data, FrameData::InterimTranscription(frame) if frame.text == partial)) == 1).await;
                harness.barrier(true).await;
                partial_counts.push(harness.before.count(|data| matches!(data, FrameData::InterimTranscription(_))));
                requests_before_finals.push(http.count());
                harness.connection.event(json!({"type":"turn.end","transcript":text})).await;
                wait(|| http.count() == index + 1).await;
                harness.barrier(true).await;
            }
            let history = harness.user_history();
            let count = http.count();
            let eager_frames = harness.before.count(|data| matches!(data, FrameData::EagerTranscription(_) | FrameData::EagerEndOfTurnCancel));
            let private_carriers = harness.before.frames.lock().unwrap().iter().any(|frame| frame.frame.native_eager_source().is_some());
            let adopted = harness.after.frames.lock().unwrap().iter().any(|frame| frame.frame.native_speculation().is_some());
            let answers = harness.output.count(|data| matches!(data, FrameData::LlmText(text) if text.text == "Committed default answer."));
            let bodies: Vec<_> = std::iter::from_fn(|| http.requests.try_recv().ok().map(|(body, _)| body)).collect();
            (partial_counts, requests_before_finals, history, count, eager_frames,
                private_carriers, adopted, answers, bodies)
            }).catch_unwind().await;
            let cleanup = finish_review(harness, &mut ws, &mut http).await;
            let (
                partial_counts,
                requests_before_finals,
                history,
                count,
                eager_frames,
                private_carriers,
                adopted,
                answers,
                bodies,
            ) = match outcome {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    assert!(
                        cleanup.is_empty(),
                        "review-default schedule panicked; cleanup: {cleanup:?}"
                    );
                    std::panic::resume_unwind(error)
                }
            };
            assert!(cleanup.is_empty(), "review-default cleanup: {cleanup:?}");
            assert_eq!(partial_counts, [1, 2]);
            assert_eq!(requests_before_finals, [0, 1]);
            assert_eq!(count, 2);
            assert_eq!(history, ["Default before restart", "Default after restart"]);
            assert_eq!(eager_frames, 0);
            assert!(!private_carriers && !adopted);
            assert_eq!(answers, 2);
            assert_eq!(bodies.len(), 2);
            assert_eq!(
                bodies[1]["messages"][0]["content"],
                "Default before restart"
            );
            assert_eq!(bodies[1]["messages"][2]["content"], "Default after restart");
        }
    }
}
