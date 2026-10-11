mod review_observations {
    use super::*;
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    async fn vad_at(harness: &Harness, speaking: bool, millis: u64) {
        let vad = VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        };
        let mut frame = Frame::new(if speaking {
            FrameData::VadUserStartedSpeaking(vad)
        } else {
            FrameData::VadUserStoppedSpeaking(vad)
        });
        frame
            .metadata
            .insert("cartesia-exact-time-ms".into(), millis.into());
        harness.send_applied(frame).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn review_resumed_vad_rearms_deadline_without_reviving_cancelled_prediction() {
        for updates in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("Only the final answer may escape."));
            let mut user_config = config(true);
            user_config.turn.stop_timeout = Duration::from_secs(5);
            user_config.eager = Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(60), 65536)
                    .unwrap(),
            );
            let capacity = ws.client.config().limits.queue_capacity;
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                user_config,
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            let outcome = AssertUnwindSafe(async {
            harness.start().await;
            let source_turn = harness.turn();
            harness.predict("Withdraw this first prediction").await;
            let adopted = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if harness.after.frames.lock().unwrap().iter().any(|frame| frame.frame.native_speculation().is_some()) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await;
            if adopted.is_err() {
                return Err("review-vad-initial-prediction: actual adoption deadline".to_string());
            }
            let observed = harness.settle_or_observe_pending_request(
                &http, 0, "review-vad-initial-prediction",
            ).await;
            observed?;
            let prediction = harness.prediction();
            let capability = harness.after.capability();
            let initial_status = capability.status();
            vad_at(&harness, false, 1000).await;
            let first_deadline = harness.pair.user().deadline().unwrap();
            harness.connection.event(json!({"type":"turn.resume"})).await;
            let cancel_applied = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let cancel_id = harness.before.frames.lock().unwrap().iter().find_map(|frame| {
                        matches!(frame.frame.native_eager_source(), Some(NativeEagerSource::Cancel(p)) if p == &prediction)
                            .then_some(frame.frame.id)
                    });
                    if cancel_id.is_some_and(|id| harness.user.applied.lock().unwrap().contains(&id)) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await;
            if cancel_applied.is_err() {
                return Err("review-vad-resume: exact Cancel application deadline".to_string());
            }
            let cancelled = capability.status() == SpeculationStatus::Cancelled && !prediction.is_live();
            if !cancelled {
                return Err("review-vad-resume: completed Cancel left the first prediction live".to_string());
            }
            harness.barrier(true).await;
            vad_at(&harness, true, 2000).await;
            let resumed_deadline = harness.pair.user().deadline().unwrap();
            vad_at(&harness, false, 3000).await;
            let second_deadline = harness.pair.user().deadline().unwrap();
            harness.advance(6000).await;
            settle_update_prefix(&mut harness, capacity, updates, 81).await;
            let at_old_edge = (http.count(), harness.pair.user().deadline().unwrap());
            harness.advance(7999).await;
            settle_update_prefix(&mut harness, capacity, updates, 82).await;
            let before_final = (
                http.count(), harness.pair.user().deadline().unwrap(),
                source_turn.is_active(), harness.user_history(),
                harness.before.count(|data| matches!(data, FrameData::InterimTranscription(_))),
            );
            harness.end_turn("Final after local speech resumes").await;
            wait(|| harness.before.frames.lock().unwrap().iter().any(|frame| {
                matches!(frame.frame.native_eager_source(), Some(NativeEagerSource::Stop { turn, commit: Some(_) }) if turn == &source_turn)
            })).await;
            harness.barrier(true).await;
            harness.advance(16000).await;
            let final_count = http.count();
            let history = harness.user_history();
            let visible = harness.completion_outputs();
            let starts = harness.before.frames.lock().unwrap().iter().filter(|frame| {
                matches!(frame.frame.native_eager_source(), Some(NativeEagerSource::Start(_)))
            }).count();
            let bodies = received_bodies(&mut http);
            Ok((initial_status, first_deadline, resumed_deadline, second_deadline,
                at_old_edge, before_final, final_count, history, visible, starts, bodies))
            }).catch_unwind().await;
            let cleanup = harness
                .finish_pending_fixture(&mut ws, &mut http, false)
                .await;
            let (
                initial_status,
                first_deadline,
                resumed_deadline,
                second_deadline,
                at_old_edge,
                before_final,
                final_count,
                history,
                visible,
                starts,
                bodies,
            ) = match outcome {
                Ok(Ok(snapshot)) => snapshot,
                Ok(Err(error)) => panic!("{error}; review-vad cleanup: {cleanup:?}"),
                Err(error) => {
                    assert!(
                        cleanup.is_empty(),
                        "review-vad schedule panicked; cleanup: {cleanup:?}"
                    );
                    std::panic::resume_unwind(error)
                }
            };
            assert!(cleanup.is_empty(), "review-vad cleanup: {cleanup:?}");
            assert_eq!(initial_status, SpeculationStatus::Pending);
            assert_eq!(first_deadline, Some(Duration::from_millis(6000)));
            assert_eq!(resumed_deadline, None);
            assert_eq!(second_deadline, Some(Duration::from_millis(8000)));
            assert_eq!(at_old_edge, (1, Some(Duration::from_millis(8000))));
            assert_eq!(before_final.0, 1);
            assert_eq!(before_final.1, Some(Duration::from_millis(8000)));
            assert!(before_final.2 && before_final.3.is_empty());
            assert_eq!(before_final.4, 0);
            assert_eq!(
                starts, 1,
                "local VAD resumption must not forge a provider turn"
            );
            assert_eq!(final_count, 2);
            assert_eq!(bodies.len(), 2);
            assert_eq!(
                bodies[1]["messages"][0]["content"],
                "Final after local speech resumes"
            );
            assert_eq!(history, ["Final after local speech resumes"]);
            assert_eq!(visible, 1);
        }
    }
}
