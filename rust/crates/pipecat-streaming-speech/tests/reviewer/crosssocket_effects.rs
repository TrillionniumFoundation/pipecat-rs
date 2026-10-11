// Reviewer-authored source-only schedules. Included inside cartesia_turns_eager::eager.
// Each pair has two live sockets, independent user/LLM owners and real HTTP peers.
mod review_crosssocket_effects {
    use super::*;
    use futures_util::FutureExt;
    use std::{any::Any, panic::AssertUnwindSafe};

    fn require(ok: bool, message: impl Into<String>) -> Result<(), String> {
        if ok {
            Ok(())
        } else {
            Err(message.into())
        }
    }

    fn outcome_error(result: Result<Result<(), String>, Box<dyn Any + Send>>) -> Option<String> {
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(panic) => Some(
                panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_else(|| "fixture schedule panicked".into()),
            ),
        }
    }

    async fn carriers(h: &Harness) -> [Frame; 4] {
        // Receipt capture is a real provider-reader progression witness. The
        // receipt probe retains frames without blocking other pipeline traffic.
        h.before.paused().await;
        h.before.paused().await;
        [
            h.before
                .native(|s| matches!(s, NativeEagerSource::Start(_))),
            h.before
                .native(|s| matches!(s, NativeEagerSource::Prediction(_))),
            h.before
                .native(|s| matches!(s, NativeEagerSource::Final(_))),
            h.before
                .native(|s| matches!(s, NativeEagerSource::Stop { .. })),
        ]
    }

    async fn replay(h: &Harness, frames: &[Frame]) {
        for frame in frames {
            // New public frame IDs rule out accidental deduplication as the
            // reason the genuine, unchanged private authority was rejected.
            h.send_applied(new_id(frame.clone())).await;
        }
    }

    fn owner_signed(provider: Provider, owner: &str) -> (String, Value, usize) {
        let (response, expected, buffered) = signed_response(provider);
        let tag = |s: String| {
            s.replace("opaque+/", &format!("{owner}-opaque+/"))
                .replace("tail+/==", &format!("{owner}-tail+/=="))
                .replace("Ready.", &format!("{owner} Ready."))
                .replace("● exact ", &format!("{owner} ● exact "))
                .replace("closed", &format!("{owner} closed"))
        };
        (
            tag(response),
            serde_json::from_str(&tag(expected.to_string())).unwrap(),
            buffered,
        )
    }

    fn exact_signed_history(
        request: &Value,
        provider: Provider,
        user: &str,
        expected: &Value,
    ) -> bool {
        if provider == Provider::Gemini {
            request["contents"][0]["parts"] == json!([{ "text": user }])
                && request["contents"][1]["parts"] == *expected
                && request["contents"].as_array().is_some_and(|m| m.len() == 2)
        } else {
            request["messages"][0]["content"] == json!([{ "type": "text", "text": user }])
                && request["messages"][1]["content"] == *expected
                && request["messages"].as_array().is_some_and(|m| m.len() == 2)
        }
    }

    fn has_tool_result(request: &Value, id: &str) -> bool {
        request["messages"].as_array().is_some_and(|messages| {
            messages
                .iter()
                .any(|m| m["role"] == "tool" && m["tool_call_id"] == id)
        })
    }

    #[tokio::test]
    async fn simultaneous_identical_sockets_reject_crossed_authority_then_both_owners_release() {
        let mut ws_a = WsPeer::new().await;
        let mut ws_b = WsPeer::new().await;
        let mut http_a = HttpPeer::new().await;
        let mut http_b = HttpPeer::new().await;
        let a = Harness::new(
            &mut ws_a,
            &http_a,
            true,
            config(true),
            Some(PauseAt::Receipts),
            Provider::OpenAi,
            None,
        )
        .await;
        let b = Harness::new(
            &mut ws_b,
            &http_b,
            true,
            config(true),
            Some(PauseAt::Receipts),
            Provider::OpenAi,
            None,
        )
        .await;
        let outcome = AssertUnwindSafe(async {
            a.start().await;
            b.start().await;
            a.predict("identical wire transcript").await;
            b.predict("identical wire transcript").await;
            let (request_a, reply_a) = http_a.request().await;
            let (request_b, reply_b) = http_b.request().await;
            reply_a.send(answer("Answer owned by A.")).unwrap();
            reply_b.send(answer("Answer owned by B.")).unwrap();
            a.buffered(3).await;
            b.buffered(3).await;
            let cap_a = a.after.capability();
            let cap_b = b.after.capability();
            a.end_turn("identical wire transcript").await;
            b.end_turn("identical wire transcript").await;
            let frames_a = carriers(&a).await;
            let frames_b = carriers(&b).await;
            a.before.release();
            b.before.release();
            require(
                a.turn() != b.turn(),
                "distinct sockets shared native turn identity",
            )?;
            require(
                a.turn().is_live() && b.turn().is_live(),
                "cross-replay was not between live sources",
            )?;
            require(
                request_a["messages"] == request_b["messages"],
                "initial HTTP contexts did not collide exactly",
            )?;

            // Cross both already-claimed Starts and predictions, then receipts
            // in opposite orders while both sources remain genuinely live.
            replay(
                &a,
                &[
                    frames_b[0].clone(),
                    frames_b[1].clone(),
                    frames_b[3].clone(),
                    frames_b[2].clone(),
                ],
            )
            .await;
            replay(
                &b,
                &[
                    frames_a[0].clone(),
                    frames_a[1].clone(),
                    frames_a[2].clone(),
                    frames_a[3].clone(),
                ],
            )
            .await;
            a.pending_fixture_barrier(false, "crosssocket-A-crossed")
                .await?;
            b.pending_fixture_barrier(false, "crosssocket-B-crossed")
                .await?;
            require(
                cap_a.status() == SpeculationStatus::Pending
                    && cap_b.status() == SpeculationStatus::Pending,
                "foreign genuine authority changed a legitimate pending capability",
            )?;
            require(
                a.quiet() && b.quiet() && http_a.count() == 1 && http_b.count() == 1,
                "crossed receipts escaped into history, output or another request",
            )?;

            // The exact originals must still work for their own owners; B also
            // proves that an early own Stop is consumed only with its own Final.
            replay(&a, &[frames_a[2].clone(), frames_a[3].clone()]).await;
            replay(&b, &[frames_b[3].clone(), frames_b[2].clone()]).await;
            a.settle_or_observe_pending_request(&http_a, 0, "crosssocket-A-owned")
                .await?;
            b.settle_or_observe_pending_request(&http_b, 0, "crosssocket-B-owned")
                .await?;
            require(
                cap_a.status() == SpeculationStatus::Released
                    && cap_b.status() == SpeculationStatus::Released,
                "cross-replay consumed or poisoned one owner's genuine commit",
            )?;
            require(
                a.user_history() == ["identical wire transcript"]
                    && b.user_history() == ["identical wire transcript"],
                "owner history changed or duplicated",
            )?;
            require(
                a.output.count(
                    |v| matches!(v, FrameData::LlmText(t) if t.text == "Answer owned by A."),
                ) == 1
                    && b.output.count(
                        |v| matches!(v, FrameData::LlmText(t) if t.text == "Answer owned by B."),
                    ) == 1,
                "a legitimate owner failed to release its own answer once",
            )?;
            require(
                a.output.count(
                    |v| matches!(v, FrameData::LlmText(t) if t.text == "Answer owned by B."),
                ) == 0
                    && b.output.count(
                        |v| matches!(v, FrameData::LlmText(t) if t.text == "Answer owned by A."),
                    ) == 0,
                "answers crossed independent owner boundaries",
            )?;
            require(
                http_a.count() == 1 && http_b.count() == 1,
                "crossed authority retried inference",
            )?;
            Ok(())
        })
        .catch_unwind()
        .await;
        let failure = outcome_error(outcome);
        let cleanup_a = a
            .finish_pending_fixture(&mut ws_a, &mut http_a, false)
            .await;
        let cleanup_b = b
            .finish_pending_fixture(&mut ws_b, &mut http_b, false)
            .await;
        assert!(
            failure.is_none() && cleanup_a.is_empty() && cleanup_b.is_empty(),
            "schedule={failure:?}; cleanup A={cleanup_a:?}; cleanup B={cleanup_b:?}"
        );
    }

    #[tokio::test]
    async fn signed_concurrent_owner_restart_keeps_foreign_receipts_out_of_both_next_requests() {
        for provider in [Provider::Anthropic, Provider::Gemini] {
            let mut ws_a = WsPeer::new().await;
            let mut ws_b = WsPeer::new().await;
            let mut http_a = HttpPeer::new().await;
            let mut http_b = HttpPeer::new().await;
            let mut a = Harness::new(
                &mut ws_a,
                &http_a,
                true,
                config(true),
                Some(PauseAt::Receipts),
                provider,
                None,
            )
            .await;
            let b = Harness::new(
                &mut ws_b,
                &http_b,
                true,
                config(true),
                Some(PauseAt::Receipts),
                provider,
                None,
            )
            .await;
            let outcome = AssertUnwindSafe(async {
                a.start().await;
                b.start().await;
                a.predict("same signed turn").await;
                b.predict("same signed turn").await;
                let (signed_a, _, buffer_a) = owner_signed(provider, "retired-A");
                let (signed_b, expected_b, buffer_b) = owner_signed(provider, "live-B");
                http_a.request().await.1.send(signed_a).unwrap();
                http_b.request().await.1.send(signed_b).unwrap();
                a.buffered(buffer_a).await;
                b.buffered(buffer_b).await;
                let cap_a = a.after.capability();
                let cap_b = b.after.capability();
                a.end_turn("same signed turn").await;
                b.end_turn("same signed turn").await;
                let frames_a = carriers(&a).await;
                let frames_b = carriers(&b).await;
                a.before.release();
                b.before.release();

                // B is still Pending, with a full signed response held. Only A
                // restarts. B's real live receipts are replayed at fresh A.
                a.restart(&mut ws_a).await;
                a.start().await;
                *http_a.automatic.lock().unwrap() = Some(plain_response(provider));
                *http_b.automatic.lock().unwrap() = Some(plain_response(provider));
                replay(&a, &frames_b).await;
                replay(&b, &[frames_a[3].clone(), frames_a[2].clone(), frames_a[1].clone(), frames_a[0].clone()]).await;
                a.settle_or_observe_pending_request(&http_a, 1, "signed-crosssocket-fresh-A").await?;
                b.pending_fixture_barrier(false, "signed-crosssocket-live-B").await?;
                require(cap_a.status() == SpeculationStatus::Cancelled && cap_b.status() == SpeculationStatus::Pending,
                    "one source's retirement or foreign receipts changed the other owner's signed authority")?;
                require(a.quiet() && b.quiet() && http_a.count() == 1 && http_b.count() == 1,
                    "retirement/cross-replay exposed a held signed continuation")?;

                a.end_turn("fresh A only").await;
                let (fresh_a, _) = http_a.request().await;
                a.settle_or_observe_pending_request(&http_a, 1, "signed-crosssocket-A-final").await?;
                replay(&b, &[frames_b[2].clone(), frames_b[3].clone()]).await;
                b.settle_or_observe_pending_request(&http_b, 0, "signed-crosssocket-B-final").await?;
                require(cap_b.status() == SpeculationStatus::Released, "B's legitimate signed response never released")?;
                require(!fresh_a.to_string().contains("retired-A") && !fresh_a.to_string().contains("live-B"),
                    "a signature contaminated A's first post-restart request")?;
                require(a.user_history() == ["fresh A only"] && b.user_history() == ["same signed turn"],
                    "crosssocket source ownership contaminated committed user history")?;

                a.send(Frame::new(FrameData::LlmRun)).await;
                b.send(Frame::new(FrameData::LlmRun)).await;
                let (next_a, _) = http_a.request().await;
                let (next_b, _) = http_b.request().await;
                a.settle_or_observe_pending_request(&http_a, 2, "signed-crosssocket-A-next").await?;
                b.settle_or_observe_pending_request(&http_b, 1, "signed-crosssocket-B-next").await?;
                require(exact_signed_history(&next_b, provider, "same signed turn", &expected_b),
                    format!("{provider:?}: B's exact signed parts/blocks changed in its actual next request: {next_b}"))?;
                for rejected in ["retired-A", "live-B", "same signed turn", "opaque+/", "tail+/=="] {
                    require(!next_a.to_string().contains(rejected),
                        format!("{provider:?}: A's actual next request contains forbidden {rejected}"))?;
                }
                require(!next_b.to_string().contains("retired-A"), "A's signature crossed into B's next request")?;
                require(http_a.count() == 3 && http_b.count() == 2, "signed crosssocket schedule duplicated a request")?;
                Ok(())
            }).catch_unwind().await;
            let failure = outcome_error(outcome);
            let cleanup_a = a
                .finish_pending_fixture(&mut ws_a, &mut http_a, false)
                .await;
            let cleanup_b = b
                .finish_pending_fixture(&mut ws_b, &mut http_b, false)
                .await;
            assert!(failure.is_none() && cleanup_a.is_empty() && cleanup_b.is_empty(),
                "provider={provider:?}; schedule={failure:?}; cleanup A={cleanup_a:?}; cleanup B={cleanup_b:?}");
        }
    }

    #[tokio::test]
    async fn crossed_live_receipts_cannot_resurrect_cancelled_tool_or_steal_other_tool_continuation(
    ) {
        let mut ws_a = WsPeer::new().await;
        let mut ws_b = WsPeer::new().await;
        let mut http_a = HttpPeer::new().await;
        let mut http_b = HttpPeer::new().await;
        let (tools_a, constructed_a, polled_a) = tools();
        let (tools_b, constructed_b, polled_b) = tools();
        let mut a = Harness::new(
            &mut ws_a,
            &http_a,
            true,
            config(true),
            Some(PauseAt::Receipts),
            Provider::OpenAi,
            Some(tools_a),
        )
        .await;
        let b = Harness::new(
            &mut ws_b,
            &http_b,
            true,
            config(true),
            Some(PauseAt::Receipts),
            Provider::OpenAi,
            Some(tools_b),
        )
        .await;
        let outcome =
            AssertUnwindSafe(async {
                a.start().await;
                b.start().await;
                a.predict("book").await;
                b.predict("book").await;
                let (_, reply_a) = http_a.request().await;
                let (_, reply_b) = http_b.request().await;
                let cap_a = a.after.capability();
                let cap_b = b.after.capability();
                reply_a.send(tool_response("cancelled-A-book")).unwrap();
                wait(|| cap_a.status() == SpeculationStatus::Cancelled).await;
                // B's HTTP response is deliberately still in flight. A has already
                // decoded a real tool intent and cancelled speculation safely.
                a.end_turn("book").await;
                b.end_turn("book").await;
                let frames_a = carriers(&a).await;
                let frames_b = carriers(&b).await;
                a.before.release();
                b.before.release();
                require(
                    a.turn().is_live() && b.turn().is_live(),
                    "tool cross-replay lacked two live sources",
                )?;
                *http_a.automatic.lock().unwrap() = Some(answer("Forbidden A fallback."));
                replay(
                    &a,
                    &[
                        frames_b[0].clone(),
                        frames_b[1].clone(),
                        frames_b[3].clone(),
                        frames_b[2].clone(),
                    ],
                )
                .await;
                replay(
                    &b,
                    &[
                        frames_a[0].clone(),
                        frames_a[1].clone(),
                        frames_a[2].clone(),
                        frames_a[3].clone(),
                    ],
                )
                .await;
                a.settle_or_observe_pending_request(&http_a, 1, "tool-crosssocket-cancelled-A")
                    .await?;
                b.pending_fixture_barrier(false, "tool-crosssocket-inflight-B")
                    .await?;
                require(
                    cap_a.status() == SpeculationStatus::Cancelled
                        && cap_b.status() == SpeculationStatus::Pending,
                    "live foreign receipts revived A or prematurely committed B",
                )?;
                require(
                    constructed_a.load(Ordering::SeqCst) == 0
                        && polled_a.load(Ordering::SeqCst) == 0
                        && constructed_b.load(Ordering::SeqCst) == 0
                        && polled_b.load(Ordering::SeqCst) == 0,
                    "crossed receipts constructed or polled a held tool",
                )?;
                require(
                    a.quiet() && b.quiet() && http_a.count() == 1 && http_b.count() == 1,
                    "foreign receipts started an ordinary fallback or changed history",
                )?;

                a.restart(&mut ws_a).await;
                require(
                    cap_b.status() == SpeculationStatus::Pending,
                    "A restart cancelled B's outstanding tool response",
                )?;
                replay(&b, &[frames_b[2].clone(), frames_b[3].clone()]).await;
                // Observe the actual boundary before allowing tool SSE to arrive.
                // A pre-LLM application witness alone can race this transition.
                wait(|| {
                    matches!(
                        cap_b.status(),
                        SpeculationStatus::Released | SpeculationStatus::Cancelled
                    )
                })
                .await;
                require(
                    cap_b.status() == SpeculationStatus::Released,
                    "own B receipts did not release exact authority",
                )?;
                reply_b.send(tool_response("committed-B-book")).unwrap();
                wait(|| polled_b.load(Ordering::SeqCst) == 1).await;
                let (continuation_b, reply) = http_b.request().await;
                reply.send(answer("B booked once.")).unwrap();
                b.settle_or_observe_pending_request(&http_b, 0, "tool-crosssocket-B-continuation")
                    .await?;
                require(has_tool_result(&continuation_b, "committed-B-book")
                && !continuation_b.to_string().contains("cancelled-A-book"),
                "B's actual tool next request lost its own result or imported A's cancelled call")?;

                *http_a.automatic.lock().unwrap() = None;
                a.start().await;
                // Duplicate B authority is now Released, but is still genuine and
                // must not consume fresh A's pending ordinary turn.
                replay(&a, &[frames_b[2].clone(), frames_b[3].clone()]).await;
                a.end_turn("book").await;
                let (fresh_a, reply) = http_a.request().await;
                reply.send(tool_response("fresh-A-book")).unwrap();
                wait(|| polled_a.load(Ordering::SeqCst) == 1).await;
                let (continuation_a, reply) = http_a.request().await;
                reply.send(answer("A booked once.")).unwrap();
                a.settle_or_observe_pending_request(&http_a, 1, "tool-crosssocket-A-continuation")
                    .await?;
                require(
                    has_tool_result(&continuation_a, "fresh-A-book"),
                    "A's genuine post-restart tool result was lost",
                )?;
                for request in [&fresh_a, &continuation_a] {
                    require(
                        !request.to_string().contains("cancelled-A-book")
                            && !request.to_string().contains("committed-B-book"),
                        "A's actual next request contains a cancelled or foreign tool call",
                    )?;
                }
                require(
                    a.user_history() == ["book"] && b.user_history() == ["book"],
                    "tool schedule duplicated user history",
                )?;
                require(
                    http_a.count() == 3 && http_b.count() == 2,
                    "tool isolation duplicated a request",
                )?;
                Ok(())
            })
            .catch_unwind()
            .await;
        let failure = outcome_error(outcome);
        let cleanup_a = a
            .finish_pending_fixture(&mut ws_a, &mut http_a, false)
            .await;
        let cleanup_b = b
            .finish_pending_fixture(&mut ws_b, &mut http_b, false)
            .await;
        assert!(
            failure.is_none() && cleanup_a.is_empty() && cleanup_b.is_empty(),
            "schedule={failure:?}; cleanup A={cleanup_a:?}; cleanup B={cleanup_b:?}"
        );
        // Count through terminal cleanup too: End must not revive the rejected
        // intent or execute a second effect after the visible continuation.
        assert_eq!(constructed_a.load(Ordering::SeqCst), 1);
        assert_eq!(polled_a.load(Ordering::SeqCst), 1);
        assert_eq!(constructed_b.load(Ordering::SeqCst), 1);
        assert_eq!(polled_b.load(Ordering::SeqCst), 1);
    }
}
