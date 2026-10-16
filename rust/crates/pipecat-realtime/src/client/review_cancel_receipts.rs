//! Model-sink receipt ownership tests; real WS/pair recovery is tested separately.
use super::*;
use std::task::Context;

#[derive(Default)]
struct CancelSink {
    sent: Vec<Value>,
    fail_flush: bool,
    change_epoch_on_flush: Option<Arc<AtomicU64>>,
}
impl Sink<Message> for CancelSink {
    type Error = tungstenite::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let Message::Text(text) = item else {
            panic!("expected JSON");
        };
        self.sent.push(serde_json::from_str(&text).unwrap());
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.fail_flush {
            return Poll::Ready(Err(tungstenite::Error::Io(std::io::Error::other(
                "model flush failure",
            ))));
        }
        if let Some(epoch) = self.change_epoch_on_flush.take() {
            epoch.fetch_add(1, Ordering::AcqRel);
        }
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
fn actor() -> Actor {
    let (tx, _rx) = mpsc::channel(8);
    let mut actor = Actor::new(
        RealtimeConfig::new("model-only"),
        Arc::new(AtomicU64::new(1)),
        Arc::new(Mutex::new(())),
        tx,
    );
    actor.pipeline_turns = true;
    actor.audio_preroll = Some(Preroll::new(2));
    actor.active = Some("known-response".into());
    actor.responses.push_back((
        "known-response".into(),
        ResponseContext {
            generation: 0,
            context_id: None,
            inference_fence: None,
        },
    ));
    actor
}
fn error(id: Option<&str>, code: &str) -> ProviderError {
    ProviderError {
        kind: "invalid_request_error".into(),
        code: Some(code.into()),
        message: "diagnostic".into(),
        param: None,
        event_id: id.map(str::to_owned),
    }
}
fn receipt(id: &str, response: &str) -> CancelReceipt {
    CancelReceipt {
        request_id: id.into(),
        response_id: response.into(),
        response_generation: 0,
        interrupt_generation: 1,
    }
}
#[tokio::test]
async fn cancel_receipt_requires_flush_and_epoch_admission() {
    let mut failed = actor();
    let mut failed_sink = CancelSink {
        fail_flush: true,
        ..Default::default()
    };
    let result = failed
        .write_cancel_before(
            &mut failed_sink,
            Instant::now() + Duration::from_secs(1),
            1,
            true,
        )
        .await;
    assert_eq!(result, Err(RealtimeError::Transport));
    assert_eq!(failed_sink.sent.len(), 1);
    assert!(!failed
        .cancel_receipts
        .consume(&error(Some("pc_evt_1"), "response_cancel_not_active")));

    let mut stale = actor();
    let mut stale_sink = CancelSink::default();
    let result = stale
        .write_cancel_before(
            &mut stale_sink,
            Instant::now() + Duration::from_secs(1),
            0,
            true,
        )
        .await;
    assert_eq!(result, Ok(false));
    assert!(stale_sink.sent.is_empty());
    assert!(!stale
        .cancel_receipts
        .consume(&error(Some("pc_evt_1"), "response_cancel_not_active")));
}
#[tokio::test]
async fn cancel_receipt_keeps_exact_id_across_epoch_change_and_truncate_write() {
    let mut actor = actor();
    let mut sink = CancelSink {
        change_epoch_on_flush: Some(actor.generation.clone()),
        ..Default::default()
    };
    let result = actor
        .write_cancel_before(&mut sink, Instant::now() + Duration::from_secs(1), 1, true)
        .await;
    actor.write_before(&mut sink, json!({"type":"conversation.item.truncate", "item_id":"item", "content_index":0, "audio_end_ms":0}), Instant::now() + Duration::from_secs(1)).await.unwrap();
    assert_eq!(result, Ok(true));
    assert_eq!(actor.epoch(), 2);
    assert_eq!(sink.sent[0]["event_id"], "pc_evt_1");
    assert_eq!(sink.sent[1]["event_id"], "pc_evt_2");
    assert!(!actor
        .cancel_receipts
        .consume(&error(Some("pc_evt_2"), "response_cancel_not_active")));
    assert!(actor
        .cancel_receipts
        .consume(&error(Some("pc_evt_1"), "response_cancel_not_active")));
    assert!(!actor
        .cancel_receipts
        .consume(&error(Some("pc_evt_1"), "response_cancel_not_active")));
}
#[tokio::test]
async fn cancel_receipt_rejects_pending_only_and_standalone_ownership() {
    for pending_only in [true, false] {
        let mut actor = actor();
        if pending_only {
            actor.active = None;
            actor.pending = Some(ResponseContext {
                generation: 0,
                context_id: None,
                inference_fence: None,
            });
        } else {
            actor.pipeline_turns = false;
        }
        let mut sink = CancelSink::default();
        let sent = actor
            .write_cancel_before(&mut sink, Instant::now() + Duration::from_secs(1), 1, true)
            .await;
        assert_eq!(sent, Ok(true));
        assert_eq!(sink.sent.len(), 1);
        assert!(!actor
            .cancel_receipts
            .consume(&error(Some("pc_evt_1"), "response_cancel_not_active")));
    }
}
#[test]
fn cancel_receipts_are_single_use_code_bound_and_fifo_bounded() {
    let mut receipts = CancelReceipts::new(4096);
    for i in 0..65 {
        receipts.insert(receipt(&format!("cancel-{i}"), "response"));
    }
    assert!(!receipts.consume(&error(Some("cancel-0"), "response_cancel_not_active")));
    assert!(!receipts.consume(&error(None, "response_cancel_not_active")));
    assert!(!receipts.consume(&error(Some("unknown"), "response_cancel_not_active")));
    assert!(!receipts.consume(&error(
        Some("cancel-1"),
        "conversation_already_has_active_response"
    )));
    assert!(receipts.consume(&error(Some("cancel-1"), "response_cancel_not_active")));
    assert!(!receipts.consume(&error(Some("cancel-1"), "response_cancel_not_active")));
    assert!(receipts.consume(&error(Some("cancel-64"), "response_cancel_not_active")));
}
#[test]
fn cancel_receipt_payload_budget_evicts_and_rejects_oversized_entries() {
    let mut receipts = CancelReceipts::new(4);
    receipts.insert(receipt("a", "xx"));
    receipts.insert(receipt("b", "xx"));
    assert!(!receipts.consume(&error(Some("a"), "response_cancel_not_active")));
    receipts.insert(receipt("huge", "xx"));
    assert!(!receipts.consume(&error(Some("huge"), "response_cancel_not_active")));
    assert!(receipts.consume(&error(Some("b"), "response_cancel_not_active")));
}
