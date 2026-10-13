//! Deterministic ledger regressions. Real sockets/PCM live in integration tests.
use super::{
    state::{Command, OutboundId, Pending, Phase, Shared},
    ReceiptBackend, ReceiptConfig, MAX_ID,
};
use crate::wire::V2Control;
use pipecat_audio::PcmFormat;
use pipecat_core::{AudioRawFrame, Frame, FrameData};
use pipecat_media_transport::{OutputBackend, PlaybackReceipt};
use pipecat_transports::Packet;
use std::{sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::Instant};

fn ledger() -> (Arc<Shared>, tokio::sync::mpsc::Receiver<Command>) {
    Shared::new("a".repeat(32), 24000, ReceiptConfig::default())
}
fn register(
    shared: &Shared,
    id: u64,
    interruptible: bool,
    phase: Phase,
) -> oneshot::Receiver<Result<PlaybackReceipt, pipecat_core::ProcessorError>> {
    let (done, waiting) = oneshot::channel();
    let mut state = shared.state.lock().unwrap();
    state.chunk_id = id;
    state.pending = Some(Pending {
        id,
        epoch: state.play_epoch,
        nonce: "b".repeat(32),
        interruptible,
        phase,
        acked: false,
        revoked: false,
        deadline: Instant::now() + Duration::from_secs(1),
        done: Some(done),
    });
    waiting
}
fn ack(shared: &Shared, id: u64, epoch: u64) {
    shared
        .receipt(V2Control::ChunkComplete {
            version: 2,
            session_id: shared.session_id.clone(),
            epoch,
            chunk_id: id,
            receipt_nonce: "b".repeat(32),
        })
        .unwrap();
}
fn cutoff_ack(shared: &Shared) -> V2Control {
    let state = shared.state.lock().unwrap();
    let cutoff = state.cutoff.as_ref().unwrap();
    V2Control::CutoffComplete {
        version: 2,
        session_id: shared.session_id.clone(),
        epoch: cutoff.epoch,
        cutoff_id: cutoff.id,
        receipt_nonce: cutoff.nonce.clone(),
        all: cutoff.all,
    }
}
#[test]
fn registered_ack_cannot_skip_dispatch_and_sending_ack_needs_enqueue() {
    let (shared, _commands) = ledger();
    let mut waiting = register(&shared, 1, true, Phase::Registered);
    ack(&shared, 1, 1);
    assert!(!shared.state.lock().unwrap().pending.as_ref().unwrap().acked);
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(shared
        .begin_chunk(Command {
            id: 1,
            packet: Packet::Text("unit-only".into())
        })
        .is_some());
    ack(&shared, 1, 1);
    assert!(shared.state.lock().unwrap().pending.as_ref().unwrap().acked);
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    shared.enqueued(OutboundId::Chunk(1));
    assert!(matches!(
        waiting.try_recv(),
        Ok(Ok(PlaybackReceipt::Played))
    ));
    assert!(shared.state.lock().unwrap().pending.is_none());
}
#[test]
fn sending_ack_followed_by_enqueue_failure_or_revocation_never_plays() {
    for close in [true, false] {
        let (shared, _commands) = ledger();
        let mut waiting = register(&shared, 1, true, Phase::Sending);
        ack(&shared, 1, 1);
        if close {
            shared.close();
        } else {
            shared.request_cutoff(false, true).unwrap();
        }
        shared.enqueued(OutboundId::Chunk(1));
        assert!(!matches!(
            waiting.try_recv(),
            Ok(Ok(PlaybackReceipt::Played))
        ));
    }
}
#[test]
fn expired_cached_ack_cannot_be_promoted_on_late_enqueue_resumption() {
    let (shared, _commands) = ledger();
    let mut waiting = register(&shared, 1, true, Phase::Sending);
    ack(&shared, 1, 1);
    shared
        .state
        .lock()
        .unwrap()
        .pending
        .as_mut()
        .unwrap()
        .deadline = Instant::now();
    shared.enqueued(OutboundId::Chunk(1));
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    shared.close();
    assert!(matches!(waiting.try_recv(), Ok(Err(_))));
}
#[test]
fn old_guard_cannot_revoke_a_successor_pending_record() {
    let (shared, _commands) = ledger();
    let mut old = register(&shared, 1, true, Phase::Enqueued);
    ack(&shared, 1, 1);
    assert!(matches!(old.try_recv(), Ok(Ok(PlaybackReceipt::Played))));
    let mut current = register(&shared, 2, true, Phase::Enqueued);
    shared.revoke(1);
    let state = shared.state.lock().unwrap();
    assert!(!state.closed);
    assert!(!state.gate_closed);
    assert!(!state.pending.as_ref().unwrap().revoked);
    drop(state);
    ack(&shared, 2, 1);
    assert!(matches!(
        current.try_recv(),
        Ok(Ok(PlaybackReceipt::Played))
    ));
}
#[tokio::test]
async fn selective_upgrade_has_a_new_identity_and_old_ack_cannot_complete_it() {
    let (shared, _commands) = ledger();
    let mut waiting = register(&shared, 1, true, Phase::Enqueued);
    let token = shared.request_cutoff(false, true).unwrap();
    shared.mark_media(token).unwrap();
    assert!(matches!(
        waiting.try_recv(),
        Ok(Ok(PlaybackReceipt::Discarded))
    ));
    let old = shared.next_cutoff().unwrap().unwrap();
    let old_ack = cutoff_ack(&shared);
    shared.enqueued(old.id);
    shared.request_cutoff(true, false).unwrap();
    let new = shared.next_cutoff().unwrap().unwrap();
    let new_ack = cutoff_ack(&shared);
    match (&old_ack, &new_ack) {
        (
            V2Control::CutoffComplete {
                epoch: old_epoch,
                cutoff_id: old_id,
                receipt_nonce: old_nonce,
                all: false,
                ..
            },
            V2Control::CutoffComplete {
                epoch: new_epoch,
                cutoff_id: new_id,
                receipt_nonce: new_nonce,
                all: true,
                ..
            },
        ) => {
            assert!(new_epoch > old_epoch);
            assert!(new_id > old_id);
            assert_ne!(new_nonce, old_nonce);
        }
        _ => panic!("wrong cutoff identities"),
    }
    shared.receipt(old_ack).unwrap();
    shared.enqueued(new.id);
    assert!(shared.state.lock().unwrap().cutoff.is_some());
    shared.receipt(new_ack).unwrap();
    assert!(shared.state.lock().unwrap().cutoff.is_none());
    assert!(shared.state.lock().unwrap().gate_closed);
    let backend = ReceiptBackend {
        shared: shared.clone(),
    };
    tokio::time::timeout(Duration::from_millis(100), backend.interrupt(None))
        .await
        .unwrap()
        .unwrap();
    assert!(
        shared.state.lock().unwrap().gate_closed,
        "untagged hook cannot open a media gate"
    );
    shared.probe(token).unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        shared.release(token, Instant::now() + Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!shared.state.lock().unwrap().gate_closed);
}
#[tokio::test]
async fn latest_mark_and_flush_need_the_media_hook_before_reopening() {
    let (shared, _commands) = ledger();
    let _waiting = register(&shared, 1, true, Phase::Enqueued);
    let first = shared.request_cutoff(false, true).unwrap();
    let outbound = shared.next_cutoff().unwrap().unwrap();
    let receipt = cutoff_ack(&shared);
    let latest = shared.request_cutoff(false, true).unwrap();
    assert!(latest > first);
    shared.mark_media(first).unwrap();
    shared.enqueued(outbound.id);
    shared.receipt(receipt).unwrap();
    assert!(shared.state.lock().unwrap().gate_closed);
    shared.mark_media(latest).unwrap();
    assert!(
        shared.state.lock().unwrap().gate_closed,
        "actor mark/ACK must not reopen"
    );
    let backend = ReceiptBackend {
        shared: shared.clone(),
    };
    tokio::time::timeout(Duration::from_millis(100), backend.interrupt(None))
        .await
        .unwrap()
        .unwrap();
    assert!(shared.state.lock().unwrap().gate_closed);
    shared.probe(first).unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        shared.release(first, Instant::now() + Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        shared.state.lock().unwrap().gate_closed,
        "old release cannot open the latest gate"
    );
    shared.probe(latest).unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        shared.release(latest, Instant::now() + Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!shared.state.lock().unwrap().gate_closed);
    assert_eq!(shared.state.lock().unwrap().media_gate, latest);
    assert!(shared.mark_media(latest + 1).is_err());
}
#[tokio::test]
async fn uninterruptible_first_poll_after_socket_gate_uses_retained_play_epoch() {
    let (shared, mut commands) = ledger();
    let backend = ReceiptBackend {
        shared: shared.clone(),
    };
    backend
        .start(PcmFormat::new(24000, 1).unwrap(), &[None])
        .await
        .unwrap();
    let token = shared.request_cutoff(false, true).unwrap();
    // Two samples are sufficient here: integration tests check real chunking.
    let mut frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![1, 0, 2, 0], 24000, 1).unwrap(),
    ));
    frame.interruptible = false;
    let playing = backend.play_audio(&frame);
    tokio::pin!(playing);
    let command = tokio::time::timeout(Duration::from_millis(100), async {
        tokio::select! {
            result = &mut playing => panic!("uninterruptible play ended before receipt: {result:?}"),
            command = commands.recv() => command.unwrap(),
        }
    }).await.unwrap();
    let (id, epoch, nonce) = {
        let state = shared.state.lock().unwrap();
        let pending = state.pending.as_ref().unwrap();
        assert_eq!(pending.epoch, 1);
        assert!(state.epoch > pending.epoch);
        (pending.id, pending.epoch, pending.nonce.clone())
    };
    let outbound = shared.begin_chunk(command).unwrap();
    shared.enqueued(outbound.id);
    shared
        .receipt(V2Control::ChunkComplete {
            version: 2,
            session_id: shared.session_id.clone(),
            epoch,
            chunk_id: id,
            receipt_nonce: nonce,
        })
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), playing)
            .await
            .unwrap()
            .unwrap(),
        PlaybackReceipt::Played
    );
    shared.mark_media(token).unwrap();
    tokio::time::timeout(Duration::from_millis(100), backend.interrupt(None))
        .await
        .unwrap()
        .unwrap();
}
#[test]
fn exhausted_wire_identity_never_wraps_or_reuses_zero() {
    let (shared, _commands) = ledger();
    shared.state.lock().unwrap().epoch = MAX_ID;
    assert!(shared.request_cutoff(false, false).is_err());
    assert_eq!(shared.state.lock().unwrap().epoch, MAX_ID);
    assert!(shared.state.lock().unwrap().closed);
}

#[tokio::test]
async fn pure_backend_flush_advances_epoch_without_bypassing_a_media_gate() {
    let (shared, _commands) = ledger();
    let _waiting = register(&shared, 1, true, Phase::Enqueued);
    shared.revoke(1);
    assert!(!shared.state.lock().unwrap().gate_closed);
    let outbound = shared.next_cutoff().unwrap().unwrap();
    let receipt = cutoff_ack(&shared);
    shared.enqueued(outbound.id);
    shared.receipt(receipt).unwrap();
    let state = shared.state.lock().unwrap();
    assert!(state.cutoff.is_none());
    assert_eq!(state.play_epoch, state.epoch);
    assert!(state.play_epoch > 1);
}

#[test]
fn gate_and_cutoff_counters_retire_before_reusing_an_identity() {
    for gate in [true, false] {
        let (shared, _commands) = ledger();
        let mut waiting = register(&shared, 1, true, Phase::Enqueued);
        {
            let mut state = shared.state.lock().unwrap();
            if gate {
                state.latest_gate = MAX_ID;
            } else {
                state.cutoff_id = MAX_ID;
            }
        }
        assert!(shared.request_cutoff(false, gate).is_err());
        let state = shared.state.lock().unwrap();
        assert!(state.closed);
        assert!(state.pending.is_none());
        assert_eq!(
            if gate {
                state.latest_gate
            } else {
                state.cutoff_id
            },
            MAX_ID
        );
        drop(state);
        assert!(!matches!(
            waiting.try_recv(),
            Ok(Ok(PlaybackReceipt::Played))
        ));
    }
}
#[tokio::test]
async fn chunk_counter_exhaustion_closes_admission_before_reuse() {
    let (shared, _commands) = ledger();
    let backend = ReceiptBackend {
        shared: shared.clone(),
    };
    backend
        .start(PcmFormat::new(24000, 1).unwrap(), &[None])
        .await
        .unwrap();
    shared.state.lock().unwrap().chunk_id = MAX_ID;
    let frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![1, 0], 24000, 1).unwrap(),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), backend.play_audio(&frame))
            .await
            .unwrap()
            .is_err()
    );
    assert!(shared.state.lock().unwrap().closed);
    assert_eq!(shared.state.lock().unwrap().chunk_id, MAX_ID);
}
#[test]
fn wrong_and_replayed_receipts_never_refresh_an_absolute_deadline() {
    let (shared, _commands) = ledger();
    let mut waiting = register(&shared, 2, true, Phase::Enqueued);
    let deadline = shared.deadline();
    for (id, epoch) in [(1, 1), (3, 1), (2, 0), (2, 2)] {
        ack(&shared, id, epoch);
    }
    for (session, nonce) in [
        ("c".repeat(32), "b".repeat(32)),
        (shared.session_id.clone(), "d".repeat(32)),
    ] {
        shared
            .receipt(V2Control::ChunkComplete {
                version: 2,
                session_id: session,
                epoch: 1,
                chunk_id: 2,
                receipt_nonce: nonce,
            })
            .unwrap();
    }
    assert_eq!(shared.deadline(), deadline);
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    ack(&shared, 2, 1);
    assert!(matches!(
        waiting.try_recv(),
        Ok(Ok(PlaybackReceipt::Played))
    ));
    ack(&shared, 2, 1);
    assert!(shared.state.lock().unwrap().pending.is_none());
}

#[tokio::test]
async fn completed_all_cutoff_never_allows_new_uninterruptible_old_epoch() {
    let (shared, mut commands) = ledger();
    let backend = ReceiptBackend {
        shared: shared.clone(),
    };
    backend
        .start(PcmFormat::new(24000, 1).unwrap(), &[None])
        .await
        .unwrap();
    let token = shared.request_cutoff(false, true).unwrap();
    let mut old = register(&shared, 1, false, Phase::Enqueued);
    shared.revoke(1); // Uninterruptible Drop requires the all-audio cutoff.
    assert!(matches!(old.try_recv(), Ok(Ok(PlaybackReceipt::Discarded))));
    let outbound = shared.next_cutoff().unwrap().unwrap();
    let receipt = cutoff_ack(&shared);
    assert!(matches!(
        &receipt,
        V2Control::CutoffComplete { all: true, .. }
    ));
    shared.enqueued(outbound.id);
    shared.receipt(receipt).unwrap();
    {
        let state = shared.state.lock().unwrap();
        assert!(state.gate_closed);
        assert!(state.cutoff.is_none());
        assert!(state.play_epoch < state.all_cutoff_floor);
    }
    let mut frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![1, 0], 24000, 1).unwrap(),
    ));
    frame.interruptible = false;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), backend.play_audio(&frame))
            .await
            .unwrap()
            .unwrap(),
        PlaybackReceipt::Discarded
    );
    assert!(matches!(
        commands.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    shared.mark_media(token).unwrap();
    shared.probe(token).unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        shared.release(token, Instant::now() + Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap();
    let state = shared.state.lock().unwrap();
    assert!(!state.gate_closed);
    assert!(state.play_epoch >= state.all_cutoff_floor);
}
