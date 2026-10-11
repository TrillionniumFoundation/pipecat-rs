//! Exact socket-local eager ownership. D protects passive descriptors only;
//! every core operation and notification happens after releasing D.
use pipecat_core::{
    EagerSourceSessionOwner, FrameData, NativeEagerCommit, NativeEagerPrediction, NativeEagerTurn,
    ProcessorContext, ProcessorError,
};
use std::sync::{Arc, Mutex};

const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

enum Position {
    NoTurn,
    StartMapping(Option<NativeEagerCommit>),
    AwaitingBarrier(NativeEagerTurn),
    Ready(NativeEagerTurn),
    FinalOffering(NativeEagerTurn),
    FinalInstalled(NativeEagerCommit),
    EmptyClosed(NativeEagerTurn),
    SuppressedPreserving(NativeEagerCommit),
    Retired,
    Completed,
}
struct Descriptor {
    position: Position,
    draining: bool,
    suppressed: bool,
}
// A snapshot must never retain a commit: helper-only strong receipt references
// may not outlive reader completion and masquerade as a real queued final.
enum DrainDecision {
    Preserve,
    Retire,
    Prior(NativeEagerTurn),
}

pub(crate) struct EagerSession {
    owner: EagerSourceSessionOwner,
    descriptor: Mutex<Descriptor>,
    #[cfg(test)]
    actor_close: Mutex<Option<ActorCloseGate>>,
    #[cfg(test)]
    reader_next: Mutex<Option<ActorCloseGate>>,
    #[cfg(test)]
    reader_drop: Mutex<Option<tokio::sync::oneshot::Sender<bool>>>,
    #[cfg(test)]
    actor_return: Mutex<Option<tokio::sync::oneshot::Sender<bool>>>,
}
#[cfg(test)]
struct ActorCloseGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}
impl EagerSession {
    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
        Arc::new(Self::new(
            EagerSourceSessionOwner::new(MAX_PAYLOAD_BYTES).unwrap(),
        ))
    }
    #[cfg(test)]
    pub(crate) fn hold_actor_close(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let previous = self.actor_close.lock().unwrap().replace(ActorCloseGate {
            entered: entered_tx,
            release: release_rx,
        });
        drop(previous);
        (entered_rx, release_tx)
    }
    #[cfg(test)]
    pub(crate) async fn actor_before_close(&self, cancel: &crate::CancellationToken) {
        let gate = self.actor_close.lock().unwrap().take();
        Self::wait_test_gate(gate, cancel).await;
    }
    #[cfg(test)]
    pub(crate) fn hold_reader_next(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let previous = self.reader_next.lock().unwrap().replace(ActorCloseGate {
            entered: entered_tx,
            release: release_rx,
        });
        drop(previous);
        (entered_rx, release_tx)
    }
    #[cfg(test)]
    pub(crate) async fn reader_before_next(&self, cancel: &crate::CancellationToken) {
        let gate = self.reader_next.lock().unwrap().take();
        Self::wait_test_gate(gate, cancel).await;
    }
    #[cfg(test)]
    async fn wait_test_gate(gate: Option<ActorCloseGate>, cancel: &crate::CancellationToken) {
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            // Test scaffolding only observes/pauses; actual reader/actor paths
            // retain the authority decision after cancellation or bounded escape.
            tokio::select! { biased;
                _ = cancel.cancelled() => (),
                _ = tokio::time::timeout(std::time::Duration::from_secs(5), gate.release) => (),
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn watch_reader_drop(&self) -> tokio::sync::oneshot::Receiver<bool> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let previous = self.reader_drop.lock().unwrap().replace(sender);
        drop(previous);
        receiver
    }
    #[cfg(test)]
    pub(crate) fn reader_dropped(&self, normal: bool) {
        let sender = self.reader_drop.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(normal);
        }
    }
    #[cfg(test)]
    pub(crate) fn watch_actor_return(&self) -> tokio::sync::oneshot::Receiver<bool> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let previous = self.actor_return.lock().unwrap().replace(sender);
        drop(previous);
        receiver
    }
    #[cfg(test)]
    pub(crate) fn actor_returned(&self, success: bool) {
        let sender = self.actor_return.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(success);
        }
    }
    pub(crate) fn from_context(ctx: &ProcessorContext) -> Result<Arc<Self>, ProcessorError> {
        Ok(Arc::new(Self::new(EagerSourceSessionOwner::from_context(
            MAX_PAYLOAD_BYTES,
            ctx,
        )?)))
    }
    fn new(owner: EagerSourceSessionOwner) -> Self {
        Self {
            owner,
            #[cfg(test)]
            actor_close: Mutex::new(None),
            #[cfg(test)]
            reader_next: Mutex::new(None),
            #[cfg(test)]
            reader_drop: Mutex::new(None),
            #[cfg(test)]
            actor_return: Mutex::new(None),
            descriptor: Mutex::new(Descriptor {
                position: Position::NoTurn,
                draining: false,
                suppressed: false,
            }),
        }
    }
    /// Descriptor-only claim; the caller can hold its emission gate here.
    pub(crate) fn claim_start(&self) -> bool {
        let Ok(mut descriptor) = self.descriptor.lock() else {
            return false;
        };
        if descriptor.draining || descriptor.suppressed {
            return false;
        }
        if matches!(
            descriptor.position,
            Position::StartMapping(_) | Position::Retired | Position::Completed
        ) {
            return false;
        }
        let prior = std::mem::replace(&mut descriptor.position, Position::NoTurn);
        descriptor.position = Position::StartMapping(match prior {
            Position::FinalInstalled(commit) => Some(commit),
            _ => None,
        });
        true
    }
    /// No adapter lock may be held across this notifying core operation.
    pub(crate) fn begin_start(&self) -> Result<Option<NativeEagerTurn>, ProcessorError> {
        {
            let descriptor = self.lock()?;
            if descriptor.draining
                || descriptor.suppressed
                || !matches!(descriptor.position, Position::StartMapping(_))
            {
                return Ok(None);
            }
        }
        let result = self.owner.begin_turn();
        let mut descriptor = self.lock()?;
        if descriptor.draining
            || descriptor.suppressed
            || !matches!(descriptor.position, Position::StartMapping(_))
        {
            // prepare_end already resolved the begin_turn/begin_drain winner.
            // In particular do not revoke an old installed final when drain won.
            return Ok(None);
        }
        let turn = result?;
        let prior = std::mem::replace(
            &mut descriptor.position,
            Position::AwaitingBarrier(turn.clone()),
        );
        drop(descriptor);
        drop(prior);
        Ok(Some(turn))
    }
    pub(crate) fn accepts_start(&self, turn: &NativeEagerTurn) -> bool {
        self.descriptor.lock().is_ok_and(|descriptor| {
            !descriptor.draining && !descriptor.suppressed
                && matches!(&descriptor.position, Position::AwaitingBarrier(current) if current == turn)
        })
    }
    /// A flush acknowledgment becomes Ready only at this installation point.
    pub(crate) fn mark_ready(&self, turn: &NativeEagerTurn) -> bool {
        let Ok(mut descriptor) = self.descriptor.lock() else {
            return false;
        };
        if descriptor.draining
            || descriptor.suppressed
            || !matches!(&descriptor.position, Position::AwaitingBarrier(current) if current == turn)
        {
            return false;
        }
        descriptor.position = Position::Ready(turn.clone());
        true
    }
    pub(crate) fn ready_turn(&self) -> Option<NativeEagerTurn> {
        let descriptor = self.descriptor.lock().ok()?;
        if descriptor.suppressed {
            return None;
        }
        match &descriptor.position {
            Position::Ready(turn) => Some(turn.clone()),
            _ => None,
        }
    }
    pub(crate) fn accepts_prediction(&self, turn: &NativeEagerTurn) -> bool {
        self.descriptor.lock().is_ok_and(|descriptor| {
            !descriptor.draining
                && !descriptor.suppressed
                && matches!(&descriptor.position, Position::Ready(current) if current == turn)
        })
    }
    pub(crate) fn predict(
        &self,
        turn: &NativeEagerTurn,
        payload: FrameData,
    ) -> Result<Option<NativeEagerPrediction>, ProcessorError> {
        if !self.accepts_prediction(turn) {
            return Ok(None);
        }
        let result = self.owner.predict(turn, payload);
        if !self.accepts_prediction(turn) {
            if let Ok(prediction) = &result {
                // Drain may win after core allocation. An unpublished token
                // must not linger as a live prediction behind that cutoff.
                self.owner.resume(prediction);
            }
            return Ok(None);
        }
        result.map(Some)
    }
    pub(crate) fn resume(&self, prediction: &NativeEagerPrediction) -> bool {
        self.owner.resume(prediction)
    }
    pub(crate) fn offer_final(
        &self,
        turn: &NativeEagerTurn,
        payload: FrameData,
    ) -> Result<Option<NativeEagerCommit>, ProcessorError> {
        if !self.begin_offer(turn)? {
            return Ok(None);
        }
        let result = self.owner.offer_final(turn, payload);
        self.install_final(turn, result)
    }
    fn begin_offer(&self, turn: &NativeEagerTurn) -> Result<bool, ProcessorError> {
        let mut descriptor = self.lock()?;
        if descriptor.suppressed
            || !matches!(&descriptor.position, Position::Ready(current) if current == turn)
        {
            return Ok(false);
        }
        descriptor.position = Position::FinalOffering(turn.clone());
        Ok(true)
    }
    fn install_final(
        &self,
        turn: &NativeEagerTurn,
        result: Result<NativeEagerCommit, ProcessorError>,
    ) -> Result<Option<NativeEagerCommit>, ProcessorError> {
        let mut descriptor = self.lock()?;
        if descriptor.suppressed
            || !matches!(&descriptor.position, Position::FinalOffering(current) if current == turn)
        {
            drop(descriptor);
            // Core allocation is not our publication cutoff. A skipped Start
            // may retire this receipt before it has ever left the mapper.
            drop(result);
            return Ok(None);
        }
        let commit = result?;
        descriptor.position = Position::FinalInstalled(commit.clone());
        Ok(Some(commit))
    }
    pub(crate) fn accepts_final(&self, commit: &NativeEagerCommit) -> bool {
        self.descriptor.lock().is_ok_and(|descriptor| {
            matches!(&descriptor.position, Position::FinalInstalled(current) | Position::SuppressedPreserving(current) if current == commit)
        })
    }
    pub(crate) fn offer_empty_stop(&self, turn: &NativeEagerTurn) -> Result<bool, ProcessorError> {
        if !self.begin_offer(turn)? {
            return Ok(false);
        }
        let result = self.owner.offer_empty_stop(turn);
        let mut descriptor = self.lock()?;
        if descriptor.suppressed
            || !matches!(&descriptor.position, Position::FinalOffering(current) if current == turn)
        {
            return Ok(false);
        }
        result?;
        descriptor.position = Position::EmptyClosed(turn.clone());
        Ok(true)
    }
    pub(crate) fn accepts_empty_stop(&self, turn: &NativeEagerTurn) -> bool {
        self.descriptor.lock().is_ok_and(|descriptor| {
            !descriptor.suppressed
                && matches!(&descriptor.position, Position::EmptyClosed(current) if current == turn)
        })
    }
    pub(crate) fn prepare_end(&self) {
        let decision = self.descriptor.lock().ok().map(|mut descriptor| {
            descriptor.draining = true;
            match &descriptor.position {
                Position::AwaitingBarrier(_) | Position::StartMapping(None) => {
                    DrainDecision::Retire
                }
                Position::StartMapping(Some(commit)) => DrainDecision::Prior(commit.turn().clone()),
                _ => DrainDecision::Preserve,
            }
        });
        let Some(decision) = decision else {
            self.owner.revoke();
            return;
        };
        self.owner.begin_drain();
        self.apply_drain(decision);
    }
    pub(crate) fn skipped_start(&self) {
        let decision = self.descriptor.lock().ok().map(|mut descriptor| {
            descriptor.draining = true;
            descriptor.suppressed = true;
            let old = std::mem::replace(&mut descriptor.position, Position::Retired);
            match old {
                Position::FinalInstalled(commit) | Position::SuppressedPreserving(commit) => {
                    descriptor.position = Position::SuppressedPreserving(commit);
                    (DrainDecision::Preserve, None)
                }
                Position::StartMapping(Some(commit)) => {
                    let turn = commit.turn().clone();
                    descriptor.position = Position::SuppressedPreserving(commit);
                    (DrainDecision::Prior(turn), None)
                }
                other => (DrainDecision::Retire, Some(other)),
            }
        });
        let Some((decision, old)) = decision else {
            self.owner.revoke();
            return;
        };
        drop(old);
        self.owner.begin_drain();
        self.apply_drain(decision);
    }
    fn apply_drain(&self, decision: DrainDecision) {
        match decision {
            DrainDecision::Preserve => (),
            DrainDecision::Prior(turn) if turn.is_live() => (),
            DrainDecision::Prior(_) | DrainDecision::Retire => self.revoke(),
        }
    }
    pub(crate) fn revoke(&self) {
        let old = self.descriptor.lock().ok().map(|mut descriptor| {
            descriptor.draining = true;
            descriptor.suppressed = true;
            std::mem::replace(&mut descriptor.position, Position::Retired)
        });
        drop(old);
        self.owner.revoke();
    }
    pub(crate) fn complete_drain(&self) -> bool {
        let old = self.descriptor.lock().ok().map(|mut descriptor| {
            descriptor.draining = true;
            std::mem::replace(&mut descriptor.position, Position::Completed)
        });
        let Some(old) = old else {
            self.owner.revoke();
            return false;
        };
        // Drop all internal receipt proof BEFORE complete_drain: real native
        // queues/consumer stages, never this bookkeeping, justify preservation.
        drop(old);
        self.owner.complete_drain()
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Descriptor>, ProcessorError> {
        self.descriptor
            .lock()
            .map_err(|_| ProcessorError::fatal("Cartesia eager descriptor poisoned"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{NativeEagerConsumer, SpeculationOwner, TranscriptionFrame};

    fn ready(session: &EagerSession) -> NativeEagerTurn {
        assert!(session.claim_start());
        let turn = session.begin_start().unwrap().unwrap();
        assert!(session.accepts_start(&turn));
        assert!(session.mark_ready(&turn));
        turn
    }
    fn payload(eager: bool) -> FrameData {
        let value = TranscriptionFrame {
            text: "the exact transcript".into(),
            user_id: "speaker".into(),
            timestamp: "one immutable timestamp".into(),
            language: Some("en".into()),
            result: Some(serde_json::json!({"literal": [1, "same"]})),
            finalized: !eager,
        };
        if eager {
            FrameData::EagerTranscription(value)
        } else {
            FrameData::Transcription(value)
        }
    }
    fn predicted(session: &EagerSession, turn: &NativeEagerTurn) -> NativeEagerPrediction {
        session.predict(turn, payload(true)).unwrap().unwrap()
    }
    fn final_receipt(session: &EagerSession, turn: &NativeEagerTurn) -> NativeEagerCommit {
        session.offer_final(turn, payload(false)).unwrap().unwrap()
    }

    #[test]
    fn unresolved_start_retires_before_reader_or_barrier_can_resume() {
        let session = EagerSession::for_test();
        assert!(session.claim_start());
        let turn = session.begin_start().unwrap().unwrap();
        assert!(turn.is_active());
        session.prepare_end();
        assert!(!turn.is_active());
        assert!(!session.mark_ready(&turn));
        assert!(!session.accepts_start(&turn));
    }

    #[test]
    fn acknowledged_but_not_installed_ready_is_still_unresolved() {
        // The caller has received its real flush acknowledgement at this pause,
        // but only mark_ready is the adapter's installation boundary.
        let session = EagerSession::for_test();
        assert!(session.claim_start());
        let turn = session.begin_start().unwrap().unwrap();
        session.prepare_end();
        assert!(!session.mark_ready(&turn));
        assert!(!turn.is_active());
    }

    #[test]
    fn start_mapping_drain_winner_preserves_prior_installed_final() {
        let session = EagerSession::for_test();
        let a = ready(&session);
        let final_a = final_receipt(&session, &a);
        assert!(session.claim_start());
        // Pause before begin_turn(B). End cannot wait for that reader.
        session.prepare_end();
        assert!(session.begin_start().unwrap().is_none());
        assert!(final_a.is_live());
        assert!(session.complete_drain());
        assert!(final_a.is_live());
    }

    #[test]
    fn start_mapping_begin_turn_winner_retires_unpublished_b() {
        let session = EagerSession::for_test();
        let a = ready(&session);
        let final_a = final_receipt(&session, &a);
        assert!(session.claim_start());
        // Pause after actual core begin_turn(B), before helper installation.
        let b = session.owner.begin_turn().unwrap();
        assert!(!a.is_live());
        session.prepare_end();
        assert!(!b.is_active());
        assert!(!final_a.is_live());
    }

    #[test]
    fn skipped_start_while_mapping_uses_the_same_prior_turn_winner() {
        for begin_wins in [false, true] {
            let session = EagerSession::for_test();
            let a = ready(&session);
            let final_a = final_receipt(&session, &a);
            assert!(session.claim_start());
            let b = begin_wins.then(|| session.owner.begin_turn().unwrap());
            session.skipped_start();
            assert_eq!(final_a.is_live(), !begin_wins);
            assert!(b.is_none_or(|turn| !turn.is_active()));
            assert!(session.begin_start().unwrap().is_none());
        }
    }

    #[test]
    fn final_offering_before_core_offer_loses_to_skipped_start() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        assert!(session.begin_offer(&turn).unwrap());
        session.skipped_start();
        let result = session.owner.offer_final(&turn, payload(false));
        assert!(result.is_err());
        assert!(session.install_final(&turn, result).unwrap().is_none());
        assert!(!turn.is_active());
    }

    #[test]
    fn final_offering_after_core_offer_still_loses_until_installation() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        assert!(session.begin_offer(&turn).unwrap());
        let commit = session.owner.offer_final(&turn, payload(false)).unwrap();
        assert!(commit.is_live());
        session.skipped_start();
        assert!(!commit.is_live());
        assert!(session.install_final(&turn, Ok(commit)).unwrap().is_none());
        assert!(!turn.is_active());
    }

    #[test]
    fn installed_receipt_is_preserved_even_before_native_publication() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let prediction = predicted(&session, &turn);
        let commit = final_receipt(&session, &turn);
        session.skipped_start();
        assert!(session.accepts_final(&commit));
        assert!(commit.is_live());
        assert!(session.ready_turn().is_none());
        assert!(session.complete_drain());
        assert!(commit.is_live());
        assert!(prediction.is_live());
        assert!(!turn.is_active());
    }

    #[test]
    fn clean_eof_allows_trailing_final_at_each_offer_pause() {
        for cutoff in 0..3 {
            let session = EagerSession::for_test();
            let turn = ready(&session);
            let prediction = predicted(&session, &turn);
            assert!(session.begin_offer(&turn).unwrap());
            if cutoff == 0 {
                session.prepare_end();
            }
            let result = session.owner.offer_final(&turn, payload(false));
            if cutoff == 1 {
                session.prepare_end();
            }
            let commit = session.install_final(&turn, result).unwrap().unwrap();
            if cutoff == 2 {
                session.prepare_end();
            }
            assert!(session.accepts_final(&commit));
            assert!(session.complete_drain());
            assert!(commit.is_live());
            assert!(prediction.is_live());
            assert!(!turn.is_active());
        }
    }

    #[test]
    fn helper_only_receipt_cannot_preserve_prediction_at_eof() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let prediction = predicted(&session, &turn);
        let commit = final_receipt(&session, &turn);
        drop(commit); // All genuine Final/Stop queues/stages have dropped theirs.
        session.prepare_end();
        assert!(session.complete_drain());
        assert!(!turn.is_live());
        assert!(!prediction.is_live());
        let consumer = NativeEagerConsumer::new();
        assert!(!turn.claim_start(&consumer));
        assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).is_none());
    }

    #[test]
    fn queued_real_receipt_preserves_start_and_prediction_adoption_at_eof() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let prediction = predicted(&session, &turn);
        let commit = final_receipt(&session, &turn);
        session.prepare_end();
        assert!(session.complete_drain());
        let consumer = NativeEagerConsumer::new();
        assert!(turn.claim_start(&consumer));
        let _owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        assert!(commit.claim_final(&consumer));
        assert!(commit.claim_stop(&consumer));
    }

    #[test]
    fn normal_no_final_eof_retires_recovery_and_prediction() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let prediction = predicted(&session, &turn);
        session.prepare_end();
        // Ordinary EOF alone permits trailing final while waiting for reader.
        assert!(turn.is_active());
        assert!(session.complete_drain());
        assert!(!turn.is_active());
        assert!(!prediction.is_live());
    }

    #[test]
    fn empty_stop_is_owner_offered_once_and_not_a_final_receipt() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let consumer = NativeEagerConsumer::new();
        assert!(turn.claim_start(&consumer));
        assert!(!turn.claim_empty_stop(&consumer));
        assert!(session.offer_empty_stop(&turn).unwrap());
        assert!(session.accepts_empty_stop(&turn));
        assert!(turn.claim_empty_stop(&consumer));
        assert!(!turn.claim_empty_stop(&consumer));
        assert!(!session.offer_empty_stop(&turn).unwrap());
        session.prepare_end();
        assert!(session.complete_drain());
        assert!(!turn.is_active());
    }

    #[test]
    fn duplicate_final_and_post_drain_prediction_are_not_published() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        session.prepare_end();
        assert!(session.predict(&turn, payload(true)).unwrap().is_none());
        let commit = final_receipt(&session, &turn);
        assert!(session
            .offer_final(&turn, payload(false))
            .unwrap()
            .is_none());
        assert!(session.accepts_final(&commit));
    }

    #[test]
    fn explicit_retirement_after_clean_eof_remains_destructive() {
        let session = EagerSession::for_test();
        let turn = ready(&session);
        let commit = final_receipt(&session, &turn);
        session.prepare_end();
        assert!(session.complete_drain());
        assert!(commit.is_live());
        session.revoke();
        assert!(!commit.is_live());
    }
}
