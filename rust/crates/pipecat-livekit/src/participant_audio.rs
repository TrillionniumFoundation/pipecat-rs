//! Current-publication requests owned by the existing room loop, not a mute policy.
use std::time::Duration;

use livekit::{prelude::RemoteTrackPublication, track::TrackKind};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{ConnectionState, Error, LiveKitConfig, Result};

pub(crate) fn ready(state: &ConnectionState, cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(Error::Closed);
    }
    match state {
        ConnectionState::Connected => Ok(()),
        ConnectionState::Closed | ConnectionState::Failed(_) => Err(Error::Closed),
        ConnectionState::Connecting | ConnectionState::Reconnecting => Err(Error::NotReady),
    }
}

pub(crate) fn sdk_ready(state: livekit::ConnectionState) -> Result<()> {
    match state {
        livekit::ConnectionState::Connected => Ok(()),
        livekit::ConnectionState::Disconnected => Err(Error::Closed),
        livekit::ConnectionState::Reconnecting => Err(Error::NotReady),
    }
}

pub(crate) struct AudioControl {
    tx: mpsc::Sender<AudioRequest>,
    state: watch::Receiver<ConnectionState>,
    cancel: CancellationToken,
    timeout: Duration,
}
impl AudioControl {
    pub(crate) fn channel(
        capacity: usize,
        state: watch::Receiver<ConnectionState>,
        cancel: CancellationToken,
        timeout: Duration,
    ) -> (Self, mpsc::Receiver<AudioRequest>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                state,
                cancel,
                timeout,
            },
            rx,
        )
    }

    pub(crate) async fn set_subscribed(&self, identity: &str, subscribed: bool) -> Result<()> {
        ready(&self.state.borrow(), &self.cancel)?;
        if identity.is_empty() || identity.len() > 256 {
            return Err(Error::InvalidConfig("invalid participant identity"));
        }
        let cancel = CancellationToken::new();
        let _guard = CancelRequest(cancel.clone());
        let (reply, mut received) = oneshot::channel();
        let expires = Instant::now()
            .checked_add(self.timeout)
            .ok_or(Error::InvalidConfig(
                "participant audio timeout exceeds clock range",
            ))?;
        self.tx
            .try_send(AudioRequest {
                identity: identity.to_owned(),
                subscribed,
                cancel,
                expires,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        let mut state = self.state.clone();
        let deadline = tokio::time::sleep_until(expires);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Error::Closed),
                changed = state.changed() => {
                    changed.map_err(|_| Error::Closed)?;
                    ready(&state.borrow_and_update(), &self.cancel)?;
                }
                _ = &mut deadline => return Err(Error::Timeout("participant audio subscription")),
                result = &mut received => return result.map_err(|_| Error::Closed)?,
            }
        }
    }
}
struct CancelRequest(CancellationToken);
impl Drop for CancelRequest {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(crate) struct AudioRequest {
    identity: String,
    subscribed: bool,
    cancel: CancellationToken,
    expires: Instant,
    reply: oneshot::Sender<Result<()>>,
}
impl AudioRequest {
    /// Invoked only by the room owner. Success is local setter completion, not SFU receipt.
    pub(crate) fn execute(
        self,
        state: &ConnectionState,
        cancel: &CancellationToken,
        apply: impl FnOnce(&str, bool, &dyn Fn() -> Result<()>) -> Result<()>,
    ) {
        let check = || {
            ready(state, cancel)?;
            if self.cancel.is_cancelled() || self.reply.is_closed() {
                return Err(Error::Interrupted);
            }
            if Instant::now() >= self.expires {
                return Err(Error::Timeout("participant audio subscription"));
            }
            Ok(())
        };
        let result = check().and_then(|()| apply(&self.identity, self.subscribed, &check));
        let _ = self.reply.send(result);
    }
}

/// This interface exposes the actual production setter boundary to no-room tests.
pub(crate) trait AudioPublication {
    fn kind(&self) -> TrackKind;
    fn set_subscribed(&self, subscribed: bool);
}
impl AudioPublication for RemoteTrackPublication {
    fn kind(&self) -> TrackKind {
        RemoteTrackPublication::kind(self)
    }
    fn set_subscribed(&self, subscribed: bool) {
        RemoteTrackPublication::set_subscribed(self, subscribed);
    }
}

/// Select before obtaining publications; the caller supplies only current remotes.
/// No retained identities, deduplication or future-track policy is installed.
pub(crate) fn apply_current<P, A: AudioPublication>(
    config: &LiveKitConfig,
    identity: &str,
    subscribed: bool,
    remotes: impl IntoIterator<Item = (String, P)>,
    publications: impl FnOnce(P) -> Vec<A>,
    check: impl Fn() -> Result<()>,
) -> Result<()> {
    if !config.accepts(identity) {
        return Ok(());
    }
    let Some((_, participant)) = remotes.into_iter().find(|(current, _)| current == identity)
    else {
        return Ok(());
    };
    for publication in publications(participant) {
        check()?;
        if publication.kind() == TrackKind::Audio {
            publication.set_subscribed(subscribed);
        }
    }
    Ok(())
}
