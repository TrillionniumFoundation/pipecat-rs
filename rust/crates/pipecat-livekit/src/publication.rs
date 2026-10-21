//! One room owner's sequential publication transaction. Mocks test ownership only.
use crate::{Error, LiveKitConfig, Result};
use async_trait::async_trait;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait PublicationOwner: Send {
    async fn publish_audio(&mut self) -> Result<()>;
    async fn publish_video(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
}
pub(crate) async fn initialize(
    owner: &mut impl PublicationOwner,
    config: &LiveKitConfig,
    cancel: &CancellationToken,
) -> Result<()> {
    let result = async {
        if config.output_format.is_some() {
            tokio::select! { biased;
                _ = cancel.cancelled() => return Err(Error::Closed),
                result = timeout(config.operation_timeout, owner.publish_audio()) => result.map_err(|_| Error::Timeout("audio track publication"))??,
            }
        }
        if config.video_output.is_some() {
            tokio::select! { biased;
                _ = cancel.cancelled() => return Err(Error::Closed),
                result = timeout(config.operation_timeout, owner.publish_video()) => result.map_err(|_| Error::Timeout("video track publication"))??,
            }
        }
        if cancel.is_cancelled() { return Err(Error::Closed); }
        Ok(())
    }.await;
    match result {
        Ok(()) => Ok(()),
        Err(primary) => {
            let cleanup = timeout(config.close_timeout, owner.rollback())
                .await
                .map_err(|_| Error::Timeout("publication rollback"))
                .and_then(|result| result);
            Err(with_cleanup(primary, cleanup))
        }
    }
}
pub(crate) fn with_cleanup(primary: Error, cleanup: Result<()>) -> Error {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => Error::Sdk(format!("{primary}; rollback: {cleanup}")),
    }
}
pub(crate) fn combine_cleanup(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match primary {
        Ok(()) => cleanup,
        Err(primary) => Err(with_cleanup(primary, cleanup)),
    }
}

/// Send ownership to the waiting caller. A vanished caller signals the existing room
/// owner; it must run its normal bounded close path rather than detach another task.
pub(crate) fn transfer_ready<T>(
    sender: tokio::sync::oneshot::Sender<Result<T>>,
    value: T,
    cancel: &CancellationToken,
) {
    if sender.send(Ok(value)).is_err() {
        cancel.cancel();
    }
}

#[cfg(test)]
mod tests;
