use crate::{Error, Result};
use bytes::Bytes;

pub(crate) async fn payload(mut frame: moq_net::frame::Consumer, limit: usize) -> Result<Bytes> {
    if frame.size > limit as u64 {
        return Err(Error::Limit("wire frame"));
    }
    frame.read_all().await.map_err(|e| {
        if matches!(e, moq_net::Error::Old) {
            Error::Stale
        } else {
            Error::protocol(e)
        }
    })
}

pub(crate) fn version() -> moq_net::Version {
    "moq-lite-06".parse().expect("pinned protocol version")
}

pub(crate) async fn subscribe(
    broadcast: &moq_net::broadcast::Consumer,
    name: &str,
    max_age: std::time::Duration,
) -> Result<moq_net::track::Subscriber> {
    broadcast
        .track(name)
        .map_err(Error::protocol)?
        .subscribe(moq_net::track::Subscription::default().with_max_age(max_age))
        .await
        .map_err(Error::protocol)
}

/// Snapshot and RFC 7396 deltas are interpreted by the source-compatible codec.
pub(crate) async fn audio_catalog(
    broadcast: &moq_net::broadcast::Consumer,
    limit: usize,
) -> Result<(String, hang::catalog::AudioConfig)> {
    let mut track = subscribe(broadcast, "catalog.json", std::time::Duration::ZERO)
        .await?
        .ordered();
    let mut decoder =
        moq_json::snapshot::Decoder::<hang::catalog::Catalog>::new(Default::default());
    let mut group: Option<CatalogGroup> = None;
    let mut first = true;
    let mut finished = false;
    let mut updates = 0usize;
    enum Next {
        Group(Option<Box<moq_net::group::Consumer>>),
        Frame(Option<Bytes>),
    }
    loop {
        let has_group = group.is_some();
        // A new snapshot supersedes even a partially received old frame.
        let next = tokio::select! {
            biased;
            next = track.next_group(), if !finished => Next::Group(next.map_err(Error::protocol)?.map(Box::new)),
            frame = async { group.as_mut().expect("guarded group").next(limit).await }, if has_group => Next::Frame(frame?),
            else => break,
        };
        let bytes = match next {
            Next::Group(Some(next)) => {
                group = Some(CatalogGroup {
                    group: *next,
                    frame: None,
                });
                first = true;
                decoder = moq_json::snapshot::Decoder::new(Default::default());
                continue;
            }
            Next::Group(None) => {
                finished = true;
                continue;
            }
            Next::Frame(None) => {
                group = None;
                continue;
            }
            Next::Frame(Some(bytes)) => bytes,
        };
        updates += 1;
        if updates > 256 {
            return Err(Error::Limit("catalog updates before audio"));
        }
        if first {
            decoder.snapshot(&bytes).map_err(Error::protocol)?;
            first = false;
        } else {
            decoder.delta(&bytes).map_err(Error::protocol)?;
        }
        let value = decoder
            .value()
            .ok_or_else(|| Error::Protocol("missing catalog snapshot".into()))?;
        super::transcript::json_bytes(value, limit)
            .map_err(|_| Error::Limit("reconstructed catalog"))?;
        let catalog = decoder
            .decode()
            .map_err(Error::protocol)?
            .ok_or_else(|| Error::Protocol("missing catalog".into()))?;
        catalog.check_renditions().map_err(Error::protocol)?;
        if let Some((name, config)) = catalog.audio.renditions.into_iter().next() {
            return Ok((name, config));
        }
    }
    Err(Error::Protocol(
        "catalog ended before audio appeared".into(),
    ))
}

struct CatalogGroup {
    group: moq_net::group::Consumer,
    frame: Option<moq_net::frame::Consumer>,
}
impl CatalogGroup {
    async fn next(&mut self, limit: usize) -> Result<Option<Bytes>> {
        if self.frame.is_none() {
            self.frame = match self.group.next_frame().await {
                Ok(Some(frame)) => Some(frame),
                // Snapshot groups can be reclaimed or overtaken. Only track failure is fatal.
                Ok(None) | Err(_) => return Ok(None),
            };
        }
        let frame = self.frame.as_mut().expect("frame present");
        if frame.size > limit as u64 {
            return Err(Error::Limit("wire frame"));
        }
        // read_all does not advance until ready. Keep its consumer across a track
        // finish notification; only a replacement snapshot discards this state.
        let bytes = frame.read_all().await;
        self.frame = None;
        Ok(bytes.ok())
    }
}
