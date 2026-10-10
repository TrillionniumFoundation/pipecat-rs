use std::{borrow::Cow, time::Duration};

use livekit::webrtc::{audio_frame::AudioFrame, audio_source::native::NativeAudioSource};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, SourceAccepted};

pub(crate) struct AudioRequest {
    frame: AudioFrame<'static>,
    epoch: u64,
    cancel: CancellationToken,
    reply: oneshot::Sender<Result<SourceAccepted>>,
}
#[derive(Clone)]
pub(crate) struct AudioPublisher {
    tx: mpsc::Sender<AudioRequest>,
    epoch: watch::Sender<u64>,
    cancel: CancellationToken,
    timeout: Duration,
}
impl AudioPublisher {
    pub(crate) fn channel(
        capacity: usize,
        cancel: CancellationToken,
        timeout: Duration,
    ) -> (Self, mpsc::Receiver<AudioRequest>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (epoch, _) = watch::channel(0);
        (
            Self {
                tx,
                epoch,
                cancel,
                timeout,
            },
            rx,
        )
    }
    pub(crate) fn interrupt(&self) {
        self.epoch
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
    pub(crate) fn generation(&self) -> u64 {
        *self.epoch.borrow()
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.epoch.subscribe()
    }
    pub(crate) async fn publish(&self, frame: AudioFrame<'static>) -> Result<SourceAccepted> {
        if self.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        let request_cancel = CancellationToken::new();
        let _guard = CancelRequest(request_cancel.clone());
        let (reply, received) = oneshot::channel();
        let request = AudioRequest {
            frame,
            epoch: *self.epoch.borrow(),
            cancel: request_cancel,
            reply,
        };
        self.tx.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::QueueFull,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Error::Closed),
            result = tokio::time::timeout(self.timeout, received) => result.map_err(|_| Error::Timeout("audio publication"))?.map_err(|_| Error::Closed)?,
        }
    }
}
struct CancelRequest(CancellationToken);
impl Drop for CancelRequest {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// One owner serializes SDK calls. Cancelled capture callbacks are drained before
/// admitting another chunk; clear_buffer alone does not clear the SDK callback.
pub(crate) async fn run_publisher(
    source: NativeAudioSource,
    mut rx: mpsc::Receiver<AudioRequest>,
    mut epoch: watch::Receiver<u64>,
    cancel: CancellationToken,
    source_queue_ms: u32,
    operation_timeout: Duration,
) -> Result<()> {
    loop {
        let request = tokio::select! { biased; _ = cancel.cancelled() => break, request = rx.recv() => request };
        let Some(request) = request else {
            break;
        };
        if request.cancel.is_cancelled() || request.epoch != *epoch.borrow_and_update() {
            let _ = request.reply.send(Err(Error::Interrupted));
            continue;
        }
        let chunk_samples = if source_queue_ms == 0 {
            request.frame.sample_rate / 100 * request.frame.num_channels
        } else {
            request.frame.sample_rate * source_queue_ms * request.frame.num_channels / 1000
        } as usize;
        if source_queue_ms == 0 && request.frame.data.len() != chunk_samples {
            let _ = request.reply.send(Err(Error::Audio(
                "unbuffered native source requires exactly 10ms per frame".into(),
            )));
            continue;
        }
        let outcome = async {
            for samples in request.frame.data.chunks(chunk_samples) {
                if cancel.is_cancelled() {
                    return Err(Error::Closed);
                }
                if request.cancel.is_cancelled() || request.epoch != *epoch.borrow_and_update() {
                    return Err(Error::Interrupted);
                }
                let frame = AudioFrame {
                    data: Cow::Borrowed(samples),
                    sample_rate: request.frame.sample_rate,
                    num_channels: request.frame.num_channels,
                    samples_per_channel: samples.len() as u32 / request.frame.num_channels,
                };
                let capture = source.capture_frame(&frame);
                tokio::pin!(capture);
                // Poll capture first so the cancellation branch only drains a call
                // that actually started; this SDK call contains exactly one chunk.
                let capture_result = tokio::select! {
                    biased;
                    result = &mut capture => Some(result.map_err(|e| Error::Sdk(e.to_string()))),
                    _ = cancel.cancelled() => None,
                    _ = request.cancel.cancelled() => None,
                    _ = epoch.changed() => None,
                    _ = tokio::time::sleep(operation_timeout) => None,
                };
                match capture_result {
                    Some(result) => result?,
                    None => {
                        let interrupted = cancel.is_cancelled()
                            || request.cancel.is_cancelled()
                            || request.epoch != *epoch.borrow();
                        source.clear_buffer();
                        tokio::time::timeout(operation_timeout, &mut capture)
                            .await
                            .map_err(|_| Error::Timeout("cancelled native capture drain"))?
                            .map_err(|e| Error::Sdk(e.to_string()))?;
                        return if cancel.is_cancelled() {
                            Err(Error::Closed)
                        } else if interrupted {
                            Err(Error::Interrupted)
                        } else {
                            Err(Error::Timeout("native audio capture"))
                        };
                    }
                }
                if cancel.is_cancelled() {
                    return Err(Error::Closed);
                }
                if request.cancel.is_cancelled() || request.epoch != *epoch.borrow_and_update() {
                    return Err(Error::Interrupted);
                }
            }
            Ok(SourceAccepted {
                samples_per_channel: request.frame.samples_per_channel,
            })
        }
        .await;
        let fatal = matches!(outcome, Err(Error::Timeout(_)) | Err(Error::Sdk(_)));
        if outcome.is_err() {
            source.clear_buffer();
        }
        let error = if fatal {
            Some(outcome.as_ref().unwrap_err().to_string())
        } else {
            None
        };
        let _ = request.reply.send(outcome);
        if let Some(error) = error {
            return Err(Error::Sdk(error));
        }
    }
    source.clear_buffer();
    rx.close();
    while let Some(request) = rx.recv().await {
        let _ = request.reply.send(Err(Error::Closed));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use livekit::{
        prelude::LocalAudioTrack,
        webrtc::{
            audio_source::{AudioSourceOptions, RtcAudioSource},
            audio_stream::native::NativeAudioStream,
        },
    };

    fn frame(samples: usize, value: i16) -> AudioFrame<'static> {
        AudioFrame {
            data: Cow::Owned(vec![value; samples]),
            sample_rate: 16000,
            num_channels: 1,
            samples_per_channel: samples as u32,
        }
    }
    #[tokio::test]
    async fn bounded_publication_admission_and_cancelled_request() {
        let cancel = CancellationToken::new();
        let (publisher, mut rx) = AudioPublisher::channel(1, cancel, Duration::from_secs(1));
        let clone = publisher.clone();
        let first = tokio::spawn(async move { clone.publish(frame(160, 1)).await });
        tokio::task::yield_now().await;
        assert!(matches!(
            publisher.publish(frame(160, 2)).await,
            Err(Error::QueueFull)
        ));
        first.abort();
        let _ = first.await;
        assert!(rx.recv().await.unwrap().cancel.is_cancelled());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_interruption_drains_callback_before_next_publication() {
        let source = NativeAudioSource::new(AudioSourceOptions::default(), 16000, 1, 20);
        let track = LocalAudioTrack::create_audio_track(
            "cancel-test",
            RtcAudioSource::Native(source.clone()),
        );
        let mut stream = NativeAudioStream::new(track.rtc_track(), 16000, 1);
        let cancel = CancellationToken::new();
        let (publisher, rx) = AudioPublisher::channel(4, cancel.clone(), Duration::from_secs(2));
        let task = tokio::spawn(run_publisher(
            source,
            rx,
            publisher.subscribe(),
            cancel.clone(),
            20,
            Duration::from_secs(1),
        ));
        let clone = publisher.clone();
        let pending = tokio::spawn(async move { clone.publish(frame(16000, 111)).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if stream.next().await.unwrap().data.contains(&111) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        publisher.interrupt();
        assert!(matches!(pending.await.unwrap(), Err(Error::Interrupted)));
        assert!(publisher.publish(frame(160, 222)).await.is_ok());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if stream.next().await.unwrap().data.contains(&222) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        for _ in 0..5 {
            let audio = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap();
            assert!(
                !audio.data.contains(&111),
                "cancelled chunks must not resume after the next publication"
            );
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_shutdown_joins_pending_capture() {
        let source = NativeAudioSource::new(AudioSourceOptions::default(), 16000, 1, 10);
        let cancel = CancellationToken::new();
        let (publisher, rx) = AudioPublisher::channel(2, cancel.clone(), Duration::from_secs(2));
        let task = tokio::spawn(run_publisher(
            source,
            rx,
            publisher.subscribe(),
            cancel.clone(),
            10,
            Duration::from_secs(1),
        ));
        let clone = publisher.clone();
        let pending = tokio::spawn(async move { clone.publish(frame(16000, 1)).await });
        tokio::time::sleep(Duration::from_millis(15)).await;
        cancel.cancel();
        assert!(matches!(pending.await.unwrap(), Err(Error::Closed)));
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
