use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Probe {
    calls: Vec<&'static str>,
    live: usize,
}
struct MockOwner {
    probe: Arc<Mutex<Probe>>,
    fail: Option<&'static str>,
    pending: Option<&'static str>,
    rollback_fails: bool,
}
impl Drop for MockOwner {
    fn drop(&mut self) {
        self.probe.lock().unwrap().live = 0;
    }
}
impl MockOwner {
    async fn publish(&mut self, stage: &'static str) -> Result<()> {
        {
            let mut probe = self.probe.lock().unwrap();
            probe.calls.push(stage);
            probe.live += 1;
        }
        if self.pending == Some(stage) {
            std::future::pending::<()>().await;
        }
        if self.fail == Some(stage) {
            return Err(Error::Sdk(format!("{stage} failure")));
        }
        Ok(())
    }
}
#[async_trait]
impl PublicationOwner for MockOwner {
    async fn publish_audio(&mut self) -> Result<()> {
        self.publish("audio").await
    }
    async fn publish_video(&mut self) -> Result<()> {
        self.publish("video").await
    }
    async fn rollback(&mut self) -> Result<()> {
        {
            let mut probe = self.probe.lock().unwrap();
            probe.calls.push("rollback");
            probe.live = 0;
        }
        if self.rollback_fails {
            Err(Error::Sdk("cleanup failure".into()))
        } else {
            Ok(())
        }
    }
}
fn fixture() -> (MockOwner, LiveKitConfig) {
    (
        MockOwner {
            probe: Arc::new(Mutex::new(Probe::default())),
            fail: None,
            pending: None,
            rollback_fails: false,
        },
        LiveKitConfig {
            video_output: Some(crate::VideoOutputConfig::default()),
            operation_timeout: Duration::from_millis(30),
            close_timeout: Duration::from_millis(30),
            ..Default::default()
        },
    )
}
#[tokio::test]
async fn mock_publication_orders_all_enabled_media_before_ready() {
    for (audio, video, expected) in [
        (true, true, vec!["audio", "video"]),
        (true, false, vec!["audio"]),
        (false, true, vec!["video"]),
        (false, false, vec![]),
    ] {
        let (mut owner, mut config) = fixture();
        if !audio {
            config.output_format = None;
        }
        if !video {
            config.video_output = None;
        }
        initialize(&mut owner, &config, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(owner.probe.lock().unwrap().calls, expected);
        let probe = owner.probe.clone();
        drop(owner);
        assert_eq!(probe.lock().unwrap().live, 0);
    }
}
#[tokio::test]
async fn mock_partial_publication_failure_releases_and_preserves_both_errors() {
    for stage in ["audio", "video"] {
        let (mut owner, config) = fixture();
        owner.fail = Some(stage);
        owner.rollback_fails = true;
        let error = initialize(&mut owner, &config, &CancellationToken::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!("{stage} failure")) && error.contains("cleanup failure"));
        assert_eq!(owner.probe.lock().unwrap().live, 0);
        assert_eq!(owner.probe.lock().unwrap().calls.last(), Some(&"rollback"));
    }
}
#[tokio::test]
async fn mock_publication_timeout_and_cancel_rollback_at_each_await() {
    for stage in ["audio", "video"] {
        for cancel_instead in [false, true] {
            let (mut owner, config) = fixture();
            owner.pending = Some(stage);
            let cancel = CancellationToken::new();
            let trigger = cancel.clone();
            let probe = owner.probe.clone();
            let cancel_task = async move {
                if cancel_instead {
                    loop {
                        if probe.lock().unwrap().calls.contains(&stage) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    trigger.cancel();
                }
            };
            let (result, ()) = timeout(Duration::from_secs(2), async {
                tokio::join!(initialize(&mut owner, &config, &cancel), cancel_task)
            })
            .await
            .expect("bounded mock cancellation scenario");
            assert!(if cancel_instead {
                matches!(result, Err(Error::Closed))
            } else {
                matches!(result, Err(Error::Timeout(_)))
            });
            assert_eq!(owner.probe.lock().unwrap().live, 0);
        }
    }
}
#[tokio::test]
async fn mock_already_cancelled_setup_never_publishes() {
    let (mut owner, config) = fixture();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        initialize(&mut owner, &config, &cancel).await,
        Err(Error::Closed)
    ));
    assert_eq!(owner.probe.lock().unwrap().calls, ["rollback"]);
}

#[tokio::test]
async fn mock_failed_readiness_transfer_drops_payload_and_signals_existing_owner() {
    let (mut owner, config) = fixture();
    let cancel = CancellationToken::new();
    initialize(&mut owner, &config, &cancel).await.unwrap();
    let probe = owner.probe.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    drop(receiver);
    transfer_ready(sender, owner, &cancel);
    assert!(cancel.is_cancelled());
    assert_eq!(probe.lock().unwrap().live, 0);
}
#[tokio::test]
async fn mock_successful_readiness_transfers_ownership_without_cancellation() {
    let (mut owner, config) = fixture();
    let cancel = CancellationToken::new();
    initialize(&mut owner, &config, &cancel).await.unwrap();
    let probe = owner.probe.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    transfer_ready(sender, owner, &cancel);
    assert!(!cancel.is_cancelled());
    let owner = receiver.await.unwrap().unwrap();
    assert_eq!(probe.lock().unwrap().live, 2);
    drop(owner);
    assert_eq!(probe.lock().unwrap().live, 0);
}
