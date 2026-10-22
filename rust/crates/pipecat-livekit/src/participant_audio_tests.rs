//! No joined room: shared selection, owner command and setter-boundary contracts.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::poll;
use livekit::track::TrackKind;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::participant_audio::{
    apply_current, sdk_ready, AudioControl, AudioPublication, AudioRequest,
};
use crate::{ConnectionState, Error, LiveKitConfig, Result};

type Calls = Arc<Mutex<Vec<(&'static str, bool)>>>;
#[derive(Clone)]
struct Publication {
    sid: &'static str,
    kind: TrackKind,
    calls: Calls,
    cancel_after_set: Option<CancellationToken>,
}
impl AudioPublication for Publication {
    fn kind(&self) -> TrackKind {
        self.kind
    }
    fn set_subscribed(&self, subscribed: bool) {
        self.calls.lock().unwrap().push((self.sid, subscribed));
        if let Some(cancel) = &self.cancel_after_set {
            cancel.cancel();
        }
    }
}
fn publication(sid: &'static str, kind: TrackKind, calls: &Calls) -> Publication {
    Publication {
        sid,
        kind,
        calls: calls.clone(),
        cancel_after_set: None,
    }
}
fn control(
    capacity: usize,
    timeout: Duration,
) -> (
    AudioControl,
    mpsc::Receiver<AudioRequest>,
    watch::Sender<ConnectionState>,
    CancellationToken,
) {
    let cancel = CancellationToken::new();
    let (state_tx, state) = watch::channel(ConnectionState::Connected);
    let (control, requests) = AudioControl::channel(capacity, state, cancel.clone(), timeout);
    (control, requests, state_tx, cancel)
}
fn apply(
    identity: &str,
    subscribed: bool,
    remotes: Vec<(String, Vec<Publication>)>,
    config: &LiveKitConfig,
) -> Result<()> {
    apply_current(config, identity, subscribed, remotes, |p| p, || Ok(()))
}

#[test]
fn exact_identity_and_audio_only_multiple_publications_use_shared_setter_boundary() {
    let calls = Calls::default();
    let remotes = vec![
        (
            "Alice".into(),
            vec![publication("wrong-case", TrackKind::Audio, &calls)],
        ),
        (
            "alice".into(),
            vec![
                publication("audio-1", TrackKind::Audio, &calls),
                publication("camera", TrackKind::Video, &calls),
                publication("audio-2", TrackKind::Audio, &calls),
            ],
        ),
        (
            "bob".into(),
            vec![publication("other", TrackKind::Audio, &calls)],
        ),
    ];
    for subscribed in [false, false, true, true] {
        apply(
            "alice",
            subscribed,
            remotes.clone(),
            &LiveKitConfig::default(),
        )
        .unwrap();
    }
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            ("audio-1", false),
            ("audio-2", false),
            ("audio-1", false),
            ("audio-2", false),
            ("audio-1", true),
            ("audio-2", true),
            ("audio-1", true),
            ("audio-2", true),
        ]
    );
}

#[test]
fn unknown_local_sid_and_no_audio_targets_do_not_select_remote_audio() {
    let calls = Calls::default();
    // A local publication is deliberately outside the current-remotes snapshot.
    let _local = publication("local-audio", TrackKind::Audio, &calls);
    let remotes = vec![
        (
            "alice".into(),
            (
                "participant-sid",
                vec![publication("audio", TrackKind::Audio, &calls)],
            ),
        ),
        (
            "camera-only".into(),
            (
                "camera-participant-sid",
                vec![publication("video", TrackKind::Video, &calls)],
            ),
        ),
        ("empty".into(), ("empty-participant-sid", vec![])),
    ];
    for target in [
        "missing",
        "local",
        "participant-sid",
        "camera-only",
        "empty",
    ] {
        for subscribed in [false, true] {
            apply_current(
                &LiveKitConfig::default(),
                target,
                subscribed,
                remotes.clone(),
                |(_, publications)| publications,
                || Ok(()),
            )
            .unwrap();
        }
    }
    assert!(calls.lock().unwrap().is_empty());
}

#[test]
fn configured_filter_blocks_both_requests_before_publication_lookup() {
    let config = LiveKitConfig {
        participant_filter: vec!["allowed".into()],
        ..Default::default()
    };
    for subscribed in [false, true] {
        apply_current::<_, Publication>(
            &config,
            "excluded",
            subscribed,
            [("excluded".into(), ())],
            |_| panic!("filtered identity must not obtain publications"),
            || Ok(()),
        )
        .unwrap();
    }
    let calls = Calls::default();
    apply(
        "allowed",
        true,
        vec![(
            "allowed".into(),
            vec![
                publication("audio", TrackKind::Audio, &calls),
                publication("video", TrackKind::Video, &calls),
            ],
        )],
        &config,
    )
    .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![("audio", true)]);
}

#[test]
fn current_snapshot_only_missing_participant_and_new_publications_install_no_policy() {
    let calls = Calls::default();
    apply("alice", false, vec![], &LiveKitConfig::default()).unwrap();
    let first = publication("first", TrackKind::Audio, &calls);
    apply(
        "alice",
        false,
        vec![("alice".into(), vec![first.clone()])],
        &LiveKitConfig::default(),
    )
    .unwrap();
    let later = publication("later", TrackKind::Audio, &calls);
    assert_eq!(*calls.lock().unwrap(), vec![("first", false)]);
    apply(
        "alice",
        true,
        vec![("alice".into(), vec![first, later])],
        &LiveKitConfig::default(),
    )
    .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        vec![("first", false), ("first", true), ("later", true)]
    );
}

#[tokio::test]
async fn invalid_identity_is_rejected_before_admission_and_utf8_limit_is_bytes() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    for identity in [String::new(), "x".repeat(257), "é".repeat(129)] {
        assert!(matches!(
            control.set_subscribed(&identity, true).await,
            Err(Error::InvalidConfig(_))
        ));
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
    for identity in ["é".repeat(128), " ".into()] {
        let mut request = Box::pin(control.set_subscribed(&identity, true));
        assert!(poll!(&mut request).is_pending());
        requests.try_recv().unwrap().execute(
            &ConnectionState::Connected,
            &cancel,
            |actual, subscribed, check| {
                assert_eq!(actual, identity);
                assert!(subscribed);
                check()
            },
        );
        request.await.unwrap();
    }
    let (overflow_control, mut requests, _state, _cancel) = self::control(1, Duration::MAX);
    assert!(matches!(
        overflow_control.set_subscribed("alice", true).await,
        Err(Error::InvalidConfig(
            "participant audio timeout exceeds clock range"
        ))
    ));
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn admission_readiness_matches_existing_connection_conventions() {
    let (control, mut requests, state, cancel) = control(1, Duration::from_secs(1));
    for current in [
        ConnectionState::Connecting,
        ConnectionState::Reconnecting,
        ConnectionState::Closed,
        ConnectionState::Failed("failed".into()),
    ] {
        state.send_replace(current.clone());
        let result = control.set_subscribed("alice", false).await;
        assert!(match current {
            ConnectionState::Connecting | ConnectionState::Reconnecting => {
                matches!(result, Err(Error::NotReady))
            }
            _ => matches!(result, Err(Error::Closed)),
        });
        assert!(requests.try_recv().is_err());
    }
    state.send_replace(ConnectionState::Connected);
    cancel.cancel();
    assert!(matches!(
        control.set_subscribed("alice", true).await,
        Err(Error::Closed)
    ));
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn owner_rechecks_readiness_and_sdk_state_before_request_boundary() {
    for current in [ConnectionState::Reconnecting, ConnectionState::Closed] {
        let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
        let mut waiting = Box::pin(control.set_subscribed("alice", false));
        assert!(poll!(&mut waiting).is_pending());
        requests
            .try_recv()
            .unwrap()
            .execute(&current, &cancel, |_, _, _| panic!("not ready"));
        let result = waiting.await;
        assert!(if current == ConnectionState::Reconnecting {
            matches!(result, Err(Error::NotReady))
        } else {
            matches!(result, Err(Error::Closed))
        });
    }
    for sdk in [
        livekit::ConnectionState::Reconnecting,
        livekit::ConnectionState::Disconnected,
    ] {
        let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
        let mut waiting = Box::pin(control.set_subscribed("alice", true));
        assert!(poll!(&mut waiting).is_pending());
        requests
            .try_recv()
            .unwrap()
            .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
                sdk_ready(sdk)
            });
        let result = waiting.await;
        assert!(if sdk == livekit::ConnectionState::Reconnecting {
            matches!(result, Err(Error::NotReady))
        } else {
            matches!(result, Err(Error::Closed))
        });
    }
    sdk_ready(livekit::ConnectionState::Connected).unwrap();
}

#[tokio::test]
async fn owner_acknowledges_false_true_requests_without_remote_confirmation() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    let calls = Calls::default();
    for subscribed in [false, true] {
        let mut waiting = Box::pin(control.set_subscribed("alice", subscribed));
        assert!(poll!(&mut waiting).is_pending());
        requests.try_recv().unwrap().execute(
            &ConnectionState::Connected,
            &cancel,
            |identity, requested, check| {
                sdk_ready(livekit::ConnectionState::Connected)?;
                apply_current(
                    &LiveKitConfig::default(),
                    identity,
                    requested,
                    [(
                        "alice".into(),
                        vec![publication("audio", TrackKind::Audio, &calls)],
                    )],
                    |p| p,
                    check,
                )
            },
        );
        waiting.await.unwrap();
    }
    assert_eq!(
        *calls.lock().unwrap(),
        vec![("audio", false), ("audio", true)]
    );
}

#[tokio::test]
async fn dropped_queued_caller_cannot_apply_later_or_cancel_the_session() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", false));
    assert!(poll!(&mut waiting).is_pending());
    drop(waiting);
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
            panic!("dropped command applied")
        });
    assert!(!cancel.is_cancelled());
}

#[tokio::test]
async fn timed_out_queued_caller_cannot_apply_later() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_millis(1));
    assert!(matches!(
        control.set_subscribed("alice", true).await,
        Err(Error::Timeout("participant audio subscription"))
    ));
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
            panic!("timed out command applied")
        });
    assert!(!cancel.is_cancelled());
}

#[tokio::test]
async fn owner_rejects_expired_queue_even_before_caller_repolls_timeout() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_millis(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", true));
    assert!(poll!(&mut waiting).is_pending());
    tokio::time::sleep(Duration::from_millis(5)).await;
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
            panic!("expired command applied")
        });
    assert!(matches!(
        waiting.await,
        Err(Error::Timeout("participant audio subscription"))
    ));
}

#[tokio::test]
async fn queue_full_and_closed_owner_are_explicit() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", false));
    assert!(poll!(&mut waiting).is_pending());
    assert!(matches!(
        control.set_subscribed("bob", true).await,
        Err(Error::QueueFull)
    ));
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, check| check());
    waiting.await.unwrap();
    let mut waiting = Box::pin(control.set_subscribed("alice", true));
    assert!(poll!(&mut waiting).is_pending());
    drop(requests);
    assert!(matches!(waiting.await, Err(Error::Closed)));
    assert!(matches!(
        control.set_subscribed("alice", true).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn caller_observed_reconnect_invalidates_queued_request_even_if_owner_later_reconnects() {
    let (control, mut requests, state, cancel) = control(1, Duration::from_secs(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", true));
    assert!(poll!(&mut waiting).is_pending());
    state.send_replace(ConnectionState::Reconnecting);
    assert!(matches!(
        poll!(&mut waiting),
        std::task::Poll::Ready(Err(Error::NotReady))
    ));
    state.send_replace(ConnectionState::Connected);
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
            panic!("invalidated command applied")
        });

    // No reconnect epoch: a complete cycle missed by this caller may survive.
    let (control, mut requests, state, cancel) = self::control(1, Duration::from_secs(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", true));
    assert!(poll!(&mut waiting).is_pending());
    state.send_replace(ConnectionState::Reconnecting);
    state.send_replace(ConnectionState::Connected);
    let applied = std::cell::Cell::new(false);
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, check| {
            sdk_ready(livekit::ConnectionState::Connected)?;
            check()?;
            applied.set(true);
            Ok(())
        });
    waiting.await.unwrap();
    assert!(applied.get());
}

#[tokio::test]
async fn session_cancel_rejects_queued_requests() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    let mut waiting = Box::pin(control.set_subscribed("alice", false));
    assert!(poll!(&mut waiting).is_pending());
    cancel.cancel();
    requests
        .try_recv()
        .unwrap()
        .execute(&ConnectionState::Connected, &cancel, |_, _, _| {
            panic!("closed session applied command")
        });
    assert!(matches!(waiting.await, Err(Error::Closed)));
}

#[tokio::test]
async fn cancellation_between_setters_stops_remaining_requests_without_rollback() {
    let (control, mut requests, _state, cancel) = control(1, Duration::from_secs(1));
    let calls = Calls::default();
    let mut first = publication("first", TrackKind::Audio, &calls);
    first.cancel_after_set = Some(cancel.clone());
    let second = publication("second", TrackKind::Audio, &calls);
    let mut waiting = Box::pin(control.set_subscribed("alice", false));
    assert!(poll!(&mut waiting).is_pending());
    requests.try_recv().unwrap().execute(
        &ConnectionState::Connected,
        &cancel,
        |identity, subscribed, check| {
            apply_current(
                &LiveKitConfig::default(),
                identity,
                subscribed,
                [("alice".into(), vec![first, second])],
                |p| p,
                check,
            )
        },
    );
    assert!(matches!(waiting.await, Err(Error::Closed)));
    assert_eq!(*calls.lock().unwrap(), vec![("first", false)]);
}
