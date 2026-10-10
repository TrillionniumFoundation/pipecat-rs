use pipecat_workers::*;
use serde_json::Value;
use std::{future::Future, time::Duration};

const LIMIT: Duration = Duration::from_secs(3);
async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("operation stalled")
}
fn fixture(capacity: usize) -> (LocalBus, JobClient, Subscription, Subscription) {
    let bus = LocalBus::new(capacity).unwrap();
    let registry = WorkerRegistry::new("review");
    assert!(registry.register(WorkerReady {
        worker_name: "speaker".into(),
        runner: "review".into()
    }));
    let peer = bus.subscribe_target("speaker").unwrap();
    let voice = bus.subscribe_target("voice").unwrap();
    let client = JobClient::new("voice", bus.clone(), registry);
    (bus, client, peer, voice)
}
async fn request_address(peer: &Subscription) -> (String, String) {
    bounded(async {
        loop {
            let message = peer.recv().await.unwrap().unwrap();
            if let MessageData::JobRequest { job_id, .. } = message.data {
                return (message.source, job_id);
            }
        }
    })
    .await
}
fn speech(source: &str, target: Option<&str>, text: &str) -> BusMessage {
    BusMessage {
        source: source.into(),
        target: target.map(str::to_owned),
        data: MessageData::Speak {
            text: text.into(),
            append_to_context: true,
        },
    }
}
async fn complete(bus: &LocalBus, address: &str, id: &str) {
    bus.publish_wait(BusMessage {
        source: "speaker".into(),
        target: Some(address.into()),
        data: MessageData::JobResponse {
            job_id: id.into(),
            status: JobStatus::Completed,
            response: None,
            urgent: false,
        },
    })
    .await
    .unwrap();
}
fn delivered(voice: &Subscription) -> Vec<BusMessage> {
    let mut messages = vec![];
    while let Some(message) = voice.try_recv().unwrap() {
        if message.target.as_deref() == Some("voice")
            && matches!(message.data, MessageData::Speak { .. })
        {
            messages.push(message);
        }
    }
    messages
}

#[tokio::test]
async fn speech_relay_preserves_order_and_fields_for_single_and_group_requests() {
    for group in [false, true] {
        let (bus, client, peer, voice) = fixture(8);
        let result = if group {
            let mut job = client
                .request_group(["speaker"], JobGroupParams::default())
                .unwrap();
            tokio::spawn(async move {
                job.result()
                    .await
                    .unwrap()
                    .responses
                    .remove("speaker")
                    .unwrap()
            })
        } else {
            let mut job = client.request("speaker", JobParams::default()).unwrap();
            tokio::spawn(async move { job.result().await.unwrap() })
        };
        let (address, id) = request_address(&peer).await;
        bus.publish_wait(speech("speaker", Some(&address), "one"))
            .await
            .unwrap();
        bus.publish_wait(speech("speaker", Some(&address), "two"))
            .await
            .unwrap();
        complete(&bus, &address, &id).await;
        assert_eq!(bounded(result).await.unwrap().status, JobStatus::Completed);
        // No additional wait: both speech messages must be admitted before the result.
        assert_eq!(
            delivered(&voice),
            vec![
                speech("speaker", Some("voice"), "one"),
                speech("speaker", Some("voice"), "two")
            ]
        );
    }
}

#[tokio::test]
async fn wrong_peer_broadcast_and_unrelated_inbox_messages_are_never_relayed() {
    let (bus, client, peer, voice) = fixture(8);
    let mut job = client.request("speaker", JobParams::default()).unwrap();
    let (address, id) = request_address(&peer).await;
    bus.publish_wait(speech("impostor", Some(&address), "wrong peer"))
        .await
        .unwrap();
    bus.publish_wait(speech("speaker", None, "broadcast"))
        .await
        .unwrap();
    bus.publish_wait(speech(
        "speaker",
        Some("another-private-inbox"),
        "unrelated",
    ))
    .await
    .unwrap();
    bus.publish_wait(BusMessage {
        source: "speaker".into(),
        target: Some(address.clone()),
        data: MessageData::JobResponse {
            job_id: "wrong-job".into(),
            status: JobStatus::Completed,
            response: Some(Value::String("wrong".into())),
            urgent: false,
        },
    })
    .await
    .unwrap();
    complete(&bus, &address, &id).await;
    assert_eq!(bounded(job.result()).await.unwrap().response, None);
    assert!(delivered(&voice).is_empty());
}

#[tokio::test]
async fn blocked_speech_is_cancelled_on_cancel_timeout_or_handle_drop() {
    for mode in ["cancel", "timeout", "drop"] {
        let (bus, client, peer, voice) = fixture(1);
        bus.publish(speech("fixture", Some("voice"), "occupied"))
            .unwrap();
        let mut job = Some(
            client
                .request(
                    "speaker",
                    JobParams {
                        timeout: Some(if mode == "timeout" {
                            Duration::from_millis(40)
                        } else {
                            LIMIT
                        }),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
        let (address, _) = request_address(&peer).await;
        bus.publish_wait(speech("speaker", Some(&address), "cancelled speech"))
            .await
            .unwrap();
        match mode {
            "cancel" => {
                job.as_ref().unwrap().cancel(None).await.unwrap();
                assert_eq!(
                    bounded(job.as_mut().unwrap().result())
                        .await
                        .unwrap()
                        .status,
                    JobStatus::Cancelled
                );
            }
            "timeout" => assert!(matches!(
                bounded(job.as_mut().unwrap().result()).await,
                Err(JobError::Timeout { .. })
            )),
            _ => drop(job.take()),
        }
        bounded(async {
            while bus.has_target_subscriber(&address).unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            delivered(&voice),
            vec![speech("fixture", Some("voice"), "occupied")]
        );
        // A late message for the dead request cannot reach a replacement request.
        bus.publish_wait(speech("speaker", Some(&address), "late old speech"))
            .await
            .unwrap();
        let mut next = client
            .request(
                "speaker",
                JobParams {
                    timeout: Some(LIMIT),
                    ..Default::default()
                },
            )
            .unwrap();
        let (next_address, next_id) = request_address(&peer).await;
        assert_ne!(next_address, address);
        bus.publish_wait(speech("speaker", Some(&next_address), "new speech"))
            .await
            .unwrap();
        complete(&bus, &next_address, &next_id).await;
        assert_eq!(
            bounded(next.result()).await.unwrap().status,
            JobStatus::Completed
        );
        assert_eq!(
            delivered(&voice),
            vec![speech("speaker", Some("voice"), "new speech")],
            "mode={mode}"
        );
    }
}

#[tokio::test]
async fn completed_private_inbox_cannot_relay_speech_into_a_later_request() {
    let (bus, client, peer, voice) = fixture(8);
    let mut first = client.request("speaker", JobParams::default()).unwrap();
    let (old_address, old_id) = request_address(&peer).await;
    complete(&bus, &old_address, &old_id).await;
    bounded(first.result()).await.unwrap();
    let mut next = client.request("speaker", JobParams::default()).unwrap();
    let (address, id) = request_address(&peer).await;
    bus.publish_wait(speech("speaker", Some(&old_address), "stale"))
        .await
        .unwrap();
    bus.publish_wait(speech("speaker", Some(&address), "current"))
        .await
        .unwrap();
    complete(&bus, &address, &id).await;
    bounded(next.result()).await.unwrap();
    assert_eq!(
        delivered(&voice),
        vec![speech("speaker", Some("voice"), "current")]
    );
}
