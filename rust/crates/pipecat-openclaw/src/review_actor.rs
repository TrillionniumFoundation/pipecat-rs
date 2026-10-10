//! Exact client actor under deterministic writer-lane pressure.
//! The normal writer is intentionally not scheduled, removing socket-buffer races.
use super::*;

async fn accepted_actor() -> (
    Actor,
    mpsc::Receiver<Write>,
    mpsc::Receiver<Write>,
    GatewayRun,
) {
    let config = GatewayConfig {
        allow_loopback_cleartext: true,
        ..Default::default()
    };
    let (writes, mut normal) = mpsc::channel(4);
    let (controls, urgent) = mpsc::channel(4);
    let mut actor = Actor {
        config,
        connection: "review-connection".into(),
        runs: HashMap::new(),
        aliases: HashMap::new(),
        pending: HashMap::new(),
        unrouted: VecDeque::new(),
        unrouted_bytes: 0,
        writes,
        controls,
    };
    let (reply, response) = oneshot::channel();
    actor
        .command(Command::Start {
            message: "first".into(),
            session: "review-session".into(),
            reply,
        })
        .unwrap();
    let submitted = normal.recv().await.unwrap();
    let request: Value = serde_json::from_str(&submitted.wire).unwrap();
    submitted.stage.store(2, Ordering::Release);
    actor
        .message(
            &json!({"type":"res","id":request["id"],"ok":true,"payload":{"runId":"old-remote"}})
                .to_string(),
        )
        .unwrap();
    let run = response.await.unwrap().unwrap();
    (actor, normal, urgent, run)
}

#[tokio::test]
async fn review_abort_cancels_not_yet_written_steer_and_restores_old_run() {
    let (mut actor, mut normal, mut urgent, mut run) = accepted_actor().await;
    let (reply, response) = oneshot::channel();
    actor
        .command(Command::Steer {
            handle: run.handle(),
            message: "must not reach remote".into(),
            reply,
        })
        .unwrap();
    let queued = normal.recv().await.unwrap();
    assert_eq!(queued.stage.load(Ordering::Acquire), 0);
    assert_ne!(run.identity().run_id, "old-remote");
    actor.message(&json!({"type":"event","event":"chat","payload":{"runId":"old-remote","state":"delta","deltaText":"old pending delta"}}).to_string()).unwrap();
    let (reply, abort_response) = oneshot::channel();
    actor
        .command(Command::Abort {
            handle: run.handle(),
            reply,
        })
        .unwrap();
    let abort = urgent.recv().await.unwrap();
    let request: Value = serde_json::from_str(&abort.wire).unwrap();
    assert_eq!(request["method"], "chat.abort");
    assert_eq!(
        request["params"]["runId"], "old-remote",
        "abort must not overtake queued steer using nonexistent replacement id"
    );
    assert_eq!(
        queued.stage.load(Ordering::Acquire),
        3,
        "cancelled steer remains writable"
    );
    assert!(queued
        .stage
        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        .is_err());
    assert!(matches!(
        response.await.unwrap(),
        Err(GatewayError::BeforeWrite(_))
    ));
    assert_eq!(run.identity().run_id, "old-remote");
    assert!(
        matches!(run.next().await,Some(RunEvent::Text{run_id,text}) if run_id=="old-remote" && text=="old pending delta")
    );
    abort.stage.store(2, Ordering::Release);
    actor
        .message(
            &json!({"type":"res","id":request["id"],"ok":true,"payload":{"aborted":true}})
                .to_string(),
        )
        .unwrap();
    assert!(abort_response.await.unwrap().unwrap());
    actor.message(&json!({"type":"event","event":"chat","payload":{"runId":"old-remote","state":"aborted"}}).to_string()).unwrap();
    assert!(
        matches!(run.next().await,Some(RunEvent::End(end)) if end.status==RunStatus::Cancelled)
    );
    assert!(run.next().await.is_none());
}

#[tokio::test]
async fn review_abort_preserves_already_submitted_steer_identity() {
    let (mut actor, mut normal, mut urgent, run) = accepted_actor().await;
    let (reply, response) = oneshot::channel();
    actor
        .command(Command::Steer {
            handle: run.handle(),
            message: "submitted replacement".into(),
            reply,
        })
        .unwrap();
    let submitted = normal.recv().await.unwrap();
    submitted.stage.store(1, Ordering::Release);
    let replacement = run.identity().run_id;
    let steer: Value = serde_json::from_str(&submitted.wire).unwrap();
    let (reply, abort_response) = oneshot::channel();
    actor
        .command(Command::Abort {
            handle: run.handle(),
            reply,
        })
        .unwrap();
    let abort = urgent.recv().await.unwrap();
    let request: Value = serde_json::from_str(&abort.wire).unwrap();
    assert_eq!(request["params"]["runId"], replacement);
    assert_eq!(submitted.stage.load(Ordering::Acquire), 1);
    assert!(actor.pending.contains_key(steer["id"].as_str().unwrap()));
    actor.message(&json!({"type":"res","id":steer["id"],"ok":true,"payload":{"runId":replacement,"status":"started"}}).to_string()).unwrap();
    response.await.unwrap().unwrap();
    actor
        .message(
            &json!({"type":"res","id":request["id"],"ok":true,"payload":{"aborted":false}})
                .to_string(),
        )
        .unwrap();
    assert!(!abort_response.await.unwrap().unwrap());
}
