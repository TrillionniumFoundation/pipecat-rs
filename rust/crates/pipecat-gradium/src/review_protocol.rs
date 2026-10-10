//! Source-private independent tests appended only in the immutable review copy.
use super::*;
use serde_json::json;
fn step(index: u64, p: f64) -> Value {
    json!({"type":"step","step_idx":index,"vad":[{"horizon_s":3.0,"inactivity_prob":p}]})
}
#[tokio::test(start_paused = true)]
async fn review_four_phase_boundary_holds_until_exact_trailing_deadline() {
    let mut c = GradiumSttConfig::new("fixture");
    c.enable_turn_detection = true;
    c.settings.turn.post_flush_cooldown_frames = 2;
    let turn = c.settings.turn.clone();
    let mut protocol = Protocol::new();
    assert!(protocol
        .message(step(1, 0.1), &c, &turn)
        .unwrap()
        .events
        .is_empty());
    assert_eq!(protocol.phase, GradiumTurnPhase::Idle);
    assert!(protocol
        .message(step(2, 0.5), &c, &turn)
        .unwrap()
        .events
        .is_empty());
    assert_eq!(protocol.phase, GradiumTurnPhase::Armed);
    assert_eq!(
        protocol.message(step(3, 0.1), &c, &turn).unwrap().events,
        vec![GradiumSttEvent::TurnStarted]
    );
    protocol
        .message(json!({"type":"text","text":"first"}), &c, &turn)
        .unwrap();
    let stopped = protocol.message(step(4, 0.5), &c, &turn).unwrap();
    assert_eq!(stopped.events, vec![GradiumSttEvent::TurnStopped]);
    assert!(stopped.flush);
    assert_eq!(protocol.phase, GradiumTurnPhase::Ending);
    assert_eq!(protocol.start_flush(&c).unwrap(), 1);
    assert!(
        protocol
            .message(step(5, 0.1), &c, &turn)
            .unwrap()
            .events
            .is_empty(),
        "ENDING must suppress reopening before acknowledgment"
    );
    protocol
        .message(json!({"type":"flushed","flush_id":"1"}), &c, &turn)
        .unwrap();
    let deadline = protocol.pending.as_ref().unwrap().final_at.unwrap();
    assert_eq!(deadline, Instant::now() + Duration::from_millis(100));
    protocol
        .message(json!({"type":"text","text":"tail"}), &c, &turn)
        .unwrap();
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(Instant::now() < deadline);
    for index in 6..=8 {
        assert!(
            protocol
                .message(step(index, 0.1), &c, &turn)
                .unwrap()
                .events
                .is_empty(),
            "ENDING must suppress reopening after cooldown but before final"
        );
    }
    assert_eq!(protocol.phase, GradiumTurnPhase::Ending);
    tokio::time::advance(Duration::from_millis(1)).await;
    let Some(GradiumSttEvent::Transcript(t)) = protocol.finalize_flush(&c) else {
        panic!("final transcript")
    };
    assert_eq!(t.text, "first tail");
    assert!(t.is_final);
    assert_eq!(protocol.phase, GradiumTurnPhase::Armed);
    assert_eq!(
        protocol.message(step(9, 0.1), &c, &turn).unwrap().events,
        vec![GradiumSttEvent::TurnStarted]
    );
}

#[tokio::test(start_paused = true)]
async fn review_nearest_horizon_tie_and_invalid_step_are_not_boolean_vad() {
    let mut c = GradiumSttConfig::new("fixture");
    c.enable_turn_detection = true;
    c.settings.turn.eot_horizon_s = 2.0;
    let turn = c.settings.turn.clone();
    let mut protocol = Protocol::new();
    let observations = json!({"type":"step","step_idx":1,"vad":[
        {"horizon_s":1.0,"inactivity_prob":0.9},
        {"horizon_s":3.0,"inactivity_prob":0.1}
    ]});
    protocol.message(observations, &c, &turn).unwrap();
    assert_eq!(
        protocol.phase,
        GradiumTurnPhase::Armed,
        "source uses first closest tie"
    );
    for bad in [json!(-0.1), json!(1.1), json!("0.1"), Value::Null] {
        assert!(protocol
            .message(
                json!({"type":"step","step_idx":2,"vad":[{"horizon_s":2.0,"inactivity_prob":bad}]}),
                &c,
                &turn
            )
            .is_err());
        assert_eq!(protocol.phase, GradiumTurnPhase::Armed);
    }
    assert_eq!(
        protocol.message(step(2, 0.1), &c, &turn).unwrap().events,
        vec![GradiumSttEvent::TurnStarted]
    );
    assert!(
        protocol.message(step(2, 0.9), &c, &turn).is_err(),
        "duplicate index cannot close turn"
    );
    assert!(
        protocol.message(step(1, 0.9), &c, &turn).is_err(),
        "older index cannot close turn"
    );
    assert_eq!(protocol.phase, GradiumTurnPhase::Open);
}

#[tokio::test(start_paused = true)]
async fn review_settings_update_is_atomic_and_connection_change_requires_restart() {
    let initial = crate::GradiumSttSettings::default();
    assert!(initial
        .merge(
            json!({"eot_threshold":0.8,"unsupported":true})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(initial.turn.eot_threshold, 0.5);
    let changed = initial
        .merge(json!({"eot_threshold":0.8}).as_object().unwrap())
        .unwrap();
    assert!(initial.same_connection(&changed));
    assert!(!initial.same_connection(
        &initial
            .merge(json!({"language":"fr"}).as_object().unwrap())
            .unwrap()
    ));
}
