//! Independently authored controlled-clock actor timer boundaries.
use super::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn review_due_tick_finalizes_before_admitting_ready_late_text() {
    let config = GradiumSttConfig::new("fixture");
    let mut protocol = Protocol::new();
    protocol.phase = crate::GradiumTurnPhase::Ending;
    protocol
        .message(
            json!({"type":"text","text":"old turn"}),
            &config,
            &config.settings.turn,
        )
        .unwrap();
    protocol.start_flush(&config).unwrap();
    protocol
        .message(
            json!({"type":"flushed","flush_id":"1"}),
            &config,
            &config.settings.turn,
        )
        .unwrap();
    let (events, mut receive) = mpsc::channel(2);
    let admission = Mutex::new(Admission::default());
    let last_read = Instant::now();
    tokio::time::advance(Duration::from_millis(99)).await;
    tick(&mut protocol, &config, &events, &admission, None, last_read).unwrap();
    assert!(receive.try_recv().is_err());
    tokio::time::advance(Duration::from_millis(1)).await;
    tick(&mut protocol, &config, &events, &admission, None, last_read).unwrap();
    let final_event = receive.try_recv().unwrap();
    assert!(matches!(final_event,GradiumSttEvent::Transcript(t) if t.is_final&&t.text=="old turn"));
    let outcome = protocol
        .message(
            json!({"type":"text","text":"late next turn"}),
            &config,
            &config.settings.turn,
        )
        .unwrap();
    assert!(
        matches!(&outcome.events[0],GradiumSttEvent::Transcript(t) if !t.is_final&&t.text=="late next turn")
    );
    assert_eq!(protocol.phase, crate::GradiumTurnPhase::Armed);
}

#[tokio::test(start_paused = true)]
async fn review_terminal_deadline_wins_over_simultaneously_due_trailing_final() {
    let config = GradiumSttConfig::new("fixture");
    let mut protocol = Protocol::new();
    protocol
        .message(
            json!({"type":"text","text":"expired output"}),
            &config,
            &config.settings.turn,
        )
        .unwrap();
    protocol.start_flush(&config).unwrap();
    protocol
        .message(
            json!({"type":"flushed","flush_id":"1"}),
            &config,
            &config.settings.turn,
        )
        .unwrap();
    let (events, mut receive) = mpsc::channel(2);
    let admission = Mutex::new(Admission::default());
    let last_read = Instant::now();
    let finish = last_read + Duration::from_millis(100);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(
        tick(
            &mut protocol,
            &config,
            &events,
            &admission,
            Some(finish),
            last_read
        ),
        Err(GradiumError::Timeout("finish"))
    );
    assert!(
        receive.try_recv().is_err(),
        "an expired session emitted a delayed final"
    );
}
