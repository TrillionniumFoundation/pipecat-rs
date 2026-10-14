use pipecat_browser_client::toasts::*;
use serde_json::{json, Value};
fn event(payload: Value) -> Value {
    json!({"label":"rtvi-ai","type":"ui-command","data":{"command":"toast","payload":payload}})
}
fn state() -> Toasts {
    let mut s = Toasts::default();
    s.begin(7);
    s
}
#[test]
fn pinned_fields_defaults_nulls_empty_strings_and_literal_text() {
    let mut s = state();
    s.apply(7, 10, &event(json!({"title":"hello"}))).unwrap();
    let t = &s.entries()[0];
    assert_eq!(t.title, "hello");
    assert_eq!(t.subtitle, None);
    assert_eq!(t.description, None);
    assert!(!t.image_omitted);
    assert_eq!(t.expires_at_ms, 5010);
    s.apply(7, 11, &event(json!({"title":"", "subtitle":null, "description":null, "image_url":null, "duration_ms":null}))).unwrap();
    assert_eq!(s.entries()[1].expires_at_ms, 5011);
    s.apply(7, 12, &event(json!({"title":"<script>alert(1)</script>","subtitle":"", "description":"[click](javascript:alert(1))", "image_url":"javascript:alert(1)", "duration_ms":1, "actions":[{"url":"evil"}], "unknown":42}))).unwrap();
    let t = &s.entries()[2];
    assert_eq!(t.title, "<script>alert(1)</script>");
    assert_eq!(t.subtitle.as_deref(), Some(""));
    assert_eq!(
        t.description.as_deref(),
        Some("[click](javascript:alert(1))")
    );
    assert!(t.image_omitted);
    assert_eq!(t.expires_at_ms, 13);
    s.apply(
        7,
        12,
        &event(json!({"title":"", "description":"", "image_url":""})),
    )
    .unwrap();
    assert!(s.entries()[3].image_omitted);
}
#[test]
fn strict_types_and_durations_reject_without_expiring_or_allocating() {
    let mut s = state();
    s.apply(7, 0, &event(json!({"title":"keep"}))).unwrap();
    let before = s.entries().to_vec();
    for payload in [
        json!(null),
        json!([]),
        json!({}),
        json!({"title":null}),
        json!({"title":1}),
        json!({"title":true}),
    ] {
        assert!(s.apply(7, 10000, &event(payload)).is_err());
        assert_eq!(s.entries(), before);
    }
    for field in ["subtitle", "description", "image_url"] {
        for bad in [json!(true), json!(1), json!([]), json!({})] {
            let mut p = json!({"title":"bad"});
            p[field] = bad;
            assert!(s.apply(7, 10000, &event(p)).is_err());
            assert_eq!(s.entries(), before);
        }
    }
    for bad in [
        json!(0),
        json!(-1),
        json!(1.0),
        json!(1.5),
        json!(30001),
        json!(u64::MAX),
        json!(true),
        json!("5000"),
        json!([]),
    ] {
        assert!(s
            .apply(7, 10000, &event(json!({"title":"bad","duration_ms":bad})))
            .is_err());
        assert_eq!(s.entries(), before);
    }
    s.apply(7, 1, &event(json!({"title":"next","duration_ms":30000})))
        .unwrap();
    assert_eq!(s.entries()[1].id, 2);
    assert_eq!(s.entries()[1].expires_at_ms, 30001);
}
#[test]
fn unrelated_envelopes_do_not_advance_clock_or_expire() {
    let mut s = state();
    s.apply(7, 0, &event(json!({"title":"keep"}))).unwrap();
    let before = s.entries().to_vec();
    for e in [
        json!(null),
        json!({}),
        json!({"label":"other","type":"ui-command","data":{"command":"toast"}}),
        json!({"label":"rtvi-ai","type":"other","data":{"command":"toast"}}),
        json!({"label":"rtvi-ai","type":"ui-command","data":{"command":"navigate","payload":"x".repeat(9000)}}),
        json!({"label":"rtvi-ai","type":"ui-command","data":null}),
    ] {
        assert!(!s.apply(7, u64::MAX, &e).unwrap());
        assert_eq!(s.entries(), before);
    }
    s.apply(7, 1, &event(json!({"title":"next"}))).unwrap();
    assert_eq!(s.entries()[1].expires_at_ms, 5001);
}
#[test]
fn lifecycle_stale_epochs_closed_state_and_old_timer_identity() {
    let mut s = Toasts::default();
    let e = event(json!({"title":"toast"}));
    assert!(!s.apply(7, u64::MAX, &e).unwrap());
    s.begin(7);
    s.apply(7, 0, &e).unwrap();
    assert!(!s.apply(6, u64::MAX, &e).unwrap());
    assert!(!s.expire(6, u64::MAX));
    assert!(!s.dismiss(6, 1));
    assert!(s.dismiss(7, 1));
    s.apply(7, 1, &e).unwrap();
    assert_eq!(s.entries()[0].id, 2);
    assert!(!s.dismiss(7, 1));
    assert!(s.expire(7, 5001));
    s.apply(7, 5001, &e).unwrap();
    assert_eq!(s.entries()[0].id, 3);
    assert!(!s.dismiss(7, 2));
    s.begin(8);
    assert!(s.entries().is_empty());
    s.apply(8, 0, &e).unwrap();
    assert_eq!(s.entries()[0].id, 1);
    assert!(!s.expire(7, u64::MAX));
    assert!(!s.dismiss(7, 1));
    assert_eq!(s.entries().len(), 1);
    s.close();
    assert!(s.entries().is_empty());
    assert!(!s.apply(8, 1, &e).unwrap());
    assert!(!s.expire(8, 99999));
    assert!(!s.dismiss(8, 1));
}
#[test]
fn expiry_boundary_backward_clock_and_checked_deadline() {
    let mut s = state();
    s.apply(7, 100, &event(json!({"title":"a","duration_ms":10})))
        .unwrap();
    assert!(!s.expire(7, 109));
    assert!(!s.expire(7, 0));
    s.apply(7, 0, &event(json!({"title":"b","duration_ms":1})))
        .unwrap();
    assert_eq!(s.entries()[1].expires_at_ms, 110);
    let before = s.entries().to_vec();
    assert!(s
        .apply(7, u64::MAX, &event(json!({"title":"overflow"})))
        .is_err());
    assert_eq!(s.entries(), before);
    assert!(s.expire(7, 110));
    assert!(!s.expire(7, 110));
    s.begin(8);
    s.apply(
        8,
        u64::MAX - 1,
        &event(json!({"title":"last","duration_ms":1})),
    )
    .unwrap();
    assert_eq!(s.entries()[0].expires_at_ms, u64::MAX);
    assert!(s.expire(8, u64::MAX));
}
#[test]
fn capacity_evicts_oldest_live_only_after_validation() {
    let mut s = state();
    for i in 0..4 {
        s.apply(
            7,
            0,
            &event(json!({"title":i.to_string(),"duration_ms":if i == 1 {1} else {100}})),
        )
        .unwrap();
    }
    let before = s.entries().to_vec();
    assert!(s
        .apply(7, 1000, &event(json!({"title":"bad","duration_ms":0})))
        .is_err());
    assert_eq!(s.entries(), before);
    s.apply(7, 1, &event(json!({"title":"4"}))).unwrap();
    assert_eq!(
        s.entries().iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![1, 3, 4, 5]
    );
    s.apply(7, 1, &event(json!({"title":"5"}))).unwrap();
    assert_eq!(
        s.entries().iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
    assert!(!s.dismiss(7, 1));
}
#[test]
fn every_utf8_limit_is_in_bytes_and_atomic() {
    for (field, limit) in [
        ("title", MAX_TITLE_BYTES),
        ("subtitle", MAX_SUBTITLE_BYTES),
        ("description", MAX_DESCRIPTION_BYTES),
        ("image_url", MAX_IMAGE_URL_BYTES),
    ] {
        let mut s = state();
        let mut p = json!({"title":"ok"});
        p[field] = json!("é".repeat(limit / 2));
        s.apply(7, 0, &event(p.clone())).unwrap();
        let before = s.entries().to_vec();
        p[field] = json!(format!("{}x", "é".repeat(limit / 2)));
        assert!(s.apply(7, 99999, &event(p)).is_err(), "{field}");
        assert_eq!(s.entries(), before);
    }
}
#[test]
fn full_serialized_event_limit_includes_escaping_and_unknown_fields() {
    let mut s = state();
    let mut e = event(json!({"title":"ok", "extra":""}));
    let overhead = serde_json::to_vec(&e).unwrap().len();
    e["data"]["payload"]["extra"] = json!("x".repeat(MAX_EVENT_BYTES - overhead));
    assert_eq!(serde_json::to_vec(&e).unwrap().len(), MAX_EVENT_BYTES);
    s.apply(7, 0, &e).unwrap();
    let before = s.entries().to_vec();
    e["data"]["payload"]["extra"] = json!("x".repeat(MAX_EVENT_BYTES - overhead + 1));
    assert!(s.apply(7, 99999, &e).is_err());
    assert_eq!(s.entries(), before);
    let escaped = event(json!({"title":"ok","description":"\u{0000}".repeat(1400)}));
    assert!(serde_json::to_vec(&escaped).unwrap().len() > MAX_EVENT_BYTES);
    assert!(s.apply(7, 99999, &escaped).is_err());
    assert_eq!(s.entries(), before);
}
