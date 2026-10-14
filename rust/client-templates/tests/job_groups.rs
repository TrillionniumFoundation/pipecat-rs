use pipecat_browser_client::job_groups::*;
use serde_json::{json, Value};
fn event(data: Value) -> Value {
    json!({"label":"rtvi-ai","type":"ui-job-group","data":data})
}
fn apply(state: &mut JobGroups, data: Value) -> bool {
    state.apply(7, &event(data)).unwrap()
}
fn state() -> JobGroups {
    let mut s = JobGroups::default();
    s.begin(7);
    s
}
fn start(s: &mut JobGroups, id: &str) {
    apply(s, json!({"kind":"group_started","job_id":id}));
}
fn complete(s: &mut JobGroups, id: &str) {
    apply(s, json!({"kind":"group_completed","job_id":id}));
}

#[test]
fn four_kinds_defaults_null_optionals_and_literal_json_status() {
    let mut s = state();
    assert!(apply(
        &mut s,
        json!({"kind":"group_started","job_id":"a","workers":null,"label":null})
    ));
    let g = &s.groups()[0];
    assert_eq!(g.label, None);
    assert!(g.cancellable);
    assert_eq!(g.at, 0);
    assert!(!g.completed);
    assert!(g.workers.is_empty());
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"a","worker_name":"w","data":{"html":"<script>alert(1)</script>"},"at":99}),
    );
    assert_eq!(
        s.groups()[0].workers[0].data.as_deref(),
        Some("{\"html\":\"<script>alert(1)</script>\"}")
    );
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":"not-a-known-status <b>","response":[1,true],"at":1}),
    );
    let w = &s.groups()[0].workers[0];
    assert_eq!(w.at, 1);
    assert!(w.completed);
    assert_eq!(w.status.as_deref(), Some("not-a-known-status <b>"));
    assert_eq!(w.response.as_deref(), Some("[1,true]"));
    assert!(w.data.is_some());
    assert!(
        !s.groups()[0].completed,
        "worker completion cannot infer group completion"
    );
    complete(&mut s, "a");
    assert!(s.groups()[0].completed);
    assert_eq!(s.groups()[0].at, 0);
    start(&mut s, "b");
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"b","worker_name":"w","data":null}),
    );
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"b","worker_name":"w","status":"","response":null}),
    );
    assert_eq!(s.groups()[1].workers[0].status.as_deref(), Some(""));
    assert_eq!(s.groups()[1].workers[0].response, None);
}

#[test]
fn sessions_and_wrong_envelopes_cannot_change_state() {
    let mut s = JobGroups::default();
    let e = event(json!({"kind":"group_started","job_id":"a"}));
    assert!(!s.apply(7, &e).unwrap());
    s.begin(7);
    assert!(s.apply(7, &e).unwrap());
    assert!(!s.apply(6, &e).unwrap());
    for (field, value) in [
        ("label", json!("other")),
        ("type", json!("job-group")),
        ("label", Value::Null),
    ] {
        let mut other = e.clone();
        other[field] = value;
        assert!(!s.apply(7, &other).unwrap());
    }
    s.begin(8);
    assert!(s.groups().is_empty());
    assert!(!s.is_incomplete());
    assert!(!s.apply(7, &e).unwrap());
    assert!(s.apply(8, &e).unwrap());
    s.close();
    assert!(s.groups().is_empty());
    assert!(!s.is_incomplete());
    assert!(!s.apply(8, &e).unwrap());
}

#[test]
fn terminal_states_and_reused_ids_never_reopen() {
    let mut s = state();
    start(&mut s, "a");
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":"done"}),
    );
    let snapshot = s.groups().to_vec();
    for data in [
        json!({"kind":"job_update","job_id":"a","worker_name":"w","data":"late","at":900}),
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":"changed"}),
        json!({"kind":"group_started","job_id":"a","workers":["replacement"]}),
    ] {
        assert!(!apply(&mut s, data));
        assert_eq!(s.groups(), snapshot);
    }
    complete(&mut s, "a");
    let snapshot = s.groups().to_vec();
    for data in [
        json!({"kind":"job_update","job_id":"a","worker_name":"new","data":1}),
        json!({"kind":"job_completed","job_id":"a","worker_name":"new","status":"done"}),
        json!({"kind":"group_completed","job_id":"a","at":999}),
        json!({"kind":"group_started","job_id":"a"}),
    ] {
        assert!(!apply(&mut s, data));
        assert_eq!(s.groups(), snapshot);
    }
}

#[test]
fn interleaving_preserves_wire_order_without_time_reordering() {
    let mut s = state();
    apply(
        &mut s,
        json!({"kind":"group_started","job_id":"a","workers":["one","two"],"label":"First","cancellable":false,"at":50}),
    );
    start(&mut s, "b");
    for (g, w, data, at) in [
        ("a", "two", 1, 100),
        ("b", "one", 2, 12),
        ("a", "one", 3, 8),
        ("a", "two", 4, 1),
    ] {
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":g,"worker_name":w,"data":data,"at":at}),
        );
    }
    assert_eq!(s.groups()[0].label.as_deref(), Some("First"));
    assert!(!s.groups()[0].cancellable);
    assert_eq!(s.groups()[0].workers[0].data.as_deref(), Some("3"));
    assert_eq!(s.groups()[0].workers[1].data.as_deref(), Some("4"));
    assert_eq!(s.groups()[0].workers[1].at, 1);
    assert_eq!(s.groups()[1].workers[0].data.as_deref(), Some("2"));
    assert!(!apply(
        &mut s,
        json!({"kind":"job_update","job_id":"unknown","worker_name":"w"})
    ));
    assert!(!apply(
        &mut s,
        json!({"kind":"group_completed","job_id":"unknown"})
    ));
    assert_eq!(s.groups().len(), 2);
}

#[test]
fn invalid_events_are_atomic_and_do_not_poison_id_ledger() {
    let mut s = state();
    start(&mut s, "a");
    let snapshot = s.groups().to_vec();
    let invalid = vec![
        json!({"kind":"bogus","job_id":"a"}),
        json!({"kind":"group_started","job_id":"bad","workers":["w","w"]}),
        json!({"kind":"group_started","job_id":"bad","workers":[1]}),
        json!({"kind":"group_started","job_id":"bad","workers":[""]}),
        json!({"kind":"group_started","job_id":"bad","workers":["x".repeat(129)]}),
        json!({"kind":"group_started","job_id":"bad","workers":(0..17).map(|i|i.to_string()).collect::<Vec<_>>()}),
        json!({"kind":"group_started","job_id":"bad","label":"x".repeat(513)}),
        json!({"kind":"group_started","job_id":"bad","label":3}),
        json!({"kind":"group_started","job_id":"bad","cancellable":null}),
        json!({"kind":"group_started","job_id":"bad","cancellable":"true"}),
        json!({"kind":"group_started","job_id":""}),
        json!({"kind":"group_started","job_id":"é".repeat(65)}),
        json!({"kind":"job_update","job_id":"a","worker_name":""}),
        json!({"kind":"job_update","job_id":"a","worker_name":"w","at":-1}),
        json!({"kind":"job_update","job_id":"a","worker_name":"w","at":null}),
        json!({"kind":"job_update","job_id":"a","worker_name":"w","at":1.5}),
        json!({"kind":"job_update","job_id":"a","worker_name":"w","data":"x".repeat(4095)}),
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":"x".repeat(257)}),
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":null}),
        json!({"kind":"job_completed","job_id":"a","worker_name":"w","status":"done","response":"x".repeat(4095)}),
        json!({"kind":"group_completed","job_id":1}),
    ];
    for data in invalid {
        assert!(s.apply(7, &event(data.clone())).is_err(), "accepted {data}");
        assert_eq!(s.groups(), snapshot);
        assert!(s.is_incomplete());
    }
    start(&mut s, "bad");
    assert_eq!(s.groups().len(), 2);
}

#[test]
fn exact_field_and_event_byte_limits() {
    let mut s = state();
    let key = "é".repeat(64);
    apply(
        &mut s,
        json!({"kind":"group_started","job_id":key,"workers":["w".repeat(128)],"label":"x".repeat(512)}),
    );
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":key,"worker_name":"w".repeat(128),"data":"x".repeat(4094)}),
    );
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":key,"worker_name":"w".repeat(128),"status":"x".repeat(256),"response":"x".repeat(4094)}),
    );
    assert_eq!(s.groups()[0].workers[0].data.as_ref().unwrap().len(), 4096);
    let mut e = event(json!({"kind":"group_started","job_id":"boundary"}));
    e["padding"] = json!("");
    let base = serde_json::to_vec(&e).unwrap().len();
    e["padding"] = json!("x".repeat(MAX_EVENT_BYTES - base));
    assert_eq!(serde_json::to_vec(&e).unwrap().len(), MAX_EVENT_BYTES);
    assert!(s.apply(7, &e).unwrap());
    e["padding"] = json!("x".repeat(MAX_EVENT_BYTES - base + 1));
    let snapshot = s.groups().to_vec();
    assert!(s.apply(7, &e).is_err());
    assert_eq!(s.groups(), snapshot);
}

#[test]
fn group_capacity_rejection_and_eviction_keep_tombstones() {
    let mut s = state();
    for i in 0..8 {
        start(&mut s, &i.to_string());
    }
    let snapshot = s.groups().to_vec();
    start(&mut s, "omitted");
    assert_eq!(s.groups(), snapshot);
    assert!(s.is_incomplete());
    complete(&mut s, "4");
    complete(&mut s, "1");
    start(&mut s, "new");
    assert_eq!(s.groups().len(), 8);
    assert_eq!(s.groups()[0].job_id, "0");
    assert!(
        !s.groups().iter().any(|g| g.job_id == "1"),
        "oldest completed by insertion order"
    );
    assert!(s.groups().iter().any(|g| g.job_id == "4"));
    let snapshot = s.groups().to_vec();
    start(&mut s, "1");
    start(&mut s, "omitted");
    assert_eq!(s.groups(), snapshot);
    assert!(!apply(
        &mut s,
        json!({"kind":"job_update","job_id":"1","worker_name":"w"})
    ));
}

#[test]
fn seen_id_saturation_refuses_new_ids_but_preserves_live_updates() {
    let mut s = state();
    for i in 0..64 {
        let id = i.to_string();
        start(&mut s, &id);
        complete(&mut s, &id);
    }
    assert_eq!(s.groups().len(), 8);
    assert!(s.is_incomplete());
    let snapshot = s.groups().to_vec();
    start(&mut s, "65");
    assert_eq!(s.groups(), snapshot);
    s.begin(7);
    for i in 0..64 {
        start(&mut s, &i.to_string());
    }
    assert_eq!(s.groups().len(), 8);
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"0","worker_name":"w","data":5}),
    );
    assert_eq!(s.groups()[0].workers[0].data.as_deref(), Some("5"));
    complete(&mut s, "0");
    start(&mut s, "new");
    assert_eq!(s.groups().len(), 8);
    assert!(s.groups().iter().any(|g| g.job_id == "0"));
    s.begin(8);
    assert!(!s.is_incomplete());
    assert!(s
        .apply(8, &event(json!({"kind":"group_started","job_id":"new"})))
        .unwrap());
}

#[test]
fn worker_capacity_omissions_do_not_change_existing_workers() {
    let mut s = state();
    start(&mut s, "a");
    for i in 0..16 {
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":"a","worker_name":i.to_string(),"data":i}),
        );
    }
    let snapshot = s.groups().to_vec();
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"a","worker_name":"extra","status":"done"}),
    );
    assert_eq!(s.groups()[0].workers, snapshot[0].workers);
    assert_eq!(s.groups()[0].completed, snapshot[0].completed);
    assert!(s.is_incomplete());
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"a","worker_name":"0","status":"done"}),
    );
    assert_eq!(s.groups()[0].workers[0].status.as_deref(), Some("done"));
}

#[test]
fn retained_budget_boundary_and_rejection_are_transactional() {
    let mut s = state();
    start(&mut s, "g");
    // 15 workers: one byte group ID + 15 * (one byte name + 4096 byte JSON).
    for i in 0..15 {
        let name = char::from(b'a' + i).to_string();
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":"g","worker_name":name,"data":"x".repeat(4094)}),
        );
    }
    assert_eq!(s.retained_content_bytes(), 61456);
    // Last worker can hold exactly 4079 bytes of JSON, including its quotes.
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"g","worker_name":"z","data":"x".repeat(4077)}),
    );
    assert_eq!(s.retained_content_bytes(), MAX_CONTENT_BYTES);
    assert!(!s.is_incomplete());
    let snapshot = s.groups().to_vec();
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"g","worker_name":"z","data":"x".repeat(4078),"at":99}),
    );
    assert_eq!(s.groups(), snapshot);
    assert!(s.is_incomplete());
    apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"g","worker_name":"z","status":"done"}),
    );
    let terminal = &s.groups()[0].workers[15];
    assert!(
        terminal.completed,
        "validated completion remains terminal despite omitted result"
    );
    assert_eq!(terminal.status, None);
    assert_eq!(terminal.response, None);
    assert_eq!(terminal.data, snapshot[0].workers[15].data);
    assert_eq!(s.retained_content_bytes(), MAX_CONTENT_BYTES);
    let terminal_snapshot = s.groups().to_vec();
    start(&mut s, "omitted");
    assert_eq!(s.groups(), terminal_snapshot);
    assert!(!apply(
        &mut s,
        json!({"kind":"job_update","job_id":"g","worker_name":"z","data":null}),
    ));
    assert!(!apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"g","worker_name":"z","status":""}),
    ));
    assert_eq!(s.groups(), terminal_snapshot);
    // Free capacity through a different, still-live worker.
    apply(
        &mut s,
        json!({"kind":"job_update","job_id":"g","worker_name":"a","data":null}),
    );
    start(&mut s, "omitted");
    assert_eq!(s.groups().len(), 1, "budget-rejected start has tombstone");
    start(&mut s, "new");
    assert_eq!(s.groups().len(), 2);
    assert!(s.is_incomplete());
}

#[test]
fn matching_errors_latch_incomplete_but_stale_or_other_envelopes_do_not() {
    let mut s = state();
    start(&mut s, "a");
    let snapshot = s.groups().to_vec();
    let invalid = event(json!({"kind":"unknown"}));
    assert!(!s.apply(6, &invalid).unwrap());
    assert!(!s.is_incomplete());
    let mut unrelated = invalid.clone();
    unrelated["type"] = json!("other");
    assert!(!s.apply(7, &unrelated).unwrap());
    assert!(!s.is_incomplete());
    assert!(s.apply(7, &invalid).is_err());
    assert!(s.is_incomplete());
    assert_eq!(s.groups(), snapshot);
    complete(&mut s, "a");
    assert!(s.is_incomplete());
    s.begin(7);
    assert!(!s.is_incomplete());
    start(&mut s, "a");
    let snapshot = s.groups().to_vec();
    let oversized =
        event(json!({"kind":"group_completed","job_id":"a","padding":"x".repeat(MAX_EVENT_BYTES)}));
    assert!(s.apply(7, &oversized).is_err());
    assert!(s.is_incomplete());
    assert_eq!(s.groups(), snapshot);
    s.close();
    assert!(!s.is_incomplete());
    assert!(!s.apply(7, &invalid).unwrap());
    assert!(!s.is_incomplete());
}

#[test]
fn omitted_result_for_new_worker_retains_terminal_fact_when_name_fits() {
    let mut s = state();
    start(&mut s, "g");
    for i in 0..15 {
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":"g","worker_name":char::from(b'a' + i).to_string(),"data":"x".repeat(4094)}),
        );
    }
    assert!(apply(
        &mut s,
        json!({"kind":"job_completed","job_id":"g","worker_name":"z","status":"done","response":"x".repeat(4094),"at":33})
    ));
    let worker = &s.groups()[0].workers[15];
    assert!(worker.completed);
    assert_eq!(worker.at, 33);
    assert_eq!(worker.status, None);
    assert_eq!(worker.response, None);
    assert!(s.is_incomplete());
    assert!(s.retained_content_bytes() <= MAX_CONTENT_BYTES);
    let snapshot = s.groups().to_vec();
    assert!(!apply(
        &mut s,
        json!({"kind":"job_update","job_id":"g","worker_name":"z","data":1})
    ));
    assert_eq!(s.groups(), snapshot);
}

#[test]
fn unknown_worker_completion_tombstones_survive_capacity_release_and_saturate_safely() {
    for omitted_count in [1, 17] {
        let mut s = state();
        start(&mut s, "g");
        start(&mut s, "h");
        for i in 0..15 {
            apply(
                &mut s,
                json!({"kind":"job_update","job_id":"g","worker_name":char::from(b'a' + i).to_string(),"data":"x".repeat(4094)}),
            );
        }
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":"g","worker_name":"z","data":"x".repeat(4076)}),
        );
        assert_eq!(s.retained_content_bytes(), MAX_CONTENT_BYTES);
        for i in 0..omitted_count {
            apply(
                &mut s,
                json!({"kind":"job_completed","job_id":"h","worker_name":format!("omitted-{i}"),"status":"done"}),
            );
        }
        assert!(s.groups()[1].workers.is_empty());
        assert!(s.is_incomplete());
        apply(
            &mut s,
            json!({"kind":"job_update","job_id":"g","worker_name":"a","data":null}),
        );
        assert!(!apply(
            &mut s,
            json!({"kind":"job_update","job_id":"h","worker_name":"omitted-0","data":1})
        ));
        assert!(!apply(
            &mut s,
            json!({"kind":"job_completed","job_id":"h","worker_name":"omitted-0","status":"changed"})
        ));
        let changed = apply(
            &mut s,
            json!({"kind":"job_update","job_id":"h","worker_name":"new","data":1}),
        );
        assert_eq!(
            changed,
            omitted_count == 1,
            "ledger saturation seals unknown workers only"
        );
        assert!(apply(
            &mut s,
            json!({"kind":"job_update","job_id":"g","worker_name":"a","data":2})
        ));
    }
}
