use pipecat_text::*;
use std::sync::{Arc, Mutex};
#[test]
fn callbacks_receive_full_match_once_and_survive_reset() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut a = PatternPairAggregator::default();
    a.add_pattern("think", "<think>", "</think>", MatchAction::Remove)
        .unwrap();
    a.add_pattern("em", "<em>", "</em>", MatchAction::Keep)
        .unwrap();
    for name in ["think", "em"] {
        let seen = calls.clone();
        a.on_pattern_match(name, move |matched| {
            seen.lock().unwrap().push(matched.clone());
            Ok(())
        });
    }
    a.aggregate("Hello <think> private. stuff </think> I am <em>very</em> happy.");
    a.aggregate(" Next.");
    assert_eq!(calls.lock().unwrap().len(), 2);
    assert_eq!(calls.lock().unwrap()[0].text, "private. stuff");
    assert_eq!(
        calls.lock().unwrap()[0].full_match,
        "<think> private. stuff </think>"
    );
    assert_eq!(a.take_matches().len(), 2);
    assert!(a.take_matches().is_empty());
    a.reset();
    a.aggregate("<think>new</think>");
    assert_eq!(calls.lock().unwrap().len(), 3);
    a.handle_interruption();
    a.aggregate("<em>newer</em>");
    assert_eq!(calls.lock().unwrap().len(), 4);
}
#[test]
fn callback_errors_are_reported_without_interrupting_text() {
    let mut a = PatternPairAggregator::default();
    a.add_pattern("think", "<think>", "</think>", MatchAction::Remove)
        .unwrap();
    a.on_pattern_match("think", |_| Err("handler failed".into()));
    a.aggregate("Hello <think>private</think> world!");
    assert_eq!(a.flush().unwrap().text, "Hello  world!");
    assert_eq!(a.take_handler_errors(), ["think: handler failed"]);
    assert!(a.take_handler_errors().is_empty());
}
#[test]
fn registration_rejects_reserved_types_and_replaces_in_order() {
    let mut a = PatternPairAggregator::default();
    for name in ["sentence", "token", "word"] {
        assert!(a.add_pattern(name, "[", "]", MatchAction::Remove).is_err());
    }
    assert!(a
        .add_pattern("invalid", "", "]", MatchAction::Keep)
        .is_err());
    a.add_pattern("custom", "[", "]", MatchAction::Remove)
        .unwrap();
    a.add_pattern("custom", "[", "]", MatchAction::Aggregate)
        .unwrap();
    let output = a.aggregate_matches("[ content ]");
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].text, "content");
    assert_eq!(output[0].kind, "custom");
    assert_eq!(output[0].full_match, "[ content ]");
}
#[test]
fn unicode_delimiters_reassemble_and_partial_flush_is_safe() {
    let mut a = PatternPairAggregator::new(AggregationType::Token, None);
    a.add_pattern("reasoning", "【思考】", "【終】", MatchAction::Remove)
        .unwrap();
    assert_eq!(a.aggregate("你好【思")[0].text, "你好");
    assert!(a.aggregate("考】秘密【終】").is_empty());
    assert_eq!(a.take_matches()[0].text, "秘密");
    assert_eq!(a.aggregate("good【")[0].text, "good");
    assert!(a.flush().is_none());
}
#[test]
fn skip_tag_reset_clears_state_and_keeps_registered_tags() {
    let mut a = SkipTagsAggregator::new(
        vec![("<spell>".into(), "</spell>".into())],
        AggregationType::Token,
        None,
    );
    assert!(a.aggregate("<spell>old").is_empty());
    a.handle_interruption();
    assert_eq!(a.aggregate("fresh")[0].text, "fresh");
    assert!(a.aggregate("<spe").is_empty());
    assert_eq!(a.aggregate("ll>new</spell>")[0].text, "<spell>new</spell>");
}
