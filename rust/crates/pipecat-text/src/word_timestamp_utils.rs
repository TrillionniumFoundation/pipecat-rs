use crate::alnum_utils::has_alnum;
/// Merge punctuation-only tokens into the preceding word, preserving its time.
pub fn merge_punct_tokens<T: AsRef<str>>(word_times: &[(T, f64)]) -> Vec<(String, f64)> {
    let mut merged: Vec<(String, f64)> = vec![];
    for (word, timestamp) in word_times {
        let word = word.as_ref();
        if has_alnum(word) {
            merged.push((word.to_owned(), *timestamp));
        } else if let Some((previous, _)) = merged.last_mut() {
            previous.push_str(word);
        }
    }
    merged
        .into_iter()
        .map(|(word, ts)| (word.trim().to_owned(), ts))
        .collect()
}
