//! Marker removal is an event projection; stored provider history is untouched.
use super::{
    USER_TURN_COMPLETE_MARKER, USER_TURN_INCOMPLETE_LONG_MARKER, USER_TURN_INCOMPLETE_SHORT_MARKER,
};
use pipecat_core::ProcessorError;
use std::collections::BTreeSet;

const DEFAULT_MARKERS: [&str; 3] = [
    USER_TURN_COMPLETE_MARKER,
    USER_TURN_INCOMPLETE_SHORT_MARKER,
    USER_TURN_INCOMPLETE_LONG_MARKER,
];
const MAX_CUSTOM_MARKERS: usize = 64;
const MAX_MARKER_BYTES: usize = 256;
const MAX_TOTAL_BYTES: usize = 4096;

#[derive(Default)]
pub(crate) struct AssistantTranscriptMarkers {
    custom: BTreeSet<String>,
    bytes: usize,
}
impl AssistantTranscriptMarkers {
    /// Validate before any marker causes a context write. Repeated observations
    /// are idempotent and do not consume the per-turn budget again.
    pub(crate) fn observe(&mut self, marker: &str) -> Result<(), ProcessorError> {
        if marker.is_empty() || marker.len() > MAX_MARKER_BYTES {
            return Err(ProcessorError::new(
                "assistant transcript marker must contain 1..=256 bytes",
            ));
        }
        if DEFAULT_MARKERS.contains(&marker) || self.custom.contains(marker) {
            return Ok(());
        }
        if self.custom.len() == MAX_CUSTOM_MARKERS || marker.len() > MAX_TOTAL_BYTES - self.bytes {
            return Err(ProcessorError::new(
                "assistant transcript marker retention limit exceeded",
            ));
        }
        self.custom.insert(marker.to_owned());
        self.bytes += marker.len();
        Ok(())
    }
    pub(crate) fn clear(&mut self) {
        self.custom.clear();
        self.bytes = 0;
    }
    pub(crate) fn project(&self, text: &str) -> String {
        let mut markers = DEFAULT_MARKERS.to_vec();
        markers.extend(self.custom.iter().map(String::as_str));
        // Source uses a set; deterministic longest-first removal avoids stripping
        // only the short prefix of an overlapping observed marker.
        markers.sort_unstable_by(|left, right| {
            right.len().cmp(&left.len()).then_with(|| left.cmp(right))
        });
        let mut projected = text.to_owned();
        let mut removed = false;
        for marker in markers {
            if projected.contains(marker) {
                projected = projected.replace(marker, "");
                removed = true;
            }
        }
        if removed {
            projected.trim().to_owned()
        } else {
            projected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unmarked_projection_does_not_trim_and_overlapping_markers_are_deterministic() {
        let mut markers = AssistantTranscriptMarkers::default();
        assert_eq!(markers.project("  unchanged  "), "  unchanged  ");
        markers.observe("<DONE>").unwrap();
        markers.observe("DONE").unwrap();
        assert_eq!(markers.project("  <DONE> hello DONE  "), "hello");
        assert_eq!(markers.project("  untouched  "), "  untouched  ");
        markers.clear();
        assert_eq!(markers.project("  <DONE>  "), "  <DONE>  ");
    }
}
