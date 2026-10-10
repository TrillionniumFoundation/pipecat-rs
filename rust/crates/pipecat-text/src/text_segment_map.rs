//! Character-indexed alignment of synthesis, display, and original LLM text.
//!
//! These cursors describe provider reports, never audio-device playout. A
//! transformed span is atomic: its original position cannot honestly be
//! interpolated from the length of its spoken expansion.
use crate::alnum_utils::{
    advance_by_alnums, advance_by_chars, alnum_only, fold_for_matching, has_alnum, is_alnum,
    strip_trailing_punctuation,
};
use crate::markup_utils::{
    raw_offset_after_clean_chars, split_markup_runs, strip_complete_markup, strip_markup,
};
use crate::unicode_categories::is_punctuation;
use std::collections::HashMap;
use std::fmt;

/// Budgets apply before tokenization, diffing, or accepting a provider event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlignmentLimits {
    pub max_text_bytes: usize,
    pub max_tokens: usize,
    pub max_segments: usize,
    pub max_word_bytes: usize,
    pub max_diff_work: usize,
    pub lookahead_words: usize,
}
impl Default for AlignmentLimits {
    fn default() -> Self {
        Self {
            max_text_bytes: 65_536,
            max_tokens: 4096,
            max_segments: 4096,
            max_word_bytes: 4096,
            max_diff_work: 4_000_000,
            lookahead_words: 3,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlignmentError {
    LimitExceeded(&'static str),
    InvalidSegments,
    UnalignedLlmText,
    StaleGeneration,
    UnknownContext,
    UnknownSegment,
    ContextClosed,
    IdentityExhausted,
    InvalidLimits,
    /// An event is incompatible with the context's declared reporting mode.
    UnsupportedOperation(&'static str),
}
impl fmt::Display for AlignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AlignmentError {}

/// A half-open range measured in Unicode scalar values, not UTF-8 bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CharacterSpan {
    pub start: usize,
    pub end: usize,
}

/// An aligned piece. Use explicit pieces when transform provenance is available.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextSegment {
    pub original: String,
    pub tts: String,
    pub original_start: usize,
    pub original_end: usize,
}
impl TextSegment {
    pub fn is_transformed(&self) -> bool {
        self.tts != strip_complete_markup(&self.tts)
            || alnum_only(&self.original) != alnum_only(&self.tts)
            || self
                .original
                .split(is_space)
                .filter(|s| !s.is_empty())
                .count()
                != self.tts.split(is_space).filter(|s| !s.is_empty()).count()
    }
    pub fn tts_alnum_count(&self) -> usize {
        count(&alnum_only(&self.tts))
    }
    pub fn original_alnum_count(&self) -> usize {
        count(&alnum_only(&self.original))
    }
}

pub(crate) fn is_space(c: char) -> bool {
    crate::alnum_utils::is_whitespace(c)
}
pub(crate) fn trim(text: &str) -> &str {
    text.trim_matches(is_space)
}
pub(crate) fn count(text: &str) -> usize {
    text.chars().count()
}
pub(crate) fn slice(text: &str, start: usize, end: usize) -> &str {
    let a = crate::string::char_to_byte(text, start);
    let b = crate::string::char_to_byte(text, end);
    &text[a..b]
}
pub(crate) fn tail(text: &str, start: usize) -> &str {
    &text[crate::string::char_to_byte(text, start)..]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HopKind {
    Placed,
    Crosses,
    Exhausted,
    NoMatch,
}
#[derive(Clone, Copy, Debug)]
struct Hop {
    kind: HopKind,
    advance: usize,
    consumed: usize,
    resynchronized: bool,
}
impl Hop {
    fn placed(advance: usize) -> Self {
        Self {
            kind: HopKind::Placed,
            advance,
            consumed: 0,
            resynchronized: false,
        }
    }
    fn kind(kind: HopKind) -> Self {
        Self {
            kind,
            advance: 0,
            consumed: 0,
            resynchronized: false,
        }
    }
}

/// Immutable text alignment with mutable provider-report cursors.
#[derive(Clone, Debug)]
pub struct TextSegmentMap {
    tts_text: String,
    original_text: String,
    llm_text: String,
    llm_plain_matches_original: bool,
    segments: Vec<TextSegment>,
    limits: AlignmentLimits,
    seg_idx: usize,
    seg_raw_pos: usize,
    raw_base: usize,
    user_facing_pos: usize,
    llm_pos: usize,
    llm_reported_pos: usize,
    last_completed: Option<usize>,
    last_overflow: Option<String>,
    last_leading_duplicate: usize,
    last_resynchronized: bool,
}
impl TextSegmentMap {
    pub fn new(
        tts_text: &str,
        original_text: &str,
        llm_text: Option<&str>,
        limits: AlignmentLimits,
    ) -> Result<Self, AlignmentError> {
        Self::validate_texts(tts_text, original_text, llm_text, limits)?;
        let segments = build_segments(tts_text, original_text, limits)?;
        Self::from_segments(tts_text, original_text, llm_text, segments, limits)
    }
    /// Validate explicit, contiguous segments without guessing transform boundaries.
    pub fn from_segments(
        tts_text: &str,
        original_text: &str,
        llm_text: Option<&str>,
        segments: Vec<TextSegment>,
        limits: AlignmentLimits,
    ) -> Result<Self, AlignmentError> {
        Self::validate_texts(tts_text, original_text, llm_text, limits)?;
        if segments.len() > limits.max_segments {
            return Err(AlignmentError::LimitExceeded("segments"));
        }
        let mut original_pos = 0;
        let mut original_bytes = 0;
        let mut tts_bytes = 0;
        for seg in &segments {
            let end = original_pos + count(&seg.original);
            if seg.original_start != original_pos
                || seg.original_end != end
                || !original_text[original_bytes..].starts_with(&seg.original)
                || !tts_text[tts_bytes..].starts_with(&seg.tts)
            {
                return Err(AlignmentError::InvalidSegments);
            }
            original_pos = end;
            original_bytes += seg.original.len();
            tts_bytes += seg.tts.len();
        }
        if original_bytes != original_text.len() || tts_bytes != tts_text.len() {
            return Err(AlignmentError::InvalidSegments);
        }
        Ok(Self {
            tts_text: tts_text.into(),
            original_text: original_text.into(),
            llm_text: llm_text.unwrap_or(original_text).into(),
            llm_plain_matches_original: strip_complete_markup(llm_text.unwrap_or(original_text))
                == strip_complete_markup(original_text),
            segments,
            limits,
            seg_idx: 0,
            seg_raw_pos: 0,
            raw_base: 0,
            user_facing_pos: 0,
            llm_pos: 0,
            llm_reported_pos: 0,
            last_completed: None,
            last_overflow: None,
            last_leading_duplicate: 0,
            last_resynchronized: false,
        })
    }
    fn validate_texts(
        tts: &str,
        original: &str,
        llm: Option<&str>,
        limits: AlignmentLimits,
    ) -> Result<(), AlignmentError> {
        if limits.max_text_bytes == 0
            || limits.max_word_bytes == 0
            || limits.max_tokens == 0
            || limits.max_segments == 0
            || limits.max_diff_work == 0
        {
            return Err(AlignmentError::InvalidLimits);
        }
        if [tts, original, llm.unwrap_or(original)]
            .iter()
            .any(|s| s.len() > limits.max_text_bytes)
        {
            return Err(AlignmentError::LimitExceeded("text bytes"));
        }
        // Counting original letters is only a valid LLM projection under this
        // contract. Reject unrelated originals instead of silently misattributing.
        if llm.is_some_and(|llm| alnum_only(llm) != alnum_only(original)) {
            return Err(AlignmentError::UnalignedLlmText);
        }
        Ok(())
    }
    pub fn segments(&self) -> &[TextSegment] {
        &self.segments
    }
    pub fn tts_text(&self) -> &str {
        &self.tts_text
    }
    pub fn original_text(&self) -> &str {
        &self.original_text
    }
    pub fn llm_text(&self) -> &str {
        &self.llm_text
    }
    pub fn raw_pos(&self) -> usize {
        self.raw_base + self.seg_raw_pos
    }
    pub fn user_facing_pos(&self) -> usize {
        self.user_facing_pos
    }
    pub fn llm_pos(&self) -> usize {
        self.llm_pos
    }
    /// Provider-report position, including any explicitly signaled resynchronization.
    /// This is not an audible position or a transport acknowledgment.
    pub fn llm_reported_pos(&self) -> usize {
        self.llm_reported_pos
    }
    pub fn last_completed_segment(&self) -> Option<&TextSegment> {
        self.last_completed.map(|i| &self.segments[i])
    }
    pub fn last_overflow(&self) -> Option<&str> {
        self.last_overflow.as_deref()
    }
    pub fn last_leading_duplicate(&self) -> usize {
        self.last_leading_duplicate
    }
    pub fn last_resynchronized(&self) -> bool {
        self.last_resynchronized
    }
    pub fn in_transformed_segment(&self) -> bool {
        self.segments
            .get(self.seg_idx)
            .is_some_and(|s| s.is_transformed() && self.seg_raw_pos > 0)
    }
    pub fn is_complete(&self) -> bool {
        let Some(seg) = self.segments.get(self.seg_idx) else {
            return true;
        };
        let remaining = tail(&seg.tts, self.seg_raw_pos);
        !has_alnum(remaining)
            && !pending_separated_punctuation(remaining)
            && self.segments[self.seg_idx + 1..]
                .iter()
                .all(|s| !has_alnum(&s.tts))
    }
    pub fn word_belongs_current_segment(&self, word: &str) -> Result<bool, AlignmentError> {
        self.check_word(word)?;
        if word.is_empty() {
            return Ok(true);
        }
        let (hops, _) = self.plan_hops(word);
        let can = self.seg_idx < self.segments.len()
            && hops.last().is_none_or(|h| h.kind != HopKind::NoMatch);
        Ok(can || (!has_alnum(word) && self.symbol_belongs_here(word)))
    }
    pub fn advance_word(&mut self, word: &str) -> Result<(), AlignmentError> {
        self.check_word(word)?;
        self.last_completed = None;
        self.last_overflow = None;
        self.last_leading_duplicate = 0;
        self.last_resynchronized = false;
        if word.is_empty() {
            return Ok(());
        }
        self.last_leading_duplicate = self.leading_duplicate_len(word);
        let (hops, overflow) = self.plan_hops(word);
        for hop in hops {
            self.last_resynchronized |= hop.resynchronized;
            let seg = self.segments[self.seg_idx].clone();
            let new_pos = match hop.kind {
                HopKind::Placed | HopKind::NoMatch => self.seg_raw_pos + hop.advance,
                HopKind::Crosses | HopKind::Exhausted => count(&seg.tts),
            };
            self.advance_cursors(&seg, new_pos);
        }
        if !overflow.is_empty() {
            self.last_overflow = Some(overflow);
        }
        Ok(())
    }
    fn check_word(&self, word: &str) -> Result<(), AlignmentError> {
        if word.len() > self.limits.max_word_bytes {
            Err(AlignmentError::LimitExceeded("word bytes"))
        } else {
            Ok(())
        }
    }
    fn plan_hops(&self, word: &str) -> (Vec<Hop>, String) {
        let mut idx = self.seg_idx;
        let mut pos = self.seg_raw_pos;
        let mut word = word;
        let mut hops = Vec::new();
        while !word.is_empty() && idx < self.segments.len() {
            let hop = classify_hop(
                tail(&self.segments[idx].tts, pos),
                word,
                self.limits.lookahead_words,
            );
            hops.push(hop);
            if matches!(hop.kind, HopKind::Placed | HopKind::NoMatch) {
                return (hops, String::new());
            }
            word = tail(word, hop.consumed);
            idx += 1;
            pos = 0;
        }
        (hops, word.to_owned())
    }
    fn advance_cursors(&mut self, seg: &TextSegment, mut new_pos: usize) {
        if seg.is_transformed() {
            if !has_alnum(tail(&seg.tts, new_pos)) {
                new_pos = count(&seg.tts);
            }
        } else {
            let crossed = slice(&seg.tts, self.seg_raw_pos, new_pos);
            let n = count(&alnum_only(crossed));
            self.user_facing_pos = if n > 0 {
                advance_by_alnums(&self.original_text, self.user_facing_pos, n)
            } else if seg.original != seg.tts {
                // Equal alphanumeric content does not make punctuation widths
                // interchangeable ("a------ b" versus "a b").
                self.user_facing_pos
            } else {
                seg.original_start + count(slice(&seg.tts, 0, new_pos).trim_end_matches(is_space))
            }
            .min(seg.original_end);
            let attributed = advance_by_alnums(&self.llm_text, self.llm_pos, n);
            // Raw synthesis character counts cannot spend original letters
            // beyond this step's alphanumeric budget.
            let reported = advance_by_chars(&self.llm_text, self.llm_reported_pos, count(crossed));
            self.llm_reported_pos = if seg.original == seg.tts && self.llm_plain_matches_original {
                reported
            } else {
                reported.min(attributed)
            };
            self.llm_pos = attributed.max(self.llm_reported_pos);
        }
        self.seg_raw_pos = new_pos;
        if new_pos >= count(&seg.tts) {
            if seg.is_transformed() {
                self.user_facing_pos = seg.original_end;
                self.llm_pos =
                    advance_by_alnums(&self.llm_text, self.llm_pos, seg.original_alnum_count());
                self.llm_reported_pos = self.llm_pos;
            }
            self.last_completed = Some(self.seg_idx);
            self.seg_idx += 1;
            self.raw_base += count(&seg.tts);
            self.seg_raw_pos = 0;
        }
    }
    fn leading_duplicate_len(&self, word: &str) -> usize {
        let i = word
            .chars()
            .take_while(|c| is_space(*c) || is_punctuation(*c))
            .count();
        if i == count(word) {
            return 0;
        }
        let mark = slice(word, 0, i).trim_matches(is_space);
        let n = count(mark);
        if n == 0
            || self.llm_pos < n
            || slice(&self.llm_text, self.llm_pos - n, self.llm_pos) != mark
        {
            0
        } else {
            i
        }
    }
    fn symbol_belongs_here(&self, word: &str) -> bool {
        let chars: Vec<_> = self.tts_text.chars().collect();
        let mut start = self.raw_pos();
        while start > 0
            && !is_alnum(chars[start - 1])
            && !is_space(chars[start - 1])
            && chars[start - 1] != '>'
        {
            start -= 1;
        }
        if tail(&self.tts_text, start).contains(word) {
            return true;
        }
        if self.seg_idx >= self.segments.len() {
            return false;
        }
        tail(&self.tts_text, self.raw_pos())
            .trim_start_matches(is_space)
            .chars()
            .next()
            .is_some_and(|c| !is_alnum(c))
    }
    pub fn reset(&mut self) {
        self.seg_idx = 0;
        self.seg_raw_pos = 0;
        self.raw_base = 0;
        self.user_facing_pos = 0;
        self.llm_pos = 0;
        self.llm_reported_pos = 0;
        self.last_completed = None;
        self.last_overflow = None;
        self.last_leading_duplicate = 0;
        self.last_resynchronized = false;
    }
}
fn pending_separated_punctuation(text: &str) -> bool {
    let clean = strip_complete_markup(text);
    clean.starts_with(is_space)
        && clean
            .trim_matches(is_space)
            .chars()
            .next()
            .is_some_and(is_punctuation)
}
fn variants(word: &str) -> Vec<String> {
    let stripped = strip_trailing_punctuation(word);
    if stripped == word {
        return vec![word.into()];
    }
    let last = slice(word, 0, count(word) - 1).to_owned();
    if last == stripped {
        vec![word.into(), stripped]
    } else {
        vec![word.into(), last, stripped]
    }
}
fn literal_hop(candidates: &[(String, usize)], word: &str, boundary: bool) -> Option<Hop> {
    for word in variants(word) {
        if word.is_empty() {
            continue;
        }
        for (candidate, offset) in candidates {
            if candidate.starts_with(&word) {
                if !boundary
                    || !tail(candidate, count(&word))
                        .chars()
                        .next()
                        .is_some_and(is_alnum)
                {
                    return Some(Hop::placed(offset + count(&word)));
                }
            } else if !candidate.is_empty() && word.starts_with(candidate) {
                return Some(Hop {
                    consumed: count(candidate),
                    ..Hop::kind(HopKind::Crosses)
                });
            }
        }
    }
    None
}
fn folded_hop(candidates: &[(String, usize)], word: &str) -> Option<Hop> {
    let candidates = candidates
        .iter()
        .map(|(c, p)| (fold_for_matching(c), *p))
        .collect::<Vec<_>>();
    literal_hop(&candidates, &fold_for_matching(word), true)
}
fn classify_hop(remaining: &str, word: &str, lookahead: usize) -> Hop {
    let lead_ws = remaining.chars().take_while(|c| is_space(*c)).count();
    let lead_non = remaining
        .chars()
        .take_while(|c| !is_alnum(*c) && *c != '<')
        .count();
    let mut offsets = vec![0, lead_ws, lead_non];
    offsets.dedup();
    let candidates = offsets
        .into_iter()
        .map(|p| (tail(remaining, p).to_owned(), p))
        .collect::<Vec<_>>();
    if let Some(h) = literal_hop(&candidates, word, false).or_else(|| folded_hop(&candidates, word))
    {
        return h;
    }
    let stripped = remaining.trim_start_matches(is_space);
    let haystack = strip_markup(stripped);
    for candidate in variants(&strip_markup(word)) {
        if !candidate.is_empty() && haystack.starts_with(&candidate) {
            return Hop::placed(
                lead_ws + raw_offset_after_clean_chars(stripped, count(&candidate)),
            );
        }
    }
    if !remaining.contains('<') {
        let chars: Vec<_> = remaining.chars().collect();
        let starts = chars
            .iter()
            .enumerate()
            .filter(|(i, c)| is_alnum(**c) && (*i == 0 || !is_alnum(chars[*i - 1])))
            .map(|(i, _)| i);
        for offset in starts.skip(1).take(lookahead) {
            if let Some(mut h) = folded_hop(&[(tail(remaining, offset).into(), offset)], word) {
                if h.kind == HopKind::Placed {
                    h.resynchronized = true;
                    return h;
                }
            }
        }
    }
    if !has_alnum(remaining) {
        return Hop::kind(HopKind::Exhausted);
    }
    let advance = lead_ws
        + tail(remaining, lead_ws)
            .chars()
            .take_while(|c| !is_alnum(*c) && !is_space(*c))
            .count();
    Hop {
        advance,
        ..Hop::kind(HopKind::NoMatch)
    }
}

// Ratcliff/Obershelp matching with the same earliest-i, then earliest-j tie
// break as SequenceMatcher(autojunk=False). Work is bounded across all ranges.
fn build_segments(
    tts: &str,
    original: &str,
    limits: AlignmentLimits,
) -> Result<Vec<TextSegment>, AlignmentError> {
    fn tokenize(text: &str, limit: usize) -> Result<Vec<&str>, AlignmentError> {
        let mut out = Vec::new();
        let mut begin = 0;
        let mut whitespace = false;
        for (i, c) in text.char_indices() {
            if is_space(c) != whitespace {
                if out.len() >= limit {
                    return Err(AlignmentError::LimitExceeded("tokens"));
                }
                out.push(&text[begin..i]);
                begin = i;
                whitespace = !whitespace;
            }
        }
        let needed = if whitespace { 2 } else { 1 };
        if out.len().checked_add(needed).is_none_or(|n| n > limit) {
            return Err(AlignmentError::LimitExceeded("tokens"));
        }
        out.push(&text[begin..]);
        if whitespace {
            out.push("");
        }
        Ok(out)
    }
    let a = tokenize(original, limits.max_tokens)?;
    let b = tokenize(tts, limits.max_tokens)?;
    let mut positions: HashMap<&str, Vec<usize>> = HashMap::new();
    for (j, token) in b.iter().enumerate() {
        positions.entry(token).or_default().push(j);
    }
    let mut work = 0usize;
    let mut queue = vec![(0, a.len(), 0, b.len())];
    let mut blocks = Vec::new();
    while let Some((alo, ahi, blo, bhi)) = queue.pop() {
        let (mut ai, mut bj, mut size) = (alo, blo, 0);
        let mut previous: HashMap<usize, usize> = HashMap::new();
        for (i, token) in a.iter().enumerate().take(ahi).skip(alo) {
            let mut current = HashMap::new();
            if let Some(js) = positions.get(token) {
                for &j in js {
                    work += 1;
                    if work > limits.max_diff_work {
                        return Err(AlignmentError::LimitExceeded("diff work"));
                    }
                    if j < blo || j >= bhi {
                        continue;
                    }
                    let k = j
                        .checked_sub(1)
                        .and_then(|j| previous.get(&j))
                        .copied()
                        .unwrap_or(0)
                        + 1;
                    current.insert(j, k);
                    if k > size {
                        ai = i + 1 - k;
                        bj = j + 1 - k;
                        size = k;
                    }
                }
            }
            previous = current;
        }
        if size > 0 {
            blocks.push((ai, bj, size));
            if alo < ai && blo < bj {
                queue.push((alo, ai, blo, bj));
            }
            if ai + size < ahi && bj + size < bhi {
                queue.push((ai + size, ahi, bj + size, bhi));
            }
        }
    }
    blocks.sort_unstable();
    let mut merged: Vec<(usize, usize, usize)> = Vec::new();
    for (i, j, n) in blocks {
        if let Some(last) = merged.last_mut() {
            if last.0 + last.2 == i && last.1 + last.2 == j {
                last.2 += n;
                continue;
            }
        }
        merged.push((i, j, n));
    }
    merged.push((a.len(), b.len(), 0));
    let (mut i, mut j, mut pos) = (0, 0, 0);
    let mut segments = Vec::new();
    let mut append = |original: String, tts: String| -> Result<(), AlignmentError> {
        if segments.len() >= limits.max_segments {
            return Err(AlignmentError::LimitExceeded("segments"));
        }
        let end = pos + count(&original);
        segments.push(TextSegment {
            original,
            tts,
            original_start: pos,
            original_end: end,
        });
        pos = end;
        Ok(())
    };
    for (ai, bj, n) in merged {
        if i < ai || j < bj {
            append(a[i..ai].concat(), b[j..bj].concat())?;
        }
        if n > 0 {
            for part in split_markup_runs(&a[ai..ai + n].concat()) {
                append(part.clone(), part)?;
            }
        }
        i = ai + n;
        j = bj + n;
    }
    Ok(segments)
}
