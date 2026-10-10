//! Ordered, native user-start and mute policies with explicit monotonic time.
use crate::UserIdleController;
use pipecat_core::{FrameData, ProcessorError};
use regex::{Regex, RegexBuilder};
use std::sync::OnceLock;
use std::time::Duration;

const MAX_STRATEGIES: usize = 64;
const MAX_WAKE_CHARS: usize = 250;
const MAX_WAKE_BYTES: usize = MAX_STRATEGIES * MAX_WAKE_CHARS * 4;
const MAX_PATTERN_BYTES: usize = 64 * 1024;

/// A start proposal carries the decision independently of its input frame.
/// Already-announced external starts always disable both effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserTurnStartedParams {
    pub enable_interruptions: bool,
    pub enable_user_speaking_frames: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserStartStrategyEvent {
    WakePhraseDetected {
        strategy_index: usize,
        phrase: String,
    },
    WakePhraseTimeout {
        strategy_index: usize,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserStartStrategyOutput {
    pub start: Option<UserTurnStartedParams>,
    pub reset_aggregation: bool,
    pub events: Vec<UserStartStrategyEvent>,
}

#[derive(Debug, Clone)]
enum StartKind {
    Vad,
    Transcription {
        use_interim: bool,
    },
    External,
    MinWords {
        min_words: usize,
        use_interim: bool,
        bot_speaking: bool,
    },
    WakePhrase {
        phrases: Vec<String>,
        patterns: Vec<Regex>,
        timeout: Duration,
        single_activation: bool,
        deadline: Option<Duration>,
        accumulated: String,
    },
}

/// One typed built-in start strategy. Use an ordered `UserTurnStartStrategies`
/// list to combine policies; a wake gate should precede its fallback detectors.
#[derive(Debug, Clone)]
pub struct UserTurnStartStrategy {
    kind: StartKind,
    enable_interruptions: bool,
}
impl UserTurnStartStrategy {
    pub fn vad() -> Self {
        Self::new(StartKind::Vad)
    }
    pub fn transcription(use_interim: bool) -> Self {
        Self::new(StartKind::Transcription { use_interim })
    }
    pub fn external() -> Self {
        Self::new(StartKind::External)
    }
    /// Counts words in each hypothesis, never across separate transcripts.
    /// When the bot is silent the threshold is one word.
    pub fn min_words(min_words: usize, use_interim: bool) -> Self {
        Self::new(StartKind::MinWords {
            min_words,
            use_interim,
            bot_speaking: false,
        })
    }
    /// Matches final transcript frames, ignoring punctuation, case, and optional
    /// spaces between phrase words. Retains at most 250 Unicode characters.
    /// `single_activation` fixes the keepalive window at detection time; ordinary
    /// mode refreshes it on user/bot activity. Turn stop does not shorten it.
    pub fn wake_phrase(
        phrases: Vec<String>,
        timeout: Duration,
        single_activation: bool,
    ) -> Result<Self, ProcessorError> {
        if phrases.is_empty()
            || phrases.len() > MAX_STRATEGIES
            || timeout.is_zero()
            || timeout > Duration::from_secs(86_400)
            || phrases.iter().any(|p| {
                p.trim_matches(whitespace).is_empty() || p.chars().count() > MAX_WAKE_CHARS
            })
        {
            return Err(ProcessorError::new("wake phrases require 1..64 nonempty phrases of at most 250 characters and a timeout within (0, 86400] seconds"));
        }
        let patterns = phrases
            .iter()
            .map(|p| {
                let words: Vec<_> = p
                    .split(whitespace)
                    .filter(|word| !word.is_empty())
                    .map(|word| regex::escape(&word.chars().map(fold_turkic_i).collect::<String>()))
                    .collect();
                RegexBuilder::new(&format!("(?i)^(?:{})", words.join(r"[\s\x1c-\x1f]*")))
                    .size_limit(MAX_PATTERN_BYTES)
                    .dfa_size_limit(MAX_PATTERN_BYTES)
                    .build()
                    .map_err(|error| ProcessorError::new(format!("invalid wake phrase: {error}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(StartKind::WakePhrase {
            phrases,
            patterns,
            timeout,
            single_activation,
            deadline: None,
            accumulated: String::new(),
        }))
    }
    pub fn with_interruptions(mut self, enabled: bool) -> Self {
        self.enable_interruptions = enabled;
        self
    }
    fn new(kind: StartKind) -> Self {
        Self {
            kind,
            enable_interruptions: true,
        }
    }
    fn reset(&mut self) {
        match &mut self.kind {
            StartKind::MinWords { bot_speaking, .. } => *bot_speaking = false,
            StartKind::WakePhrase {
                deadline,
                accumulated,
                ..
            } => {
                *deadline = None;
                accumulated.clear();
            }
            _ => {}
        }
    }
}

/// Strategies run in order until one starts a turn or a wake gate blocks the
/// remainder. Stopping this chain never consumes the original pipeline frame.
/// Only an external resolver consumes proposed start frames at its owner.
#[derive(Debug, Clone)]
pub struct UserTurnStartStrategies {
    strategies: Vec<UserTurnStartStrategy>,
    now: Duration,
}
impl Default for UserTurnStartStrategies {
    fn default() -> Self {
        Self {
            strategies: vec![
                UserTurnStartStrategy::vad(),
                UserTurnStartStrategy::transcription(true),
            ],
            now: Duration::ZERO,
        }
    }
}
impl UserTurnStartStrategies {
    pub(crate) fn remove_transcription_start(&mut self) {
        self.strategies
            .retain(|strategy| !matches!(strategy.kind, StartKind::Transcription { .. }));
    }
    /// An empty list deliberately disables starts. `None` in `UserTurnConfig`
    /// retains the existing native default controller behavior.
    pub fn new(strategies: Vec<UserTurnStartStrategy>) -> Result<Self, ProcessorError> {
        if strategies.len() > MAX_STRATEGIES {
            return Err(ProcessorError::new(
                "at most 64 user start strategies are supported",
            ));
        }
        let mut phrase_count = 0;
        let mut phrase_bytes = 0;
        for strategy in &strategies {
            if let StartKind::WakePhrase { phrases, .. } = &strategy.kind {
                phrase_count += phrases.len();
                phrase_bytes += phrases.iter().map(String::len).sum::<usize>();
            }
        }
        if phrase_count > MAX_STRATEGIES || phrase_bytes > MAX_WAKE_BYTES {
            return Err(ProcessorError::new(
                "user start chain supports at most 64 total wake phrases and 64000 phrase bytes",
            ));
        }
        let mut result = Self {
            strategies,
            now: Duration::ZERO,
        };
        result.reset();
        Ok(result)
    }
    pub fn resolves_proposed_turn_start_frames(&self) -> bool {
        self.strategies
            .iter()
            .any(|s| matches!(s.kind, StartKind::External))
    }
    pub fn deadline(&self) -> Option<Duration> {
        self.strategies
            .iter()
            .filter_map(|s| match s.kind {
                StartKind::WakePhrase { deadline, .. } => deadline,
                _ => None,
            })
            .min()
    }
    pub fn reset(&mut self) {
        self.now = Duration::ZERO;
        for strategy in &mut self.strategies {
            strategy.reset();
        }
    }
    pub fn cleanup(&mut self) {
        self.reset();
    }
    pub fn handle_user_turn_started(&mut self, now: Duration) {
        self.now = self.now.max(now);
        for strategy in &mut self.strategies {
            match &mut strategy.kind {
                StartKind::MinWords { bot_speaking, .. } => *bot_speaking = false,
                StartKind::WakePhrase {
                    deadline: Some(deadline),
                    timeout,
                    single_activation: false,
                    ..
                } => {
                    *deadline = self.now.saturating_add(*timeout);
                }
                _ => {}
            }
        }
    }
    /// Start policies retain their state at turn stop. In particular, the wake
    /// keepalive and bot-speaking state are turn-start, not turn-stop, semantics.
    pub fn handle_user_turn_stopped(&mut self) {}
    pub fn advance(&mut self, now: Duration) -> Vec<UserStartStrategyEvent> {
        self.now = self.now.max(now);
        let mut events = Vec::new();
        for (index, strategy) in self.strategies.iter_mut().enumerate() {
            if let StartKind::WakePhrase {
                deadline,
                accumulated,
                ..
            } = &mut strategy.kind
            {
                if deadline.is_some_and(|deadline| self.now >= deadline) {
                    *deadline = None;
                    accumulated.clear();
                    events.push(UserStartStrategyEvent::WakePhraseTimeout {
                        strategy_index: index,
                    });
                }
            }
        }
        events
    }
    pub fn process(&mut self, frame: &FrameData, now: Duration) -> UserStartStrategyOutput {
        let mut output = UserStartStrategyOutput {
            events: self.advance(now),
            ..Default::default()
        };
        for (index, strategy) in self.strategies.iter_mut().enumerate() {
            let mut adopt = false;
            let trigger = match &mut strategy.kind {
                StartKind::Vad => matches!(
                    frame,
                    FrameData::VadUserStartedSpeaking(_)
                        | FrameData::VadUserStartedSpeakingDetailed(_)
                ),
                StartKind::Transcription { use_interim } => {
                    transcript(frame, *use_interim).is_some()
                }
                StartKind::External => {
                    adopt = matches!(frame, FrameData::UserStartedSpeaking);
                    adopt || matches!(frame, FrameData::ProposedUserStartedSpeaking)
                }
                StartKind::MinWords {
                    min_words,
                    use_interim,
                    bot_speaking,
                } => {
                    match frame {
                        FrameData::BotStartedSpeaking => *bot_speaking = true,
                        FrameData::BotStoppedSpeaking => *bot_speaking = false,
                        _ => {}
                    }
                    if let Some(text) = transcript(frame, *use_interim) {
                        let trigger = text
                            .split(whitespace)
                            .filter(|word| !word.is_empty())
                            .count()
                            >= if *bot_speaking { *min_words } else { 1 };
                        output.reset_aggregation |= !trigger;
                        trigger
                    } else {
                        false
                    }
                }
                StartKind::WakePhrase {
                    phrases,
                    patterns,
                    timeout,
                    single_activation,
                    deadline,
                    accumulated,
                } => {
                    if deadline.is_some() {
                        if !*single_activation
                            && (transcript(frame, false).is_some()
                                || matches!(
                                    frame,
                                    FrameData::UserSpeaking
                                        | FrameData::BotSpeaking
                                        | FrameData::VadUserStartedSpeaking(_)
                                        | FrameData::VadUserStartedSpeakingDetailed(_)
                                ))
                        {
                            *deadline = Some(self.now.saturating_add(*timeout));
                        }
                        false
                    } else {
                        let Some(text) = transcript(frame, false) else {
                            break;
                        };
                        let tail: Vec<_> = accumulated
                            .chars()
                            .chain(std::iter::once(' '))
                            .chain(text.chars().filter(|c| word_char(*c) || whitespace(*c)))
                            .rev()
                            .take(MAX_WAKE_CHARS)
                            .collect();
                        *accumulated = tail.into_iter().rev().map(fold_turkic_i).collect();
                        if let Some(found) = patterns
                            .iter()
                            .position(|pattern| matches_phrase(accumulated, pattern))
                        {
                            *deadline = Some(self.now.saturating_add(*timeout));
                            accumulated.clear();
                            output
                                .events
                                .push(UserStartStrategyEvent::WakePhraseDetected {
                                    strategy_index: index,
                                    phrase: phrases[found].clone(),
                                });
                            true
                        } else {
                            output.reset_aggregation = true;
                            break;
                        }
                    }
                }
            };
            if trigger {
                output.start = Some(UserTurnStartedParams {
                    enable_interruptions: !adopt && strategy.enable_interruptions,
                    enable_user_speaking_frames: !adopt,
                });
                break;
            }
        }
        output
    }
}
fn transcript(frame: &FrameData, use_interim: bool) -> Option<&str> {
    match frame {
        FrameData::Transcription(t) => Some(&t.text),
        FrameData::TranscriptionDetailed(t) => Some(&t.text.text),
        FrameData::InterimTranscription(t) if use_interim => Some(&t.text),
        FrameData::InterimTranscriptionDetailed(t) if use_interim => Some(&t.text.text),
        _ => None,
    }
}
fn word_char(c: char) -> bool {
    static WORD: OnceLock<Regex> = OnceLock::new();
    WORD.get_or_init(|| Regex::new(r"^[\p{L}\p{N}_]\z").expect("valid Unicode word character"))
        .is_match(c.encode_utf8(&mut [0; 4]))
}
fn whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}
fn fold_turkic_i(c: char) -> char {
    match c {
        '\u{0130}' | '\u{0131}' => 'i',
        _ => c,
    }
}
fn matches_phrase(text: &str, pattern: &Regex) -> bool {
    let mut previous = None;
    for (index, c) in text.char_indices() {
        let boundary = word_char(c) != previous.is_some_and(word_char);
        previous = Some(c);
        if !boundary {
            continue;
        }
        if let Some(found) = pattern.find(&text[index..]) {
            let end = index + found.end();
            if text[..end].chars().next_back().is_some_and(word_char)
                != text[end..].chars().next().is_some_and(word_char)
            {
                return true;
            }
        }
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserMuteStrategy {
    /// Mute during every bot speech, not permanently.
    Always,
    FirstSpeech,
    FunctionCall,
    UntilFirstBotComplete,
}
/// All policies observe every frame and combine with OR, without short-circuiting.
/// Tool tracking shares the native idle controller's bounded identity/finality
/// contract, so intermediate or mismatched results cannot release a mute.
#[derive(Debug, Clone, Default)]
pub struct UserMuteStrategies {
    policies: Vec<UserMuteStrategy>,
    bot_speaking: bool,
    bot_started: bool,
    first_speech_handled: bool,
    opening_failed: bool,
    tools: UserIdleController,
}
impl UserMuteStrategies {
    pub fn new(policies: Vec<UserMuteStrategy>) -> Self {
        let mut unique = Vec::new();
        for policy in policies {
            if !unique.contains(&policy) {
                unique.push(policy);
            }
        }
        Self {
            policies: unique,
            ..Default::default()
        }
    }
    pub fn reset(&mut self) {
        self.bot_speaking = false;
        self.bot_started = false;
        self.first_speech_handled = false;
        self.opening_failed = false;
        self.tools = UserIdleController::default();
    }
    pub fn cleanup(&mut self) {
        self.reset();
    }
    pub fn process(&mut self, frame: &FrameData) -> Result<bool, ProcessorError> {
        // Lifecycle frames are handled by the owning aggregator, after Start
        // delivery and before terminal cleanup, never by policy evaluation.
        if frame.is_start()
            || matches!(
                frame,
                FrameData::End(_)
                    | FrameData::EndDetailed(_)
                    | FrameData::Stop
                    | FrameData::Cancel(_)
                    | FrameData::CancelDetailed(_)
            )
        {
            return Ok(self.is_muted());
        }
        if self.policies.contains(&UserMuteStrategy::FunctionCall)
            && matches!(
                frame,
                FrameData::FunctionCallsStarted(_)
                    | FrameData::FunctionCallsStartedDetailed(_)
                    | FrameData::FunctionCallResult(_)
                    | FrameData::FunctionCallResultDetailed(_)
                    | FrameData::FunctionCallCancel(_)
            )
        {
            self.tools.process(frame, Duration::ZERO)?;
        }
        match frame {
            FrameData::BotStartedSpeaking => {
                self.bot_speaking = true;
                self.bot_started = true;
            }
            FrameData::BotStoppedSpeaking => {
                self.bot_speaking = false;
                self.first_speech_handled = true;
            }
            FrameData::Error(_) | FrameData::ErrorDetailed(_) | FrameData::FatalError(_)
                if !self.bot_started =>
            {
                self.opening_failed = true;
            }
            _ => {}
        }
        Ok(self.is_muted())
    }
    pub fn is_muted(&self) -> bool {
        self.policies.iter().any(|policy| match policy {
            UserMuteStrategy::Always => self.bot_speaking,
            UserMuteStrategy::FirstSpeech => self.bot_speaking && !self.first_speech_handled,
            UserMuteStrategy::FunctionCall => self.tools.function_calls_in_progress(),
            UserMuteStrategy::UntilFirstBotComplete => {
                !self.first_speech_handled && !self.opening_failed
            }
        })
    }
}
