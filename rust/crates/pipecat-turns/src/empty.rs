//! Bounded recovery for voice turns that finish without recognized text.

/// The source cascade prompt asks about missing speech without inventing it.
pub const DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT: &str = concat!(
    "The user may have said something while you were responding, but it was not ",
    "recognized. Your response was cut off there, and the conversation only includes ",
    "the part they heard, which may be none of it. Briefly ask them to repeat what they said, and ",
    "repeat any question of yours they may have missed."
);

/// Recovery appends a developer message and requests one response. A missing
/// or empty prompt leaves that kind of turn unanswered. Only committed final
/// transcript content resets the consecutive-recovery budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyUserTurnConfig {
    pub interrupted_prompt: Option<String>,
    pub idle_prompt: Option<String>,
    /// Zero disables recovery; skipped turns never consume the budget.
    pub max_consecutive_recoveries: usize,
}

impl Default for EmptyUserTurnConfig {
    fn default() -> Self {
        Self {
            interrupted_prompt: Some(DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT.into()),
            idle_prompt: None,
            max_consecutive_recoveries: 1,
        }
    }
}
