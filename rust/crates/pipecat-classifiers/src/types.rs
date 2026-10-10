use crate::CancellationToken;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio::time::Instant;

/// Only static categories are retained: provider errors, replies, state, and credentials
/// are never copied into diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClassifierError {
    #[error("invalid classifier configuration: {0}")]
    Configuration(&'static str),
    #[error("classifier resource limit: {0}")]
    Limit(&'static str),
    #[error("invalid question: {0}")]
    Question(&'static str),
    #[error("invalid classifier response: {0}")]
    Response(&'static str),
    #[error("classifier inference failed")]
    Inference,
    #[error("classifier transport failed")]
    Transport,
    #[error("classifier request rejected: HTTP {0}")]
    Http(u16),
    #[error("classifier is busy: HTTP {status} after {attempts} attempts")]
    Busy { status: u16, attempts: u32 },
    #[error("classifier deadline exceeded")]
    Deadline,
    #[error("classifier request cancelled")]
    Cancelled,
    #[error("classifier client is closed")]
    Closed,
    #[error("classifier usage overflow")]
    UsageOverflow,
    #[error("classifier state lock poisoned")]
    State,
}
pub type Result<T> = std::result::Result<T, ClassifierError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YesNoQuestion {
    pub instructions: Value,
    pub yes: Option<Value>,
    pub no: Option<Value>,
}
impl YesNoQuestion {
    pub fn new(instructions: impl Into<Value>) -> Self {
        Self {
            instructions: instructions.into(),
            yes: None,
            no: None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceQuestion {
    pub instructions: Value,
    pub options: BTreeMap<String, Option<Value>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreQuestion {
    pub instructions: Value,
    /// Ordered from lowest to highest. At least two levels are required.
    pub levels: Vec<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClassifierQuestion {
    YesNo(YesNoQuestion),
    Choice(ChoiceQuestion),
    Score(ScoreQuestion),
}
impl From<YesNoQuestion> for ClassifierQuestion {
    fn from(q: YesNoQuestion) -> Self {
        Self::YesNo(q)
    }
}
impl From<ChoiceQuestion> for ClassifierQuestion {
    fn from(q: ChoiceQuestion) -> Self {
        Self::Choice(q)
    }
}
impl From<ScoreQuestion> for ClassifierQuestion {
    fn from(q: ScoreQuestion) -> Self {
        Self::Score(q)
    }
}
pub type Questions = BTreeMap<String, ClassifierQuestion>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YesNoResult {
    pub probability: f64,
}
impl YesNoResult {
    pub fn is_yes(&self) -> bool {
        self.probability >= 0.5
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceResult {
    pub choice: String,
    pub probabilities: BTreeMap<String, f64>,
    pub confidence: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreLevel {
    pub level: Value,
    pub probability: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreResult {
    pub score: f64,
    pub levels: Vec<ScoreLevel>,
    pub confidence: f64,
}
impl ScoreResult {
    pub fn probability(&self, level: &Value) -> Option<f64> {
        self.levels
            .iter()
            .find(|item| &item.level == level)
            .map(|item| item.probability)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClassifierResult {
    YesNo(YesNoResult),
    Choice(ChoiceResult),
    Score(ScoreResult),
}
/// Provenance describes the source of numbers, never measured calibration quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbabilityProvenance {
    LlmEstimate,
    JevProvider,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JevUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}
impl JevUsage {
    pub fn total_tokens(&self) -> Result<u64> {
        self.input_tokens
            .checked_add(self.output_tokens)
            .ok_or(ClassifierError::UsageOverflow)
    }
    pub(crate) fn checked_add(self, other: Self) -> Result<Self> {
        let next = Self {
            input_tokens: self
                .input_tokens
                .checked_add(other.input_tokens)
                .ok_or(ClassifierError::UsageOverflow)?,
            output_tokens: self
                .output_tokens
                .checked_add(other.output_tokens)
                .ok_or(ClassifierError::UsageOverflow)?,
        };
        next.total_tokens()?;
        Ok(next)
    }
}
/// Metrics are delivered inline with results. No callbacks, detached tasks, or
/// unbounded metric queue can delay cancellation or retain caller state.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassificationMetrics {
    pub classifier: String,
    pub model: Option<String>,
    pub elapsed: Duration,
    pub usage: Option<JevUsage>,
    pub provenance: ProbabilityProvenance,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ClassificationBatch<T = ClassifierResult> {
    pub answers: BTreeMap<String, T>,
    pub metrics: ClassificationMetrics,
}
impl<T> ClassificationBatch<T> {
    fn map<U>(self, mut f: impl FnMut(T) -> Result<U>) -> Result<ClassificationBatch<U>> {
        Ok(ClassificationBatch {
            answers: self
                .answers
                .into_iter()
                .map(|(name, result)| Ok((name, f(result)?)))
                .collect::<Result<_>>()?,
            metrics: self.metrics,
        })
    }
}
#[derive(Debug, Clone, Default)]
pub struct CallOptions {
    pub cancellation: CancellationToken,
    /// Optional earlier absolute deadline. It never extends the configured timeout.
    pub deadline: Option<Instant>,
}
impl CallOptions {
    pub(crate) fn deadline(&self, timeout: Duration) -> Instant {
        let local = Instant::now() + timeout;
        self.deadline.map_or(local, |deadline| deadline.min(local))
    }
    pub(crate) fn check(&self, deadline: Instant) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(ClassifierError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ClassifierError::Deadline);
        }
        Ok(())
    }
    pub(crate) async fn run<T>(
        &self,
        deadline: Instant,
        closed: &CancellationToken,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let result = tokio::select! {
            biased;
            _ = closed.cancelled() => Err(ClassifierError::Closed),
            _ = self.cancellation.cancelled() => Err(ClassifierError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => Err(ClassifierError::Deadline),
            result = future => result,
        };
        // A future can finish synchronously after another select branch was
        // polled. Fence publication again so cancellation wins that race too.
        if closed.is_cancelled() {
            return Err(ClassifierError::Closed);
        }
        self.check(deadline)?;
        result
    }
}
/// A classifier owns no conversation state and performs one batch inference.
#[async_trait]
pub trait Classifier: Send + Sync {
    async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        options: CallOptions,
    ) -> Result<ClassificationBatch>;
    async fn yes_no(
        &self,
        state: &Value,
        questions: BTreeMap<String, YesNoQuestion>,
        options: CallOptions,
    ) -> Result<ClassificationBatch<YesNoResult>> {
        if questions.len() > 1024 {
            return Err(ClassifierError::Limit("question count"));
        }
        let questions = questions
            .into_iter()
            .map(|(name, q)| (name, q.into()))
            .collect();
        self.ask(state, &questions, options)
            .await?
            .map(|r| match r {
                ClassifierResult::YesNo(r) => Ok(r),
                _ => Err(ClassifierError::Response("wrong result type")),
            })
    }
    async fn choice(
        &self,
        state: &Value,
        questions: BTreeMap<String, ChoiceQuestion>,
        options: CallOptions,
    ) -> Result<ClassificationBatch<ChoiceResult>> {
        if questions.len() > 1024 {
            return Err(ClassifierError::Limit("question count"));
        }
        let questions = questions
            .into_iter()
            .map(|(name, q)| (name, q.into()))
            .collect();
        self.ask(state, &questions, options)
            .await?
            .map(|r| match r {
                ClassifierResult::Choice(r) => Ok(r),
                _ => Err(ClassifierError::Response("wrong result type")),
            })
    }
    async fn score(
        &self,
        state: &Value,
        questions: BTreeMap<String, ScoreQuestion>,
        options: CallOptions,
    ) -> Result<ClassificationBatch<ScoreResult>> {
        if questions.len() > 1024 {
            return Err(ClassifierError::Limit("question count"));
        }
        let questions = questions
            .into_iter()
            .map(|(name, q)| (name, q.into()))
            .collect();
        self.ask(state, &questions, options)
            .await?
            .map(|r| match r {
                ClassifierResult::Score(r) => Ok(r),
                _ => Err(ClassifierError::Response("wrong result type")),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_during_ready_future_prevents_publication() {
        let options = CallOptions::default();
        let closed = CancellationToken::new();
        let deadline = options.deadline(Duration::from_secs(1));
        assert_eq!(
            options
                .run(deadline, &closed, async {
                    closed.cancel();
                    Ok(())
                })
                .await,
            Err(ClassifierError::Closed)
        );
        let options = CallOptions::default();
        let closed = CancellationToken::new();
        assert_eq!(
            options
                .run(deadline, &closed, async {
                    options.cancellation.cancel();
                    Ok(())
                })
                .await,
            Err(ClassifierError::Cancelled)
        );
    }
}
