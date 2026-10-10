//! A captured native job identity consuming authoritative assistant completions.
use crate::AgentError;
use pipecat_turns::{AssistantJobCorrelation, AssistantTurnStopped, LLMAssistantAggregator};
use pipecat_workers::{JobContext, JobResponse};

/// One job's response binding, captured before its inference starts.
///
/// Each job must own a fresh assistant session, including replacement jobs.
/// This adapter does not schedule inference, own another engine, or look up a
/// mutable "current job". The existing NativeWorker handler returns its result.
/// Empty tool-only and interrupted turns leave the job waiting for a later
/// matching nonempty completion. Cancellation drops the handler and this binding.
/// A binding cannot be cloned or reused to issue a second successful response.
pub struct AssistantJobReply {
    job_id: String,
    requester: String,
    worker: String,
    correlation: AssistantJobCorrelation,
    answered: bool,
}
impl AssistantJobReply {
    pub fn capture(
        job: &JobContext,
        assistant: &LLMAssistantAggregator,
        generation: u64,
    ) -> Result<Self, AgentError> {
        if [&job.job_id, &job.requester, &job.worker]
            .iter()
            .any(|id| id.len() > 4096)
        {
            return Err(AgentError::Capacity("assistant job identity".into()));
        }
        let correlation = assistant
            .bind_job_generation(generation)
            .map_err(|_| AgentError::Failed("cannot bind assistant job generation".into()))?;
        Ok(Self {
            job_id: job.job_id.clone(),
            requester: job.requester.clone(),
            worker: job.worker.clone(),
            correlation,
            answered: false,
        })
    }
    pub fn correlation(&self) -> AssistantJobCorrelation {
        self.correlation
    }
    pub fn try_response(
        &mut self,
        job: &JobContext,
        completion: &AssistantTurnStopped,
    ) -> Result<Option<JobResponse>, AgentError> {
        if job.job_id != self.job_id || job.requester != self.requester || job.worker != self.worker
        {
            return Err(AgentError::Failed(
                "assistant completion belongs to another job".into(),
            ));
        }
        if self.answered
            || !self.correlation.matches(completion)
            || completion.interrupted()
            || completion.text().is_empty()
        {
            return Ok(None);
        }
        self.answered = true;
        Ok(Some(JobResponse::completed(completion.text())))
    }
}
