//! Completed-context audio health shared by native TTS adapters.
use pipecat_core::{ErrorFrame, ProcessorContext, ProcessorError, ProcessorHealthGeneration};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    generation: Option<ProcessorHealthGeneration>,
    consecutive: usize,
}

/// Report each completed context without source audio, and disable synthesis
/// after a consecutive streak. Zero disables only the permanent cutoff.
///
/// Call once, in serialization order, for each admitted context that completes,
/// including recoverable errors and timeouts. Do not call for cancellation or a skipped request.
/// `received_audio` means the source adapter observed an audio frame; an explicit
/// empty provider audio payload can count without producing playable PCM.
#[derive(Clone)]
pub struct TtsAudioWatchdog {
    max_consecutive: usize,
    state: Arc<Mutex<State>>,
}

impl Default for TtsAudioWatchdog {
    fn default() -> Self {
        Self::new(3)
    }
}

impl TtsAudioWatchdog {
    pub fn new(max_consecutive: usize) -> Self {
        Self {
            max_consecutive,
            state: Arc::default(),
        }
    }

    /// Returns whether a silent-context error was reported. Health revision and
    /// explicit restoration are both fenced, including restoration concurrent
    /// with the permanent-error decision. An error never asserts audio playout.
    pub async fn complete(
        &self,
        ctx: &ProcessorContext,
        generation: ProcessorHealthGeneration,
        context_id: &str,
        received_audio: bool,
    ) -> Result<bool, ProcessorError> {
        if !ctx.is_current() || !generation.snapshot.is_usable {
            return Ok(false);
        }
        let count = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("TTS audio watchdog lock poisoned"))?;
            let current = ctx.health().generation();
            if current != generation || !current.snapshot.is_usable {
                return Ok(false);
            }
            if state.generation != Some(current) {
                state.generation = Some(current);
                state.consecutive = 0;
            }
            if received_audio {
                state.consecutive = 0;
                return Ok(false);
            }
            state.consecutive = state.consecutive.saturating_add(1);
            state.consecutive
        };
        let permanent = self.max_consecutive != 0 && count >= self.max_consecutive;
        let message = if permanent {
            format!("{count} consecutive TTS contexts completed with no audio")
        } else {
            format!("TTS context {context_id} completed with no audio")
        };
        ctx.push_error_if_health_generation(ErrorFrame::new(message), permanent, generation)
            .await
    }
}
