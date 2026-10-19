//! Recoverable owner for one opt-in initialization run. The socket JoinHandle
//! stays in `GeminiSession`; cancellation of any waiter never takes it away.
use crate::{GeminiSender, GeminiSession, SelectedJoinObserver};
use pipecat_core::{ProcessorContext, ProcessorError};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{watch, Mutex as AsyncMutex},
    time::Instant,
};

pub(crate) struct SelectedSessionOwner {
    pub(crate) run_id: u64,
    setup: Option<(u64, usize)>,
    pub(crate) session: AsyncMutex<GeminiSession>,
    pub(crate) sender: GeminiSender,
    pub(crate) joined: SelectedJoinObserver,
    accepting: AtomicBool,
    receiver: Mutex<Option<(ProcessorContext, u64)>>,
    terminal: Mutex<Terminal>,
    completion: watch::Sender<Option<Result<(), String>>>,
    budget: Duration,
}
#[derive(Default)]
struct Terminal {
    deadline: Option<Instant>,
    failure: Option<String>,
    result: Option<Result<(), String>>,
}
fn failure(message: impl Into<String>) -> ProcessorError {
    ProcessorError::fatal(message)
}
pub(crate) fn combine(
    first: Result<(), ProcessorError>,
    second: Result<(), ProcessorError>,
) -> Result<(), ProcessorError> {
    match (first, second) {
        (Err(mut first), Err(second)) => {
            if first.message != second.message {
                first
                    .message
                    .push_str(&format!("; cleanup: {}", second.message));
            }
            Err(first)
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
impl SelectedSessionOwner {
    pub(crate) fn new(
        run_id: u64,
        session: GeminiSession,
        budget: Duration,
        setup: Option<&ProcessorContext>,
    ) -> Arc<Self> {
        let sender = session.sender();
        let joined = session.join_observer();
        let (completion, _) = watch::channel(None);
        Arc::new(Self {
            run_id,
            setup: setup.map(|ctx| {
                let identity = ctx.processor_identity();
                (identity.runtime_id, identity.index)
            }),
            session: AsyncMutex::new(session),
            sender,
            joined,
            accepting: AtomicBool::new(true),
            receiver: Mutex::new(None),
            terminal: Mutex::new(Terminal::default()),
            completion,
            budget,
        })
    }
    pub(crate) fn belongs_to(&self, ctx: &ProcessorContext) -> bool {
        let identity = ctx.processor_identity();
        self.setup
            .is_none_or(|setup| setup == (identity.runtime_id, identity.index))
    }
    pub(crate) fn accepts_input(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
    pub(crate) fn close_input(&self) -> Result<(), ProcessorError> {
        self.accepting.store(false, Ordering::Release);
        let result = self
            .sender
            .close_selected_admission(self.run_id)
            .map_err(super::processor_error);
        self.record_result(&result);
        result
    }
    pub(crate) fn bind_receiver(&self, ctx: ProcessorContext, id: u64) {
        *self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((ctx, id));
    }
    pub(crate) fn record_failure(&self, message: impl Into<String>) {
        let mut terminal = self
            .terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let message = message.into();
        if let Some(existing) = &mut terminal.failure {
            if !existing.contains(&message) {
                existing.push_str("; ");
                existing.push_str(&message);
            }
        } else {
            terminal.failure = Some(message);
        }
        if terminal.result.is_some() {
            terminal.result = Some(Err(terminal.failure.clone().expect("recorded failure")));
        }
    }
    /// Persist a known outcome before any cancellable cleanup await. This does
    /// not publish completion or claim that the actor has actually joined.
    pub(crate) fn record_result(&self, result: &Result<(), ProcessorError>) {
        if let Err(error) = result {
            self.record_failure(error.message.clone());
        }
    }
    pub(crate) fn complete(&self, result: &Result<(), ProcessorError>) {
        self.record_result(result);
        self.completion.send_replace(Some(
            result.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
        ));
    }
    pub(crate) fn terminal_deadline(&self) -> Result<Instant, ProcessorError> {
        let mut terminal = self
            .terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(deadline) = terminal.deadline {
            return Ok(deadline);
        }
        let deadline = Instant::now()
            .checked_add(self.budget)
            .ok_or_else(|| failure("Gemini selected terminal deadline overflow"))?;
        terminal.deadline = Some(deadline);
        Ok(deadline)
    }
    fn cancel_receiver(&self) {
        self.sender.cancel();
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some((ctx, id)) = receiver {
            ctx.cancel_task(id);
        }
    }
    async fn completion(&self) -> Result<(), ProcessorError> {
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result.map_err(failure);
            }
            completion
                .changed()
                .await
                .map_err(|_| failure("Gemini receive owner disappeared"))?;
        }
    }
    /// One fixed receive-drain budget per run, then cancellation of the exact
    /// registered receive wrapper. The session has its own fixed cooperative
    /// shutdown/write deadline and awaits that same handle after forced abort.
    /// No deadline is refreshed by reentry or an outer cancelled future.
    pub(crate) async fn settle(&self, graceful: bool) -> Result<(), ProcessorError> {
        if let Err(error) = self.close_input() {
            self.record_failure(error.message);
        }
        let settled = self
            .terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .result
            .clone();
        if let Some(result) = settled {
            return result.map_err(failure);
        }
        let deadline = self.terminal_deadline()?;
        if graceful {
            if !self.joined.joined() {
                match tokio::time::timeout_at(deadline, self.sender.finish()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => self.record_failure(error.to_string()),
                    Err(_) => {
                        self.record_failure("Gemini selected finish admission deadline exceeded")
                    }
                }
            }
            let failed = self
                .terminal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .failure
                .is_some();
            if !failed {
                match tokio::time::timeout_at(deadline, self.completion()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => self.record_failure(error.message),
                    Err(_) => {
                        self.record_failure("Gemini selected receive drain deadline exceeded")
                    }
                }
            }
        }
        // Abort is only a request. Retaining and borrowing the session below is
        // what actually recovers/awaits its actor; core separately joins wrapper.
        if !graceful || self.completion.borrow().is_none() {
            self.cancel_receiver();
        }
        let mut session = self.session.lock().await;
        if let Err(error) = session.selected_shutdown().await {
            self.record_failure(error.to_string());
        }
        let mut terminal = self.terminal.lock().unwrap_or_else(|e| e.into_inner());
        let result = terminal.failure.clone().map_or(Ok(()), Err);
        // The observer is only set by an actual JoinHandle await. A future may
        // be cancelled before this point without publishing a fictitious join.
        if self.joined.joined() {
            terminal.result = Some(result.clone());
        }
        drop(terminal);
        self.completion.send_replace(Some(result.clone()));
        result.map_err(failure)
    }
}
