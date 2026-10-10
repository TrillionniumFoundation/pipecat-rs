use crate::{ActionConfig, ActionHandler, FlowError, FlowHandle};
use pipecat_core::{Frame, FrameData, TtsSpeakFrame};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

/// Sequential action executor. Drain acknowledgements establish ordering at
/// the sink boundary; enqueuing TTS is never treated as completed speech.
#[derive(Default)]
pub struct ActionManager {
    handlers: Mutex<BTreeMap<String, ActionHandler>>,
    deferred: Mutex<Vec<ActionConfig>>,
    ongoing: AtomicUsize,
}
struct Ongoing<'a>(&'a AtomicUsize);
impl Drop for Ongoing<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ActionManager {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_action(
        &self,
        name: impl Into<String>,
        handler: ActionHandler,
    ) -> Result<(), FlowError> {
        let name = name.into();
        if matches!(name.as_str(), "tts_say" | "end_conversation" | "function") {
            return Err(FlowError::Config(
                "built-in action types cannot be overridden".into(),
            ));
        }
        self.handlers
            .lock()
            .map_err(|_| FlowError::Action("handler lock poisoned".into()))?
            .insert(name, handler);
        Ok(())
    }
    pub fn ongoing_actions(&self) -> usize {
        self.ongoing.load(Ordering::SeqCst)
    }
    pub fn clear_deferred_post_actions(&self) -> Result<(), FlowError> {
        self.deferred
            .lock()
            .map_err(|_| FlowError::Action("deferred lock poisoned".into()))?
            .clear();
        Ok(())
    }
    pub fn schedule_deferred_post_actions(
        &self,
        actions: Vec<ActionConfig>,
    ) -> Result<(), FlowError> {
        *self
            .deferred
            .lock()
            .map_err(|_| FlowError::Action("deferred lock poisoned".into()))? = actions;
        Ok(())
    }
    pub(crate) fn take_deferred(&self) -> Result<Vec<ActionConfig>, FlowError> {
        if self.ongoing_actions() != 0 {
            return Ok(vec![]);
        }
        Ok(std::mem::take(&mut *self.deferred.lock().map_err(
            |_| FlowError::Action("deferred lock poisoned".into()),
        )?))
    }
    pub(crate) fn validate(
        &self,
        actions: &[ActionConfig],
        handle: &FlowHandle,
    ) -> Result<(), FlowError> {
        for action in actions {
            if matches!(action.action_type.as_str(), "tts_say" | "end_conversation") {
                continue;
            }
            self.resolve(action, handle)?;
        }
        Ok(())
    }
    pub(crate) fn prepare(
        &self,
        actions: &[ActionConfig],
        handle: &FlowHandle,
    ) -> Result<(), FlowError> {
        for action in actions {
            if matches!(
                action.action_type.as_str(),
                "tts_say" | "end_conversation" | "function"
            ) {
                continue;
            }
            if let Some(name) = &action.handler {
                let handler = handle
                    .action_handler(name)
                    .ok_or_else(|| FlowError::Action(format!("missing action handler '{name}'")))?;
                self.handlers
                    .lock()
                    .map_err(|_| FlowError::Action("handler lock poisoned".into()))?
                    .entry(action.action_type.clone())
                    .or_insert(handler);
            }
            self.resolve(action, handle)?;
        }
        self.validate(actions, handle)
    }
    fn resolve(
        &self,
        action: &ActionConfig,
        handle: &FlowHandle,
    ) -> Result<ActionHandler, FlowError> {
        if action.action_type != "function" {
            if let Some(handler) = self
                .handlers
                .lock()
                .map_err(|_| FlowError::Action("handler lock poisoned".into()))?
                .get(&action.action_type)
                .cloned()
            {
                return Ok(handler);
            }
        }
        if let Some(name) = &action.handler {
            return handle
                .action_handler(name)
                .ok_or_else(|| FlowError::Action(format!("missing action handler '{name}'")));
        }
        self.handlers
            .lock()
            .map_err(|_| FlowError::Action("handler lock poisoned".into()))?
            .get(&action.action_type)
            .cloned()
            .ok_or_else(|| {
                FlowError::Action(format!("unregistered action type '{}'", action.action_type))
            })
    }
    pub async fn execute_actions(
        &self,
        actions: &[ActionConfig],
        handle: FlowHandle,
    ) -> Result<(), FlowError> {
        self.execute(actions, handle, false).await
    }
    pub(crate) async fn execute_post_actions(
        &self,
        actions: &[ActionConfig],
        handle: FlowHandle,
    ) -> Result<(), FlowError> {
        self.execute(actions, handle, true).await
    }
    async fn execute(
        &self,
        actions: &[ActionConfig],
        handle: FlowHandle,
        mut response_pending: bool,
    ) -> Result<(), FlowError> {
        self.validate(actions, &handle)?;
        if actions.is_empty() {
            return Ok(());
        }
        self.ongoing.fetch_add(1, Ordering::SeqCst);
        let _ongoing = Ongoing(&self.ongoing);
        let mut queued = false;
        for action in actions {
            handle.check_active()?;
            // Native inference is asynchronous. Pipeline effects queued as
            // post-actions must not overtake the response they follow.
            let drained_response = response_pending
                && matches!(
                    action.action_type.as_str(),
                    "tts_say" | "end_conversation" | "function"
                );
            if drained_response {
                handle.drain().await?;
                response_pending = false;
            }
            match action.action_type.as_str() {
                "tts_say" => {
                    if let Some(text) = action.text().filter(|s| !s.is_empty()) {
                        handle
                            .queue_frame(Frame::new(FrameData::TtsSpeak(TtsSpeakFrame {
                                text: text.into(),
                                append_to_context: action.append_text_to_context()?,
                            })))
                            .await?;
                        queued = true;
                    }
                }
                "end_conversation" => {
                    if let Some(text) = action.text().filter(|s| !s.is_empty()) {
                        handle
                            .queue_frame(Frame::new(FrameData::TtsSpeak(TtsSpeakFrame {
                                text: text.into(),
                                append_to_context: action.append_text_to_context()?,
                            })))
                            .await?;
                    }
                    handle.end().await?;
                    return Ok(());
                }
                kind => {
                    // A function action always executes at the downstream
                    // boundary, including when it is the first post-action.
                    if queued || (kind == "function" && !drained_response) {
                        handle.drain().await?;
                        queued = false;
                    }
                    let handler = self.resolve(action, &handle)?;
                    handle
                        .cancellable(handler.call(action.clone(), handle.clone()))
                        .await?;
                }
            }
        }
        if queued {
            handle.drain().await?;
        }
        Ok(())
    }
}
