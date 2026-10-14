//! A composition has one graceful commit point, shared by all owned runtimes.
use super::*;
use std::sync::Weak;

#[derive(Clone)]
pub(super) struct LifecycleOwner {
    pub(super) runtime: Weak<Runtime>,
    pub(super) index: usize,
}
pub(super) struct LifecycleGroup {
    pub(super) admission: Arc<Mutex<()>>,
    pub(super) members: Mutex<Vec<Weak<Runtime>>>,
    pub(super) sealed: AtomicBool,
    pub(super) failure: Mutex<Option<PipelineError>>,
    root: u64,
    pub(super) heartbeats: Option<Mutex<crate::heartbeat::HeartbeatState>>,
}
impl LifecycleGroup {
    pub(super) fn new(root: u64, heartbeats: Option<HeartbeatConfig>) -> Self {
        Self {
            admission: Arc::new(Mutex::new(())),
            members: Mutex::new(vec![]),
            sealed: AtomicBool::new(false),
            failure: Mutex::new(None),
            root,
            heartbeats: heartbeats
                .map(crate::heartbeat::HeartbeatState::new)
                .map(Mutex::new),
        }
    }
}
impl Runtime {
    pub(super) fn owned_admission(&self, source: &ProcessorContext) -> bool {
        !self.lifecycle.sealed.load(Ordering::SeqCst)
            && !source.runtime.is_finished()
            && self.lifecycle_owner.as_ref().is_some_and(|owner| {
                owner.index == source.index
                    && owner
                        .runtime
                        .upgrade()
                        .is_some_and(|runtime| Arc::ptr_eq(&runtime, &source.runtime))
            })
    }
    pub(super) fn sealed_control(frame: &Frame) -> bool {
        frame.data.is_cancel()
            || matches!(
                frame.data,
                FrameData::Interruption
                    | FrameData::InterruptionWorker
                    | FrameData::ProcessorResume { .. }
                    | FrameData::ProcessorResumeUrgent { .. }
                    | FrameData::ProcessorResumeTarget(_)
                    | FrameData::ProcessorPauseUrgent { .. }
            )
    }
    // Controls outlive removal from the task queue: result/error publication
    // remains owned until the completed BackgroundTask is dropped.
    fn locally_drained(&self) -> bool {
        self.pending_policy.lock().unwrap().is_none()
            && self.queues.iter().all(|queue| queue.is_idle())
            && self
                .nodes
                .iter()
                .all(|node| node.controls.lock().unwrap().is_empty())
    }
    pub(super) fn enforce_shutdown_deadline(&self) {
        let _admission = self.admission.lock().unwrap();
        // Sealed roots still await descendant cleanup and retain their deadline.
        // A completed state wins over a supervisor tick selected before commit.
        if !self.state.borrow().is_finished()
            && self
                .ending
                .lock()
                .unwrap()
                .is_some_and(|start| start.elapsed() >= self.config.shutdown_timeout)
        {
            self.fail(PipelineError::ShutdownTimeout);
        }
    }
    /// All member admissions, queues and task registrations share this lock.
    /// Closing descendants is deferred until their owners no longer have work
    /// that can re-enter them in either direction.
    pub(super) fn finish_if_drained(&self) {
        if self.id != self.lifecycle.root {
            if self.lifecycle_owner.as_ref().is_none_or(|owner| {
                owner
                    .runtime
                    .upgrade()
                    .is_none_or(|runtime| runtime.state.borrow().is_finished())
            }) {
                self.finish(WorkerState::Cancelled);
            }
            return;
        }
        let _admission = self.admission.lock().unwrap();
        if self.state.borrow().is_finished() {
            return;
        }
        let failure = self.lifecycle.failure.lock().unwrap().clone();
        if let Some(failure) = failure {
            self.fail(failure);
            return;
        }
        let mut children = self.draining_children.lock().unwrap();
        if let Some(descendants) = children.as_ref() {
            if self.locally_drained()
                && descendants
                    .iter()
                    .all(|runtime| runtime.cleanup_complete.load(Ordering::SeqCst))
            {
                self.publish_root_terminal();
                *children = None;
            }
            return;
        }
        let members: Vec<_> = self
            .lifecycle
            .members
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        if members.iter().any(|runtime| {
            !runtime.state.borrow().is_finished()
                && (runtime.graceful_terminal.lock().unwrap().is_none()
                    || !runtime.locally_drained())
        }) {
            return;
        }
        self.lifecycle.sealed.store(true, Ordering::SeqCst);
        for runtime in &members {
            for node in &runtime.nodes {
                node.closed.store(true, Ordering::SeqCst);
            }
        }
        let descendants: Vec<_> = members
            .into_iter()
            .filter(|runtime| runtime.id != self.id)
            .collect();
        for runtime in descendants.iter().rev() {
            if !runtime.state.borrow().is_finished() {
                runtime.publish_graceful_terminal();
            }
        }
        if descendants
            .iter()
            .all(|runtime| runtime.cleanup_complete.load(Ordering::SeqCst))
        {
            self.publish_root_terminal();
        } else {
            *children = Some(descendants);
        }
    }
    fn publish_root_terminal(&self) {
        let failure = self.lifecycle.failure.lock().unwrap().clone();
        if let Some(failure) = failure {
            self.fail(failure);
        } else {
            self.publish_graceful_terminal();
        }
    }
    fn publish_graceful_terminal(&self) {
        let Some((frame, state)) = self.graceful_terminal.lock().unwrap().take() else {
            return;
        };
        let result = self.output.lock().unwrap().as_ref().map(|output| {
            output.try_send(frame).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PipelineError::Backpressure(
                    "worker output queue is full; consume output concurrently".into(),
                ),
                mpsc::error::TrySendError::Closed(_) => PipelineError::Closed,
            })
        });
        match result {
            Some(Ok(())) => self.finish(state),
            Some(Err(error)) => self.fail(error),
            None => self.fail(PipelineError::Closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const DEADLINE: Duration = Duration::from_secs(3);
    #[derive(Default)]
    struct Capture(Mutex<Option<ProcessorContext>>);
    impl Capture {
        fn context(&self) -> ProcessorContext {
            self.0.lock().unwrap().as_ref().unwrap().clone()
        }
    }
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "same-display-name"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.0.lock().unwrap() = Some(ctx.clone());
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.0.lock().unwrap() = Some(ctx.clone());
            ctx.push(frame, direction).await
        }
    }
    async fn recv(worker: &mut PipelineWorker) -> DirectedFrame {
        tokio::time::timeout(DEADLINE, worker.recv())
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn staged_child_accepts_only_fresh_registered_owner_and_keeps_external_cutoff() {
        let owner = Arc::new(Capture::default());
        let impostor = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone(), impostor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        parent
            .sender()
            .send(Frame::text("scope"), Direction::Downstream)
            .await
            .unwrap();
        recv(&mut parent).await;
        let stale = owner.context();
        let mut child = Pipeline::new(vec![Arc::new(IdentityProcessor)])
            .inherit_lifecycle(&stale)
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut child).await;
        child.end().await.unwrap();
        child.sender().wait_terminal_staged().await.unwrap();
        assert!(!child.has_finished());
        assert_eq!(
            child
                .sender()
                .send(Frame::text("external"), Direction::Downstream)
                .await,
            Err(PipelineError::Closed)
        );
        assert_eq!(
            child.sender().try_send_from(
                Frame::text("impostor"),
                Direction::Downstream,
                &impostor.context()
            ),
            Err(PipelineError::Closed)
        );
        assert_eq!(
            child
                .sender()
                .try_send_from(Frame::end(), Direction::Downstream, &stale),
            Err(PipelineError::Closed)
        );
        parent
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        recv(&mut parent).await;
        child
            .sender()
            .try_send_from(Frame::text("stale"), Direction::Downstream, &stale)
            .unwrap();
        parent
            .sender()
            .send(Frame::text("fresh-scope"), Direction::Downstream)
            .await
            .unwrap();
        recv(&mut parent).await;
        let input = Frame::text("fresh");
        let id = input.id;
        child
            .sender()
            .try_send_from(input, Direction::Upstream, &owner.context())
            .unwrap();
        let output = recv(&mut child).await;
        assert_eq!(output.frame.id, id);
        assert_eq!(output.direction, Direction::Upstream);
        parent.end().await.unwrap();
        let (_, report) = tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        let (frames, report) = tokio::time::timeout(DEADLINE, child.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.dropped_frames, 1);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].frame.is_terminal());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn owner_drop_cancels_explicit_child_and_unstarted_builder_is_weak() {
        let owner = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        let weak_parent = Arc::downgrade(&parent.runtime);
        let builder = Pipeline::new(vec![]).inherit_lifecycle(&owner.context());
        let mut child = Pipeline::new(vec![])
            .inherit_lifecycle(&owner.context())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut child).await;
        let weak_child = Arc::downgrade(&child.runtime);
        drop(parent);
        owner.0.lock().unwrap().take();
        let (_, report) = tokio::time::timeout(DEADLINE, child.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Cancelled);
        tokio::time::timeout(DEADLINE, async {
            while weak_parent.upgrade().is_some() || weak_child.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            builder.start(PipelineConfig::default()).await,
            Err(PipelineError::Closed)
        ));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_dynamic_children_are_pruned_from_bounded_group() {
        let owner = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        for _ in 0..24 {
            let mut child = Pipeline::new(vec![])
                .inherit_lifecycle(&owner.context())
                .start(PipelineConfig::default())
                .await
                .unwrap();
            recv(&mut child).await;
            child.cancel("completed dynamic child").await.unwrap();
            child.collect().await.unwrap();
            assert!(parent.runtime.lifecycle.members.lock().unwrap().len() <= 2);
        }
        parent.end().await.unwrap();
        tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .unwrap();
    }
    struct SetupFailure(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait]
    impl FrameProcessor for SetupFailure {
        fn name(&self) -> &str {
            "dynamic-child-setup-failure"
        }
        async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
            Err(ProcessorError::fatal("dynamic child setup failed"))
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dynamic_child_setup_failure_fails_owner_and_releases_cleanup() {
        let owner = Arc::new(Capture::default());
        let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        let child = Pipeline::new(vec![Arc::new(SetupFailure(cleaned.clone()))])
            .inherit_lifecycle(&owner.context())
            .start(PipelineConfig::default())
            .await;
        if let Ok(child) = child {
            assert!(tokio::time::timeout(DEADLINE, child.collect())
                .await
                .unwrap()
                .is_err());
        }
        assert!(tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .is_err());
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_composition_membership_limit_is_explicit_backpressure() {
        let owner = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        let member = Arc::downgrade(&parent.runtime);
        *parent.runtime.lifecycle.members.lock().unwrap() = vec![member; 4096];
        let result = Pipeline::new(vec![])
            .inherit_lifecycle(&owner.context())
            .start(PipelineConfig::default())
            .await;
        assert!(
            matches!(result,Err(PipelineError::Backpressure(message)) if message=="composition runtime limit reached")
        );
        *parent.runtime.lifecycle.members.lock().unwrap() = vec![Arc::downgrade(&parent.runtime)];
        parent.end().await.unwrap();
        tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn taken_background_result_owns_drain_until_error_publication_finishes() {
        let owner = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let task_release = release.clone();
        owner
            .context()
            .spawn("held-completion-result", false, async move {
                task_release.acquire().await.unwrap().forget();
                Err(ProcessorError::new(
                    "background result published before End",
                ))
            })
            .unwrap();
        // Hold exactly the ownership the supervisor retains while publishing a
        // completed task's result, after removing it from the background queue.
        let task = parent.runtime.nodes[0]
            .background
            .lock()
            .unwrap()
            .pop()
            .unwrap();
        parent.end().await.unwrap();
        parent.sender().wait_terminal_staged().await.unwrap();
        parent.runtime.finish_if_drained();
        assert!(
            !parent.has_finished(),
            "task result publication is still owned"
        );
        release.add_permits(1);
        complete_background(task, &owner.context()).await;
        let (frames, report) = tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert!(frames[0].frame.data.as_error_frame().is_some());
        assert!(frames[1].frame.is_terminal());
        assert_eq!(report.errors.len(), 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn committed_terminal_cannot_be_replaced_by_late_deadline_tick() {
        for state in [
            WorkerState::Ended,
            WorkerState::Stopped,
            WorkerState::Cancelled,
        ] {
            let mut worker = Pipeline::new(vec![])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            recv(&mut worker).await;
            *worker.runtime.ending.lock().unwrap() = Some(Instant::now() - Duration::from_secs(60));
            {
                let _admission = worker.runtime.admission.lock().unwrap();
                worker.runtime.finish(state);
            }
            worker.runtime.enforce_shutdown_deadline();
            assert_eq!(*worker.runtime.state.borrow(), state);
            assert!(worker.runtime.failure.lock().unwrap().is_none());
            let (_, report) = tokio::time::timeout(DEADLINE, worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(report.state, state);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_and_sealed_roots_both_retain_the_shutdown_deadline() {
        for sealed in [false, true] {
            let mut worker = Pipeline::new(vec![])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            recv(&mut worker).await;
            worker
                .runtime
                .lifecycle
                .sealed
                .store(sealed, Ordering::SeqCst);
            *worker.runtime.ending.lock().unwrap() = Some(Instant::now() - Duration::from_secs(60));
            worker.runtime.enforce_shutdown_deadline();
            assert_eq!(*worker.runtime.state.borrow(), WorkerState::Failed);
            assert!(matches!(
                tokio::time::timeout(DEADLINE, worker.collect())
                    .await
                    .unwrap(),
                Err(PipelineError::ShutdownTimeout)
            ));
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_children_release_while_root_sender_is_retained() {
        let owner = Arc::new(Capture::default());
        let mut parent = Pipeline::new(vec![owner.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut parent).await;
        let retained_sender = parent.sender();
        let mut child = Pipeline::new(vec![])
            .inherit_lifecycle(&owner.context())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv(&mut child).await;
        let weak_child = Arc::downgrade(&child.runtime);
        child.end().await.unwrap();
        child.sender().wait_terminal_staged().await.unwrap();
        parent.end().await.unwrap();
        tokio::time::timeout(DEADLINE, parent.collect())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(DEADLINE, child.collect())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            while weak_child.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed descendant retained by finished root sender");
        assert_eq!(retained_sender.state(), WorkerState::Ended);
    }
}
