// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::*;
use pipecat_pipeline::*;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Recorder(Mutex<Vec<ObservationEvent>>);
#[async_trait]
impl PipelineObserver for Recorder {
    async fn on_event(&self, event: ObservationEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_observers_cover_nested_parallel_runtimes_until_root_end() {
    for sync in [false, true] {
        let inner: Arc<dyn FrameProcessor> = Arc::new(
            ParallelPipeline::new(vec![
                vec![Arc::new(IdentityProcessor)],
                vec![Arc::new(IdentityProcessor)],
            ])
            .unwrap(),
        );
        let branches = vec![
            vec![inner],
            vec![Arc::new(IdentityProcessor) as Arc<dyn FrameProcessor>],
        ];
        let outer: Arc<dyn FrameProcessor> = if sync {
            Arc::new(SyncParallelPipeline::new(branches).unwrap())
        } else {
            Arc::new(ParallelPipeline::new(branches).unwrap())
        };
        let recorder = Arc::new(Recorder::default());
        let mut pipeline = Pipeline::new(vec![outer, Arc::new(IdentityProcessor)]);
        let handle = pipeline.add_observer(
            recorder.clone(),
            ObserverOptions {
                capacity: 4096,
                drain_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        );
        let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .data
            .is_start());
        let input = Frame::text("private transcript must never enter observation");
        let id = input.id;
        worker
            .sender()
            .send(input, Direction::Downstream)
            .await
            .unwrap();
        let output = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.frame.id, id);
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        let events = recorder.0.lock().unwrap();
        let runtimes: BTreeSet<_> = events
            .iter()
            .filter_map(|event| match event.observation.as_ref() {
                PipelineObservation::RuntimeStarted { runtime_id, .. } => Some(*runtime_id),
                _ => None,
            })
            .collect();
        assert_eq!(runtimes.len(), 5);
        let text_runtimes: BTreeSet<_> = events
            .iter()
            .filter_map(|event| match event.observation.as_ref() {
                PipelineObservation::Process {
                    processor, frame, ..
                } if frame.id == id => Some(processor.runtime_id),
                _ => None,
            })
            .collect();
        assert_eq!(runtimes, text_runtimes);
        assert!(!format!("{events:?}").contains("private transcript"));
        let stats = handle.stats();
        assert_eq!(stats.dropped, 0);
        assert_eq!(stats.abandoned, 0);
        assert_eq!(stats.failures, 0);
    }
}
