//! A bounded local stream for measured metrics; no external collector or I/O.
use crate::{
    MeasureMetric, StartupMeasurement, StartupTiming, UserBotLatency, UserBotLatencyReport,
};
use pipecat_core::{async_trait, Metric, ObservationEvent, PipelineObservation, PipelineObserver};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Measurement {
    Startup(StartupMeasurement),
    UserBotLatency(UserBotLatencyReport),
    /// Numeric MetricsFrame data already reported by a service. No service
    /// interval is inferred from a process callback or a frame type.
    ReportedService {
        timestamp_ns: u64,
        metric: Metric,
    },
    /// Rich service timer output explicitly provided at its measured boundary.
    Service {
        timestamp_ns: u64,
        metric: MeasureMetric,
    },
    ObservationLoss {
        dropped_total: u64,
    },
    TruncatedMetrics {
        frame_id: u64,
        omitted: usize,
    },
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsStreamStats {
    pub published: u64,
    pub dropped: u64,
}
#[derive(Default)]
struct Counters {
    published: AtomicU64,
    dropped: AtomicU64,
}
#[derive(Clone)]
pub struct MetricsPublisher {
    sender: mpsc::Sender<Measurement>,
    counters: Arc<Counters>,
}
pub struct MetricsStream {
    receiver: mpsc::Receiver<Measurement>,
    counters: Arc<Counters>,
}
impl MetricsPublisher {
    /// False means a full/closed stream; producers never await backpressure.
    pub fn publish(&self, measurement: Measurement) -> bool {
        if self.sender.try_send(measurement).is_ok() {
            self.counters.published.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
    pub fn service(&self, timestamp_ns: u64, metric: MeasureMetric) -> bool {
        self.publish(Measurement::Service {
            timestamp_ns,
            metric,
        })
    }
    pub fn stats(&self) -> MetricsStreamStats {
        stats(&self.counters)
    }
}
impl MetricsStream {
    pub async fn recv(&mut self) -> Option<Measurement> {
        self.receiver.recv().await
    }
    pub fn try_recv(&mut self) -> Result<Measurement, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
    /// Stop accepting new records and drain records already accepted.
    pub fn close(&mut self) {
        self.receiver.close();
    }
    pub fn stats(&self) -> MetricsStreamStats {
        stats(&self.counters)
    }
}
fn stats(counters: &Counters) -> MetricsStreamStats {
    MetricsStreamStats {
        published: counters.published.load(Ordering::Relaxed),
        dropped: counters.dropped.load(Ordering::Relaxed),
    }
}
/// Capacity must be positive. No task is created by the stream itself.
pub fn metrics_stream(capacity: usize) -> Result<(MetricsPublisher, MetricsStream), &'static str> {
    if capacity == 0 {
        return Err("metrics stream capacity must be positive");
    }
    let (sender, receiver) = mpsc::channel(capacity);
    let counters = Arc::new(Counters::default());
    Ok((
        MetricsPublisher {
            sender,
            counters: counters.clone(),
        },
        MetricsStream { receiver, counters },
    ))
}
#[derive(Default)]
struct Reducers {
    startup: StartupTiming,
    latency: UserBotLatency,
    dropped: u64,
}
/// Register with observe_every_push=true: startup timing needs every Start hop.
/// Service/turn events use first_push to avoid counting forwarded frames again.
/// Reusing one instance across unrelated pipelines mixes timelines; allocate one
/// observer per root pipeline, which its nested runtimes may inherit.
pub struct MetricsObserver {
    reducers: Mutex<Reducers>,
    publisher: MetricsPublisher,
}
impl MetricsObserver {
    pub fn new(publisher: MetricsPublisher) -> Self {
        Self {
            reducers: Mutex::new(Reducers::default()),
            publisher,
        }
    }
}
#[async_trait]
impl PipelineObserver for MetricsObserver {
    async fn on_event(&self, event: ObservationEvent) {
        let mut reducers = self.reducers.lock().unwrap();
        if event.dropped_before != reducers.dropped {
            reducers.startup.reset();
            reducers.latency.reset();
            reducers.dropped = event.dropped_before;
            self.publisher.publish(Measurement::ObservationLoss {
                dropped_total: event.dropped_before,
            });
        }
        if let Some(report) = reducers.startup.observe(&event.observation) {
            self.publisher.publish(Measurement::Startup(report));
        }
        if let Some(report) = reducers.latency.observe(&event.observation) {
            self.publisher.publish(Measurement::UserBotLatency(report));
        }
        if let PipelineObservation::Push {
            frame,
            first_push: true,
            timestamp_ns,
            ..
        } = &*event.observation
        {
            for metric in &frame.metrics {
                if valid_metric(metric) {
                    self.publisher.publish(Measurement::ReportedService {
                        timestamp_ns: *timestamp_ns,
                        metric: metric.clone(),
                    });
                }
            }
            if frame.omitted_metrics > 0 {
                self.publisher.publish(Measurement::TruncatedMetrics {
                    frame_id: frame.id,
                    omitted: frame.omitted_metrics,
                });
            }
        }
    }
}
fn valid_metric(metric: &Metric) -> bool {
    match metric {
        Metric::Ttfb { seconds, .. } | Metric::Processing { seconds, .. } => {
            seconds.is_finite() && *seconds >= 0.0
        }
        Metric::SttUsage { audio_seconds, .. } => {
            audio_seconds.is_finite() && *audio_seconds >= 0.0
        }
        Metric::LlmUsage { .. } | Metric::TtsUsage { .. } => true,
    }
}
