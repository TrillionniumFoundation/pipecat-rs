//! Run with `cargo run -p pipecat-core --example processor_usable -- continue`
//! (or `end` / `cancel`). No providers, network, credentials or retries.
use pipecat_core::*;
use std::sync::Arc;

#[derive(Default)]
struct DemoService {
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for DemoService {
    fn name(&self) -> &str {
        "demo-service"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !matches!(frame.data, FrameData::Text(_))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Text(text) = &frame.data {
            let category = match text.text.as_str() {
                "temporary failure" => Some(ErrorCategory::Server),
                "rejected settings" => Some(ErrorCategory::InvalidRequest),
                _ => None,
            };
            if let Some(category) = category {
                return ctx
                    .push_error(ErrorFrame {
                        category,
                        ..ErrorFrame::new(text.text.clone())
                    })
                    .await;
            }
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = match std::env::args().nth(1).as_deref().unwrap_or("continue") {
        "continue" => ProcessorUnusablePolicy::Continue,
        "end" => ProcessorUnusablePolicy::End,
        "cancel" => ProcessorUnusablePolicy::Cancel,
        _ => return Err("expected continue, end or cancel".into()),
    };
    let service = Arc::new(DemoService::default());
    let mut worker = Pipeline::new(vec![service.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: policy,
            ..PipelineConfig::default()
        })
        .await?;
    worker.recv().await.ok_or("missing Start")?;
    for text in ["temporary failure", "rejected settings"] {
        worker
            .sender()
            .send(Frame::text(text), Direction::Downstream)
            .await?;
        let frame = worker.recv().await.ok_or("missing error")?.frame;
        let error = frame.data.as_error_frame().ok_or("expected error")?;
        let origin = frame.error_origin().ok_or("missing local attribution")?;
        println!(
            "{:?}: usable={} revision={}",
            error.category, origin.reported_health.is_usable, origin.reported_health.revision
        );
    }
    if policy == ProcessorUnusablePolicy::Continue {
        // An application has fixed the configuration. Only new work resumes;
        // the failed requests are never replayed automatically.
        service.health.set_usable(true);
        worker
            .sender()
            .send(
                Frame::text("new work after recovery"),
                Direction::Downstream,
            )
            .await?;
        let frame = worker.recv().await.ok_or("missing restored output")?;
        if let FrameData::Text(text) = frame.frame.data {
            println!("{}", text.text);
        }
        worker.end().await?;
    }
    let (_, report) = worker.collect().await?;
    println!(
        "{:?}; {} reported errors",
        report.state, report.total_errors
    );
    Ok(())
}
