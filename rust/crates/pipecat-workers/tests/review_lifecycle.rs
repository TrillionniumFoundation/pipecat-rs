//! Adversarial lifecycle regressions for bounded worker RPC.
use pipecat_workers::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Notify;

const LIMIT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn update_request_is_serviced_while_in_flight_capacity_is_full() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let entered = Arc::new(Notify::new());
    let notify = entered.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .max_jobs(1)
                .on_job("", false, move |mut context| {
                    let notify = notify.clone();
                    async move {
                        notify.notify_one();
                        assert!(context.update_requested().await);
                        JobResponse::completed(42)
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    tokio::time::timeout(LIMIT, entered.notified())
        .await
        .unwrap();
    job.request_update().await.unwrap();
    let result = tokio::time::timeout(LIMIT, job.result()).await;
    worker.cancel("test cleanup");
    tokio::time::timeout(LIMIT, worker.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "the update request cannot wait for the job slot it must release"
    );
    assert_eq!(result.unwrap().unwrap(), JobResponse::completed(42));
}

#[tokio::test]
async fn cancelling_before_readiness_does_not_later_execute_the_job() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let mut job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    job.cancel(Some("cancel before worker exists".into()))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    async { JobResponse::completed(42) }
                })
                .unwrap(),
        )
        .unwrap();
    let result = tokio::time::timeout(LIMIT, job.result()).await;
    worker.cancel("test cleanup");
    tokio::time::timeout(LIMIT, worker.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a cancelled request must not start when readiness arrives"
    );
    assert_eq!(result.unwrap().unwrap().status, JobStatus::Cancelled);
}

#[tokio::test]
async fn bus_close_resolves_jobs_waiting_for_readiness() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let mut job = runner
        .client("caller")
        .request("missing", Default::default())
        .unwrap();
    tokio::task::yield_now().await;
    runner.bus().close().unwrap();
    let result = tokio::time::timeout(LIMIT, job.result()).await;
    assert!(
        result.is_ok(),
        "closed bus leaves a readiness-only job driver running forever"
    );
    assert!(matches!(
        result.unwrap(),
        Err(JobError::Bus(BusError::Closed))
    ));
}

#[tokio::test]
async fn update_request_cannot_overtake_its_admitted_job_request() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, |mut context| async move {
                    assert!(context.update_requested().await);
                    JobResponse::completed(42)
                })
                .unwrap(),
        )
        .unwrap();
    let response = runner.bus().subscribe_target("caller").unwrap();
    runner
        .bus()
        .publish(BusMessage {
            source: "caller".into(),
            target: Some("worker".into()),
            data: MessageData::JobRequest {
                job_id: "admitted".into(),
                name: None,
                payload: None,
            },
        })
        .unwrap();
    runner
        .bus()
        .publish(BusMessage {
            source: "caller".into(),
            target: Some("worker".into()),
            data: MessageData::JobUpdateRequest {
                job_id: "admitted".into(),
            },
        })
        .unwrap();
    let result = tokio::time::timeout(LIMIT, response.recv()).await;
    worker.cancel("test cleanup");
    tokio::time::timeout(LIMIT, worker.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "system-priority update overtook the admitted job and was lost"
    );
    assert!(matches!(
        result.unwrap().unwrap().unwrap().data,
        MessageData::JobResponse {
            status: JobStatus::Completed,
            ..
        }
    ));
}
