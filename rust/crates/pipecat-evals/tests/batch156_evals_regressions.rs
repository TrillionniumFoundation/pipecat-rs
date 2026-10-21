//! Source-bound aggregate: each existing module executes once in this target.
#[path = "contracts.rs"]
mod contracts;
#[path = "judging.rs"]
mod judging;
#[path = "loopback.rs"]
mod loopback;
#[path = "process_hosting.rs"]
mod process_hosting;
#[path = "review_judge.rs"]
mod review_judge;
