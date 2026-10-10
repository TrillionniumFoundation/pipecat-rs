//! Native, non-interactive project initialization.
use pipecat_project::{
    cli::{Command, HELP},
    generate_cancellable, supported_options, ProjectError,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(if matches!(error, ProjectError::Cancelled) {
                130
            } else {
                1
            });
        }
    }
}
async fn run() -> Result<(), ProjectError> {
    let args = std::env::args_os()
        .skip(1)
        .map(|arg| {
            arg.into_string()
                .map_err(|_| ProjectError::Configuration("arguments must be UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match Command::parse(args)? {
        Command::Help => print!("{HELP}"),
        Command::ListOptions => println!(
            "{}",
            serde_json::to_string_pretty(&supported_options())
                .map_err(|_| ProjectError::Serialization)?
        ),
        Command::Generate {
            plan,
            dry_run: true,
        } => println!(
            "{}",
            serde_json::to_string_pretty(&plan.summary())
                .map_err(|_| ProjectError::Serialization)?
        ),
        Command::Generate {
            plan,
            dry_run: false,
        } => {
            let cancelled = Arc::new(AtomicBool::new(false));
            let worker_cancel = cancelled.clone();
            let mut worker = tokio::task::spawn_blocking(move || {
                generate_cancellable(&plan, |_| worker_cancel.load(Ordering::Relaxed))
            });
            let result = tokio::select! {
                result = &mut worker => result,
                signal = tokio::signal::ctrl_c() => {
                    cancelled.store(true, Ordering::Relaxed);
                    let result = worker.await;
                    signal?;
                    result
                }
            }
            .map_err(|_| ProjectError::Configuration("generator task failed"))??;
            println!("Created {}", result.display());
            println!("Next: read README.md, then cargo run -- --help from the project directory");
        }
    }
    Ok(())
}
