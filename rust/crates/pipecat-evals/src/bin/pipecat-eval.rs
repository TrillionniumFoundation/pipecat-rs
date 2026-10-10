//! Native scripted evaluation CLI; exit 0 for pass, 1 for failed runs, 2 for invalid input.
use pipecat_evals::{
    process::ProcessOptions,
    release::{run_release, ReleaseManifest},
    Cancellation, EvalSession, ScenarioFile, SessionOptions,
};
#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = run(&args).await;
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
async fn run(args: &[String]) -> Result<bool, pipecat_evals::EvalError> {
    let suite = args.len() == 2 && args[0] == "suite";
    let single =
        (args.len() == 3 || (args.len() == 4 && args[3] == "--stop-bot")) && args[0] == "run";
    if !suite && !single {
        return Err(pipecat_evals::EvalError::Scenario(
            "Usage: pipecat-eval run SCENARIOS.yaml ws://HOST:PORT[/PATH] [--stop-bot]\n       pipecat-eval suite NATIVE-MANIFEST.yaml".into()));
    }
    // Parse every source and option before installing the signal task or launching a bot.
    let manifest = if suite {
        Some(ReleaseManifest::load(&args[1])?)
    } else {
        None
    };
    let mut sessions = Vec::new();
    if single {
        let file = ScenarioFile::load(&args[1])?;
        let options = SessionOptions {
            stop_bot: args.len() == 4,
            ..Default::default()
        };
        if options.stop_bot && file.scenarios.len() > 1 {
            return Err(pipecat_evals::EvalError::Scenario(
                "--stop-bot with several scenarios needs independent bot endpoints; use run_suite"
                    .into(),
            ));
        }
        sessions = file
            .scenarios
            .into_iter()
            .map(|scenario| EvalSession::new(scenario, &args[2], options.clone()))
            .collect::<Result<Vec<_>, _>>()?;
    }
    let cancel = Cancellation::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancel.cancel();
        }
    });
    let result = async {
        if let Some(manifest) = manifest {
            let result = run_release(
                manifest,
                SessionOptions::default(),
                ProcessOptions::default(),
                cancel,
            )
            .await?;
            println!("{}", serde_json::to_string(&result)?);
            Ok(result.passed)
        } else {
            let mut passed = true;
            for session in sessions {
                let result = session.run(cancel.clone()).await;
                passed &= result.passed;
                println!("{}", serde_json::to_string(&result)?);
            }
            Ok(passed)
        }
    }
    .await;
    signal.abort();
    let _ = signal.await;
    result
}
