//! Separate loopback text-eval entry point. This does not start the voice runner.
#[path = "../eval_app.rs"]
mod eval_app;

use pipecat_evals::{
    bot_host::{EvalBotHost, EvalBotHostConfig},
    Cancellation,
};
use serde::Deserialize;
use std::{
    ffi::OsString,
    fs::File,
    io::Read,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

const HELP: &str = "eval-bot --config PATH [--port PORT]\n\nSeparate native text-eval host; not the production voice application.\n  --config PATH  Required strict version-1 JSON model configuration\n  --port PORT    Literal IPv4-loopback listener port (default 7860)\n  --help, -h     Show help without config, credentials, or sockets\n\nConfigure an explicit literal-loopback HTTP chat-completions URL and model.\nNo .env, credentials, default model, endpoint discovery, or model download.\n";
const MAX_CONFIG_BYTES: u64 = 16 * 1024;

#[derive(Debug, PartialEq, Eq)]
enum Arguments {
    Help,
    Run { config: PathBuf, port: u16 },
}
impl Arguments {
    fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, &'static str> {
        let mut args = args.into_iter();
        let mut config = None;
        let mut port = None;
        while let Some(arg) = args.next() {
            match arg.to_str() {
                Some("--help" | "-h") => return Ok(Self::Help),
                Some("--config") if config.is_none() => {
                    let value = args.next().ok_or("--config requires a path")?;
                    if value.is_empty() || value.to_string_lossy().starts_with('-') {
                        return Err("--config requires a path");
                    }
                    config = Some(PathBuf::from(value));
                }
                Some("--port") if port.is_none() => {
                    let value = args.next().ok_or("--port requires a port")?;
                    let value = value.to_str().ok_or("port must be 1..65535")?;
                    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err("port must be 1..65535");
                    }
                    let value = value.parse::<u16>().map_err(|_| "port must be 1..65535")?;
                    if value == 0 {
                        return Err("port must be 1..65535");
                    }
                    port = Some(value);
                }
                _ => return Err("unknown or repeated option; use --help"),
            }
        }
        Ok(Self::Run {
            config: config.ok_or("--config is required")?,
            port: port.unwrap_or(7860),
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelConfig {
    native_text_bot_version: u32,
    llm_url: String,
    model: String,
}
impl ModelConfig {
    fn parse(bytes: &[u8]) -> Result<eval_app::TextBot, &'static str> {
        if bytes.len() > MAX_CONFIG_BYTES as usize {
            return Err("model config exceeds 16 KiB");
        }
        if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
            return Err("model config must be a JSON object");
        }
        let config: Self = serde_json::from_slice(bytes)
            .map_err(|_| "invalid model config; required fields only, without duplicates")?;
        if config.native_text_bot_version != 1 {
            return Err("native_text_bot_version must be 1");
        }
        eval_app::TextBot::new(config.llm_url, config.model)
    }
}

fn read_config(path: &Path) -> Result<eval_app::TextBot, &'static str> {
    // Check before opening so named pipes/devices do not block ordinary input.
    if !std::fs::metadata(path)
        .map_err(|_| "cannot inspect model config")?
        .is_file()
    {
        return Err("model config must be a regular JSON file");
    }
    let file = File::open(path).map_err(|_| "cannot open model config")?;
    if !file
        .metadata()
        .map_err(|_| "cannot inspect opened model config")?
        .is_file()
    {
        return Err("model config must be a regular JSON file");
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read model config")?;
    ModelConfig::parse(&bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Arguments::Run { config, port } = Arguments::parse(std::env::args_os().skip(1))? else {
        print!("{HELP}");
        return Ok(());
    };
    // Configuration and endpoint validation finish before opening any socket.
    let bot = read_config(&config)?;
    let host = EvalBotHost::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        EvalBotHostConfig::default(),
    )
    .await?;
    eprintln!("native text eval host: ws://{}", host.local_addr()?);
    let cancel = Cancellation::new();
    let service = host.serve(&bot, cancel.clone());
    tokio::pin!(service);
    let report = tokio::select! {
        result = &mut service => result?,
        signal = tokio::signal::ctrl_c() => {
            cancel.cancel();
            let report = service.await?;
            signal?;
            report
        }
    };
    eprintln!("native text eval host closed: {report:?}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(|value| OsString::from(*value)).collect()
    }

    #[test]
    fn cli_is_explicit_and_rejects_duplicates() {
        assert_eq!(Arguments::parse(args(&["--help"])), Ok(Arguments::Help));
        assert_eq!(
            Arguments::parse(args(&["--config", "bot.json"])),
            Ok(Arguments::Run {
                config: "bot.json".into(),
                port: 7860
            })
        );
        for values in [
            vec![],
            vec!["--config"],
            vec!["--config", "--port", "7900"],
            vec!["--config", "a", "--config", "b"],
            vec!["--config", "a", "--port", "1", "--port", "2"],
            vec!["--config", "a", "--port", "0"],
            vec!["--config", "a", "--port", "65536"],
            vec!["--config", "a", "--port", "+1"],
            vec!["--llm-url", "SECRET"],
        ] {
            assert!(Arguments::parse(args(&values)).is_err());
        }
    }

    #[test]
    fn strict_config_errors_do_not_echo_input() {
        for bytes in [
            b"[]".as_slice(),
            b"null",
            br#"{"native_text_bot_version":1,"llm_url":"SECRET","model":"x"}"#,
            br#"{"native_text_bot_version":2,"llm_url":"","model":""}"#,
            br#"{"native_text_bot_version":1,"llm_url":"","model":"","SECRET":"SECRET"}"#,
            br#"{"native_text_bot_version":1,"llm_url":"","llm_url":"SECRET","model":"x"}"#,
            br#"{"native_text_bot_version":1,"model":"SECRET"}"#,
        ] {
            let error = ModelConfig::parse(bytes)
                .err()
                .expect("invalid config accepted");
            assert!(!error.contains("SECRET"));
        }
        assert!(ModelConfig::parse(&vec![b' '; 16 * 1024 + 1]).is_err());
    }
}
