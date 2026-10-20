//! Finite JSON contexts through the same public AgentCoreTextApp used by callers.
use pipecat_aws_agentcore::{AgentCoreConfig, AgentCoreTextApp};
use pipecat_aws_common::AwsCredentials;
use pipecat_core::{
    async_trait, ChatRole, ContextMessage, Direction, Frame, FrameData, FrameProcessor,
    LlmContextData, MessageContent, MessagePart, ProcessorContext, ProcessorError, ToolSet,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
const MAX_INPUT: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const HELP: &str = "AgentCore finite context-to-text\n\
Usage: agentcore_text --help | --check-config | --run INPUT.json OUTPUT.jsonl\n\
Explicit environment only: PIPECAT_AGENTCORE_REGION, PIPECAT_AGENTCORE_RUNTIME_ARN,\n\
PIPECAT_AGENTCORE_ACCESS_KEY_ID, PIPECAT_AGENTCORE_SECRET_ACCESS_KEY, optional\n\
PIPECAT_AGENTCORE_SESSION_TOKEN, PIPECAT_AGENTCORE_CONNECT_ADDRESS (numeric IP:443),\n\
or PIPECAT_AGENTCORE_LOOPBACK_ORIGIN (http[s]://literal-loopback:port).\n\
Hosted HTTPS requires a caller-resolved IP:443; original regional Host/TLS/SigV4\n\
identity is preserved. No runtime DNS, SDK, profiles, metadata chain or retries.\n\
Help reads no configuration. Check validates config without networking or files.\n\
Input is a regular non-symlink JSON file, at most 1 MiB: {\"contexts\":[{\"messages\":[\n\
{\"role\":\"user\",\"content\":\"hello\"}]}]}. Only system/user/assistant text messages\n\
and arrays of {\"type\":\"text\",\"text\":\"...\"} are supported. Unknown fields fail.\n\
Only the LAST user/system message becomes prompt; no history/session reuse.\n\
Output is a NEW bounded JSONL file, never overwritten, with start/text/end events.\n\
End events delimit one-second quiet-gap segments, not HTTP invocation completion.\n\
The final report record marks complete or partial. On any admitted failure the\n\
partial file is retained. Local cancellation does not prove remote agents stopped.\n\
Race-resistant file handling requires Linux. No speech, tools or live AWS claim.";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    contexts: Vec<Context>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    messages: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    role: Role,
    content: Content,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    System,
    User,
    Assistant,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Parts(Vec<Part>),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Part {
    #[serde(rename = "type")]
    kind: PartKind,
    text: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum PartKind {
    Text,
}
impl Input {
    fn into_contexts(self) -> Vec<LlmContextData> {
        self.contexts
            .into_iter()
            .map(|c| LlmContextData {
                messages: c
                    .messages
                    .into_iter()
                    .map(|m| ContextMessage {
                        role: match m.role {
                            Role::System => ChatRole::System,
                            Role::User => ChatRole::User,
                            Role::Assistant => ChatRole::Assistant,
                        },
                        content: match m.content {
                            Content::Text(s) => MessageContent::Text(s),
                            Content::Parts(p) => MessageContent::Parts(
                                p.into_iter()
                                    .map(|p| {
                                        let _ = p.kind;
                                        MessagePart::Text { text: p.text }
                                    })
                                    .collect(),
                            ),
                        },
                        name: None,
                        tool_call_id: None,
                        tool_calls: vec![],
                        extra: BTreeMap::new(),
                    })
                    .collect(),
                tools: ToolSet::NotGiven,
                tool_choice: None,
            })
            .collect()
    }
}
fn env(name: &str, max: usize, required: bool) -> Result<Option<String>, &'static str> {
    match std::env::var_os(format!("PIPECAT_AGENTCORE_{name}")) {
        None if required => Err("required explicit AgentCore environment missing"),
        None => Ok(None),
        Some(v) if v.is_empty() || v.len() > max => {
            Err("AgentCore environment value exceeds bound or is empty")
        }
        Some(v) => v
            .into_string()
            .map(Some)
            .map_err(|_| "AgentCore environment is not UTF-8"),
    }
}
fn config() -> Result<AgentCoreConfig, &'static str> {
    let region = env("REGION", 63, true)?.unwrap();
    let arn = env("RUNTIME_ARN", 256, true)?.unwrap();
    let access = env("ACCESS_KEY_ID", 256, true)?.unwrap();
    let secret = env("SECRET_ACCESS_KEY", 4096, true)?.unwrap();
    let token = env("SESSION_TOKEN", 16384, false)?;
    let credentials =
        AwsCredentials::new(access, secret, token).map_err(|_| "invalid explicit credentials")?;
    let mut c =
        AgentCoreConfig::new(region, arn, credentials).map_err(|_| "invalid AgentCore identity")?;
    if let Some(origin) = env("LOOPBACK_ORIGIN", 256, false)? {
        c = c
            .with_loopback_endpoint(&origin)
            .map_err(|_| "invalid literal-loopback origin")?;
    }
    if let Some(address) = env("CONNECT_ADDRESS", 64, false)? {
        c = c
            .with_connect_address(
                address
                    .parse()
                    .map_err(|_| "connect address must be numeric IP:443")?,
            )
            .map_err(|_| "invalid connect address")?;
    }
    c.validate()
        .map_err(|_| "invalid AgentCore configuration")?;
    Ok(c)
}
struct OutputTarget {
    path: PathBuf,
    directory: File,
    named_parent: PathBuf,
}
fn output_path(input: &Path, output: &Path) -> Result<OutputTarget, &'static str> {
    let name = output.file_name().ok_or("output must name a new file")?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()
        .map_err(|_| "output parent unavailable")?;
    let target = parent.join(name);
    if target == input {
        return Err("input/output alias rejected");
    }
    match std::fs::symlink_metadata(&target) {
        Ok(_) => Err("output must not exist"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(target_os = "linux")]
            {
                use std::os::{
                    fd::AsRawFd,
                    unix::fs::{MetadataExt, OpenOptionsExt},
                };
                let before = std::fs::symlink_metadata(&parent)
                    .map_err(|_| "output parent identity unavailable")?;
                let directory = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(&parent)
                    .map_err(|_| "output parent open failed")?;
                let opened = directory
                    .metadata()
                    .map_err(|_| "output parent metadata unavailable")?;
                if !before.is_dir() || (before.dev(), before.ino()) != (opened.dev(), opened.ino())
                {
                    return Err("output parent identity changed");
                }
                let path =
                    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
                Ok(OutputTarget {
                    path,
                    directory,
                    named_parent: parent,
                })
            }
            #[cfg(not(target_os = "linux"))]
            {
                Err("race-resistant output requires Linux")
            }
        }
        Err(_) => Err("output location unavailable"),
    }
}
#[cfg(target_os = "linux")]
fn read_input(input: &Path, output: &Path) -> Result<(Vec<u8>, OutputTarget), &'static str> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let identity = |m: &std::fs::Metadata| {
        (
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
            m.ctime(),
            m.ctime_nsec(),
        )
    };
    let before = std::fs::symlink_metadata(input).map_err(|_| "input unavailable")?;
    if !before.is_file() || before.len() == 0 || before.len() > MAX_INPUT as u64 {
        return Err("input must be a regular non-symlink file, 1 byte..1 MiB");
    }
    let canonical = input
        .canonicalize()
        .map_err(|_| "input identity unavailable")?;
    let target = output_path(&canonical, output)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(input)
        .map_err(|_| "input open failed")?;
    let opened = file.metadata().map_err(|_| "input metadata failed")?;
    if !opened.is_file() || identity(&before) != identity(&opened) {
        return Err("input identity changed before read");
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_INPUT as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "input read failed")?;
    let after = file.metadata().map_err(|_| "input metadata failed")?;
    let named = std::fs::symlink_metadata(input).map_err(|_| "input identity changed")?;
    if bytes.len() as u64 != opened.len()
        || !named.is_file()
        || identity(&opened) != identity(&after)
        || identity(&opened) != identity(&named)
    {
        return Err("input identity or content changed while reading");
    }
    Ok((bytes, target))
}
#[cfg(not(target_os = "linux"))]
fn read_input(_: &Path, _: &Path) -> Result<(Vec<u8>, OutputTarget), &'static str> {
    Err("race-resistant file handling requires Linux")
}
struct Recording {
    file: File,
    path: PathBuf,
    _directory: File,
    bytes: usize,
}
#[derive(Default)]
struct JsonlOutput(Mutex<Option<Recording>>);
impl JsonlOutput {
    fn create(&self, target: OutputTarget) -> Result<(), &'static str> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let opened = target
                .directory
                .metadata()
                .map_err(|_| "output directory metadata unavailable")?;
            let named = std::fs::symlink_metadata(&target.named_parent)
                .map_err(|_| "output directory changed")?;
            if !named.is_dir() || (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
                return Err("output parent changed before create");
            }
        }
        let path = target.path;
        let file = options
            .open(&path)
            .map_err(|_| "output create-new failed; no existing file overwritten")?;
        if !file
            .metadata()
            .map_err(|_| "output metadata unavailable")?
            .is_file()
        {
            return Err("output is not regular file");
        }
        *self.0.lock().map_err(|_| "output lock failed")? = Some(Recording {
            file,
            path,
            _directory: target.directory,
            bytes: 0,
        });
        Ok(())
    }
    fn write(&self, event: serde_json::Value) -> Result<(), &'static str> {
        let mut bytes = serde_json::to_vec(&event).map_err(|_| "output serialization failed")?;
        bytes.push(b'\n');
        let mut lock = self.0.lock().map_err(|_| "output lock failed")?;
        let recording = lock.as_mut().ok_or("output not initialized")?;
        if recording.bytes.saturating_add(bytes.len()) > MAX_OUTPUT {
            return Err("output exceeded 16 MiB; file is partial");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let opened = recording
                .file
                .metadata()
                .map_err(|_| "output metadata unavailable")?;
            let named = std::fs::symlink_metadata(&recording.path)
                .map_err(|_| "output identity changed")?;
            if !named.is_file() || (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
                return Err("output identity changed");
            }
        }
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let n = recording
                .file
                .write(rest)
                .map_err(|_| "output write failed; file is partial")?;
            if n == 0 {
                return Err("output made no progress; file is partial");
            }
            recording.bytes += n;
            rest = &rest[n..];
        }
        recording
            .file
            .flush()
            .map_err(|_| "output flush failed; file is partial")
    }
}
#[async_trait]
impl FrameProcessor for JsonlOutput {
    fn name(&self) -> &str {
        "AgentCoreJsonlOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && ctx.is_current() {
            let event = match &frame.data {
                FrameData::LlmResponseStart => Some(serde_json::json!({"event":"start"})),
                FrameData::LlmText(text) => {
                    Some(serde_json::json!({"event":"text","text":text.text}))
                }
                FrameData::LlmResponseEnd => Some(serde_json::json!({"event":"end"})),
                _ => None,
            };
            if let Some(event) = event {
                self.write(event).map_err(ProcessorError::fatal)?;
            }
        }
        ctx.push(frame, direction).await
    }
}
async fn run(args: Vec<OsString>) -> Result<(), &'static str> {
    if args.len() > 3 || args.iter().any(|a| a.is_empty() || a.len() > 4096) {
        return Err("invalid bounded arguments; use --help");
    }
    if matches!(args.as_slice(), [a] if a == "--help") {
        println!("{HELP}");
        return Ok(());
    }
    if matches!(args.as_slice(), [a] if a == "--check-config") {
        config()?;
        println!("configuration valid; no network or file effects");
        return Ok(());
    }
    let [command, input, output] = args.as_slice() else {
        return Err("use --help, --check-config or --run INPUT OUTPUT");
    };
    if command != "--run" {
        return Err("unknown command; use --help");
    }
    let config = config()?;
    let (bytes, target) = read_input(Path::new(input), Path::new(output))?;
    let input: Input =
        serde_json::from_slice(&bytes).map_err(|_| "malformed or unsupported input schema")?;
    let output = Arc::new(JsonlOutput::default());
    let app = AgentCoreTextApp::new(config, input.into_contexts(), output.clone())
        .map_err(|_| "invalid or excessive complete context input")?;
    output.create(target)?;
    let running = app
        .start()
        .await
        .map_err(|_| "application startup failed; output file is partial")?;
    let report = running.finish().await;
    output.write(serde_json::json!({"event":"report", "complete":report.is_success(), "partial":!report.is_success(), "pipeline_joined":report.pipeline_joined, "invocations":report.progress.admitted, "completed_at_eof":report.progress.completed_at_eof, "owners_released":report.progress.owners_released}))?;
    if report.is_success() {
        Ok(())
    } else {
        Err("application failed or cancelled; output file is partial")
    }
}
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).take(4).collect();
    if let Err(error) = run(args).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn replaced_parent_directory_cannot_redirect_create_new() {
        use std::os::unix::fs::symlink;
        let root =
            std::env::temp_dir().join(format!("agentcore-parent-race-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let parent = root.join("parent");
        let other = root.join("other");
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(&other).unwrap();
        let target = output_path(&root.join("input.json"), &parent.join("output.jsonl")).unwrap();
        std::fs::rename(&parent, root.join("moved")).unwrap();
        symlink(&other, &parent).unwrap();
        assert!(JsonlOutput::default().create(target).is_err());
        assert!(!other.join("output.jsonl").exists());
        assert!(!root.join("moved/output.jsonl").exists());
        std::fs::remove_file(parent).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn whole_input_rejects_unknown_and_non_text_fields() {
        for value in [
            r#"{"contexts":[{"messages":[{"role":"user","content":"ok","name":"no"}]}]}"#,
            r#"{"contexts":[{"messages":[{"role":"tool","content":"no"}]}]}"#,
            r#"{"contexts":[{"messages":[{"role":"user","content":[{"type":"image","text":"no"}]}]}]}"#,
            r#"{"contexts":[{"messages":[],"tools":[]}]}"#,
        ] {
            assert!(serde_json::from_str::<Input>(value).is_err());
        }
    }
}
