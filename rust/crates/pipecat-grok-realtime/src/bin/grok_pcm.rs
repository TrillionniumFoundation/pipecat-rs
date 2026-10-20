//! Opt-in, finite recorded PCM through the public Grok realtime application.
//! A regular-file write is delivery only, never a playback receipt.
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use pipecat_grok_realtime::{
    FunctionTool, GrokAppConfig, GrokAppReport, GrokLocalTool, GrokRealtimeApp,
};
use pipecat_processors::tools::RegisteredTool;
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_INPUT: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const CHUNK_BYTES: usize = 4800;
const MAX_OUTPUT_CHUNK: usize = 192_000;
const MAX_ARGS: usize = 16;
const HELP: &str = "Grok finite recorded PCM (Linux only)\n\
Usage: grok_pcm [--help | --check]\n\
  grok_pcm --enable --input INPUT.pcm --output OUTPUT.pcm --responses N\n\
    [--run] [--echo-tool] [--timeout-ms N] [--max-output-bytes N]\n\
No arguments, help and check are disabled/offline: no key reads, network or files.\n\
Active execution reads ONLY GROK_PCM_API_KEY and exactly one explicit address:\n\
GROK_PCM_CONNECT_ADDRESS=numeric-IP:443 (fixed api.x.ai TLS/Host/model identity),\n\
or GROK_PCM_LOOPBACK_ADDRESS=literal-loopback:port (offline fixtures only).\n\
Check validates non-secret address configuration with a synthetic key.\n\
Input: nonempty regular non-symlink raw signed PCM16 little-endian, mono 24000 Hz,\n\
at most 1 MiB, whole two-byte samples. Input is preflighted, then sent in bounded\n\
4800-byte chunks while the application continuously drains real output.\n\
Output: a NEW regular file, mode 0600, at most 16 MiB; never overwritten.\n\
Real-time output is collected in a preallocated bounded memory buffer. Only after\n\
all network/application owners join is it written to the retained file handle.\n\
Final file persistence is outside the network cancellation deadline and may block\n\
on the OS/filesystem. No crash-durability or hard realtime guarantee is made.\n\
Symlink path components, parent traversals and input/output aliases are rejected.\n\
--responses is REQUIRED (1..64): completed generated-speech responses, not tool-\n\
only responses. Server VAD is enabled; PCM EOF is NOT a server commit receipt.\n\
--run explicitly requests inference after PCM; it is not enabled by default.\n\
--timeout-ms bounds waiting (100..120000, default 30000); an absent response\n\
fails and cancels locally. --max-output-bytes is even (2..16777216).\n\
--echo-tool offers only echo({\"text\":\"...\"}), a bounded deterministic final tool.\n\
No shell, network, filesystem or hosted tool callback is offered.\n\
Exit zero requires the response condition, causal tool/assistant drain, finalized\n\
recording and successful application owner joins. Failures retain an identified\n\
partial file; diagnostics omit keys, transcripts, tool payloads and provider text.\n\
Recording is not audible playback or proof of user-heard history. No live-provider\n\
acceptance, remote cancellation, rollback, retry or reconnect is claimed.";

struct Options {
    input: PathBuf,
    output: PathBuf,
    responses: usize,
    timeout: Duration,
    max_output: usize,
    explicit_run: bool,
    echo_tool: bool,
}
fn number(value: &OsString, min: usize, max: usize) -> Result<usize, &'static str> {
    let value = value.to_str().ok_or("numeric argument is not UTF-8")?;
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid numeric argument");
    }
    value
        .parse::<usize>()
        .ok()
        .filter(|v| (min..=max).contains(v))
        .ok_or("numeric argument outside supported bounds")
}
fn options(args: &[OsString]) -> Result<Options, &'static str> {
    if args.first().is_none_or(|a| a != "--enable") {
        return Err("active execution requires --enable; use --help");
    }
    let (mut input, mut output, mut responses) = (None, None, None);
    let (mut timeout, mut max_output) = (None, None);
    let (mut explicit_run, mut echo_tool) = (false, false);
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--run" && !explicit_run {
            explicit_run = true;
        } else if arg == "--echo-tool" && !echo_tool {
            echo_tool = true;
        } else {
            let value = args.get(index + 1).ok_or("option requires a value")?;
            match arg.to_str() {
                Some("--input") if input.is_none() => input = Some(PathBuf::from(value)),
                Some("--output") if output.is_none() => output = Some(PathBuf::from(value)),
                Some("--responses") if responses.is_none() => {
                    responses = Some(number(value, 1, 64)?)
                }
                Some("--timeout-ms") if timeout.is_none() => {
                    timeout = Some(number(value, 100, 120_000)?)
                }
                Some("--max-output-bytes") if max_output.is_none() => {
                    let n = number(value, 2, MAX_OUTPUT)?;
                    if !n.is_multiple_of(2) {
                        return Err("output bound must contain whole PCM16 samples");
                    }
                    max_output = Some(n);
                }
                _ => return Err("unknown or duplicate option; use --help"),
            }
            index += 1;
        }
        index += 1;
    }
    Ok(Options {
        input: input.ok_or("--input is required")?,
        output: output.ok_or("--output is required")?,
        responses: responses.ok_or("--responses is required")?,
        timeout: Duration::from_millis(timeout.unwrap_or(30_000) as u64),
        max_output: max_output.unwrap_or(MAX_OUTPUT),
        explicit_run,
        echo_tool,
    })
}
fn environment(name: &str, max: usize) -> Result<Option<String>, &'static str> {
    match std::env::var_os(name) {
        None => Ok(None),
        Some(value) if value.is_empty() || value.len() > max => {
            Err("explicit Grok environment is empty or exceeds its bound")
        }
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_| "explicit Grok environment is not UTF-8"),
    }
}
fn configuration(active: bool, timeout: Duration) -> Result<GrokAppConfig, &'static str> {
    // Do not even inspect the credential variable in default/help/check mode.
    let key = if active {
        environment("GROK_PCM_API_KEY", 4096)?.ok_or("GROK_PCM_API_KEY is required")?
    } else {
        "offline-config-validation-only".to_owned()
    };
    let hosted = environment("GROK_PCM_CONNECT_ADDRESS", 64)?;
    let local = environment("GROK_PCM_LOOPBACK_ADDRESS", 64)?;
    let mut config = match (hosted, local) {
        (Some(address), None) => {
            let address: SocketAddr = address.parse().map_err(|_| "numeric IP:443 required")?;
            if address.port() != 443 || address.ip().is_unspecified() || address.ip().is_multicast()
            {
                return Err("invalid hosted connect address");
            }
            GrokAppConfig::new(key, address)
        }
        (None, Some(address)) => {
            let address: SocketAddr = address.parse().map_err(|_| "literal loopback required")?;
            if !address.ip().is_loopback() || address.port() == 0 {
                return Err("literal loopback and nonzero port required");
            }
            GrokAppConfig::loopback(key, address)
        }
        _ => return Err("exactly one explicit Grok connect or loopback address is required"),
    };
    config.connection.limits.connect_timeout = timeout.min(Duration::from_secs(10));
    config.connection.limits.operation_timeout = timeout.min(Duration::from_secs(5));
    config.connection.limits.response_timeout = timeout;
    config.connection.limits.close_timeout = timeout.min(Duration::from_secs(1));
    config.limits.session_timeout = timeout;
    config.limits.max_tool_duration = timeout.min(Duration::from_secs(5));
    config.limits.drain_timeout = timeout.min(Duration::from_secs(10));
    config.limits.shutdown_timeout = Duration::from_secs(2);
    Ok(config)
}
fn tools(enabled: bool, timeout: Duration) -> Vec<GrokLocalTool> {
    if !enabled {
        return vec![];
    }
    vec![GrokLocalTool {
        schema: FunctionTool::new(
            "echo",
            "Return the supplied text unchanged; no external effects.",
            serde_json::json!({
                "type":"object", "properties":{"text":{"type":"string","maxLength":1024}},
                "required":["text"], "additionalProperties":false
            }),
        ),
        handler: RegisteredTool {
            timeout: timeout.min(Duration::from_secs(5)),
            ..RegisteredTool::new(Arc::new(|call| {
                Box::pin(async move {
                    let object = call.arguments.as_object().ok_or("invalid echo argument")?;
                    let text = object
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .filter(|text| text.len() <= 1024)
                        .ok_or("invalid echo text")?;
                    if object.len() != 1 {
                        return Err("unsupported echo argument".into());
                    }
                    Ok(serde_json::json!({"text":text}))
                })
            }))
        },
    }]
}

#[cfg(target_os = "linux")]
mod files {
    use super::*;
    use std::os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    };

    fn identity(m: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
        (
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
            m.ctime(),
            m.ctime_nsec(),
        )
    }
    struct Directory {
        file: File,
        named: PathBuf,
    }
    impl Directory {
        fn open(path: &Path) -> Result<Self, &'static str> {
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .map_err(|_| "current directory unavailable")?
                    .join(path)
            };
            let open = |p: &Path| {
                OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(p)
            };
            let mut directory = Self {
                file: open(Path::new("/")).map_err(|_| "root open failed")?,
                named: PathBuf::from("/"),
            };
            for part in absolute.components() {
                match part {
                    Component::RootDir | Component::CurDir => continue,
                    Component::Normal(name) => {
                        let anchored = directory.path(name);
                        let before = std::fs::symlink_metadata(&anchored)
                            .map_err(|_| "directory unavailable")?;
                        if !before.is_dir() {
                            return Err("symlink or non-directory path component rejected");
                        }
                        let file = open(&anchored).map_err(|_| "directory open failed")?;
                        let after = file
                            .metadata()
                            .map_err(|_| "directory metadata unavailable")?;
                        if (before.dev(), before.ino()) != (after.dev(), after.ino()) {
                            return Err("directory identity changed");
                        }
                        directory.named.push(name);
                        directory.file = file;
                    }
                    _ => return Err("parent traversal or unsupported path component rejected"),
                }
            }
            directory.verify()?;
            Ok(directory)
        }
        fn path(&self, name: &std::ffi::OsStr) -> PathBuf {
            PathBuf::from(format!("/proc/self/fd/{}", self.file.as_raw_fd())).join(name)
        }
        fn verify(&self) -> Result<(), &'static str> {
            let opened = self
                .file
                .metadata()
                .map_err(|_| "directory metadata unavailable")?;
            let named =
                std::fs::symlink_metadata(&self.named).map_err(|_| "directory identity changed")?;
            if !named.is_dir() || (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
                return Err("directory identity changed");
            }
            Ok(())
        }
    }
    fn split(path: &Path) -> Result<(Directory, OsString), &'static str> {
        let name = path
            .file_name()
            .filter(|n| !n.is_empty())
            .ok_or("file name required")?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        Ok((Directory::open(parent)?, name.to_os_string()))
    }
    pub(super) struct Input {
        pub(super) bytes: Vec<u8>,
        file: File,
        directory: Directory,
        name: OsString,
        before: std::fs::Metadata,
    }
    impl Input {
        pub(super) fn read(path: &Path) -> Result<Self, &'static str> {
            let (directory, name) = split(path)?;
            let anchored = directory.path(&name);
            let before = std::fs::symlink_metadata(&anchored).map_err(|_| "input unavailable")?;
            if !before.is_file()
                || before.len() == 0
                || before.len() > MAX_INPUT as u64
                || !before.len().is_multiple_of(2)
            {
                return Err("input requires regular non-symlink, aligned PCM16, 2 bytes..1 MiB");
            }
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&anchored)
                .map_err(|_| "input open failed")?;
            let opened = file.metadata().map_err(|_| "input metadata unavailable")?;
            if !opened.is_file() || identity(&before) != identity(&opened) {
                return Err("input identity changed before read");
            }
            let mut bytes = Vec::with_capacity(opened.len() as usize);
            Read::by_ref(&mut file)
                .take(MAX_INPUT as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "input read failed")?;
            if bytes.len() as u64 != opened.len() {
                return Err("input size changed during read");
            }
            let input = Self {
                bytes,
                file,
                directory,
                name,
                before,
            };
            input.verify()?;
            Ok(input)
        }
        pub(super) fn verify(&self) -> Result<(), &'static str> {
            self.directory.verify()?;
            let opened = self
                .file
                .metadata()
                .map_err(|_| "input metadata unavailable")?;
            let named = std::fs::symlink_metadata(self.directory.path(&self.name))
                .map_err(|_| "input identity changed")?;
            if !named.is_file()
                || identity(&self.before) != identity(&opened)
                || identity(&opened) != identity(&named)
            {
                return Err("input identity or content changed");
            }
            Ok(())
        }
    }
    pub(super) struct Target {
        directory: Directory,
        name: OsString,
    }
    impl Target {
        pub(super) fn prepare(path: &Path, input: &Input) -> Result<Self, &'static str> {
            let (directory, name) = split(path)?;
            // Existing hard links, aliases, symlinks and other objects all fail.
            if directory.named == input.directory.named && name == input.name {
                return Err("input/output alias rejected");
            }
            match std::fs::symlink_metadata(directory.path(&name)) {
                Ok(_) => return Err("output must not exist"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err("output location unavailable"),
            }
            Ok(Self { directory, name })
        }
    }
    struct Recording {
        file: File,
        target: Target,
        device: u64,
        inode: u64,
        buffer: Box<[u8]>,
        bytes: usize,
        persisted: usize,
        sealed: bool,
        path_identity_preserved: bool,
        file_flushed: bool,
    }
    impl Recording {
        fn verify_original(&self) -> Result<(), &'static str> {
            let opened = self
                .file
                .metadata()
                .map_err(|_| "output metadata unavailable")?;
            if !opened.is_file()
                || (opened.dev(), opened.ino()) != (self.device, self.inode)
                || opened.len() != self.persisted as u64
            {
                return Err("retained output identity or content changed");
            }
            Ok(())
        }
        fn verify_path(&self) -> Result<(), &'static str> {
            self.target.directory.verify()?;
            let named = std::fs::symlink_metadata(self.target.directory.path(&self.target.name))
                .map_err(|_| "output path identity changed")?;
            if !named.is_file() || (named.dev(), named.ino()) != (self.device, self.inode) {
                return Err("output path identity changed");
            }
            Ok(())
        }
    }
    pub(super) struct PcmOutput {
        recording: Mutex<Option<Recording>>,
        limit: usize,
    }
    impl PcmOutput {
        pub(super) fn new(limit: usize) -> Self {
            Self {
                recording: Mutex::new(None),
                limit,
            }
        }
        pub(super) fn create(&self, target: Target, input: &Input) -> Result<(), &'static str> {
            input.verify()?;
            target.directory.verify()?;
            let mut storage = Vec::new();
            storage
                .try_reserve_exact(self.limit)
                .map_err(|_| "recording buffer allocation failed")?;
            storage.resize(self.limit, 0);
            let buffer = storage.into_boxed_slice();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(target.directory.path(&target.name))
                .map_err(|_| "output create-new failed; no existing file overwritten")?;
            let metadata = file.metadata().map_err(|_| "output metadata unavailable")?;
            let recording = Recording {
                file,
                target,
                device: metadata.dev(),
                inode: metadata.ino(),
                buffer,
                bytes: 0,
                persisted: 0,
                sealed: false,
                path_identity_preserved: true,
                file_flushed: false,
            };
            // Preserve identity even when a last-moment swap makes verification fail.
            let mut lock = self.recording.lock().map_err(|_| "output lock failed")?;
            *lock = Some(recording);
            let recording = lock.as_mut().ok_or("output missing")?;
            recording.verify_original()?;
            let path = recording.verify_path();
            recording.path_identity_preserved = path.is_ok();
            path
        }
        fn buffer(&self, audio: &[u8]) -> Result<(), &'static str> {
            let mut lock = self.recording.lock().map_err(|_| "output lock failed")?;
            let recording = lock.as_mut().ok_or("output not initialized")?;
            if recording.sealed {
                return Err("output already sealed");
            }
            let end = recording
                .bytes
                .checked_add(audio.len())
                .ok_or("output byte overflow")?;
            if audio.len() > MAX_OUTPUT_CHUNK
                || !audio.len().is_multiple_of(2)
                || end > recording.buffer.len()
            {
                return Err("output limit or PCM16 alignment failure");
            }
            // No filesystem access or allocation while network/pipeline owners run.
            recording.buffer[recording.bytes..end].copy_from_slice(audio);
            recording.bytes = end;
            Ok(())
        }
        fn seal(&self) -> Result<(), &'static str> {
            let mut lock = self.recording.lock().map_err(|_| "output lock failed")?;
            lock.as_mut().ok_or("output not initialized")?.sealed = true;
            Ok(())
        }
        /// Called only after all application owners have joined. File syscalls
        /// cannot promise a deadline, so none run on an active application lane.
        pub(super) fn persist(&self) -> Result<(), &'static str> {
            let mut lock = self.recording.lock().map_err(|_| "output lock failed")?;
            let recording = lock.as_mut().ok_or("output not initialized")?;
            recording.sealed = true;
            // A renamed pathname is not authority to abandon or redirect the
            // original retained FD. Validate that FD, preserve its PCM, and
            // report pathname/parent substitution independently as failure.
            recording.verify_original()?;
            let path_matches = recording.verify_path().is_ok();
            recording.path_identity_preserved &= path_matches;
            let Recording {
                file,
                buffer,
                bytes,
                persisted,
                ..
            } = recording;
            while *persisted < *bytes {
                let count = file
                    .write(&buffer[*persisted..*bytes])
                    .map_err(|_| "output write failed")?;
                if count == 0 {
                    return Err("output write made no progress");
                }
                *persisted += count;
            }
            file.flush().map_err(|_| "output flush failed")?;
            recording.verify_original()?;
            recording.file_flushed = true;
            let path_matches = recording.verify_path().is_ok();
            recording.path_identity_preserved &= path_matches;
            if !recording.path_identity_preserved {
                return Err("output path changed; buffered PCM persisted only to the retained original inode");
            }
            Ok(())
        }
        pub(super) fn report(
            &self,
            success: bool,
            stage: &'static str,
            responses: usize,
            application: Option<&GrokAppReport>,
            output_finalized: bool,
            owners_joined: bool,
        ) {
            let Ok(lock) = self.recording.lock() else {
                return;
            };
            if let Some(recording) = lock.as_ref() {
                println!(
                    "{}",
                    serde_json::json!({
                        "status":if success {"complete"} else {"partial"}, "stage":stage,
                        "recorded_bytes":recording.persisted, "buffered_bytes":recording.bytes,
                        "buffer_capacity":recording.buffer.len(), "required_speech_responses":responses,
                        "output":recording.target.directory.named.join(&recording.target.name).to_string_lossy(),
                        "output_device":recording.device, "output_inode":recording.inode,
                        "recording_only":true,
                    "terminal_report_received":application.is_some(),
                    "application_successful":application.is_some_and(GrokAppReport::successful),
                    "first_failure_stage":application.and_then(|r| r.first_failure_stage).map(|v| format!("{v:?}")),
                    "mapping_primary":application.and_then(|r| r.mapping_primary.as_ref()).map(|v| format!("{v:?}")),
                    "provider_primary":application.and_then(|r| r.provider_primary.as_ref()).map(|v| format!("{v:?}")),
                    "provider_cleanup":application.and_then(|r| r.provider_cleanup.as_ref()).map(|v| format!("{v:?}")),
                    "output_cleanup_failed":application.is_some_and(|r| r.cleanup_output_failed),
                    "pipeline_failure":application.and_then(|r| r.pipeline_failure).map(|v| format!("{v:?}")),
                    "pipeline_cleanup":application.map(|r| format!("{:?}", r.pipeline_cleanup)),
                    "forced_cleanup":application.is_some_and(|r| r.forced_cleanup),
                    "generated_speech_responses":application.map(|r| r.generated_speech_responses),
                    "output_finalized":output_finalized,
                    "output_path_matches":recording.path_identity_preserved,
                    "retained_file_flushed":recording.file_flushed,
                    "owners_joined":owners_joined
                    })
                );
            }
        }
    }
    #[async_trait]
    impl FrameProcessor for PcmOutput {
        fn name(&self) -> &str {
            "GrokRecordedPcmOutput"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if direction == Direction::Downstream && ctx.is_current() {
                if let FrameData::TtsAudio(audio)
                | FrameData::OutputAudio(audio)
                | FrameData::SpeechOutputAudio(audio) = &frame.data
                {
                    if audio.sample_rate != 24_000 || audio.num_channels != 1 {
                        return Err(ProcessorError::fatal(
                            "recording requires mono 24000 Hz PCM16",
                        ));
                    }
                    self.buffer(&audio.audio).map_err(ProcessorError::fatal)?;
                }
            }
            // Deliberately forwards original frames, with no Played receipt,
            // bot-speaking receipt, or acknowledge_playback call.
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.seal().map_err(ProcessorError::fatal)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};
        struct Temp(PathBuf);
        impl Temp {
            fn new() -> Self {
                static ID: AtomicU64 = AtomicU64::new(0);
                let path = std::env::temp_dir().join(format!(
                    "grok-pcm-file-tests-{}-{}",
                    std::process::id(),
                    ID.fetch_add(1, Ordering::Relaxed)
                ));
                std::fs::create_dir(&path).unwrap();
                Self(path)
            }
        }
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        #[test]
        fn process_stage_only_buffers_and_persistence_is_explicit() {
            let root = Temp::new();
            std::fs::write(root.0.join("in"), [0, 0]).unwrap();
            let input = Input::read(&root.0.join("in")).unwrap();
            let target = Target::prepare(&root.0.join("out"), &input).unwrap();
            let output = PcmOutput::new(4);
            output.create(target, &input).unwrap();
            output.buffer(&[1, 2, 3, 4]).unwrap();
            assert!(output.buffer(&[5, 6]).is_err());
            assert!(std::fs::read(root.0.join("out")).unwrap().is_empty());
            assert_eq!(
                output
                    .recording
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .buffer
                    .len(),
                4
            );
            output.seal().unwrap();
            assert!(output.buffer(&[5, 6]).is_err());
            output.persist().unwrap();
            assert_eq!(std::fs::read(root.0.join("out")).unwrap(), [1, 2, 3, 4]);
        }
        #[test]
        fn replaced_output_parent_cannot_redirect_create_new() {
            let root = Temp::new();
            std::fs::write(root.0.join("in"), [0, 0]).unwrap();
            std::fs::create_dir(root.0.join("out")).unwrap();
            std::fs::create_dir(root.0.join("other")).unwrap();
            let input = Input::read(&root.0.join("in")).unwrap();
            let target = Target::prepare(&root.0.join("out/pcm"), &input).unwrap();
            std::fs::rename(root.0.join("out"), root.0.join("moved")).unwrap();
            std::os::unix::fs::symlink(root.0.join("other"), root.0.join("out")).unwrap();
            assert!(PcmOutput::new(10).create(target, &input).is_err());
            assert!(!root.0.join("other/pcm").exists());
            assert!(!root.0.join("moved/pcm").exists());
        }
        #[test]
        fn replaced_input_before_output_create_is_rejected() {
            let root = Temp::new();
            std::fs::write(root.0.join("in"), [0, 0]).unwrap();
            let input = Input::read(&root.0.join("in")).unwrap();
            let target = Target::prepare(&root.0.join("out"), &input).unwrap();
            std::fs::rename(root.0.join("in"), root.0.join("old")).unwrap();
            std::fs::write(root.0.join("in"), [1, 1]).unwrap();
            assert!(PcmOutput::new(10).create(target, &input).is_err());
            assert!(!root.0.join("out").exists());
        }
        #[test]
        fn replaced_parent_preserves_buffer_on_original_fd_and_fails_path_identity() {
            let root = Temp::new();
            std::fs::write(root.0.join("in"), [0, 0]).unwrap();
            std::fs::create_dir(root.0.join("parent")).unwrap();
            let input = Input::read(&root.0.join("in")).unwrap();
            let target = Target::prepare(&root.0.join("parent/out"), &input).unwrap();
            let output = PcmOutput::new(10);
            output.create(target, &input).unwrap();
            output.buffer(&[6, 7]).unwrap();
            std::fs::rename(root.0.join("parent"), root.0.join("moved")).unwrap();
            std::fs::create_dir(root.0.join("parent")).unwrap();
            std::fs::write(root.0.join("parent/out"), b"keep").unwrap();
            assert!(output.persist().is_err());
            assert_eq!(std::fs::read(root.0.join("moved/out")).unwrap(), [6, 7]);
            assert_eq!(std::fs::read(root.0.join("parent/out")).unwrap(), b"keep");
            let lock = output.recording.lock().unwrap();
            let recording = lock.as_ref().unwrap();
            assert_eq!(recording.persisted, 2);
            assert!(!recording.path_identity_preserved);
            assert!(recording.file_flushed);
        }
        #[test]
        fn replaced_recording_is_not_overwritten_and_old_inode_remains_partial() {
            let root = Temp::new();
            std::fs::write(root.0.join("in"), [0, 0]).unwrap();
            let input = Input::read(&root.0.join("in")).unwrap();
            let target = Target::prepare(&root.0.join("out"), &input).unwrap();
            let output = PcmOutput::new(10);
            output.create(target, &input).unwrap();
            output.buffer(&[2, 3]).unwrap();
            std::fs::rename(root.0.join("out"), root.0.join("partial")).unwrap();
            std::fs::write(root.0.join("out"), b"keep").unwrap();
            output.buffer(&[4, 5]).unwrap();
            assert!(output.persist().is_err());
            assert_eq!(std::fs::read(root.0.join("out")).unwrap(), b"keep");
            assert_eq!(std::fs::read(root.0.join("partial")).unwrap(), [2, 3, 4, 5]);
            let lock = output.recording.lock().unwrap();
            let recording = lock.as_ref().unwrap();
            assert_eq!(recording.persisted, 4);
            assert!(!recording.path_identity_preserved);
            assert!(recording.file_flushed);
            assert_eq!(
                recording.inode,
                std::fs::metadata(root.0.join("partial")).unwrap().ino()
            );
        }
    }
}

#[cfg(target_os = "linux")]
async fn active(options: Options) -> Result<(), &'static str> {
    let mut config = configuration(true, options.timeout)?;
    config.limits.max_input_bytes = MAX_INPUT;
    config.limits.max_audio_chunk_bytes = CHUNK_BYTES;
    config.limits.max_output_bytes = options.max_output;
    let input = files::Input::read(&options.input)?;
    let target = files::Target::prepare(&options.output, &input)?;
    let output = Arc::new(files::PcmOutput::new(options.max_output));
    // Construction is validation-only. No network/output effects precede it.
    let app = GrokRealtimeApp::new(
        config,
        vec![],
        tools(options.echo_tool, options.timeout),
        output.clone(),
    )
    .map_err(|_| "invalid or excessive application configuration")?;
    if let Err(error) = output.create(target, &input) {
        output.report(
            false,
            "output_create",
            options.responses,
            None,
            false,
            false,
        );
        return Err(error);
    }
    let completion = app.completion();
    let running = match app.start().await {
        Ok(running) => running,
        Err(_) => {
            let terminal = completion.wait().await;
            let joined = terminal.as_ref().is_ok_and(|report| report.owners_joined());
            let finalized = joined && output.persist().is_ok();
            output.report(
                false,
                "startup",
                options.responses,
                terminal.as_ref().ok(),
                finalized,
                joined,
            );
            return Err("application startup failed; output is partial");
        }
    };
    let operation = async {
        for chunk in input.bytes.chunks(CHUNK_BYTES) {
            running.append_pcm(chunk).await.map_err(|_| "input")?;
        }
        if options.explicit_run {
            running.run().await.map_err(|_| "run")?;
        }
        running
            .wait_for_speech_responses(options.responses, options.timeout)
            .await
            .map_err(|_| "response_wait")?;
        Ok::<(), &'static str>(())
    };
    let stage = match tokio::time::timeout(options.timeout, operation).await {
        Ok(Ok(())) => None,
        Ok(Err(stage)) => Some(stage),
        Err(_) => Some("timeout"),
    };
    if let Some(stage) = stage {
        let _ = running.cancel().await;
        let terminal = completion.wait().await;
        let joined = terminal.as_ref().is_ok_and(|report| report.owners_joined());
        let finalized = joined && output.persist().is_ok();
        output.report(
            false,
            stage,
            options.responses,
            terminal.as_ref().ok(),
            finalized,
            joined,
        );
        return Err("application cancelled after failure or timeout; output is partial");
    }
    let report = running.end().await;
    let terminal = completion.wait().await;
    let joined = terminal.as_ref().is_ok_and(|report| report.owners_joined());
    let finalized = joined && output.persist().is_ok();
    let success = report.as_ref().is_ok_and(|report| report.successful())
        && terminal.as_ref().is_ok_and(|report| report.successful())
        && finalized;
    output.report(
        success,
        if !joined {
            "owners"
        } else if !finalized {
            "persistence"
        } else if success {
            "joined"
        } else {
            "terminal"
        },
        options.responses,
        terminal.as_ref().ok(),
        finalized,
        joined,
    );
    if success {
        Ok(())
    } else {
        Err("application drain, join or output finalization failed; output is partial")
    }
}
#[cfg(not(target_os = "linux"))]
async fn active(_: Options) -> Result<(), &'static str> {
    Err("race-resistant recorded PCM execution requires Linux")
}

async fn run(args: Vec<OsString>) -> Result<(), &'static str> {
    if args.len() > MAX_ARGS || args.iter().any(|a| a.is_empty() || a.len() > 4096) {
        return Err("invalid bounded arguments; use --help");
    }
    if args.is_empty() {
        println!("disabled; use --help for explicit opt-in recorded PCM execution");
        return Ok(());
    }
    if matches!(args.as_slice(), [a] if a == "--help") {
        println!("{HELP}");
        return Ok(());
    }
    if matches!(args.as_slice(), [a] if a == "--check") {
        let config = configuration(false, Duration::from_secs(30))?;
        // This sink is validation-only and can neither record nor start a task.
        struct ValidationSink;
        #[async_trait]
        impl FrameProcessor for ValidationSink {
            fn name(&self) -> &str {
                "GrokConfigurationCheck"
            }
            async fn process(
                &self,
                frame: Frame,
                direction: Direction,
                ctx: &ProcessorContext,
            ) -> Result<(), ProcessorError> {
                ctx.push(frame, direction).await
            }
        }
        GrokRealtimeApp::new(config, vec![], vec![], Arc::new(ValidationSink))
            .map_err(|_| "invalid application configuration")?;
        println!("configuration valid; no credentials read, network or file effects");
        return Ok(());
    }
    active(options(&args)?).await
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args_os().skip(1).take(MAX_ARGS + 1).collect();
    if let Err(error) = run(args).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
