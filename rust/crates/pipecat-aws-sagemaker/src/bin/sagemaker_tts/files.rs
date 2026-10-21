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
    ancestors: Vec<(PathBuf, u64, u64)>,
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
            ancestors: Vec::new(),
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
                    if directory.ancestors.len() >= 128 {
                        return Err("directory nesting exceeds bound");
                    }
                    directory
                        .ancestors
                        .push((directory.named.clone(), after.dev(), after.ino()));
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
        for (path, device, inode) in &self.ancestors {
            let named =
                std::fs::symlink_metadata(path).map_err(|_| "directory ancestor changed")?;
            if !named.is_dir() || (named.dev(), named.ino()) != (*device, *inode) {
                return Err("directory ancestor identity changed");
            }
        }
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
        if !before.is_file() || before.len() == 0 || before.len() > MAX_INPUT as u64 {
            return Err("input requires regular non-symlink JSON, 1 byte..1 MiB");
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
fn terminal_status(
    success: bool,
    all_skipped: bool,
    has_pcm: bool,
    canceled: bool,
) -> &'static str {
    if success {
        if all_skipped {
            "all_skipped"
        } else {
            "complete"
        }
    } else if has_pcm {
        "partial"
    } else if canceled {
        "canceled"
    } else {
        "failed"
    }
}

pub(super) struct PcmOutput {
    recording: Mutex<Option<Recording>>,
    limit: usize,
    sample_rate: u32,
    max_frame: usize,
}
impl PcmOutput {
    pub(super) fn new(limit: usize, sample_rate: u32, max_frame: usize) -> Self {
        Self {
            recording: Mutex::new(None),
            limit,
            sample_rate,
            max_frame,
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
        path?;
        input.verify()
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
        if audio.len() > self.max_frame
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
            return Err(
                "output path changed; buffered PCM persisted only to the retained original inode",
            );
        }
        Ok(())
    }
    pub(super) fn report(
        &self,
        success: bool,
        stage: &'static str,
        application: Option<&SageMakerReport>,
        output_finalized: bool,
        owners_joined: bool,
        input_unchanged: bool,
    ) {
        let lock = self.recording.lock().ok();
        let recording = lock.as_ref().and_then(|lock| lock.as_ref());
        let all_skipped = application.is_some_and(|r| {
            r.progress.utterances_seen > 0 && r.progress.skipped == r.progress.utterances_seen
        });
        let has_pcm = recording.is_some_and(|r| r.bytes > 0 || r.persisted > 0)
            || application.is_some_and(|r| r.progress.pcm_bytes_delivered > 0);
        let canceled = application.is_some_and(|r| {
            matches!(
                r.state,
                pipecat_core::WorkerState::Cancelled | pipecat_core::WorkerState::Stopped
            ) && r
                .primary
                .as_ref()
                .is_none_or(|e| *e == pipecat_aws_sagemaker::Error::Cancelled)
        });
        // Emit only fixed labels, booleans and bounded numeric counters. Neither
        // paths nor provider, credential, text or unrestricted error data escape.
        eprintln!(
            "{}",
            serde_json::json!({
                "status": terminal_status(success, all_skipped, has_pcm, canceled),
                "stage": stage,
                "recording_only": true,
                "terminal_report_received": application.is_some(),
                "application_successful": application.is_some_and(SageMakerReport::is_success),
                "primary_failure": application.is_some_and(|r| r.primary.is_some()),
                "forced_cleanup": application.is_some_and(|r|r.forced_cleanup),
                "join_unproven": application.is_none_or(|r|r.join_unproven),
                "pipeline_error_count": application.map(|r|r.total_pipeline_errors),
                "cleanup_failures": application.map(|r| r.cleanup_errors.len()),
                "utterances_seen": application.map(|r| r.progress.utterances_seen),
                "admitted": application.map(|r| r.progress.admitted),
                "skipped": application.map(|r| r.progress.skipped),
                "completed_at_eof": application.map(|r| r.progress.completed_at_eof),
                "owners_released": application.map(|r| r.progress.owners_released),
                "failed": application.map(|r| r.progress.failed),
                "cancelled_or_aborted": application.map(|r| r.progress.cancelled_or_aborted),
                "pcm_bytes_admitted": application.map(|r| r.progress.pcm_bytes_admitted),
                "pcm_bytes_delivered_to_memory": application.map(|r| r.progress.pcm_bytes_delivered),
                "pipeline_joined": application.is_some_and(|r| r.pipeline_joined),
                "owners_joined": owners_joined,
                "input_unchanged": input_unchanged,
                "output_finalized": output_finalized,
                "output_path_matches": recording.map(|r| r.path_identity_preserved),
                "retained_file_flushed": recording.map(|r| r.file_flushed),
                "recorded_bytes": recording.map(|r| r.persisted),
                "buffered_bytes": recording.map(|r| r.bytes),
                "buffer_capacity": self.limit,
                "output_device": recording.map(|r| r.device),
                "output_inode": recording.map(|r| r.inode),
            })
        );
    }
}
#[async_trait]
impl FrameProcessor for PcmOutput {
    fn name(&self) -> &str {
        "SageMakerRecordedPcmOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && ctx.is_current() {
            if let FrameData::TtsAudio(audio) = &frame.data {
                if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
                    return Err(ProcessorError::fatal(
                        "recording requires configured-rate mono PCM16",
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
                "sagemaker-pcm-file-tests-{}-{}",
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
    fn terminal_labels_distinguish_no_audio_failure_cancellation_and_real_partial_pcm() {
        assert_eq!(terminal_status(false, false, false, false), "failed");
        assert_eq!(terminal_status(false, false, false, true), "canceled");
        assert_eq!(terminal_status(false, false, true, false), "partial");
        assert_eq!(terminal_status(false, false, true, true), "partial");
        assert_eq!(terminal_status(true, true, false, false), "all_skipped");
        assert_eq!(terminal_status(true, false, true, false), "complete");
    }
    #[test]
    fn process_stage_only_buffers_and_persistence_is_explicit() {
        let root = Temp::new();
        std::fs::write(root.0.join("in"), [0, 0]).unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("out"), &input).unwrap();
        let output = PcmOutput::new(4, 24_000, 48_000);
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
        assert!(PcmOutput::new(10, 24_000, 48_000)
            .create(target, &input)
            .is_err());
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
        assert!(PcmOutput::new(10, 24_000, 48_000)
            .create(target, &input)
            .is_err());
        assert!(!root.0.join("out").exists());
    }
    #[test]
    fn replaced_parent_preserves_buffer_on_original_fd_and_fails_path_identity() {
        let root = Temp::new();
        std::fs::write(root.0.join("in"), [0, 0]).unwrap();
        std::fs::create_dir(root.0.join("parent")).unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("parent/out"), &input).unwrap();
        let output = PcmOutput::new(10, 24_000, 48_000);
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
        let output = PcmOutput::new(10, 24_000, 48_000);
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
    #[test]
    fn replaced_ancestor_symlink_to_same_directory_is_still_rejected() {
        let root = Temp::new();
        std::fs::create_dir_all(root.0.join("ancestor/parent")).unwrap();
        std::fs::write(root.0.join("in"), b"{}").unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("ancestor/parent/out"), &input).unwrap();
        std::fs::rename(root.0.join("ancestor"), root.0.join("moved")).unwrap();
        std::os::unix::fs::symlink(root.0.join("moved"), root.0.join("ancestor")).unwrap();
        assert!(PcmOutput::new(10, 24_000, 48_000)
            .create(target, &input)
            .is_err());
        assert!(!root.0.join("moved/parent/out").exists());
    }
    #[test]
    fn input_content_mutation_before_create_is_rejected() {
        let root = Temp::new();
        std::fs::write(root.0.join("in"), b"{}").unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("out"), &input).unwrap();
        std::fs::write(root.0.join("in"), b"changed").unwrap();
        assert!(PcmOutput::new(10, 24_000, 48_000)
            .create(target, &input)
            .is_err());
        assert!(!root.0.join("out").exists());
    }
    #[test]
    fn output_write_error_is_not_reported_as_recorded_or_flushed() {
        let root = Temp::new();
        std::fs::write(root.0.join("in"), b"{}").unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("out"), &input).unwrap();
        let output = PcmOutput::new(10, 24_000, 48_000);
        output.create(target, &input).unwrap();
        output.buffer(&[1, 2]).unwrap();
        // Replace only the private test's FD with a read-only FD for the SAME
        // regular inode. Identity verification still passes, write itself fails.
        output.recording.lock().unwrap().as_mut().unwrap().file =
            File::open(root.0.join("out")).unwrap();
        assert!(output.persist().is_err());
        let lock = output.recording.lock().unwrap();
        let recording = lock.as_ref().unwrap();
        assert_eq!(recording.bytes, 2);
        assert_eq!(recording.persisted, 0);
        assert!(!recording.file_flushed);
        assert!(std::fs::read(root.0.join("out")).unwrap().is_empty());
    }
    #[test]
    fn externally_modified_original_inode_is_not_overwritten() {
        let root = Temp::new();
        std::fs::write(root.0.join("in"), b"{}").unwrap();
        let input = Input::read(&root.0.join("in")).unwrap();
        let target = Target::prepare(&root.0.join("out"), &input).unwrap();
        let output = PcmOutput::new(10, 24_000, 48_000);
        output.create(target, &input).unwrap();
        output.buffer(&[1, 2]).unwrap();
        std::fs::write(root.0.join("out"), b"keep").unwrap();
        assert!(output.persist().is_err());
        assert_eq!(std::fs::read(root.0.join("out")).unwrap(), b"keep");
        let lock = output.recording.lock().unwrap();
        assert_eq!(lock.as_ref().unwrap().persisted, 0);
    }
}
