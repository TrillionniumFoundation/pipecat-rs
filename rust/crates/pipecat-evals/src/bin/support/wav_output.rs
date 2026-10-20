//! Trusted-local, no-clobber publication of bounded diagnostic PCM WAVs.
#[cfg(unix)]
mod native {
    use pipecat_evals::{BotAudioRun, EvalError, TimelineRecordingRun};
    use std::{
        fs::{self, File, OpenOptions},
        io::Write,
        os::unix::fs::{MetadataExt, OpenOptionsExt},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);

    pub struct Output {
        destination: PathBuf,
        temporary: PathBuf,
        file: Option<File>,
        identity: (u64, u64),
        label: &'static str,
    }
    impl Output {
        pub fn prepare(path: &str) -> Result<Self, EvalError> {
            Self::prepare_labeled(path, "bot audio WAV")
        }
        pub fn prepare_recording(path: &str) -> Result<Self, EvalError> {
            Self::prepare_labeled(path, "timeline recording WAV")
        }
        fn prepare_labeled(path: &str, label: &'static str) -> Result<Self, EvalError> {
            let path = Path::new(path);
            let name = path
                .file_name()
                .ok_or_else(|| EvalError::Scenario(format!("{label} needs a file path")))?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let parent = parent.canonicalize()?;
            if !parent.is_dir() {
                return Err(EvalError::Scenario(format!(
                    "{label} parent must be a directory"
                )));
            }
            let destination = parent.join(name);
            match fs::symlink_metadata(&destination) {
                Ok(_) => {
                    return Err(EvalError::Scenario(format!(
                        "{label} destination already exists"
                    )))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            for _ in 0..32 {
                let temporary = parent.join(format!(
                    ".pipecat-bot-audio-{}-{}.tmp",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                let file = match OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&temporary)
                {
                    Ok(file) => file,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error.into()),
                };
                let metadata = file.metadata().map_err(|error| {
                    EvalError::Scenario(format!(
                        "cannot identify new WAV temporary {}; retained for inspection: {error}",
                        temporary.display()
                    ))
                })?;
                return Ok(Self {
                    destination,
                    temporary,
                    file: Some(file),
                    identity: (metadata.dev(), metadata.ino()),
                    label,
                });
            }
            Err(EvalError::Scenario(format!(
                "{label} temporary name collisions"
            )))
        }
        fn owns_temporary(&self) -> bool {
            fs::symlink_metadata(&self.temporary)
                .is_ok_and(|m| m.is_file() && (m.dev(), m.ino()) == self.identity)
        }
        pub fn publish(self, run: &BotAudioRun) -> Result<Option<usize>, EvalError> {
            self.publish_bytes(run.to_wav()?.as_deref(), 16 * 1024 * 1024 + 44)
        }
        pub fn publish_recording(
            self,
            run: &TimelineRecordingRun,
        ) -> Result<Option<usize>, EvalError> {
            self.publish_bytes(run.to_wav()?.as_deref(), 3_840_044)
        }
        fn publish_bytes(
            mut self,
            wav: Option<&[u8]>,
            max_bytes: usize,
        ) -> Result<Option<usize>, EvalError> {
            let Some(wav) = wav else {
                return Ok(None);
            };
            // Both typed renderers validate PCM before allocating their WAV.
            // Keep the shared disk writer independently bounded as well.
            if wav.len() > max_bytes {
                return Err(EvalError::Limit("WAV publication bytes"));
            }
            let file = self.file.as_mut().expect("owned WAV file");
            file.write_all(wav)?;
            file.flush()?;
            file.sync_all()?;
            if !self.owns_temporary() {
                return Err(EvalError::Scenario(format!(
                    "{} temporary identity changed",
                    self.label
                )));
            }
            // Atomic same-directory no-clobber publication. Existing files and
            // symlinks, including a target created during the run, are never replaced.
            fs::hard_link(&self.temporary, &self.destination)?;
            let published = (|| -> Result<(), EvalError> {
                let metadata = fs::symlink_metadata(&self.destination)?;
                if !metadata.is_file() || (metadata.dev(), metadata.ino()) != self.identity {
                    return Err(EvalError::Scenario("published identity changed".into()));
                }
                self.file.take();
                if !self.owns_temporary() {
                    return Err(EvalError::Scenario(
                        "temporary identity changed after publication".into(),
                    ));
                }
                fs::remove_file(&self.temporary)?;
                File::open(self.destination.parent().expect("canonical output parent"))?
                    .sync_all()?;
                Ok(())
            })();
            published.map_err(|error| {
                EvalError::Scenario(format!(
                    "WAV file was created at {} but cleanup or persistence failed: {error}",
                    self.destination.display()
                ))
            })?;
            Ok(Some(wav.len()))
        }
    }
    impl Drop for Output {
        fn drop(&mut self) {
            self.file.take();
            if self.owns_temporary() {
                let _ = fs::remove_file(&self.temporary);
            }
        }
    }
}
#[cfg(unix)]
pub use native::Output;

#[cfg(not(unix))]
pub struct Output;
#[cfg(not(unix))]
impl Output {
    pub fn prepare(_: &str) -> Result<Self, pipecat_evals::EvalError> {
        Err(pipecat_evals::EvalError::Unsupported(
            "bot audio WAV publication requires Unix".into(),
        ))
    }
    pub fn publish(
        self,
        _: &pipecat_evals::BotAudioRun,
    ) -> Result<Option<usize>, pipecat_evals::EvalError> {
        unreachable!("non-Unix preparation rejects before connection")
    }
    pub fn prepare_recording(_: &str) -> Result<Self, pipecat_evals::EvalError> {
        Err(pipecat_evals::EvalError::Unsupported(
            "timeline recording WAV publication requires Unix".into(),
        ))
    }
    pub fn publish_recording(
        self,
        _: &pipecat_evals::TimelineRecordingRun,
    ) -> Result<Option<usize>, pipecat_evals::EvalError> {
        unreachable!("non-Unix preparation rejects before connection")
    }
}
