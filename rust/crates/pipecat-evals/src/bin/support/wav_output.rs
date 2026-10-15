//! Trusted-local, no-clobber publication of bounded received bot PCM.
#[cfg(unix)]
mod native {
    use pipecat_evals::{BotAudioRun, EvalError};
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
    }
    impl Output {
        pub fn prepare(path: &str) -> Result<Self, EvalError> {
            let path = Path::new(path);
            let name = path
                .file_name()
                .ok_or_else(|| EvalError::Scenario("bot audio WAV needs a file path".into()))?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let parent = parent.canonicalize()?;
            if !parent.is_dir() {
                return Err(EvalError::Scenario(
                    "bot audio WAV parent must be a directory".into(),
                ));
            }
            let destination = parent.join(name);
            match fs::symlink_metadata(&destination) {
                Ok(_) => {
                    return Err(EvalError::Scenario(
                        "bot audio WAV destination already exists".into(),
                    ))
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
                });
            }
            Err(EvalError::Scenario(
                "bot audio WAV temporary name collisions".into(),
            ))
        }
        fn owns_temporary(&self) -> bool {
            fs::symlink_metadata(&self.temporary)
                .is_ok_and(|m| m.is_file() && (m.dev(), m.ino()) == self.identity)
        }
        pub fn publish(mut self, run: &BotAudioRun) -> Result<Option<usize>, EvalError> {
            let Some(wav) = run.to_wav()? else {
                return Ok(None);
            };
            let file = self.file.as_mut().expect("owned WAV file");
            file.write_all(&wav)?;
            file.flush()?;
            file.sync_all()?;
            if !self.owns_temporary() {
                return Err(EvalError::Scenario(
                    "bot audio WAV temporary identity changed".into(),
                ));
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
}
