//! Explicit private, Unix-local cache for complete scripted user utterances.
//!
//! The namespace is a trusted account boundary. Advisory locking and fd-relative
//! no-follow operations do not protect against a malicious same-UID process,
//! filesystem owner, root, or hostile mount replacement. Hash names do not make
//! the retained speech anonymous. No transcript or credential is persisted.
use crate::RecordedAudio;
use sha2::{Digest, Sha256};
use std::{
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        mpsc, Arc,
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

const MAX_ENTRIES: usize = 256;
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_WAV_BYTES: usize = 1_920_000 + 65_536;
const POLL_INTERVAL: Duration = Duration::from_millis(5);
const NAMESPACE: &str = ".pipecat-user-speech-v1";

/// Persistent speech is disabled unless an existing private absolute directory
/// is explicitly selected. Limits may only be reduced from 256 namespace files
/// and 64 MiB. Entries and abandoned temporary files both consume these quotas;
/// there is no eviction. The lock file alone is excluded.
#[derive(Clone, Default)]
pub enum UserSpeechCachePolicy {
    #[default]
    Disabled,
    Private {
        directory: PathBuf,
        max_entries: usize,
        max_bytes: u64,
    },
}
impl std::fmt::Debug for UserSpeechCachePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Private {
                max_entries,
                max_bytes,
                ..
            } => f
                .debug_struct("Private")
                .field("directory", &"[redacted]")
                .field("max_entries", max_entries)
                .field("max_bytes", max_bytes)
                .finish(),
        }
    }
}
impl UserSpeechCachePolicy {
    /// Select an existing private Unix directory. This performs only lexical
    /// validation; it does not probe paths, create files, or start a worker.
    pub fn private_directory(directory: impl Into<PathBuf>) -> Result<Self, crate::EvalError> {
        let policy = Self::Private {
            directory: directory.into(),
            max_entries: MAX_ENTRIES,
            max_bytes: MAX_BYTES,
        };
        policy.validate()?;
        Ok(policy)
    }
    pub fn with_limits(
        mut self,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<Self, crate::EvalError> {
        match &mut self {
            Self::Disabled => {
                return Err(crate::EvalError::Scenario(
                    "cache limits require an explicit private directory".into(),
                ))
            }
            Self::Private {
                max_entries: entries,
                max_bytes: bytes,
                ..
            } => {
                *entries = max_entries;
                *bytes = max_bytes;
            }
        }
        self.validate()?;
        Ok(self)
    }
    pub(crate) fn validate(&self) -> Result<(), crate::EvalError> {
        let Self::Private {
            directory,
            max_entries,
            max_bytes,
        } = self
        else {
            return Ok(());
        };
        if !cfg!(unix) {
            return Err(crate::EvalError::Unsupported(
                "private speech caching requires Unix".into(),
            ));
        }
        if *max_entries == 0
            || *max_entries > MAX_ENTRIES
            || *max_bytes == 0
            || *max_bytes > MAX_BYTES
        {
            return Err(crate::EvalError::Scenario("private speech cache limits must be positive and at most 256 files / 67108864 bytes".into()));
        }
        if !directory.is_absolute()
            || directory.as_os_str().len() > 4096
            || directory
                .components()
                .any(|p| !matches!(p, Component::RootDir | Component::Normal(_)))
            || directory.as_os_str().as_encoded_bytes().contains(&0)
        {
            return Err(crate::EvalError::Scenario(
                "private speech cache requires a bounded absolute path without parent components"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// All fields are effective settings. In particular callers resolve omitted
/// language to `en`, default model to `sonic-2`, and supply Cartesia API version.
#[derive(Clone)]
pub(crate) struct CacheKey(String);
impl CacheKey {
    pub(crate) fn new(
        endpoint: &str,
        api_version: &str,
        model: &str,
        voice: &str,
        language: &str,
        exact_text: &str,
    ) -> Self {
        let mut hash = Sha256::new();
        for field in [
            "pipecat-native-user-speech-cache-v1",
            "cartesia-http",
            endpoint,
            api_version,
            model,
            voice,
            language,
            "16000",
            "mono",
            "pcm_s16le",
            "speed=1.0",
            exact_text,
        ] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        Self(format!("{:x}.wav", hash.finalize()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachePersistence {
    NotWritten,
    /// The final file and directory have both been synced; no owned temp remains.
    Durable,
    /// Atomic publication occurred, but subsequent verification or sync failed.
    Uncertain,
}
#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum CacheError {
    #[error("private speech cache deadline expired")]
    Timeout,
    #[error("private speech cache cancelled")]
    Cancelled,
    #[error("private speech cache contains an unsafe path or file")]
    Unsafe,
    #[error("private speech cache namespace quota exceeded")]
    Quota,
    #[error("private speech cache I/O failed during {0}")]
    Io(&'static str),
    #[error("private speech cache transaction protocol failed")]
    Protocol,
    #[error("private speech cache worker failed to join normally")]
    Worker,
}
impl CacheError {
    pub(crate) fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }
}

pub(crate) enum CacheLookup {
    Hit(RecordedAudio),
    Miss,
}
struct Control {
    cancelled: AtomicBool,
    // 0 = pending, 1 = commit admitted, 2 = cancellation won admission.
    admission: AtomicU8,
    persistence: AtomicU8,
    #[cfg(test)]
    before_command: Option<Arc<TestGate>>,
    #[cfg(test)]
    after_empty_command: Option<Arc<TestGate>>,
    #[cfg(test)]
    before_commit: Option<Arc<TestGate>>,
    #[cfg(test)]
    after_admission: Option<Arc<TestGate>>,
    #[cfg(test)]
    fail_at: Option<FailurePoint>,
}
impl Control {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            admission: AtomicU8::new(0),
            persistence: AtomicU8::new(0),
            #[cfg(test)]
            before_command: None,
            #[cfg(test)]
            after_empty_command: None,
            #[cfg(test)]
            before_commit: None,
            #[cfg(test)]
            after_admission: None,
            #[cfg(test)]
            fail_at: None,
        }
    }
    fn cancel(&self) {
        // This CAS, rather than the subsequent flag store, competes with the
        // worker's final admission CAS. Whichever wins defines commit admission.
        let _ = self
            .admission
            .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst);
        self.cancelled.store(true, Ordering::SeqCst);
    }
    fn check(&self, deadline: Instant) -> Result<(), CacheError> {
        if self.cancelled.load(Ordering::SeqCst) || self.admission.load(Ordering::SeqCst) == 2 {
            Err(CacheError::Cancelled)
        } else if Instant::now() >= deadline {
            Err(CacheError::Timeout)
        } else {
            Ok(())
        }
    }
    fn persistence(&self) -> CachePersistence {
        match self.persistence.load(Ordering::SeqCst) {
            1 => CachePersistence::Durable,
            2 => CachePersistence::Uncertain,
            _ => CachePersistence::NotWritten,
        }
    }
    fn persist(&self, value: CachePersistence) {
        self.persistence.store(
            match value {
                CachePersistence::NotWritten => 0,
                CachePersistence::Durable => 1,
                CachePersistence::Uncertain => 2,
            },
            Ordering::SeqCst,
        );
    }
}
struct Commit {
    admitted_at: Instant,
    clip: RecordedAudio,
    deadline: Instant,
}

/// One owned native I/O worker per transaction. No `spawn_blocking` reaper or
/// detached join is used. Async join polls native completion without blocking a
/// Tokio worker, then performs the actual join. Dropping this owner cancels and
/// synchronously joins as a last-resort fallback; uninterruptible kernel I/O can
/// therefore delay Drop, and no hard I/O-cleanup deadline is promised.
pub(crate) struct CacheTransaction {
    control: Arc<Control>,
    commands: Option<mpsc::SyncSender<Commit>>,
    lookup: Option<oneshot::Receiver<Result<CacheLookup, CacheError>>>,
    finished: Option<oneshot::Receiver<Result<CachePersistence, CacheError>>>,
    result: Option<Result<CachePersistence, CacheError>>,
    worker: Option<JoinHandle<()>>,
}
impl CacheTransaction {
    pub(crate) fn start(
        policy: &UserSpeechCachePolicy,
        key: CacheKey,
        production_deadline: Instant,
    ) -> Result<Option<Self>, CacheError> {
        Self::start_control(policy, key, production_deadline, Arc::new(Control::new()))
    }
    fn start_control(
        policy: &UserSpeechCachePolicy,
        key: CacheKey,
        production_deadline: Instant,
        control: Arc<Control>,
    ) -> Result<Option<Self>, CacheError> {
        let UserSpeechCachePolicy::Private {
            directory,
            max_entries,
            max_bytes,
        } = policy
        else {
            return Ok(None);
        };
        policy.validate().map_err(|_| CacheError::Unsafe)?;
        let directory = directory.clone();
        let limits = (*max_entries, *max_bytes);
        let worker_control = control.clone();
        let (commands, command_rx) = mpsc::sync_channel(1);
        let (lookup_tx, lookup) = oneshot::channel();
        let (finished_tx, finished) = oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("user-speech-cache".into())
            .spawn(move || {
                #[cfg(unix)]
                let result = unix::run(
                    directory,
                    limits,
                    key,
                    production_deadline,
                    &worker_control,
                    command_rx,
                    lookup_tx,
                );
                #[cfg(not(unix))]
                let result = {
                    let _ = (
                        directory,
                        limits,
                        key,
                        production_deadline,
                        command_rx,
                        lookup_tx,
                    );
                    Err(CacheError::Unsafe)
                };
                // Sending never waits for an abandoned receiver. Completion is not
                // confused with joining; the owner still waits for is_finished().
                let _ = finished_tx.send(result);
            })
            .map_err(|_| CacheError::Io("worker creation"))?;
        Ok(Some(Self {
            control,
            commands: Some(commands),
            lookup: Some(lookup),
            finished: Some(finished),
            result: None,
            worker: Some(worker),
        }))
    }
    /// The receiver remains owned across cancellation. Once popped, the result
    /// returns in that same poll with no intervening await.
    pub(crate) async fn lookup(&mut self) -> Result<CacheLookup, CacheError> {
        let value = self
            .lookup
            .as_mut()
            .ok_or(CacheError::Protocol)?
            .await
            .map_err(|_| CacheError::Worker)?;
        self.lookup.take();
        value
    }
    /// Enqueue exactly one immutable successful utterance. This is synchronous;
    /// commit admission itself happens later on the worker after final checks.
    pub(crate) fn commit(
        &mut self,
        clip: RecordedAudio,
        publication_deadline: Instant,
    ) -> Result<(), CacheError> {
        self.control.check(publication_deadline)?;
        self.commands
            .take()
            .ok_or(CacheError::Protocol)?
            .try_send(Commit {
                admitted_at: Instant::now(),
                clip,
                deadline: publication_deadline,
            })
            .map_err(|_| CacheError::Protocol)
    }
    pub(crate) fn cancel(&mut self) {
        self.control.cancel();
        // Disconnect wakes recv_timeout immediately, including owner Drop.
        self.commands.take();
    }
    pub(crate) fn persistence(&self) -> CachePersistence {
        self.control.persistence()
    }
    /// Caller must first commit, observe a hit/failure, or cancel. Cancelling this
    /// future retains the command state, completed result, and same native handle.
    pub(crate) async fn join(&mut self) -> Result<CachePersistence, CacheError> {
        if self.result.is_none() {
            if let Some(receiver) = self.finished.as_mut() {
                let result = receiver.await.unwrap_or(Err(CacheError::Worker));
                self.result = Some(result);
                self.finished.take();
            }
        }
        while self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                self.result = Some(Err(CacheError::Worker));
            }
        }
        self.result.clone().unwrap_or(Err(CacheError::Worker))
    }
}
impl Drop for CacheTransaction {
    fn drop(&mut self) {
        self.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use rustix::fs::{self, AtFlags, FileType, FlockOperation, Mode, OFlags, Stat};
    use std::{
        fs::File,
        io::{Read, Write},
        os::fd::OwnedFd,
        sync::atomic::AtomicU64,
    };

    const LOCK: &str = ".lock";
    const MAX_ENUMERATED: usize = 514;
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    fn io(stage: &'static str) -> impl FnOnce(rustix::io::Errno) -> CacheError {
        move |_| CacheError::Io(stage)
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Identity {
        device: u64,
        inode: u64,
    }
    impl Identity {
        // Unix stat field widths differ across supported targets.
        #[allow(clippy::unnecessary_cast)]
        fn of(stat: &Stat) -> Self {
            Self {
                device: stat.st_dev as u64,
                inode: stat.st_ino as u64,
            }
        }
    }
    fn stat(fd: &impl std::os::fd::AsFd) -> Result<Stat, CacheError> {
        fs::fstat(fd).map_err(io("metadata"))
    }
    fn private_dir(stat: &Stat) -> Result<(), CacheError> {
        if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
            || stat.st_uid != rustix::process::geteuid().as_raw()
            || stat.st_mode & 0o7077 != 0
        {
            Err(CacheError::Unsafe)
        } else {
            Ok(())
        }
    }
    // Unix link-count widths differ across supported targets.
    #[allow(clippy::unnecessary_cast)]
    fn private_file(stat: &Stat, allowed_links: u64) -> Result<(), CacheError> {
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_uid != rustix::process::geteuid().as_raw()
            || stat.st_mode & 0o7777 != 0o600
            || stat.st_nlink as u64 != allowed_links
            || stat.st_size < 0
        {
            Err(CacheError::Unsafe)
        } else {
            Ok(())
        }
    }
    fn named_stat(dir: &OwnedFd, name: &str) -> Result<Option<Stat>, CacheError> {
        match fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => Ok(Some(stat)),
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(_) => Err(CacheError::Io("entry metadata")),
        }
    }
    fn verify_named(
        dir: &OwnedFd,
        name: &str,
        identity: Identity,
        links: u64,
    ) -> Result<(), CacheError> {
        let metadata = named_stat(dir, name)?.ok_or(CacheError::Unsafe)?;
        private_file(&metadata, links)?;
        if Identity::of(&metadata) != identity {
            return Err(CacheError::Unsafe);
        }
        Ok(())
    }
    struct Namespace {
        root: OwnedFd,
        dir: OwnedFd,
        identity: Identity,
        lock: File,
        lock_identity: Identity,
    }
    impl Namespace {
        fn open(path: &Path, control: &Control, deadline: Instant) -> Result<Self, CacheError> {
            control.check(deadline)?;
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let mut root = fs::open("/", flags, Mode::empty()).map_err(io("root open"))?;
            for component in path.components() {
                control.check(deadline)?;
                if let Component::Normal(name) = component {
                    root = fs::openat(&root, name, flags, Mode::empty())
                        .map_err(|_| CacheError::Unsafe)?;
                }
            }
            private_dir(&stat(&root)?)?;
            match fs::mkdirat(&root, NAMESPACE, Mode::RWXU) {
                Ok(()) => {
                    fs::fsync(&root).map_err(io("root sync"))?;
                }
                Err(rustix::io::Errno::EXIST) => {}
                Err(_) => return Err(CacheError::Io("namespace creation")),
            }
            let dir = fs::openat(&root, NAMESPACE, flags, Mode::empty())
                .map_err(|_| CacheError::Unsafe)?;
            let metadata = stat(&dir)?;
            private_dir(&metadata)?;
            let identity = Identity::of(&metadata);
            let lock: File = fs::openat(
                &dir,
                LOCK,
                OFlags::RDWR
                    | OFlags::CREATE
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| CacheError::Unsafe)?
            .into();
            let metadata = stat(&lock)?;
            private_file(&metadata, 1)?;
            if metadata.st_size != 0 {
                return Err(CacheError::Unsafe);
            }
            let lock_identity = Identity::of(&metadata);
            loop {
                control.check(deadline)?;
                match fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
                    Ok(()) => break,
                    Err(rustix::io::Errno::WOULDBLOCK) => std::thread::sleep(POLL_INTERVAL),
                    Err(_) => return Err(CacheError::Io("namespace lock")),
                }
            }
            let namespace = Self {
                root,
                dir,
                identity,
                lock,
                lock_identity,
            };
            namespace.verify()?;
            control.check(deadline)?;
            Ok(namespace)
        }
        fn verify(&self) -> Result<(), CacheError> {
            private_dir(&stat(&self.root)?)?;
            let metadata = stat(&self.dir)?;
            private_dir(&metadata)?;
            let named = fs::statat(&self.root, NAMESPACE, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|_| CacheError::Unsafe)?;
            private_dir(&named)?;
            if Identity::of(&metadata) != self.identity || Identity::of(&named) != self.identity {
                return Err(CacheError::Unsafe);
            }
            let lock_stat = stat(&self.lock)?;
            private_file(&lock_stat, 1)?;
            if lock_stat.st_size != 0 || Identity::of(&lock_stat) != self.lock_identity {
                return Err(CacheError::Unsafe);
            }
            verify_named(&self.dir, LOCK, self.lock_identity, 1)
        }
        fn scan(
            &self,
            limits: (usize, u64),
            control: &Control,
            deadline: Instant,
        ) -> Result<(usize, u64), CacheError> {
            self.verify()?;
            let mut entries = 0usize;
            let mut bytes = 0u64;
            let directory = fs::Dir::read_from(&self.dir).map_err(io("namespace enumeration"))?;
            for (enumerated, entry) in directory.enumerate() {
                control.check(deadline)?;
                if enumerated >= MAX_ENUMERATED {
                    return Err(CacheError::Quota);
                }
                let entry = entry.map_err(io("namespace enumeration"))?;
                let name = entry.file_name().to_str().map_err(|_| CacheError::Unsafe)?;
                if name == "." || name == ".." {
                    continue;
                }
                if name == LOCK {
                    verify_named(&self.dir, LOCK, self.lock_identity, 1)?;
                    continue;
                }
                let recognized = name.strip_suffix(".wav").is_some_and(is_digest)
                    || name.strip_prefix(".tmp-").is_some_and(is_digest);
                if !recognized {
                    return Err(CacheError::Unsafe);
                }
                let metadata = named_stat(&self.dir, name)?.ok_or(CacheError::Unsafe)?;
                private_file(&metadata, 1)?;
                if metadata.st_size as u64 > MAX_WAV_BYTES as u64 {
                    return Err(CacheError::Quota);
                }
                entries = entries.checked_add(1).ok_or(CacheError::Quota)?;
                bytes = bytes
                    .checked_add(metadata.st_size as u64)
                    .ok_or(CacheError::Quota)?;
                if entries > limits.0 || bytes > limits.1 {
                    return Err(CacheError::Quota);
                }
            }
            Ok((entries, bytes))
        }
    }
    fn is_digest(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
    struct Corrupt {
        file: File,
        identity: Identity,
        size: u64,
    }
    enum Found {
        Hit(RecordedAudio),
        Miss(Option<Corrupt>),
    }
    fn lookup(
        namespace: &Namespace,
        key: &CacheKey,
        control: &Control,
        deadline: Instant,
    ) -> Result<Found, CacheError> {
        control.check(deadline)?;
        let Some(before) = named_stat(&namespace.dir, &key.0)? else {
            return Ok(Found::Miss(None));
        };
        private_file(&before, 1)?;
        if before.st_size as u64 > MAX_WAV_BYTES as u64 {
            return Err(CacheError::Quota);
        }
        let mut file: File = fs::openat(
            &namespace.dir,
            key.0.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| CacheError::Unsafe)?
        .into();
        let opened = stat(&file)?;
        private_file(&opened, 1)?;
        let identity = Identity::of(&opened);
        if identity != Identity::of(&before) || opened.st_size != before.st_size {
            return Err(CacheError::Unsafe);
        }
        let mut bytes = Vec::with_capacity(opened.st_size as usize);
        (&mut file)
            .take(MAX_WAV_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CacheError::Io("entry read"))?;
        control.check(deadline)?;
        verify_named(&namespace.dir, &key.0, identity, 1)?;
        let after = stat(&file)?;
        private_file(&after, 1)?;
        if bytes.len() > MAX_WAV_BYTES
            || bytes.len() as u64 != opened.st_size as u64
            || after.st_size != opened.st_size
        {
            return Err(CacheError::Unsafe);
        }
        match RecordedAudio::from_wav(&bytes) {
            Ok(clip) => Ok(Found::Hit(clip)),
            Err(_) => Ok(Found::Miss(Some(Corrupt {
                file,
                identity,
                size: opened.st_size as u64,
            }))),
        }
    }
    pub(super) fn run(
        path: PathBuf,
        limits: (usize, u64),
        key: CacheKey,
        deadline: Instant,
        control: &Control,
        commands: mpsc::Receiver<Commit>,
        lookup_result: oneshot::Sender<Result<CacheLookup, CacheError>>,
    ) -> Result<CachePersistence, CacheError> {
        let prepared: Result<_, CacheError> = (|| {
            let namespace = Namespace::open(&path, control, deadline)?;
            let usage = namespace.scan(limits, control, deadline)?;
            let found = lookup(&namespace, &key, control, deadline)?;
            Ok((namespace, usage, found))
        })();
        let (namespace, usage, found) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = lookup_result.send(Err(error.clone()));
                return Err(error);
            }
        };
        let corrupt = match found {
            Found::Hit(clip) => {
                if let Err(error) = control.check(deadline) {
                    let _ = lookup_result.send(Err(error.clone()));
                    return Err(error);
                }
                let _ = lookup_result.send(Ok(CacheLookup::Hit(clip)));
                return Ok(CachePersistence::NotWritten);
            }
            Found::Miss(corrupt) => corrupt,
        };
        // Reserve the maximum bounded WAV while still retaining all old files,
        // including a corrupt replacement target and unrelated abandoned temps.
        if usage.0.checked_add(1).is_none_or(|n| n > limits.0)
            || usage
                .1
                .checked_add(MAX_WAV_BYTES as u64)
                .is_none_or(|n| n > limits.1)
        {
            let _ = lookup_result.send(Err(CacheError::Quota));
            return Err(CacheError::Quota);
        }
        if lookup_result.send(Ok(CacheLookup::Miss)).is_err() {
            return Err(CacheError::Cancelled);
        }
        #[cfg(test)]
        if let Some(gate) = &control.before_command {
            gate.block();
        }
        let command = loop {
            // A timely admitted command can be processed after production's
            // deadline if this native thread was descheduled. Look for it before
            // declaring timeout; the command records its original admission.
            match commands.try_recv() {
                Ok(command) => break command,
                Err(mpsc::TryRecvError::Disconnected) => return Err(CacheError::Cancelled),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            #[cfg(test)]
            if let Some(gate) = &control.after_empty_command {
                gate.block();
            }
            match control.check(deadline) {
                Ok(()) => {}
                Err(CacheError::Timeout) => {
                    // The owner may have queued a timely command between the
                    // first empty poll and this deadline check. Make one final
                    // nonblocking receive before declaring production timeout.
                    match commands.try_recv() {
                        Ok(command) => break command,
                        Err(_) => return Err(CacheError::Timeout),
                    }
                }
                Err(error) => return Err(error),
            }
            match commands
                .recv_timeout(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())))
            {
                Ok(command) => break command,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(CacheError::Cancelled),
            }
        };
        if command.admitted_at >= deadline {
            return Err(CacheError::Timeout);
        }
        // Once successful PCM was timely admitted, only the captured publication
        // deadline governs persistence. Cancellation is still checked by publish.
        publish(&namespace, limits, &key, corrupt, command, control)
    }
    struct Temp {
        name: String,
        file: File,
        identity: Identity,
    }
    fn new_temp(namespace: &Namespace) -> Result<Temp, CacheError> {
        for _ in 0..8 {
            let mut hash = Sha256::new();
            hash.update(std::process::id().to_be_bytes());
            hash.update(TEMP_COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes());
            hash.update(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .to_be_bytes(),
            );
            let name = format!(".tmp-{:x}", hash.finalize());
            match fs::openat(
                &namespace.dir,
                name.as_str(),
                OFlags::WRONLY
                    | OFlags::CREATE
                    | OFlags::EXCL
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(fd) => {
                    let file: File = fd.into();
                    let metadata = stat(&file)?;
                    private_file(&metadata, 1)?;
                    return Ok(Temp {
                        name,
                        file,
                        identity: Identity::of(&metadata),
                    });
                }
                Err(rustix::io::Errno::EXIST) => continue,
                Err(_) => return Err(CacheError::Io("temporary file creation")),
            }
        }
        Err(CacheError::Io("temporary file collision"))
    }
    fn remove_temp(namespace: &Namespace, temp: &Temp, links: u64) -> Result<(), CacheError> {
        let metadata = stat(&temp.file)?;
        private_file(&metadata, links)?;
        if Identity::of(&metadata) != temp.identity {
            return Err(CacheError::Unsafe);
        }
        verify_named(&namespace.dir, &temp.name, temp.identity, links)?;
        fs::unlinkat(&namespace.dir, temp.name.as_str(), AtFlags::empty())
            .map_err(io("temporary file cleanup"))
    }
    fn publish(
        namespace: &Namespace,
        limits: (usize, u64),
        key: &CacheKey,
        corrupt: Option<Corrupt>,
        command: Commit,
        control: &Control,
    ) -> Result<CachePersistence, CacheError> {
        control.check(command.deadline)?;
        let pcm = command.clip.pcm_bytes();
        if pcm.is_empty() || pcm.len() > 1_920_000 || !pcm.len().is_multiple_of(2) {
            return Err(CacheError::Protocol);
        }
        let wav = pipecat_audio::pcm_to_wav(pcm, 16000, 1).map_err(|_| CacheError::Protocol)?;
        let usage = namespace.scan(limits, control, command.deadline)?;
        if usage.0.checked_add(1).is_none_or(|n| n > limits.0)
            || usage
                .1
                .checked_add(wav.len() as u64)
                .is_none_or(|n| n > limits.1)
        {
            return Err(CacheError::Quota);
        }
        let mut temp = Some(new_temp(namespace)?);
        let mut hard_linked = false;
        let result = (|| {
            let current = temp.as_mut().ok_or(CacheError::Protocol)?;
            #[cfg(test)]
            control.fail(FailurePoint::Write)?;
            current
                .file
                .write_all(&wav)
                .map_err(|_| CacheError::Io("temporary file write"))?;
            current
                .file
                .flush()
                .map_err(|_| CacheError::Io("temporary file flush"))?;
            #[cfg(test)]
            control.fail(FailurePoint::FileSync)?;
            current
                .file
                .sync_all()
                .map_err(|_| CacheError::Io("temporary file sync"))?;
            #[cfg(test)]
            if let Some(gate) = &control.before_commit {
                gate.block();
            }
            namespace.verify()?;
            let metadata = stat(&current.file)?;
            private_file(&metadata, 1)?;
            if metadata.st_size as u64 != wav.len() as u64
                || Identity::of(&metadata) != current.identity
            {
                return Err(CacheError::Unsafe);
            }
            verify_named(&namespace.dir, &current.name, current.identity, 1)?;
            match &corrupt {
                Some(old) => {
                    let metadata = stat(&old.file)?;
                    private_file(&metadata, 1)?;
                    if Identity::of(&metadata) != old.identity
                        || metadata.st_size as u64 != old.size
                    {
                        return Err(CacheError::Unsafe);
                    }
                    verify_named(&namespace.dir, &key.0, old.identity, 1)?;
                }
                None if named_stat(&namespace.dir, &key.0)?.is_some() => {
                    return Err(CacheError::Unsafe)
                }
                None => {}
            }
            control.check(command.deadline)?;
            // Final worker-side no-await admission decision. Cancellation that
            // wins this single CAS prevents publication. Later cancellation may
            // retain valid admitted output, but cannot turn uncertain I/O into
            // a durable success or claim rollback.
            control
                .admission
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .map_err(|_| CacheError::Cancelled)?;
            #[cfg(test)]
            if let Some(gate) = &control.after_admission {
                gate.block();
            }
            if corrupt.is_some() {
                fs::renameat(
                    &namespace.dir,
                    current.name.as_str(),
                    &namespace.dir,
                    key.0.as_str(),
                )
                .map_err(io("corrupt entry replacement"))?;
                control.persist(CachePersistence::Uncertain);
                let identity = current.identity;
                temp.take();
                verify_named(&namespace.dir, &key.0, identity, 1)?;
            } else {
                fs::linkat(
                    &namespace.dir,
                    current.name.as_str(),
                    &namespace.dir,
                    key.0.as_str(),
                    AtFlags::empty(),
                )
                .map_err(io("no-clobber publication"))?;
                hard_linked = true;
                control.persist(CachePersistence::Uncertain);
                verify_named(&namespace.dir, &key.0, current.identity, 2)?;
                #[cfg(test)]
                control.fail(FailurePoint::Cleanup)?;
                remove_temp(namespace, current, 2)?;
                let identity = current.identity;
                temp.take();
                verify_named(&namespace.dir, &key.0, identity, 1)?;
            }
            // Temp unlink precedes the final directory sync: a reported durable
            // entry must not retain a second hard link after crash recovery.
            #[cfg(test)]
            control.fail(FailurePoint::DirectorySync)?;
            fs::fsync(&namespace.dir).map_err(io("publication directory sync"))?;
            control.persist(CachePersistence::Durable);
            if Instant::now() >= command.deadline {
                return Err(CacheError::Timeout);
            }
            Ok(CachePersistence::Durable)
        })();
        if let Some(temp) = temp {
            #[cfg(test)]
            control.fail(FailurePoint::Cleanup)?;
            remove_temp(namespace, &temp, if hard_linked { 2 } else { 1 })?;
            fs::fsync(&namespace.dir).map_err(io("cleanup directory sync"))?;
        }
        result
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    Write,
    FileSync,
    DirectorySync,
    Cleanup,
}
#[cfg(test)]
impl Control {
    fn fail(&self, point: FailurePoint) -> Result<(), CacheError> {
        if self.fail_at == Some(point) {
            Err(CacheError::Io("injected test failure"))
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
#[derive(Default)]
struct TestGate {
    state: std::sync::Mutex<(bool, bool)>,
    wake: std::sync::Condvar,
}
#[cfg(test)]
impl TestGate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.wake.notify_all();
        while !state.1 {
            state = self.wake.wait(state).unwrap();
        }
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.lock().unwrap().0 {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("native test gate was not reached");
    }
}
#[cfg(test)]
struct ReleaseGate(Arc<TestGate>);
#[cfg(test)]
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        os::unix::{
            ffi::OsStrExt,
            fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        },
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Root(PathBuf);
    impl Root {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "pipecat-speech-cache-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            Self(path)
        }
        fn policy(&self) -> UserSpeechCachePolicy {
            UserSpeechCachePolicy::private_directory(&self.0).unwrap()
        }
        fn namespace(&self) -> PathBuf {
            self.0.join(NAMESPACE)
        }
        fn entry(&self) -> PathBuf {
            self.namespace().join(key().0)
        }
        fn create_namespace(&self) {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(self.namespace())
                .unwrap();
        }
    }
    impl Drop for Root {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn key() -> CacheKey {
        CacheKey::new(
            "http://127.0.0.1:1234",
            "2025-04-16",
            "sonic-2",
            "fixture-voice",
            "en",
            "Exact PRIVATE text",
        )
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }
    fn clip() -> RecordedAudio {
        RecordedAudio::from_pcm16(&[0x12, 0x34, 0x56, 0x78, 0xab, 0xcd]).unwrap()
    }
    fn transaction(root: &Root) -> CacheTransaction {
        CacheTransaction::start(&root.policy(), key(), deadline())
            .unwrap()
            .unwrap()
    }
    fn write_private(path: &Path, contents: &[u8]) {
        use std::io::Write;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(contents).unwrap();
    }
    async fn cold(root: &Root) {
        let mut tx = transaction(root);
        assert!(matches!(tx.lookup().await.unwrap(), CacheLookup::Miss));
        tx.commit(clip(), deadline()).unwrap();
        assert_eq!(tx.join().await.unwrap(), CachePersistence::Durable);
        assert!(tx.worker.is_none());
    }

    #[test]
    fn disabled_and_policy_validation_have_no_filesystem_effects() {
        assert!(
            CacheTransaction::start(&UserSpeechCachePolicy::Disabled, key(), deadline())
                .unwrap()
                .is_none()
        );
        let root = Root::new();
        let missing = root.0.join("does-not-exist");
        let policy = UserSpeechCachePolicy::private_directory(&missing).unwrap();
        assert!(!missing.exists());
        assert!(policy.clone().with_limits(257, MAX_BYTES).is_err());
        assert!(policy.clone().with_limits(1, 0).is_err());
        assert!(policy.with_limits(1, 1).is_ok());
        for path in [
            PathBuf::from("relative"),
            root.0.join("../escape"),
            PathBuf::from(std::ffi::OsStr::from_bytes(b"/tmp/a\0b")),
        ] {
            assert!(UserSpeechCachePolicy::private_directory(path).is_err());
        }
        assert!(UserSpeechCachePolicy::Disabled.with_limits(1, 1).is_err());
        assert!(!root.namespace().exists());
    }
    #[test]
    fn identity_covers_exact_fields_and_unambiguous_boundaries() {
        let fields = [
            "http://127.0.0.1:1",
            "2025-04-16",
            "sonic-2",
            "voice",
            "en",
            "Case sensitive text",
        ];
        let make = |f: [&str; 6]| CacheKey::new(f[0], f[1], f[2], f[3], f[4], f[5]).0;
        let original = make(fields);
        assert_eq!(original.len(), 68);
        for index in 0..fields.len() {
            let mut changed = fields;
            changed[index] = "different";
            assert_ne!(original, make(changed));
        }
        let mut lower = fields;
        lower[5] = "case sensitive text";
        assert_ne!(original, make(lower));
        let mut a = fields;
        a[3] = "ab";
        a[4] = "c";
        let mut b = fields;
        b[3] = "a";
        b[4] = "bc";
        assert_ne!(make(a), make(b));
        assert!(!original.contains("text"));
    }
    #[tokio::test]
    async fn cold_publication_and_independent_warm_read_are_private_exact_and_joined() {
        let root = Root::new();
        cold(&root).await;
        let entries: Vec<_> = fs::read_dir(root.namespace()).unwrap().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            fs::metadata(root.namespace()).unwrap().mode() & 0o777,
            0o700
        );
        for entry in entries {
            let metadata = entry.unwrap().metadata().unwrap();
            assert_eq!(metadata.mode() & 0o777, 0o600);
            assert_eq!(metadata.nlink(), 1);
        }
        let contents = fs::read(root.entry()).unwrap();
        assert!(!contents
            .windows(b"Exact PRIVATE text".len())
            .any(|part| part == b"Exact PRIVATE text"));
        let mut warm = transaction(&root);
        let CacheLookup::Hit(actual) = warm.lookup().await.unwrap() else {
            panic!("expected independent warm hit")
        };
        assert_eq!(actual.pcm_bytes(), clip().pcm_bytes());
        assert_eq!(warm.join().await.unwrap(), CachePersistence::NotWritten);
        assert_eq!(warm.join().await.unwrap(), CachePersistence::NotWritten);
    }
    #[tokio::test]
    async fn namespace_lock_spans_cold_production_then_waiter_reads_hit() {
        let root = Root::new();
        let mut first = transaction(&root);
        assert!(matches!(first.lookup().await.unwrap(), CacheLookup::Miss));
        let mut second = transaction(&root);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), second.lookup())
                .await
                .is_err()
        );
        first.commit(clip(), deadline()).unwrap();
        first.join().await.unwrap();
        assert!(matches!(
            second.lookup().await.unwrap(),
            CacheLookup::Hit(_)
        ));
        second.join().await.unwrap();
    }
    #[tokio::test]
    async fn original_lock_deadline_expires_without_refresh_and_waiter_cancels() {
        let root = Root::new();
        let mut first = transaction(&root);
        first.lookup().await.unwrap();
        let mut second = CacheTransaction::start(
            &root.policy(),
            key(),
            Instant::now() + Duration::from_millis(40),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(second.lookup().await, Err(CacheError::Timeout)));
        assert!(matches!(second.join().await, Err(CacheError::Timeout)));
        let mut third = transaction(&root);
        third.cancel();
        assert!(matches!(third.join().await, Err(CacheError::Cancelled)));
        first.cancel();
        assert!(matches!(first.join().await, Err(CacheError::Cancelled)));
        assert!(!root.entry().exists());
    }
    #[tokio::test]
    async fn command_disconnect_wakes_the_native_worker() {
        let root = Root::new();
        let mut tx = transaction(&root);
        tx.lookup().await.unwrap();
        tx.commands.take();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), tx.join())
                .await
                .unwrap(),
            Err(CacheError::Cancelled)
        ));
        assert!(tx.worker.is_none());
    }
    #[tokio::test]
    async fn corrupt_regular_entry_is_replaced_but_old_and_temp_are_both_reserved() {
        let root = Root::new();
        root.create_namespace();
        write_private(&root.entry(), b"bad wav");
        let old_inode = fs::metadata(root.entry()).unwrap().ino();
        let restrictive = root.policy().with_limits(1, MAX_BYTES).unwrap();
        let mut denied = CacheTransaction::start(&restrictive, key(), deadline())
            .unwrap()
            .unwrap();
        assert!(matches!(denied.lookup().await, Err(CacheError::Quota)));
        assert!(matches!(denied.join().await, Err(CacheError::Quota)));
        assert_eq!(fs::read(root.entry()).unwrap(), b"bad wav");
        cold(&root).await;
        assert_ne!(fs::metadata(root.entry()).unwrap().ino(), old_inode);
        assert_eq!(fs::metadata(root.entry()).unwrap().nlink(), 1);
    }
    #[tokio::test]
    async fn abandoned_temps_count_toward_entry_and_byte_quota_and_are_never_deleted() {
        let root = Root::new();
        root.create_namespace();
        let stale = root.namespace().join(format!(".tmp-{}", "a".repeat(64)));
        write_private(&stale, b"abandoned");
        for policy in [
            root.policy().with_limits(1, MAX_BYTES).unwrap(),
            root.policy()
                .with_limits(MAX_ENTRIES, MAX_WAV_BYTES as u64 + 8)
                .unwrap(),
        ] {
            let mut tx = CacheTransaction::start(&policy, key(), deadline())
                .unwrap()
                .unwrap();
            assert!(matches!(tx.lookup().await, Err(CacheError::Quota)));
            assert!(matches!(tx.join().await, Err(CacheError::Quota)));
            assert_eq!(fs::read(&stale).unwrap(), b"abandoned");
        }
    }
    #[tokio::test]
    async fn oversized_file_is_rejected_before_read_and_wrong_rate_is_repaired() {
        let root = Root::new();
        root.create_namespace();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.entry())
            .unwrap();
        file.set_len(MAX_WAV_BYTES as u64 + 1).unwrap();
        drop(file);
        let mut denied = transaction(&root);
        assert!(matches!(denied.lookup().await, Err(CacheError::Quota)));
        assert!(matches!(denied.join().await, Err(CacheError::Quota)));
        fs::remove_file(root.entry()).unwrap();
        write_private(
            &root.entry(),
            &pipecat_audio::pcm_to_wav(clip().pcm_bytes(), 24000, 1).unwrap(),
        );
        cold(&root).await;
        let mut tx = transaction(&root);
        let CacheLookup::Hit(loaded) = tx.lookup().await.unwrap() else {
            panic!("expected repaired warm entry")
        };
        assert_eq!(loaded.pcm_bytes(), clip().pcm_bytes());
        tx.join().await.unwrap();
    }
    #[tokio::test]
    async fn no_clobber_publication_preserves_a_target_created_after_admission() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            after_admission: Some(gate.clone()),
            ..Control::new()
        });
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        tx.commit(clip(), deadline()).unwrap();
        gate.entered().await;
        write_private(&root.entry(), b"unexpected target");
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Io(_))));
        assert_eq!(fs::read(root.entry()).unwrap(), b"unexpected target");
        assert_eq!(tx.persistence(), CachePersistence::NotWritten);
        assert_eq!(fs::read_dir(root.namespace()).unwrap().count(), 2);
    }
    #[tokio::test]
    async fn existing_hit_is_rejected_when_other_files_violate_namespace_quota() {
        let root = Root::new();
        cold(&root).await;
        write_private(
            &root.namespace().join(format!(".tmp-{}", "b".repeat(64))),
            b"x",
        );
        let policy = root.policy().with_limits(1, MAX_BYTES).unwrap();
        let mut tx = CacheTransaction::start(&policy, key(), deadline())
            .unwrap()
            .unwrap();
        assert!(matches!(tx.lookup().await, Err(CacheError::Quota)));
        assert!(matches!(tx.join().await, Err(CacheError::Quota)));
    }
    #[tokio::test]
    async fn unsafe_root_component_namespace_and_mode_are_rejected_without_chmod() {
        use std::os::unix::fs::symlink;
        let target = Root::new();
        let holder = Root::new();
        let alias = holder.0.join("alias");
        symlink(&target.0, &alias).unwrap();
        for path in [alias.clone(), alias.join("nested")] {
            let mut tx = CacheTransaction::start(
                &UserSpeechCachePolicy::private_directory(path).unwrap(),
                key(),
                deadline(),
            )
            .unwrap()
            .unwrap();
            assert!(matches!(tx.lookup().await, Err(CacheError::Unsafe)));
            tx.join().await.unwrap_err();
        }
        assert!(!target.namespace().exists());
        fs::set_permissions(&target.0, fs::Permissions::from_mode(0o755)).unwrap();
        let mut tx = transaction(&target);
        assert!(matches!(tx.lookup().await, Err(CacheError::Unsafe)));
        tx.join().await.unwrap_err();
        assert_eq!(fs::metadata(&target.0).unwrap().mode() & 0o777, 0o755);
        fs::set_permissions(&target.0, fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&holder.0, target.namespace()).unwrap();
        let mut tx = transaction(&target);
        assert!(matches!(tx.lookup().await, Err(CacheError::Unsafe)));
        tx.join().await.unwrap_err();
        assert!(!holder.0.join(".lock").exists());
    }
    #[tokio::test]
    async fn symlink_fifo_directory_hardlink_public_file_and_unknown_names_are_hard_failures() {
        use std::os::unix::fs::symlink;
        for kind in 0..6 {
            let root = Root::new();
            root.create_namespace();
            let outside = root.0.join("unrelated");
            write_private(&outside, b"do not change");
            match kind {
                0 => symlink(&outside, root.entry()).unwrap(),
                1 => rustix::fs::mknodat(
                    rustix::fs::CWD,
                    root.entry(),
                    rustix::fs::FileType::Fifo,
                    rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                    0,
                )
                .unwrap(),
                2 => fs::create_dir(root.entry()).unwrap(),
                3 => fs::hard_link(&outside, root.entry()).unwrap(),
                4 => {
                    write_private(&root.entry(), b"bad");
                    fs::set_permissions(root.entry(), fs::Permissions::from_mode(0o644)).unwrap();
                }
                _ => write_private(&root.namespace().join("unrelated.txt"), b"leave me"),
            }
            let mut tx = transaction(&root);
            assert!(
                matches!(tx.lookup().await, Err(CacheError::Unsafe)),
                "kind {kind}"
            );
            assert!(matches!(tx.join().await, Err(CacheError::Unsafe)));
            assert_eq!(fs::read(&outside).unwrap(), b"do not change");
        }
    }
    #[tokio::test]
    async fn cancellation_before_begin_does_not_create_a_namespace() {
        let root = Root::new();
        let control = Arc::new(Control::new());
        control.cancel();
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        assert!(matches!(tx.lookup().await, Err(CacheError::Cancelled)));
        assert!(matches!(tx.join().await, Err(CacheError::Cancelled)));
        assert!(!root.namespace().exists());
    }
    #[tokio::test]
    async fn cancellation_before_worker_commit_admission_prevents_publication_and_cleans_temp() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            before_commit: Some(gate.clone()),
            ..Control::new()
        });
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        tx.commit(clip(), deadline()).unwrap();
        gate.entered().await;
        tx.cancel();
        assert!(tokio::time::timeout(Duration::from_millis(20), tx.join())
            .await
            .is_err());
        assert!(tx.worker.is_some());
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Cancelled)));
        assert_eq!(tx.persistence(), CachePersistence::NotWritten);
        assert!(!root.entry().exists());
        assert_eq!(fs::read_dir(root.namespace()).unwrap().count(), 1);
    }
    #[tokio::test]
    async fn cancellation_after_commit_admission_may_retain_valid_durable_output() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            after_admission: Some(gate.clone()),
            ..Control::new()
        });
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        tx.commit(clip(), deadline()).unwrap();
        gate.entered().await;
        tx.cancel();
        gate.release();
        assert_eq!(tx.join().await.unwrap(), CachePersistence::Durable);
        assert_eq!(tx.persistence(), CachePersistence::Durable);
        assert_eq!(
            RecordedAudio::from_wav(&fs::read(root.entry()).unwrap())
                .unwrap()
                .pcm_bytes(),
            clip().pcm_bytes()
        );
    }
    #[tokio::test]
    async fn changed_target_is_never_replaced_at_commit() {
        let root = Root::new();
        root.create_namespace();
        write_private(&root.entry(), b"corrupt original");
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            before_commit: Some(gate.clone()),
            ..Control::new()
        });
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        tx.commit(clip(), deadline()).unwrap();
        gate.entered().await;
        fs::rename(root.entry(), root.0.join("old")).unwrap();
        write_private(&root.entry(), b"replacement must survive");
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Unsafe)));
        assert_eq!(fs::read(root.entry()).unwrap(), b"replacement must survive");
        assert_eq!(tx.persistence(), CachePersistence::NotWritten);
    }
    #[tokio::test]
    async fn write_and_file_sync_failures_never_publish_partial_entries() {
        for point in [FailurePoint::Write, FailurePoint::FileSync] {
            let root = Root::new();
            let control = Arc::new(Control {
                fail_at: Some(point),
                ..Control::new()
            });
            let mut tx =
                CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
                    .unwrap()
                    .unwrap();
            tx.lookup().await.unwrap();
            tx.commit(clip(), deadline()).unwrap();
            assert!(matches!(tx.join().await, Err(CacheError::Io(_))));
            assert_eq!(tx.persistence(), CachePersistence::NotWritten);
            assert!(!root.entry().exists());
            assert_eq!(fs::read_dir(root.namespace()).unwrap().count(), 1);
        }
    }
    #[tokio::test]
    async fn post_commit_directory_sync_or_cleanup_failure_remains_uncertain() {
        for point in [FailurePoint::DirectorySync, FailurePoint::Cleanup] {
            let root = Root::new();
            let control = Arc::new(Control {
                fail_at: Some(point),
                ..Control::new()
            });
            let mut tx =
                CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
                    .unwrap()
                    .unwrap();
            tx.lookup().await.unwrap();
            tx.commit(clip(), deadline()).unwrap();
            assert!(matches!(tx.join().await, Err(CacheError::Io(_))));
            assert_eq!(tx.persistence(), CachePersistence::Uncertain);
            assert!(root.entry().exists());
            if point == FailurePoint::Cleanup {
                assert_eq!(fs::metadata(root.entry()).unwrap().nlink(), 2);
                let mut next = transaction(&root);
                assert!(matches!(next.lookup().await, Err(CacheError::Unsafe)));
                next.join().await.unwrap_err();
            } else {
                assert_eq!(fs::metadata(root.entry()).unwrap().nlink(), 1);
                assert_eq!(fs::read_dir(root.namespace()).unwrap().count(), 2);
            }
        }
    }
    #[tokio::test]
    async fn expired_fixed_publication_deadline_prevents_commit_but_still_joins() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            before_commit: Some(gate.clone()),
            ..Control::new()
        });
        let mut tx = CacheTransaction::start_control(&root.policy(), key(), deadline(), control)
            .unwrap()
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        tx.commit(clip(), Instant::now() + Duration::from_millis(30))
            .unwrap();
        gate.entered().await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Timeout)));
        assert!(!root.entry().exists());
        assert!(tx.worker.is_none());
    }
    #[tokio::test]
    async fn timely_commit_delayed_on_native_worker_uses_fixed_publication_deadline() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            before_command: Some(gate.clone()),
            ..Control::new()
        });
        let production_deadline = Instant::now() + Duration::from_millis(100);
        let mut tx =
            CacheTransaction::start_control(&root.policy(), key(), production_deadline, control)
                .unwrap()
                .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        gate.entered().await;
        tx.commit(clip(), deadline()).unwrap();
        tokio::time::sleep_until(production_deadline + Duration::from_millis(20)).await;
        gate.release();
        assert_eq!(tx.join().await.unwrap(), CachePersistence::Durable);
        assert_eq!(
            RecordedAudio::from_wav(&fs::read(root.entry()).unwrap())
                .unwrap()
                .pcm_bytes(),
            clip().pcm_bytes()
        );
    }
    #[tokio::test]
    async fn timely_command_between_empty_poll_and_timeout_is_not_discarded() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            after_empty_command: Some(gate.clone()),
            ..Control::new()
        });
        let production_deadline = Instant::now() + Duration::from_millis(100);
        let mut tx =
            CacheTransaction::start_control(&root.policy(), key(), production_deadline, control)
                .unwrap()
                .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        gate.entered().await;
        tx.commit(clip(), deadline()).unwrap();
        tokio::time::sleep_until(production_deadline + Duration::from_millis(20)).await;
        gate.release();
        assert_eq!(tx.join().await.unwrap(), CachePersistence::Durable);
        assert_eq!(
            RecordedAudio::from_wav(&fs::read(root.entry()).unwrap())
                .unwrap()
                .pcm_bytes(),
            clip().pcm_bytes()
        );
    }
    #[tokio::test]
    async fn late_commit_cannot_extend_the_original_production_deadline() {
        let root = Root::new();
        let gate = Arc::new(TestGate::default());
        let control = Arc::new(Control {
            before_command: Some(gate.clone()),
            ..Control::new()
        });
        let production_deadline = Instant::now() + Duration::from_millis(100);
        let mut tx =
            CacheTransaction::start_control(&root.policy(), key(), production_deadline, control)
                .unwrap()
                .unwrap();
        let _release = ReleaseGate(gate.clone());
        tx.lookup().await.unwrap();
        gate.entered().await;
        tokio::time::sleep_until(production_deadline + Duration::from_millis(20)).await;
        tx.commit(clip(), deadline()).unwrap();
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Timeout)));
        assert!(!root.entry().exists());
    }
    #[tokio::test]
    async fn final_notification_is_not_join_and_cancelled_join_retains_native_owner() {
        let gate = Arc::new(TestGate::default());
        let worker_gate = gate.clone();
        let control = Arc::new(Control::new());
        let (commands, command_rx) = mpsc::sync_channel(1);
        let (done, finished) = oneshot::channel();
        let worker = std::thread::spawn(move || {
            assert!(command_rx.recv().is_err());
            let _ = done.send(Err(CacheError::Cancelled));
            worker_gate.block();
        });
        let mut tx = CacheTransaction {
            control,
            commands: Some(commands),
            lookup: None,
            finished: Some(finished),
            result: None,
            worker: Some(worker),
        };
        let _release = ReleaseGate(gate.clone());
        tx.cancel();
        gate.entered().await;
        assert!(tokio::time::timeout(Duration::from_millis(20), tx.join())
            .await
            .is_err());
        assert!(tx.worker.is_some());
        assert!(tx.result.is_some());
        gate.release();
        assert!(matches!(tx.join().await, Err(CacheError::Cancelled)));
        assert!(matches!(tx.join().await, Err(CacheError::Cancelled)));
        assert!(tx.worker.is_none());
    }
}
