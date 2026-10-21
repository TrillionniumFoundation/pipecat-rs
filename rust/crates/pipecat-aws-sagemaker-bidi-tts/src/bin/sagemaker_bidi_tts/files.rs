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
            return Err("input requires bounded regular non-symlink JSON file");
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

pub(super) struct Recording {
    file: File,
    target: Target,
    device: u64,
    inode: u64,
    persisted: usize,
}
impl Recording {
    pub(super) fn create(target: Target, input: &Input) -> Result<Self, &'static str> {
        input.verify()?;
        target.directory.verify()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(target.directory.path(&target.name))
            .map_err(|_| "output create-new failed")?;
        let m = file.metadata().map_err(|_| "output metadata failed")?;
        let recording = Self {
            file,
            target,
            device: m.dev(),
            inode: m.ino(),
            persisted: 0,
        };
        recording.verify()?;
        input.verify()?;
        Ok(recording)
    }
    fn verify(&self) -> Result<(), &'static str> {
        self.target.directory.verify()?;
        let opened = self.file.metadata().map_err(|_| "output metadata failed")?;
        let named = std::fs::symlink_metadata(self.target.directory.path(&self.target.name))
            .map_err(|_| "output path replaced")?;
        if !opened.is_file()
            || !named.is_file()
            || (opened.dev(), opened.ino()) != (self.device, self.inode)
            || (named.dev(), named.ino()) != (self.device, self.inode)
            || opened.len() != self.persisted as u64
        {
            return Err("output identity changed");
        }
        Ok(())
    }
    /// Only called after actual application/network joins. Blocking disk work is
    /// outside network deadlines. No fsync or crash-durability guarantee.
    pub(super) fn persist(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        self.verify()?;
        while self.persisted < bytes.len() {
            let n = self
                .file
                .write(&bytes[self.persisted..])
                .map_err(|_| "output write failed")?;
            if n == 0 {
                return Err("output write stalled");
            }
            self.persisted += n;
        }
        self.file.flush().map_err(|_| "output flush failed")?;
        self.verify()
    }
}
