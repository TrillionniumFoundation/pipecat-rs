//! Bounded, data-only configuration loading. Includes are opt-in capabilities.
use crate::{bounded_yaml, FlowConfig, FlowError};
use serde_yaml::Value;
use std::{
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
};

/// Hard ceilings also bound recursive parser and conversion stack usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowLoadLimits {
    pub max_document_bytes: usize,
    pub max_total_bytes: usize,
    pub max_expanded_nodes: usize,
    pub max_depth: usize,
    pub max_expanded_scalar_bytes: usize,
    pub max_include_depth: usize,
    pub max_include_files: usize,
}
impl Default for FlowLoadLimits {
    fn default() -> Self {
        Self {
            max_document_bytes: 1024 * 1024,
            max_total_bytes: 4 * 1024 * 1024,
            max_expanded_nodes: 65_536,
            max_depth: 64,
            max_expanded_scalar_bytes: 4 * 1024 * 1024,
            max_include_depth: 8,
            max_include_files: 64,
        }
    }
}
impl FlowLoadLimits {
    fn validate(self) -> Result<(), FlowError> {
        let hard = Self::default();
        for (value, ceiling) in [
            (self.max_document_bytes, hard.max_document_bytes),
            (self.max_total_bytes, hard.max_total_bytes),
            (self.max_expanded_nodes, hard.max_expanded_nodes),
            (self.max_depth, hard.max_depth),
            (
                self.max_expanded_scalar_bytes,
                hard.max_expanded_scalar_bytes,
            ),
            (self.max_include_depth, hard.max_include_depth),
            (self.max_include_files, hard.max_include_files),
        ] {
            if value > ceiling {
                return Err(invalid(
                    "load limits may only lower the default hard ceilings",
                ));
            }
        }
        Ok(())
    }
}

/// Includes never read paths supplied by a document unless the application
/// explicitly grants a directory capability. Every include is relative to this
/// same root, matching the source loader's fixed `base_dir` semantics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FlowIncludePolicy {
    #[default]
    Disabled,
    /// Trusted application directory, never a root chosen by untrusted YAML.
    /// Unix descriptor-relative opens reject symlinks in every path component.
    /// Other platforms reject includes rather than weaken confinement.
    Local { root: PathBuf },
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowLoadOptions {
    pub limits: FlowLoadLimits,
    pub includes: FlowIncludePolicy,
}

pub(crate) fn invalid(message: impl Into<String>) -> FlowError {
    FlowError::Config(message.into())
}
pub(crate) fn limit(kind: &'static str) -> FlowError {
    FlowError::Limit(kind)
}

struct Loader {
    options: FlowLoadOptions,
    construction: bounded_yaml::ConstructionBudget,
    total_bytes: usize,
    include_files: usize,
    stack: Vec<PathBuf>,
    root: Option<(PathBuf, File)>,
    root_alias: Option<PathBuf>,
}
impl Loader {
    fn new(options: &FlowLoadOptions) -> Result<Self, FlowError> {
        options.limits.validate()?;
        let root_alias = match &options.includes {
            FlowIncludePolicy::Disabled => None,
            FlowIncludePolicy::Local { root } => {
                Some(std::path::absolute(root).map_err(|e| invalid(e.to_string()))?)
            }
        };
        let root = match &options.includes {
            FlowIncludePolicy::Disabled => None,
            FlowIncludePolicy::Local { root } => {
                #[cfg(not(unix))]
                return Err(FlowError::Unsupported(
                    "confined filesystem includes require Unix".into(),
                ));
                #[cfg(unix)]
                {
                    let root = root
                        .canonicalize()
                        .map_err(|e| invalid(format!("include root: {e}")))?;
                    let file = open_include_root(&root)?;
                    if !file
                        .metadata()
                        .map_err(|e| invalid(e.to_string()))?
                        .is_dir()
                    {
                        return Err(invalid("include root must be a directory"));
                    }
                    Some((root, file))
                }
            }
        };
        Ok(Self {
            options: options.clone(),
            construction: bounded_yaml::ConstructionBudget::new(options.limits),
            total_bytes: 0,
            include_files: 0,
            stack: vec![],
            root,
            root_alias,
        })
    }
    fn charge_bytes(&mut self, count: usize) -> Result<(), FlowError> {
        if count > self.options.limits.max_document_bytes {
            return Err(limit("flow document bytes"));
        }
        if count
            > self
                .options
                .limits
                .max_total_bytes
                .saturating_sub(self.total_bytes)
        {
            return Err(limit("flow total source bytes"));
        }
        self.total_bytes += count;
        Ok(())
    }
    fn read(&mut self, file: File) -> Result<Vec<u8>, FlowError> {
        if !file
            .metadata()
            .map_err(|e| invalid(e.to_string()))?
            .is_file()
        {
            return Err(invalid("flow sources must be regular files"));
        }
        let max = self.options.limits.max_document_bytes.min(
            self.options
                .limits
                .max_total_bytes
                .saturating_sub(self.total_bytes),
        );
        let mut bytes = Vec::new();
        file.take((max + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| invalid(e.to_string()))?;
        self.charge_bytes(bytes.len())?;
        Ok(bytes)
    }
    fn parse(&mut self, bytes: &[u8], json: bool, depth: usize) -> Result<Value, FlowError> {
        if json {
            bounded_yaml::parse_json(bytes, &mut self.construction, depth)
        } else {
            bounded_yaml::parse_yaml(bytes, &mut self.construction, depth)
        }
    }
    fn resolve(
        &mut self,
        value: &mut Value,
        depth: usize,
        include_depth: usize,
    ) -> Result<(), FlowError> {
        match value {
            Value::Tagged(tag) => {
                if self.root.is_none() {
                    return Err(FlowError::Unsupported(
                        "!include is disabled; grant an explicit local root".into(),
                    ));
                }
                let path = tag
                    .value
                    .as_str()
                    .ok_or_else(|| invalid("!include expects a string path"))?;
                let relative = safe_relative(path)?;
                if self.stack.contains(&relative) {
                    return Err(invalid("flow include cycle"));
                }
                if include_depth >= self.options.limits.max_include_depth {
                    return Err(limit("flow include depth"));
                }
                if self.include_files >= self.options.limits.max_include_files {
                    return Err(limit("flow include files"));
                }
                self.include_files += 1;
                let file = open_confined(&self.root.as_ref().expect("root checked").1, &relative)?;
                let bytes = self.read(file)?;
                self.stack.push(relative);
                // Included JSON is valid YAML too. Includes retain YAML scalar semantics.
                let mut included = self.parse(&bytes, false, depth)?;
                self.resolve(&mut included, depth, include_depth + 1)?;
                self.stack.pop();
                *value = included;
            }
            Value::Sequence(items) => {
                for item in items {
                    self.resolve(item, depth + 1, include_depth)?;
                }
            }
            Value::Mapping(items) => {
                for (_, item) in items {
                    self.resolve(item, depth + 1, include_depth)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn finish(&mut self, mut value: Value) -> Result<FlowConfig, FlowError> {
        self.resolve(&mut value, 0, 0)?;
        let value = to_json(value, Position::Root)?;
        reject_python_handlers(&value)?;
        let config: FlowConfig =
            serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }
}

pub(crate) fn from_text(
    text: &str,
    json: bool,
    options: &FlowLoadOptions,
) -> Result<FlowConfig, FlowError> {
    let mut loader = Loader::new(options)?;
    loader.charge_bytes(text.len())?;
    let value = loader.parse(text.as_bytes(), json, 0)?;
    loader.finish(value)
}
pub(crate) fn from_file(path: &Path, options: &FlowLoadOptions) -> Result<FlowConfig, FlowError> {
    let json = match path.extension().and_then(|s| s.to_str()) {
        Some("json") => true,
        Some("yaml" | "yml") => false,
        _ => {
            return Err(invalid(
                "flow file must have a .json, .yaml, or .yml extension",
            ))
        }
    };
    let mut loader = Loader::new(options)?;
    let file = if let Some((root, directory)) = &loader.root {
        // Resolve the caller's entry path lexically, never canonicalize an
        // untrusted child path through a symlink before a confined open.
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| invalid(e.to_string()))?
                .join(path)
        };
        let relative = absolute
            .strip_prefix(root)
            .or_else(|_| {
                absolute.strip_prefix(loader.root_alias.as_ref().expect("root alias exists"))
            })
            .map_err(|_| invalid("entry file must be inside the include root"))?;
        let relative = safe_relative(
            relative
                .to_str()
                .ok_or_else(|| invalid("flow path must be UTF-8"))?,
        )?;
        let file = open_confined(directory, &relative)?;
        loader.stack.push(relative);
        file
    } else {
        open_entry(path)?
    };
    let bytes = loader.read(file)?;
    let value = loader.parse(&bytes, json, 0)?;
    loader.finish(value)
}

fn safe_relative(text: &str) -> Result<PathBuf, FlowError> {
    if text.len() > 4096 {
        return Err(limit("flow include path bytes"));
    }
    if text.is_empty() || text.contains(['\\', ':', '\0']) {
        return Err(invalid(
            "include paths must be nonempty local relative paths",
        ));
    }
    let path = Path::new(text);
    if text.split('/').any(|part| matches!(part, "" | "." | ".."))
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(invalid(
            "include paths cannot be absolute or contain dot/parent components",
        ));
    }
    Ok(path.to_path_buf())
}
#[cfg(unix)]
fn open_include_root(path: &Path) -> Result<File, FlowError> {
    use rustix::fs::{open, openat, Mode, OFlags};
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(invalid("include root must be an absolute canonical path"));
    }
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOFOLLOW;
    // Canonicalization resolves intentionally granted root aliases, but cannot
    // pin their ancestors. Every subsequent lookup uses an already-open parent
    // so replacing any canonical component with a symlink fails closed.
    let mut directory = File::from(
        open("/", flags, Mode::empty()).map_err(|e| invalid(format!("include root: {e}")))?,
    );
    for component in components {
        let Component::Normal(component) = component else {
            return Err(invalid("include root must be an absolute canonical path"));
        };
        directory = File::from(
            openat(&directory, component, flags, Mode::empty())
                .map_err(|e| invalid(format!("include root: {e}")))?,
        );
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_confined(root: &File, path: &Path) -> Result<File, FlowError> {
    use rustix::fs::{openat, Mode, OFlags};
    let mut directory = root.try_clone().map_err(|e| invalid(e.to_string()))?;
    let mut components = path.components().peekable();
    while let Some(Component::Normal(component)) = components.next() {
        let mut flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
        if components.peek().is_some() {
            flags |= OFlags::DIRECTORY;
        }
        directory = File::from(
            openat(&directory, component, flags, Mode::empty())
                .map_err(|e| invalid(format!("confined include open failed: {e}")))?,
        );
    }
    Ok(directory)
}
#[cfg(not(unix))]
fn open_confined(_: &File, _: &Path) -> Result<File, FlowError> {
    Err(FlowError::Unsupported(
        "confined filesystem includes require Unix".into(),
    ))
}
fn open_entry(path: &Path) -> Result<File, FlowError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32);
    }
    options.open(path).map_err(|e| invalid(e.to_string()))
}

#[derive(Clone, Copy)]
enum Position {
    Root,
    Nodes,
    Node,
    Functions,
    Function,
    Transition,
    Cases,
    Other,
}
impl Position {
    fn key(self, key: &str) -> Self {
        match (self, key) {
            (Self::Root, "nodes") => Self::Nodes,
            (Self::Root, "global_functions") | (Self::Node, "functions") => Self::Functions,
            (Self::Nodes, _) => Self::Node,
            (Self::Function, "transition_to") => Self::Transition,
            (Self::Transition, "cases") => Self::Cases,
            _ => Self::Other,
        }
    }
}
fn to_json(value: Value, position: Position) -> Result<serde_json::Value, FlowError> {
    use serde_json::Value as Json;
    Ok(match value {
        Value::Null => Json::Null,
        Value::Bool(v) => Json::Bool(v),
        Value::Number(v) => serde_json::to_value(v).map_err(|e| invalid(e.to_string()))?,
        Value::String(v) => Json::String(v),
        Value::Sequence(values) => Json::Array(
            values
                .into_iter()
                .map(|v| {
                    to_json(
                        v,
                        if matches!(position, Position::Functions) {
                            Position::Function
                        } else {
                            Position::Other
                        },
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        Value::Mapping(values) => {
            let mut map = serde_json::Map::new();
            for (key, value) in values {
                let key = match key {
                    Value::String(key) => key,
                    Value::Bool(key) if matches!(position, Position::Cases) => key.to_string(),
                    Value::Number(key) if matches!(position, Position::Cases) => key.to_string(),
                    Value::Null if matches!(position, Position::Cases) => "None".into(),
                    _ => {
                        return Err(invalid(
                            "mapping keys must be strings except scalar branch cases",
                        ))
                    }
                };
                if key == "<<" {
                    return Err(FlowError::Unsupported("YAML merge keys".into()));
                }
                let key = if matches!(position, Position::Cases)
                    && (key.eq_ignore_ascii_case("true") || key.eq_ignore_ascii_case("false"))
                {
                    key.to_ascii_lowercase()
                } else {
                    key
                };
                if map.contains_key(&key) {
                    return Err(invalid("duplicate normalized mapping key"));
                }
                let value = to_json(value, position.key(&key))?;
                map.insert(key, value);
            }
            Json::Object(map)
        }
        Value::Tagged(_) => return Err(FlowError::Unsupported("unresolved YAML tag".into())),
    })
}
fn reject_python_handlers(value: &serde_json::Value) -> Result<(), FlowError> {
    // Function metadata and top-level handler modules are not part of the
    // declarative schema. Action handler strings name already registered Rust
    // handlers; dotted imports, file paths, or expressions are never resolved.
    let check = |name: &str| -> Result<(), FlowError> {
        if name.contains(['.', ':', '/', '\\', '(', ')']) {
            return Err(FlowError::Unsupported(
                "dynamic Python handler/import expressions; register a native handler name".into(),
            ));
        }
        Ok(())
    };
    if value.get("handlers").is_some() || value.get("handler_module").is_some() {
        return Err(FlowError::Unsupported(
            "dynamic Python handler modules".into(),
        ));
    }
    let functions = value
        .get("global_functions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            value
                .get("nodes")
                .and_then(serde_json::Value::as_object)
                .into_iter()
                .flat_map(|nodes| nodes.values())
                .flat_map(|node| {
                    node.get("functions")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                }),
        );
    for function in functions {
        if function.get("handler").is_some() {
            return Err(FlowError::Unsupported(
                "function handler objects; register native tools by name".into(),
            ));
        }
        if let Some(name) = function.get("name").and_then(serde_json::Value::as_str) {
            check(name)?;
        }
    }
    for node in value
        .get("nodes")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|nodes| nodes.values())
    {
        for key in ["pre_actions", "post_actions"] {
            for action in node
                .get(key)
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(handler) = action.get("handler").filter(|v| !v.is_null()) {
                    check(handler.as_str().ok_or_else(|| {
                        FlowError::Unsupported(
                            "dynamic handler objects; expected a native handler name".into(),
                        )
                    })?)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod root_acquisition_tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::symlink,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "pipecat-flow-root-acquisition-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn canonical_root_rejects_ancestor_symlink_replacement_before_open() {
        let fixture = Directory::new();
        fs::create_dir_all(fixture.0.join("granted/configs")).unwrap();
        fs::create_dir_all(fixture.0.join("outside/configs")).unwrap();
        let canonical_root = fixture.0.join("granted/configs").canonicalize().unwrap();

        // Deterministically replace an ancestor at the canonicalize/open boundary.
        fs::rename(fixture.0.join("granted"), fixture.0.join("parked")).unwrap();
        symlink(fixture.0.join("outside"), fixture.0.join("granted")).unwrap();
        assert!(
            open_include_root(&canonical_root).is_err(),
            "root acquisition followed a replaced ancestor symlink"
        );
    }

    #[test]
    fn canonical_root_rejects_final_symlink_replacement_before_open() {
        let fixture = Directory::new();
        fs::create_dir_all(fixture.0.join("granted/configs")).unwrap();
        fs::create_dir_all(fixture.0.join("outside")).unwrap();
        let canonical_root = fixture.0.join("granted/configs").canonicalize().unwrap();
        fs::rename(&canonical_root, fixture.0.join("parked")).unwrap();
        symlink(fixture.0.join("outside"), &canonical_root).unwrap();
        assert!(open_include_root(&canonical_root).is_err());
    }

    #[test]
    fn acquired_root_stays_pinned_when_ancestor_is_replaced() {
        let fixture = Directory::new();
        fs::create_dir_all(fixture.0.join("granted/configs")).unwrap();
        fs::create_dir_all(fixture.0.join("outside/configs")).unwrap();
        fs::write(fixture.0.join("granted/configs/marker"), "authorized").unwrap();
        fs::write(fixture.0.join("outside/configs/marker"), "outside").unwrap();
        let canonical_root = fixture.0.join("granted/configs").canonicalize().unwrap();
        let root = open_include_root(&canonical_root).unwrap();
        fs::rename(fixture.0.join("granted"), fixture.0.join("parked")).unwrap();
        symlink(fixture.0.join("outside"), fixture.0.join("granted")).unwrap();
        let mut marker = String::new();
        open_confined(&root, Path::new("marker"))
            .unwrap()
            .read_to_string(&mut marker)
            .unwrap();
        assert_eq!(marker, "authorized");
    }

    #[test]
    fn root_descriptor_requires_absolute_canonical_components() {
        assert!(open_include_root(Path::new("relative/root")).is_err());
        assert!(open_include_root(Path::new("/tmp/../tmp")).is_err());
        assert!(open_include_root(Path::new("/"))
            .unwrap()
            .metadata()
            .unwrap()
            .is_dir());
    }
}
