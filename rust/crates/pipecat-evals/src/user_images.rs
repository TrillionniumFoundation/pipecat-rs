//! Explicit trusted-local image bundles. Registration is passive; a bot must request it.
use crate::{
    scenario::ResponseParseMode, EvalError, EvalSession, ScenarioFile, ScriptScenario,
    SessionOptions,
};
use pipecat_images::OfflineRgbDecodeLimits;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(crate) const MAX_IMAGE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_IMAGE_REFERENCES: usize = 64;
pub(crate) const MAX_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
/// Owned encoded PNG/JPEG, validated without starting a decoder or network request.
#[derive(Debug)]
pub struct EncodedUserImage {
    bytes: Box<[u8]>,
    format: String,
}
impl EncodedUserImage {
    pub fn new(bytes: Vec<u8>, format: impl Into<String>) -> Result<Self, EvalError> {
        if bytes.len() > MAX_IMAGE_BYTES || bytes.capacity() > MAX_IMAGE_BYTES {
            return Err(EvalError::Limit("encoded image storage"));
        }
        let format = format.into();
        OfflineRgbDecodeLimits::default()
            .validate_encoded_image(&bytes, &format)
            .map_err(|e| EvalError::Scenario(format!("user image: {e}")))?;
        let format = match format.as_str() {
            "image/png" => "image/png",
            "image/jpeg" => "image/jpeg",
            _ => unreachable!("validated MIME"),
        }
        .to_owned();
        Ok(Self {
            bytes: bytes.into_boxed_slice(),
            format,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn format(&self) -> &str {
        &self.format
    }
    pub(crate) fn wire_bound(&self) -> Result<usize, EvalError> {
        let encoded = self
            .bytes
            .len()
            .checked_add(2)
            .and_then(|n| n.checked_div(3))
            .and_then(|n| n.checked_mul(4))
            .ok_or(EvalError::Limit("image wire bytes"))?;
        // Empty payload measures all exact envelope/escaping bytes with the largest ID.
        let empty = crate::serializer::control(
            u64::MAX,
            crate::serializer::EVAL_IMAGE,
            serde_json::json!({"image":"","format":self.format}),
        )
        .encode()?;
        encoded
            .checked_add(empty.len())
            .ok_or(EvalError::Limit("image wire bytes"))
    }
}
#[derive(Debug)]
pub struct UserImageScenarioFile {
    name: String,
    scenarios: Vec<(ScriptScenario, Vec<(usize, EncodedUserImage)>)>,
}
impl UserImageScenarioFile {
    /// Asset references are relative to the outer bundle directory, including expanded includes.
    /// Requires a trusted stable local filesystem; canonical containment is not a hostile-race sandbox.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let supplied = path.as_ref();
        let root = supplied
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .canonicalize()?;
        let path = supplied.canonicalize()?;
        let value = crate::scenario::load_recorded_value(&path, &root)?;
        let mut pending = Vec::new();
        let mut refs = 0usize;
        let mut expanded = 0usize;
        let file = ScenarioFile::from_value_with_mode(value, ResponseParseMode::Text, |raw| {
            if pending.len() >= 16 {
                return Err(EvalError::Limit("image scenarios"));
            }
            expanded = expanded
                .checked_add(serde_json::to_vec(raw)?.len())
                .ok_or(EvalError::Limit("expanded image scenario bytes"))?;
            if expanded > 4 * 1024 * 1024 {
                return Err(EvalError::Limit("expanded image scenario bytes"));
            }
            let turns = raw
                .get_mut("turns")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or_else(|| invalid("turns must be a list"))?;
            let mut paths = Vec::new();
            for (index, turn) in turns.iter_mut().enumerate() {
                let turn = turn
                    .as_object_mut()
                    .ok_or_else(|| invalid("turn must be a mapping"))?;
                if turn.get("dtmf").is_some_and(|v| !v.is_null()) {
                    return Err(EvalError::Unsupported("image scenarios with DTMF".into()));
                }
                if let Some(image) = turn.remove("image").filter(|v| !v.is_null()) {
                    let s = image
                        .as_str()
                        .filter(|s| !s.is_empty() && s.len() <= 4096)
                        .ok_or_else(|| invalid("image must be a nonempty bounded path string"))?;
                    let p = PathBuf::from(s);
                    if p.is_absolute() || s.contains("://") || s.starts_with("data:") {
                        return Err(invalid("image must name a local relative bundle asset"));
                    }
                    refs += 1;
                    if refs > MAX_IMAGE_REFERENCES {
                        return Err(EvalError::Limit("image references"));
                    }
                    paths.push((index, p));
                }
            }
            pending.push(paths);
            Ok(())
        })?;
        // Validate every scenario and all path metadata before reading any image bytes.
        let mut planned = 0usize;
        let mut canonical = Vec::new();
        for paths in pending {
            let mut out = Vec::new();
            for (index, path) in paths {
                let format = match path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(str::to_ascii_lowercase)
                    .as_deref()
                {
                    Some("png") => "image/png",
                    Some("jpg" | "jpeg") => "image/jpeg",
                    _ => return Err(invalid("image extension must be PNG or JPEG")),
                };
                let path = root.join(path).canonicalize()?;
                if !path.starts_with(&root) {
                    return Err(invalid("image escapes bundle root"));
                }
                let meta = std::fs::metadata(&path)?;
                if !meta.is_file() {
                    return Err(invalid("image must be a regular file"));
                }
                let size =
                    usize::try_from(meta.len()).map_err(|_| EvalError::Limit("image bytes"))?;
                if size == 0 || size > MAX_IMAGE_BYTES {
                    return Err(EvalError::Limit("image bytes"));
                }
                planned = planned
                    .checked_add(size)
                    .ok_or(EvalError::Limit("image bundle bytes"))?;
                if planned > MAX_BUNDLE_BYTES {
                    return Err(EvalError::Limit("image bundle bytes"));
                }
                out.push((index, path, format));
            }
            canonical.push(out);
        }
        let mut actual = 0usize;
        let mut scenarios = Vec::new();
        for (scenario, paths) in file.scenarios.into_iter().zip(canonical) {
            let mut images = Vec::new();
            for (index, path, format) in paths {
                let bytes = crate::recorded_scenario::read_regular(
                    &path,
                    &root,
                    MAX_IMAGE_BYTES.min(MAX_BUNDLE_BYTES - actual),
                )?;
                actual = actual
                    .checked_add(bytes.len())
                    .ok_or(EvalError::Limit("image bundle bytes"))?;
                if actual > MAX_BUNDLE_BYTES {
                    return Err(EvalError::Limit("image bundle bytes"));
                }
                // The bounded reader may grow capacity beyond its payload length at EOF.
                // Compact our own allocation; external owned inputs still obey the public capacity cap.
                images.push((
                    index,
                    EncodedUserImage::new(bytes.into_boxed_slice().into_vec(), format)?,
                ));
            }
            scenarios.push((scenario, images));
        }
        Ok(Self {
            name: file.name,
            scenarios,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn len(&self) -> usize {
        self.scenarios.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scenarios.is_empty()
    }
    pub fn into_sessions(
        self,
        url: &str,
        options: SessionOptions,
    ) -> Result<Vec<EvalSession>, EvalError> {
        self.scenarios
            .into_iter()
            .map(|(scenario, images)| {
                Ok(EvalSession::new(scenario, url, options.clone())?
                    .with_user_images(images)?
                    .freeze_user_images())
            })
            .collect()
    }
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
pub(crate) fn validate_inputs(
    scenario: &ScriptScenario,
    options: &SessionOptions,
    inputs: impl IntoIterator<Item = (usize, EncodedUserImage)>,
) -> Result<BTreeMap<usize, EncodedUserImage>, EvalError> {
    let mut images = BTreeMap::new();
    let mut bytes = 0usize;
    let mut wire = 0usize;
    if scenario.turns.iter().any(|t| t.dtmf.is_some()) {
        return Err(EvalError::Unsupported("image scenarios with DTMF".into()));
    }
    for (index, image) in inputs {
        if scenario.turns.get(index).is_none() {
            return Err(invalid("image turn index out of range"));
        }
        bytes = bytes
            .checked_add(image.bytes.len())
            .ok_or(EvalError::Limit("image bundle bytes"))?;
        let size = image.wire_bound()?;
        wire = wire
            .checked_add(size)
            .ok_or(EvalError::Limit("image wire bytes"))?;
        if bytes > MAX_BUNDLE_BYTES
            || size > options.transport.max_message_bytes
            || wire > options.max_wire_bytes
            || images.len() >= MAX_IMAGE_REFERENCES
            || images.len() >= options.max_wire_messages
        {
            return Err(EvalError::Limit("image inputs or wire budget"));
        }
        if images.insert(index, image).is_some() {
            return Err(invalid("duplicate image turn index"));
        }
    }
    Ok(images)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Dir(PathBuf);
    impl Dir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "pipecat-images-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn file(&self, value: serde_json::Value) -> PathBuf {
            let p = self.0.join("scenario.json");
            std::fs::write(&p, serde_json::to_vec(&value).unwrap()).unwrap();
            p
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn png() -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
            encoder.set_color(png::ColorType::Rgb);
            encoder.set_depth(png::BitDepth::Eight);
            let mut writer = encoder.write_header().unwrap();
            writer.write_image_data(&[255, 0, 0, 0, 255, 0]).unwrap();
        }
        bytes
    }
    fn asset() -> EncodedUserImage {
        EncodedUserImage::new(png(), "image/png").unwrap()
    }
    fn scenario() -> ScriptScenario {
        ScenarioFile::from_value(json!({"name":"image","turns":[{"user":"look","expect":[]}]}))
            .unwrap()
            .scenarios
            .remove(0)
    }
    fn session() -> EvalSession {
        EvalSession::new(scenario(), "ws://127.0.0.1:9", SessionOptions::default()).unwrap()
    }
    #[test]
    fn encoded_constructor_rejects_mime_magic_storage_and_header_limits() {
        assert_eq!(asset().format(), "image/png");
        for (bytes, mime) in [
            (vec![], "image/png"),
            (png(), "image/jpeg"),
            (png(), "image/gif"),
            (vec![0; 65537], "image/png"),
        ] {
            assert!(EncodedUserImage::new(bytes, mime).is_err());
        }
        let mut excess = Vec::with_capacity(65537);
        excess.extend(png());
        assert!(EncodedUserImage::new(excess, "image/png").is_err());
        let mut huge = png();
        huge[16..20].copy_from_slice(&2048u32.to_be_bytes());
        assert!(EncodedUserImage::new(huge, "image/png").is_err());
    }
    #[test]
    fn exact_wire_bound_covers_all_identifier_widths() {
        let a = asset();
        use base64::Engine;
        for id in [0, 9, 10, u64::MAX] {
            let msg=crate::serializer::control(id,crate::serializer::EVAL_IMAGE,json!({"image":base64::engine::general_purpose::STANDARD.encode(a.bytes()),"format":a.format()})).encode().unwrap();
            assert!(msg.len() <= a.wire_bound().unwrap());
            if id == u64::MAX {
                assert_eq!(msg.len(), a.wire_bound().unwrap());
            }
        }
    }
    #[test]
    fn ordinary_parser_remains_opt_out_and_bundle_owns_original_bytes() {
        let d = Dir::new();
        let bytes = png();
        std::fs::write(d.0.join("a.png"), &bytes).unwrap();
        let p=d.file(json!({"name":"image","turns":[{"image":"a.png","user":"look","expect":[]},{"image":null,"expect":[]}]}));
        assert!(ScenarioFile::load(&p).is_err());
        let file = UserImageScenarioFile::load(&p).unwrap();
        assert_eq!(file.len(), 1);
        assert_eq!(file.scenarios[0].1[0].1.bytes(), bytes);
        std::fs::write(d.0.join("a.png"), b"changed").unwrap();
        assert_eq!(file.scenarios[0].1[0].1.bytes(), bytes);
        assert_eq!(
            file.into_sessions("ws://127.0.0.1:9", SessionOptions::default())
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn all_scenario_fields_validate_before_any_asset_read() {
        let d = Dir::new();
        let p=d.file(json!({"name":"image","turns":[{"image":"missing.png","user":"look","expect":[]},{"user":"bad","bogus":true}]}));
        let err = UserImageScenarioFile::load(p).unwrap_err();
        assert!(!matches!(err, EvalError::Io(_)), "{err}");
    }
    #[test]
    fn asset_paths_reject_escape_absolute_url_unsupported_and_nonregular() {
        let d = Dir::new();
        std::fs::create_dir(d.0.join("folder.png")).unwrap();
        std::fs::write(d.0.join("a.gif"), png()).unwrap();
        for path in [
            "../missing.png",
            "/tmp/missing.png",
            "https://example.com/a.png",
            "data:image/png,abc",
            "folder.png",
            "a.gif",
            "",
        ] {
            let p = d.file(json!({"name":"image","turns":[{"image":path,"expect":[]}]}));
            assert!(UserImageScenarioFile::load(p).is_err(), "{path}");
        }
        for image in [json!(3), json!({}), json!([])] {
            let p = d.file(json!({"name":"image","turns":[{"image":image,"expect":[]}]}));
            assert!(UserImageScenarioFile::load(p).is_err());
        }
    }
    #[test]
    fn symlink_reference_mime_and_containment_remain_checked() {
        use std::os::unix::fs::symlink;
        let d = Dir::new();
        let outside = Dir::new();
        std::fs::write(outside.0.join("a.png"), png()).unwrap();
        symlink(outside.0.join("a.png"), d.0.join("outside.png")).unwrap();
        std::fs::write(d.0.join("a.png"), png()).unwrap();
        symlink("a.png", d.0.join("wrong.jpg")).unwrap();
        for path in ["outside.png", "wrong.jpg"] {
            let p = d.file(json!({"name":"image","turns":[{"image":path,"expect":[]}]}));
            assert!(UserImageScenarioFile::load(p).is_err());
        }
    }
    #[test]
    fn bounded_reference_count_and_scenario_count_fail_preflight() {
        let d = Dir::new();
        std::fs::write(d.0.join("a.png"), png()).unwrap();
        let turns = (0..65)
            .map(|_| json!({"image":"a.png","expect":[]}))
            .collect::<Vec<_>>();
        let p = d.file(json!({"name":"image","turns":turns}));
        assert!(matches!(
            UserImageScenarioFile::load(p),
            Err(EvalError::Limit("image references"))
        ));
        let entries = (0..17)
            .map(|i| json!({"name":format!("s{i}"),"turns":[{"expect":[]}]}))
            .collect::<Vec<_>>();
        let p = d.file(json!({"name":"image","scenarios":entries}));
        assert!(matches!(
            UserImageScenarioFile::load(p),
            Err(EvalError::Limit("image scenarios"))
        ));
    }
    #[test]
    fn builders_bound_indices_duplicates_wire_and_freeze() {
        assert!(session().with_user_images([(1, asset())]).is_err());
        assert!(session()
            .with_user_images([(0, asset()), (0, asset())])
            .is_err());
        let mut options = SessionOptions::default();
        options.transport.max_message_bytes = 1;
        assert!(validate_inputs(&scenario(), &options, [(0, asset())]).is_err());
        options = SessionOptions::default();
        options.max_wire_bytes = 1;
        assert!(validate_inputs(&scenario(), &options, [(0, asset())]).is_err());
        let frozen = session()
            .with_user_images([(0, asset())])
            .unwrap()
            .freeze_user_images();
        let iter = std::iter::from_fn(|| -> Option<(usize, EncodedUserImage)> {
            panic!("frozen plan must reject before consuming replacement iterator")
        });
        assert!(frozen.with_user_images(iter).is_err());
    }
    #[test]
    fn audio_and_dtmf_combinations_are_not_silently_enabled() {
        let d = Dir::new();
        for turn in [
            json!({"image":"a.png","audio":"a.wav","user":"look"}),
            json!({"image":"a.png","dtmf":"1"}),
        ] {
            let p = d.file(json!({"name":"image","turns":[turn]}));
            assert!(UserImageScenarioFile::load(p).is_err());
        }
        let clip = crate::RecordedAudio::from_pcm16(&[0, 0]).unwrap();
        assert!(session()
            .with_recorded_audio_inputs([(0, clip.clone())])
            .unwrap()
            .with_user_images([(0, asset())])
            .is_err());
        assert!(session()
            .with_user_images([(0, asset())])
            .unwrap()
            .with_recorded_audio_inputs([(0, clip)])
            .is_err());
    }
    #[test]
    fn mime_capacity_is_normalized_and_exact_encoded_limit_file_is_accepted() {
        let mut mime = String::with_capacity(100_000);
        mime.push_str("image/png");
        let image = EncodedUserImage::new(png(), mime).unwrap();
        assert!(image.format.capacity() <= 16);
        fn padded(n: usize) -> Vec<u8> {
            let mut bytes = Vec::new();
            {
                let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
                encoder.set_color(png::ColorType::Rgb);
                encoder.set_depth(png::BitDepth::Eight);
                encoder
                    .add_text_chunk("padding".into(), "x".repeat(n))
                    .unwrap();
                let mut writer = encoder.write_header().unwrap();
                writer.write_image_data(&[255, 0, 0, 0, 255, 0]).unwrap();
            }
            bytes
        }
        let overhead = padded(0).len();
        let bytes = padded(MAX_IMAGE_BYTES - overhead);
        assert_eq!(bytes.len(), MAX_IMAGE_BYTES);
        let d = Dir::new();
        std::fs::write(d.0.join("exact.png"), &bytes).unwrap();
        let p = d.file(json!({"name":"exact","turns":[{"image":"exact.png","expect":[]}]}));
        let file = UserImageScenarioFile::load(p).unwrap();
        assert_eq!(file.scenarios[0].1[0].1.bytes().len(), MAX_IMAGE_BYTES);
    }
}
