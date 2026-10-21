use crate::{unicode15, Error, SageMakerLimits, SynthesisSettings};
use serde::Serialize;

/// Source-equivalent strip/isalnum for the explicitly pinned Unicode 15 table.
pub(crate) fn normalized<'a>(
    text: &'a str,
    limits: &SageMakerLimits,
) -> Result<Option<&'a str>, Error> {
    if text.len() > limits.max_text_bytes {
        return Err(Error::TooLarge);
    }
    let text = text.trim_matches(unicode15::is_space);
    Ok((!text.is_empty() && text.chars().any(unicode15::is_alnum)).then_some(text))
}
#[derive(Serialize)]
struct Payload<'a> {
    text: &'a str,
    voice_name: &'a str,
    language_code: &'a str,
    sample_rate_hz: u32,
}
pub(crate) fn payload(
    text: &str,
    settings: &SynthesisSettings,
    limits: &SageMakerLimits,
) -> Result<Option<Vec<u8>>, Error> {
    settings.validate()?;
    let Some(text) = normalized(text, limits)? else {
        return Ok(None);
    };
    // Refuse escaped output expansion while serialization is in progress, not
    // after allocating an unbounded complete JSON representation.
    struct Bounded {
        data: Vec<u8>,
        max: usize,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max.saturating_sub(self.data.len()) {
                return Err(std::io::ErrorKind::FileTooLarge.into());
            }
            self.data.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        data: Vec::new(),
        max: limits.max_request_bytes,
    };
    serde_json::to_writer(
        &mut writer,
        &Payload {
            text,
            voice_name: &settings.voice_name,
            language_code: &settings.language_code,
            sample_rate_hz: settings.sample_rate_hz,
        },
    )
    .map_err(|_| Error::TooLarge)?;
    Ok(Some(writer.data.into_boxed_slice().into_vec()))
}

/// Incremental whole-sample adapter. One odd byte plus a 12-byte format probe;
/// each returned allocation is bounded by max_frame_bytes, independent of wire
/// chunking. A PCM prefix may have been delivered before a WAV signature fails.
pub(crate) struct Pcm {
    odd: Option<u8>,
    wire: usize,
    limit: usize,
    frame: usize,
    prefix: [u8; 12],
    prefix_len: usize,
}
impl Pcm {
    pub fn new(limits: &SageMakerLimits) -> Self {
        Self {
            odd: None,
            wire: 0,
            limit: limits.max_response_bytes,
            frame: limits.max_frame_bytes,
            prefix: [0; 12],
            prefix_len: 0,
        }
    }
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        self.wire = self.wire.checked_add(bytes.len()).ok_or(Error::TooLarge)?;
        if self.wire > self.limit {
            return Err(Error::TooLarge);
        }
        let probe = (12 - self.prefix_len).min(bytes.len());
        self.prefix[self.prefix_len..self.prefix_len + probe].copy_from_slice(&bytes[..probe]);
        self.prefix_len += probe;
        if self.prefix_len >= 4 && (&self.prefix[..4] == b"OggS" || &self.prefix[..4] == b"fLaC")
            || self.prefix_len == 12
                && &self.prefix[..4] == b"RIFF"
                && &self.prefix[8..12] == b"WAVE"
        {
            return Err(Error::Protocol("encoded audio is not raw PCM"));
        }
        let mut result = Vec::new();
        let mut input = bytes;
        if let Some(odd) = self.odd.take() {
            if let Some((&first, rest)) = input.split_first() {
                result.push(vec![odd, first]);
                input = rest;
            } else {
                self.odd = Some(odd);
            }
        }
        let even = input.len() & !1;
        for part in input[..even].chunks(self.frame) {
            result.push(part.to_vec());
        }
        if even < input.len() {
            self.odd = Some(input[even]);
        }
        Ok(result)
    }
    pub fn finish(self) -> Result<(), Error> {
        if self.odd.is_some() {
            Err(Error::Protocol("trailing partial PCM sample"))
        } else if self.wire == 0 {
            Err(Error::Protocol("empty PCM response"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_json_defaults_and_unicode15_source_gate() {
        let l = SageMakerLimits::default();
        let s = SynthesisSettings::default();
        assert_eq!(payload("  Hello \n",&s,&l).unwrap().unwrap(),br#"{"text":"Hello","voice_name":"Magpie-Multilingual.EN-US.Aria","language_code":"en-US","sample_rate_hz":24000}"#);
        for t in [
            "", " \t\n", "!!!", "😀", "\u{301}", "\u{345}", "\u{200b}", "\u{feff}",
        ] {
            assert!(normalized(t, &l).unwrap().is_none(), "{t:?}");
        }
        for t in ["漢字", "Ⅷ", "²", "\u{1e4d0}", "\0A\0"] {
            assert_eq!(normalized(t, &l).unwrap(), Some(t));
        }
        assert_eq!(
            normalized("\u{1c}\u{85}\u{a0}漢\u{1f}", &l).unwrap(),
            Some("漢")
        );
        assert_eq!(
            normalized("\u{200b}漢\u{feff}", &l).unwrap(),
            Some("\u{200b}漢\u{feff}")
        );
    }
    #[test]
    fn wire_and_request_bounds_apply_before_retention() {
        let mut l = SageMakerLimits {
            max_text_bytes: 3,
            ..Default::default()
        };
        assert_eq!(
            payload("1234", &SynthesisSettings::default(), &l),
            Err(Error::TooLarge)
        );
        l.max_text_bytes = 1024;
        l.max_request_bytes = 16;
        assert_eq!(
            payload("A", &SynthesisSettings::default(), &l),
            Err(Error::TooLarge)
        );
    }
    #[test]
    fn pcm_all_splits_zero_chunks_and_complete_samples() {
        let l = SageMakerLimits::default();
        let input: Vec<u8> = (0..30).collect();
        for split in 1..=input.len() {
            let mut p = Pcm::new(&l);
            let mut out = vec![];
            for part in input.chunks(split) {
                assert!(p.feed(&[]).unwrap().is_empty());
                for block in p.feed(part).unwrap() {
                    assert_eq!(block.len() % 2, 0);
                    out.extend(block);
                }
            }
            p.finish().unwrap();
            assert_eq!(out, input);
        }
    }
    #[test]
    fn pcm_empty_odd_limits_and_encoded_signature_fail() {
        let mut l = SageMakerLimits::default();
        assert!(Pcm::new(&l).finish().is_err());
        let mut p = Pcm::new(&l);
        p.feed(&[1]).unwrap();
        assert!(p.finish().is_err());
        l.max_response_bytes = 3;
        let mut p = Pcm::new(&l);
        p.feed(&[1, 2]).unwrap();
        assert_eq!(p.feed(&[3, 4]), Err(Error::TooLarge));
        let mut p = Pcm::new(&SageMakerLimits::default());
        for b in b"RIFF0000WAV" {
            p.feed(&[*b]).unwrap();
        }
        assert!(p.feed(b"E").is_err());
    }
}
