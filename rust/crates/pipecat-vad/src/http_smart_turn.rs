// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause

//! Opt-in HTTP Smart Turn inference on the existing owned inference thread.
//!
//! This implements the intended upstream HTTP contract, fixing its executor-to-
//! event-loop dispatch defect. It deliberately admits a narrower transport
//! profile: explicit HTTP(S) IP-literal endpoints, no proxy, redirect, or retry,
//! an exactly-200 JSON response, bounded payloads, and a positive finite timeout.
//! JSON syntax/transport failures and timeouts yield INCOMPLETE with probability
//! zero. A syntactically valid response with an invalid schema is an error.
//!
//! The endpoint must already agree on mono PCM sample rate. No rate metadata,
//! resampling, model padding, ONNX model, or model download is involved. HTTPS
//! endpoints need a trusted certificate valid for their configured IP address.
//! Hostnames are intentionally unsupported: avoiding blocking OS DNS resolution
//! permits cancellation and ordinary owned-runtime shutdown without abandoning
//! resolver tasks. No Tokio runtime or HTTP client survives a prediction call.

use std::{fmt, net::IpAddr, sync::Arc, time::Duration};

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE},
    Client, StatusCode, Url,
};

use crate::{
    MonotonicSmartTurnClock, Result, SmartTurnAnalyzer, SmartTurnBuffer, SmartTurnCancellation,
    SmartTurnClock, SmartTurnParams, SmartTurnPrediction, SmartTurnPredictor, VadError,
};

const MAX_URL_BYTES: usize = 2048;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 8192;
const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const NPY_HEADER_BYTES: usize = 128;
const MAX_SAMPLES: usize = (MAX_REQUEST_BYTES - NPY_HEADER_BYTES) / 4;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const CANCELLATION_POLL: Duration = Duration::from_millis(10);

/// Validated configuration for a single explicitly selected HTTP endpoint.
///
/// Limits are 2 KiB for the URL, 64 custom headers totaling 8 KiB (names and
/// values), 8 MiB for the NPY request, and 64 KiB for the response. The configured
/// maximum audio window must fit the request limit; oversized input is rejected,
/// never silently truncated. `params.stop_secs` is also the total HTTP deadline
/// and must be positive. Unlike upstream aiohttp, zero cannot disable it.
///
/// Debug output deliberately omits both URL and header contents. Header values
/// are never included in errors. Plain HTTP exposes audio and custom headers to
/// the network; choose HTTPS wherever confidentiality is required.
#[derive(Clone)]
pub struct HttpSmartTurnConfig {
    endpoint: Url,
    headers: HeaderMap,
    sample_rate: u32,
    params: SmartTurnParams,
    max_samples: usize,
    timeout: Duration,
}

impl fmt::Debug for HttpSmartTurnConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpSmartTurnConfig")
            .field("endpoint", &"[redacted]")
            .field("custom_header_count", &self.headers.len())
            .field("sample_rate", &self.sample_rate)
            .field("params", &self.params)
            .field("max_samples", &self.max_samples)
            .finish_non_exhaustive()
    }
}

impl HttpSmartTurnConfig {
    /// Configure the endpoint and the same audio policy used by its analyzer.
    ///
    /// Only HTTP(S) URLs whose host parses as an IP address are accepted. URL
    /// credentials and fragments are forbidden. The rate is explicit and must
    /// match the server; it is not transmitted or restricted to 16 kHz.
    pub fn new(
        endpoint: impl AsRef<str>,
        sample_rate: u32,
        params: SmartTurnParams,
    ) -> Result<Self> {
        let endpoint = validate_endpoint(endpoint.as_ref())?;
        // Reuse the existing buffer's rate and timing domain without duplicating
        // its policy or changing the local model's accepted configurations.
        let _ = SmartTurnBuffer::new(sample_rate, params.clone())?;
        if params.stop_secs <= 0.0 {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn stop_secs must be finite and positive",
            ));
        }
        let timeout = Duration::try_from_secs_f64(params.stop_secs).map_err(|_| {
            VadError::InvalidParameter("HTTP Smart Turn stop_secs is not a representable duration")
        })?;
        if timeout.is_zero() {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn stop_secs must be at least one nanosecond",
            ));
        }
        let max_samples = (params.max_duration_secs * f64::from(sample_rate)) as usize;
        if max_samples > MAX_SAMPLES {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn audio window exceeds the request byte limit",
            ));
        }
        Ok(Self {
            endpoint,
            headers: HeaderMap::new(),
            sample_rate,
            params,
            max_samples,
            timeout,
        })
    }

    /// Add or replace a custom header. Content-Type may override the source's
    /// default `application/octet-stream`; connection/framing headers may not.
    ///
    /// Values are marked sensitive internally, but transport encryption still
    /// requires HTTPS. Name/value parse failures are reported without contents.
    pub fn with_header(mut self, name: &str, value: &str) -> Result<Self> {
        if name.len().saturating_add(value.len()) > MAX_HEADER_BYTES {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn custom headers exceed the byte limit",
            ));
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            VadError::InvalidParameter("invalid HTTP Smart Turn custom header name")
        })?;
        if matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "proxy-connection"
                | "proxy-authorization"
                | "upgrade"
                | "trailer"
                | "te"
                | "expect"
        ) {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn custom header cannot override routing or framing",
            ));
        }
        let mut value = HeaderValue::from_str(value).map_err(|_| {
            VadError::InvalidParameter("invalid HTTP Smart Turn custom header value")
        })?;
        value.set_sensitive(true);
        self.headers.insert(name, value);
        let bytes = self
            .headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>();
        if self.headers.len() > MAX_HEADERS || bytes > MAX_HEADER_BYTES {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn custom headers exceed their count or byte limit",
            ));
        }
        Ok(self)
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn params(&self) -> &SmartTurnParams {
        &self.params
    }
}

fn validate_endpoint(endpoint: &str) -> Result<Url> {
    let invalid = || VadError::InvalidParameter("invalid HTTP Smart Turn endpoint configuration");
    if endpoint.len() > MAX_URL_BYTES
        || endpoint.chars().any(|ch| ch.is_control() || ch == '\\')
        || endpoint.trim() != endpoint
    {
        return Err(invalid());
    }
    let (_, authority_and_path) = endpoint.split_once("://").ok_or_else(invalid)?;
    let authority = authority_and_path
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("");
    // Check the original authority as well: URL parsing can normalize away
    // empty user information such as "http://@127.0.0.1/".
    if authority.contains('@') {
        return Err(invalid());
    }
    let url = Url::parse(endpoint).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > MAX_URL_BYTES
    {
        return Err(invalid());
    }
    let host = url.host_str().ok_or_else(invalid)?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.parse::<IpAddr>().is_err() {
        return Err(VadError::InvalidParameter(
            "HTTP Smart Turn endpoint must use an IP address, not a hostname",
        ));
    }
    Ok(url)
}

/// HTTP predictor that runs only inside the analyzer's owned standard thread.
///
/// Each call creates and destroys its current-thread Tokio runtime and client
/// on that same thread. Direct invocation from a Tokio runtime is rejected with
/// a content-free error instead of causing a nested-runtime/drop panic.
#[derive(Debug)]
pub struct HttpSmartTurnPredictor {
    config: HttpSmartTurnConfig,
}

impl HttpSmartTurnPredictor {
    pub fn new(config: HttpSmartTurnConfig) -> Result<Self> {
        // Configuration fields are private and only constructed by validation.
        Ok(Self { config })
    }

    /// Assemble the existing analyzer with no outer inference timeout. HTTP
    /// timeouts must remain INCOMPLETE, not the local timeout COMPLETE policy.
    pub fn into_analyzer(self) -> Result<SmartTurnAnalyzer> {
        self.into_analyzer_with_clock(Arc::new(MonotonicSmartTurnClock::default()))
    }

    pub fn into_analyzer_with_clock(
        self,
        clock: Arc<dyn SmartTurnClock>,
    ) -> Result<SmartTurnAnalyzer> {
        let sample_rate = self.config.sample_rate;
        let params = self.config.params.clone();
        SmartTurnAnalyzer::with_predictor(self, sample_rate, params, clock, None)
    }
}

impl SmartTurnPredictor for HttpSmartTurnPredictor {
    fn predict(
        &mut self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: &SmartTurnCancellation,
    ) -> Result<SmartTurnPrediction> {
        if cancellation.is_cancelled() {
            return Err(VadError::Cancelled);
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn synchronous prediction requires its owned non-Tokio thread",
            ));
        }
        if sample_rate != self.config.sample_rate {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn sample rate differs from the configured endpoint rate",
            ));
        }
        if samples.len() > self.config.max_samples {
            return Err(VadError::InvalidParameter(
                "HTTP Smart Turn samples exceed the configured audio window",
            ));
        }
        if samples.is_empty() {
            return Ok(incomplete());
        }
        let deadline = std::time::Instant::now() + self.config.timeout;
        let body = serialize_npy(samples, cancellation)?;
        if cancellation.is_cancelled() {
            return Err(VadError::Cancelled);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| VadError::Runtime("cannot create HTTP Smart Turn runtime".into()))?;
        let result = runtime.block_on(async {
            tokio::select! {
                biased;
                () = wait_for_cancellation(cancellation) => Err(VadError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Ok(incomplete()),
                result = request_prediction(&self.config, body) => result,
            }
        });
        // IP literals and disabled proxies avoid blocking OS DNS jobs. The
        // client/request have already been dropped; no runtime crosses threads.
        drop(runtime);
        if cancellation.is_cancelled() {
            Err(VadError::Cancelled)
        } else if std::time::Instant::now() >= deadline {
            Ok(incomplete())
        } else {
            result
        }
    }
}

async fn wait_for_cancellation(cancellation: &SmartTurnCancellation) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(CANCELLATION_POLL).await;
    }
}

fn incomplete() -> SmartTurnPrediction {
    SmartTurnPrediction {
        complete: false,
        probability: 0.0,
    }
}

/// NPY 1.0 little-endian float32, shape `(N,)`, with NumPy-style 128-byte
/// alignment. This padding belongs only to the file header, never the waveform.
fn serialize_npy(samples: &[i16], cancellation: &SmartTurnCancellation) -> Result<Vec<u8>> {
    if cancellation.is_cancelled() {
        return Err(VadError::Cancelled);
    }
    let size = samples
        .len()
        .checked_mul(4)
        .and_then(|size| size.checked_add(NPY_HEADER_BYTES))
        .filter(|&size| size <= MAX_REQUEST_BYTES)
        .ok_or(VadError::InvalidParameter(
            "HTTP Smart Turn NPY request exceeds the byte limit",
        ))?;
    let header = format!(
        "{{'descr': '<f4', 'fortran_order': False, 'shape': ({},), }}",
        samples.len()
    );
    let header_len = NPY_HEADER_BYTES - 10;
    if header.len() + 1 > header_len {
        return Err(VadError::InvalidParameter(
            "HTTP Smart Turn NPY header exceeds the format limit",
        ));
    }
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(b"\x93NUMPY\x01\x00");
    bytes.extend_from_slice(&(header_len as u16).to_le_bytes());
    bytes.extend_from_slice(header.as_bytes());
    bytes.resize(NPY_HEADER_BYTES - 1, b' ');
    bytes.push(b'\n');
    for chunk in samples.chunks(4096) {
        if cancellation.is_cancelled() {
            return Err(VadError::Cancelled);
        }
        for &sample in chunk {
            bytes.extend_from_slice(&(f32::from(sample) / 32768.0).to_le_bytes());
        }
    }
    Ok(bytes)
}

async fn request_prediction(
    config: &HttpSmartTurnConfig,
    body: Vec<u8>,
) -> Result<SmartTurnPrediction> {
    // Never format reqwest errors: they may include endpoint query credentials.
    let Ok(client) = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .pool_max_idle_per_host(0)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .timeout(config.timeout)
        .build()
    else {
        return Ok(incomplete());
    };
    let Ok(mut response) = client
        .post(config.endpoint.clone())
        .header(CONTENT_TYPE, "application/octet-stream")
        .headers(config.headers.clone())
        .body(body)
        .send()
        .await
    else {
        return Ok(incomplete());
    };
    if response.status() != StatusCode::OK
        || !is_json_content_type(response.headers().get(CONTENT_TYPE))
        || response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Ok(incomplete());
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if chunk.len() > MAX_RESPONSE_BYTES - body.len() {
                    return Ok(incomplete());
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => return Ok(incomplete()),
        }
    }
    parse_prediction(&body)
}

fn is_json_content_type(value: Option<&HeaderValue>) -> bool {
    let Some(value) = value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let media_type = value.split(';').next().unwrap_or("").trim();
    let media_type = media_type.to_ascii_lowercase();
    let Some(subtype) = media_type.strip_prefix("application/") else {
        return false;
    };
    subtype == "json"
        || (subtype.len() > "+json".len()
            && subtype.ends_with("+json")
            && subtype.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            }))
}

fn parse_prediction(body: &[u8]) -> Result<SmartTurnPrediction> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Ok(incomplete());
    };
    let invalid = || VadError::Model("invalid HTTP Smart Turn response schema".into());
    let prediction = value
        .get("prediction")
        .and_then(serde_json::Value::as_u64)
        .filter(|prediction| *prediction <= 1)
        .ok_or_else(invalid)?;
    let probability = value
        .get("probability")
        .and_then(serde_json::Value::as_f64)
        .filter(|probability| probability.is_finite() && (0.0..=1.0).contains(probability))
        .ok_or_else(invalid)?;
    Ok(SmartTurnPrediction {
        complete: prediction == 1,
        probability: probability as f32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> HttpSmartTurnConfig {
        HttpSmartTurnConfig::new(
            "http://127.0.0.1:8000/predict",
            16_000,
            SmartTurnParams::default(),
        )
        .unwrap()
    }

    #[test]
    fn npy_extremes_have_exact_header_and_no_waveform_padding() {
        let samples = [i16::MIN, -1, 0, 1, i16::MAX];
        let bytes = serialize_npy(&samples, &SmartTurnCancellation::default()).unwrap();
        assert_eq!(&bytes[..8], b"\x93NUMPY\x01\x00");
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), 118);
        assert_eq!(bytes.len(), 148);
        assert_eq!(bytes[127], b'\n');
        // Independent np.save fixture from NumPy 2.3.5, matching the fixed
        // upstream-source probe, rather than a native float round trip alone.
        assert_eq!(
            &bytes[128..],
            b"\x00\x00\x80\xbf\x00\x00\x00\xb8\x00\x00\x00\x00\x00\x00\x00\x38\x00\xfe\x7f\x3f"
        );
        let header = std::str::from_utf8(&bytes[10..128]).unwrap();
        assert_eq!(
            header.trim_end(),
            "{'descr': '<f4', 'fortran_order': False, 'shape': (5,), }"
        );
        let values: Vec<f32> = bytes[128..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|bytes| f32::from_le_bytes(*bytes))
            .collect();
        assert_eq!(
            values,
            [-1.0, -1.0 / 32768.0, 0.0, 1.0 / 32768.0, 32767.0 / 32768.0]
        );
        let empty = serialize_npy(&[], &SmartTurnCancellation::default()).unwrap();
        assert_eq!(empty.len(), 128);
        assert!(std::str::from_utf8(&empty[10..]).unwrap().contains("(0,)"));
    }

    #[test]
    fn npy_byte_limit_and_cancellation_are_explicit() {
        let samples = vec![0; MAX_SAMPLES + 1];
        assert!(matches!(
            serialize_npy(&samples, &SmartTurnCancellation::default()),
            Err(VadError::InvalidParameter(_))
        ));
        let cancellation = SmartTurnCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            serialize_npy(&[0], &cancellation),
            Err(VadError::Cancelled)
        ));
    }

    #[test]
    fn prediction_and_probability_are_independent_and_metrics_ignored() {
        assert_eq!(
            parse_prediction(
                br#"{"prediction":1,"probability":0.01,"metrics":{"probability":99}}"#
            )
            .unwrap(),
            SmartTurnPrediction {
                complete: true,
                probability: 0.01
            }
        );
        assert_eq!(
            parse_prediction(br#"{"prediction":0,"probability":0.99,"metrics":"ignored"}"#)
                .unwrap(),
            SmartTurnPrediction {
                complete: false,
                probability: 0.99
            }
        );
        assert_eq!(parse_prediction(b"not JSON").unwrap(), incomplete());
    }

    #[test]
    fn successful_json_schema_errors_are_explicit_and_content_free() {
        for value in [
            "{}",
            "[]",
            "null",
            "1",
            r#"{"prediction":1}"#,
            r#"{"probability":0.1}"#,
            r#"{"prediction":true,"probability":0.1}"#,
            r#"{"prediction":1.0,"probability":0.1}"#,
            r#"{"prediction":2,"probability":0.1}"#,
            r#"{"prediction":-1,"probability":0.1}"#,
            r#"{"prediction":1,"probability":-0.1}"#,
            r#"{"prediction":1,"probability":1.1}"#,
            r#"{"prediction":1,"probability":"synthetic-private-response"}"#,
        ] {
            let error = parse_prediction(value.as_bytes()).unwrap_err();
            assert!(matches!(error, VadError::Model(_)));
            assert_eq!(
                error.to_string(),
                "model: invalid HTTP Smart Turn response schema"
            );
        }
        for probability in [0, 1] {
            assert!(parse_prediction(
                format!(r#"{{"prediction":0,"probability":{probability}}}"#).as_bytes()
            )
            .is_ok());
        }
    }

    #[test]
    fn only_json_media_types_are_admitted() {
        for value in [
            "application/json",
            "application/json; charset=utf-8",
            "Application/Problem+JSON",
        ] {
            assert!(is_json_content_type(Some(
                &HeaderValue::from_str(value).unwrap()
            )));
        }
        for value in [
            "text/plain",
            "text/json",
            "application/+json",
            "application/jsonp",
            "application/problem+json/extra",
            "application/problem +json",
        ] {
            assert!(!is_json_content_type(Some(
                &HeaderValue::from_str(value).unwrap()
            )));
        }
        assert!(!is_json_content_type(None));
    }

    #[test]
    fn endpoints_must_be_bounded_explicit_ip_urls_without_credentials_or_fragment() {
        for endpoint in ["http://127.0.0.1/predict", "https://[::1]:443/predict"] {
            assert!(validate_endpoint(endpoint).is_ok());
        }
        for endpoint in [
            "http://localhost/predict",
            "https://example.com/predict",
            "file:///tmp/model",
            "127.0.0.1/predict",
            "http://user:secret@127.0.0.1/",
            "http://@127.0.0.1/",
            "http://127.0.0.1/#secret",
            "http://127.0.0.1/\n",
            " http://127.0.0.1/",
            "http:\\127.0.0.1/",
        ] {
            assert!(validate_endpoint(endpoint).is_err(), "accepted {endpoint}");
        }
        assert!(
            validate_endpoint(&format!("http://127.0.0.1/{}", "a".repeat(MAX_URL_BYTES))).is_err()
        );
    }

    #[test]
    fn config_rejects_unbounded_policy_and_accepts_explicit_non_model_rate() {
        for stop_secs in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e-20] {
            let params = SmartTurnParams {
                stop_secs,
                ..SmartTurnParams::default()
            };
            assert!(HttpSmartTurnConfig::new("http://127.0.0.1/", 16_000, params).is_err());
        }
        let params = SmartTurnParams {
            max_duration_secs: 600.0,
            ..SmartTurnParams::default()
        };
        assert!(HttpSmartTurnConfig::new("http://127.0.0.1/", 192_000, params).is_err());
        assert!(
            HttpSmartTurnConfig::new("http://127.0.0.1/", 48_000, SmartTurnParams::default())
                .is_ok()
        );
        assert!(
            HttpSmartTurnConfig::new("http://127.0.0.1/", 0, SmartTurnParams::default()).is_err()
        );
    }

    #[test]
    fn headers_are_bounded_validated_and_debug_is_redacted() {
        let config = HttpSmartTurnConfig::new(
            "http://127.0.0.1/private?synthetic-url-secret",
            16_000,
            SmartTurnParams::default(),
        )
        .unwrap()
        .with_header("authorization", "Bearer synthetic-header-secret")
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("synthetic"));
        assert!(!debug.contains("authorization"));
        assert!(!debug.contains("127.0.0.1"));
        assert!(config.clone().with_header("bad\nname", "value").is_err());
        assert!(config.clone().with_header("x-test", "bad\nvalue").is_err());
        for name in [
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "expect",
        ] {
            assert!(config.clone().with_header(name, "value").is_err());
        }
        assert!(config
            .clone()
            .with_header("x-test", &"a".repeat(MAX_HEADER_BYTES))
            .is_err());
        let mut config = config;
        for index in 1..MAX_HEADERS {
            config = config
                .with_header(&format!("x-test-{index}"), "value")
                .unwrap();
        }
        assert!(config.with_header("x-extra", "value").is_err());
    }

    #[test]
    fn direct_rate_mismatch_and_cancel_are_explicit() {
        let mut predictor = HttpSmartTurnPredictor::new(config()).unwrap();
        let cancellation = SmartTurnCancellation::default();
        assert!(matches!(
            predictor.predict(&[0], 8000, &cancellation),
            Err(VadError::InvalidParameter(_))
        ));
        cancellation.cancel();
        assert!(matches!(
            predictor.predict(&[0], 16_000, &cancellation),
            Err(VadError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn direct_tokio_invocation_is_rejected_without_panicking() {
        let mut predictor = HttpSmartTurnPredictor::new(config()).unwrap();
        assert!(matches!(
            predictor.predict(&[0], 16_000, &SmartTurnCancellation::default()),
            Err(VadError::InvalidParameter(_))
        ));
    }
}
