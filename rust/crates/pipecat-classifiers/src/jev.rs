use crate::{limits, llm::number, *};
use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Client, Url,
};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    fmt,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};

pub const DEFAULT_JEV_BASE_URL: &str = "https://api.typesafe.ai";
pub const DEFAULT_JEV_MODEL: &str = "jev-1.13.0";
pub const JEV_MAX_CHOICE_OPTIONS: usize = 255;

#[derive(Clone)]
pub struct JevConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub timeout: Duration,
    pub max_retries: u32,
    pub limits: ClassifierLimits,
}
impl JevConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_JEV_BASE_URL.into(),
            model: DEFAULT_JEV_MODEL.into(),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            limits: ClassifierLimits::default(),
        }
    }
}
impl fmt::Debug for JevConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JevConfig")
            .field("api_key", &"[redacted]")
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
struct Inner {
    http: Mutex<Option<Client>>,
    model: String,
    base: Url,
    timeout: Duration,
    max_retries: u32,
    limits: ClassifierLimits,
    closed: CancellationToken,
    connected: tokio::sync::Mutex<bool>,
    permits: Semaphore,
    usage: Mutex<JevUsage>,
}
/// Clones share a pool, close token and cumulative usage. Closing any clone closes
/// the client permanently and cancels all in-flight calls; reopening is explicit
/// construction of a new client. Dropping a request drops its network future.
#[derive(Clone)]
pub struct JevClient {
    inner: Arc<Inner>,
}
impl fmt::Debug for JevClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JevClient")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct JevResponse {
    pub answers: BTreeMap<String, Value>,
    pub usage: JevUsage,
}
impl JevClient {
    pub fn new(config: JevConfig) -> Result<Self> {
        config.limits.validate()?;
        config.limits.name(&config.model)?;
        if config.base_url.len() > 4096 {
            return Err(ClassifierError::Configuration("endpoint length"));
        }
        if config.api_key.is_empty() || config.api_key.len() > 16 * 1024 {
            return Err(ClassifierError::Configuration(
                "API key is empty or oversized",
            ));
        }
        if !limits::duration_valid(config.timeout) || config.max_retries > 8 {
            return Err(ClassifierError::Configuration("timeout or retry limit"));
        }
        let base = Url::parse(&config.base_url)
            .map_err(|_| ClassifierError::Configuration("invalid endpoint"))?;
        let loopback = base.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || !matches!(base.path(), "" | "/")
        {
            return Err(ClassifierError::Configuration("endpoint must be an HTTPS origin (HTTP is loopback-only), without userinfo, path, query or fragment"));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", config.api_key))
            .map_err(|_| ClassifierError::Configuration("invalid API key header"))?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let http = Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .pool_idle_timeout(Duration::from_secs(240))
            .pool_max_idle_per_host(config.limits.max_in_flight)
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .build()
            .map_err(|_| ClassifierError::Configuration("HTTP client construction failed"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                http: Mutex::new(Some(http)),
                model: config.model,
                base,
                timeout: config.timeout,
                max_retries: config.max_retries,
                permits: Semaphore::new(config.limits.max_in_flight),
                limits: config.limits,
                closed: CancellationToken::new(),
                connected: tokio::sync::Mutex::new(false),
                usage: Mutex::new(JevUsage::default()),
            }),
        })
    }
    pub fn model(&self) -> &str {
        &self.inner.model
    }
    pub fn usage(&self) -> Result<JevUsage> {
        self.inner
            .usage
            .lock()
            .map(|v| *v)
            .map_err(|_| ClassifierError::State)
    }
    pub fn is_closed(&self) -> bool {
        self.inner.closed.is_cancelled()
    }
    pub fn close(&self) {
        self.inner.closed.cancel();
        self.inner.permits.close();
        if let Ok(mut http) = self.inner.http.lock() {
            http.take();
        }
    }
    fn http(&self) -> Result<Client> {
        if self.is_closed() {
            return Err(ClassifierError::Closed);
        }
        self.inner
            .http
            .lock()
            .map_err(|_| ClassifierError::State)?
            .as_ref()
            .cloned()
            .ok_or(ClassifierError::Closed)
    }
    fn endpoint(&self, path: &str) -> Result<Url> {
        self.inner
            .base
            .join(path)
            .map_err(|_| ClassifierError::Configuration("invalid endpoint"))
    }
    /// Optional warmup, serialized across all shared handles. An error is returned
    /// for visibility; callers may continue with ask after an unsuccessful warmup.
    pub async fn connect(&self, options: CallOptions) -> Result<()> {
        let deadline = options.deadline(self.inner.timeout);
        options
            .run(deadline, &self.inner.closed, async {
                let mut connected = self.inner.connected.lock().await;
                if *connected {
                    return Ok(());
                }
                let _permit = self
                    .inner
                    .permits
                    .acquire()
                    .await
                    .map_err(|_| ClassifierError::Closed)?;
                let response = self
                    .http()?
                    .get(self.endpoint("/v1/models")?)
                    .send()
                    .await
                    .map_err(|_| ClassifierError::Transport)?;
                if response.status().as_u16() != 200 {
                    return Err(ClassifierError::Http(response.status().as_u16()));
                }
                self.body(response).await?;
                options.check(deadline)?;
                *connected = true;
                Ok(())
            })
            .await
    }
    /// Submit named typed questions using Jev's native wire mapping. Responses
    /// retain provider fields; use JevClassifier for typed answer validation.
    pub async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        options: CallOptions,
    ) -> Result<JevResponse> {
        let deadline = options.deadline(self.inner.timeout);
        options
            .run(deadline, &self.inner.closed, async {
                let _permit = self
                    .inner
                    .permits
                    .acquire()
                    .await
                    .map_err(|_| ClassifierError::Closed)?;
                self.inner.limits.request(state, questions)?;
                let mapped: BTreeMap<_, _> = questions
                    .iter()
                    .map(|(name, q)| (name, to_jev(q)))
                    .collect();
                #[derive(Serialize)]
                struct Request<'a> {
                    model: &'a str,
                    state: &'a Value,
                    questions: BTreeMap<&'a String, Value>,
                }
                let body = limits::encode(
                    &Request {
                        model: self.model(),
                        state,
                        questions: mapped,
                    },
                    self.inner.limits.max_request_bytes,
                )?;
                self.inner.limits.request_json(&body)?;
                let http = self.http()?;
                let endpoint = self.endpoint("/v1/systemone")?;
                for attempt in 0..=self.inner.max_retries {
                    options.check(deadline)?;
                    let response = http
                        .post(endpoint.clone())
                        .body(body.clone())
                        .send()
                        .await
                        .map_err(|_| ClassifierError::Transport)?;
                    let status = response.status().as_u16();
                    if status == 429 || status == 529 {
                        // Drop the body without buffering. No status body is exposed,
                        // and only an explicit busy response is safe to replay.
                        drop(response);
                        if attempt == self.inner.max_retries {
                            return Err(ClassifierError::Busy {
                                status,
                                attempts: attempt + 1,
                            });
                        }
                        tokio::time::sleep(Duration::from_millis((250u64 << attempt).min(2000)))
                            .await;
                        continue;
                    }
                    if status != 200 {
                        return Err(ClassifierError::Http(status));
                    }
                    let bytes = self.body(response).await?;
                    let data = self.inner.limits.parse(&bytes)?;
                    let data = data
                        .as_object()
                        .ok_or(ClassifierError::Response("not a JSON object"))?;
                    let usage = usage(data.get("usage"))?;
                    // Account for a completed billable response even when its answer
                    // shape is unusable, as in the pinned source.
                    {
                        let mut cumulative = self
                            .inner
                            .usage
                            .lock()
                            .map_err(|_| ClassifierError::State)?;
                        *cumulative = cumulative.checked_add(usage)?;
                    }
                    let answers = data
                        .get("answers")
                        .and_then(Value::as_object)
                        .ok_or(ClassifierError::Response("no answers"))?;
                    let answers = questions
                        .keys()
                        .map(|name| {
                            let answer = answers
                                .get(name)
                                .filter(|v| v.is_object())
                                .ok_or(ClassifierError::Response("missing or non-object answer"))?;
                            Ok((name.clone(), answer.clone()))
                        })
                        .collect::<Result<_>>()?;
                    options.check(deadline)?;
                    return Ok(JevResponse { answers, usage });
                }
                Err(ClassifierError::State)
            })
            .await
    }
    async fn body(&self, mut response: reqwest::Response) -> Result<Vec<u8>> {
        let cap = self.inner.limits.max_response_bytes;
        if response.content_length().is_some_and(|n| n > cap as u64) {
            return Err(ClassifierError::Limit("response bytes"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ClassifierError::Transport)?
        {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(ClassifierError::Limit("response bytes"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}
fn usage(value: Option<&Value>) -> Result<JevUsage> {
    let Some(value) = value.and_then(Value::as_object) else {
        return Ok(JevUsage::default());
    };
    let token = |key: &str| -> Result<u64> {
        match value.get(key) {
            None => Ok(0),
            Some(v) => v
                .as_u64()
                .ok_or(ClassifierError::Response("invalid usage counter")),
        }
    };
    let usage = JevUsage {
        input_tokens: token("input_tokens")?,
        output_tokens: token("output_tokens")?,
    };
    usage.total_tokens()?;
    Ok(usage)
}
fn criteria(value: &Option<Value>) -> Value {
    match value {
        None | Some(Value::Null) => json!(""),
        Some(Value::String(s)) if s.is_empty() => json!(""),
        Some(Value::Array(v)) if v.is_empty() => json!(""),
        Some(Value::Object(v)) if v.is_empty() => json!(""),
        Some(v) => v.clone(),
    }
}
fn to_jev(question: &ClassifierQuestion) -> Value {
    match question {
        ClassifierQuestion::YesNo(q) => {
            let mut wire = json!({"type":"noul", "instructions":q.instructions});
            if q.yes.is_some() || q.no.is_some() {
                wire["criteria"] = json!({"true":criteria(&q.yes), "false":criteria(&q.no)});
            }
            wire
        }
        ClassifierQuestion::Choice(q) => {
            json!({"type":"choice", "instructions":q.instructions, "criteria":q.options})
        }
        ClassifierQuestion::Score(q) => {
            json!({"type":"score", "instructions":q.instructions, "criteria":q.levels})
        }
    }
}

pub struct JevClassifier {
    client: JevClient,
    owns_client: bool,
    name: String,
}
impl fmt::Debug for JevClassifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JevClassifier")
            .field("client", &self.client)
            .field("owns_client", &self.owns_client)
            .finish_non_exhaustive()
    }
}
impl JevClassifier {
    pub fn owned(config: JevConfig) -> Result<Self> {
        Ok(Self {
            client: JevClient::new(config)?,
            owns_client: true,
            name: "JevClassifier".into(),
        })
    }
    pub fn shared(client: JevClient) -> Self {
        Self {
            client,
            owns_client: false,
            name: "JevClassifier".into(),
        }
    }
    pub fn with_name(mut self, name: impl Into<String>) -> Result<Self> {
        self.name = name.into();
        self.client.inner.limits.name(&self.name)?;
        Ok(self)
    }
    pub fn client(&self) -> &JevClient {
        &self.client
    }
    pub async fn setup(&self, options: CallOptions) -> Result<()> {
        self.client.connect(options).await
    }
    pub fn cleanup(&self) {
        if self.owns_client {
            self.client.close();
        }
    }
}
impl Drop for JevClassifier {
    fn drop(&mut self) {
        self.cleanup();
    }
}
#[async_trait]
impl Classifier for JevClassifier {
    async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        options: CallOptions,
    ) -> Result<ClassificationBatch> {
        let started = Instant::now();
        let deadline = options.deadline(self.client.inner.timeout);
        let request_options = CallOptions {
            deadline: Some(deadline),
            cancellation: options.cancellation.clone(),
        };
        let response = self.client.ask(state, questions, request_options).await?;
        let answers = questions
            .iter()
            .map(|(name, q)| Ok((name.clone(), from_jev(q, &response.answers[name])?)))
            .collect::<Result<_>>()?;
        options.check(deadline)?;
        if self.client.is_closed() {
            return Err(ClassifierError::Closed);
        }
        Ok(ClassificationBatch {
            answers,
            metrics: ClassificationMetrics {
                classifier: self.name.clone(),
                model: Some(self.client.model().into()),
                elapsed: started.elapsed(),
                usage: Some(response.usage),
                provenance: ProbabilityProvenance::JevProvider,
            },
        })
    }
}
fn unit(value: Option<&Value>) -> Result<f64> {
    let value = number(value)?;
    if !(0.0..=1.0).contains(&value) {
        return Err(ClassifierError::Response("probability outside [0, 1]"));
    }
    Ok(value)
}
fn probabilities(
    answer: &Map<String, Value>,
    keys: impl Iterator<Item = String>,
) -> Result<BTreeMap<String, f64>> {
    let given = match answer.get("probabilities") {
        None | Some(Value::Null) => None,
        Some(Value::Object(v)) => Some(v),
        _ => return Err(ClassifierError::Response("probabilities is not an object")),
    };
    keys.map(|key| {
        let p = given
            .and_then(|given| given.get(&key))
            .map_or(Ok(0.0), |v| unit(Some(v)))?;
        Ok((key, p))
    })
    .collect()
}
fn from_jev(question: &ClassifierQuestion, answer: &Value) -> Result<ClassifierResult> {
    let answer = answer
        .as_object()
        .ok_or(ClassifierError::Response("non-object answer"))?;
    let expected = match question {
        ClassifierQuestion::YesNo(_) => "noul",
        ClassifierQuestion::Choice(_) => "choice",
        ClassifierQuestion::Score(_) => "score",
    };
    if answer
        .get("type")
        .is_some_and(|v| v.as_str() != Some(expected))
    {
        return Err(ClassifierError::Response("wrong answer type"));
    }
    match question {
        ClassifierQuestion::YesNo(_) => Ok(ClassifierResult::YesNo(YesNoResult {
            probability: unit(answer.get("noul"))?,
        })),
        ClassifierQuestion::Choice(q) => {
            let choice = answer
                .get("choice")
                .and_then(Value::as_str)
                .filter(|c| q.options.contains_key(*c))
                .ok_or(ClassifierError::Response("choice is not an option"))?;
            Ok(ClassifierResult::Choice(ChoiceResult {
                choice: choice.into(),
                probabilities: probabilities(answer, q.options.keys().cloned())?,
                confidence: unit(answer.get("confidence"))?,
            }))
        }
        ClassifierQuestion::Score(q) => {
            let score = number(answer.get("score"))?;
            if !(0.0..=(q.levels.len() - 1) as f64).contains(&score) {
                return Err(ClassifierError::Response("score outside level range"));
            }
            let probabilities = probabilities(answer, (0..q.levels.len()).map(|i| i.to_string()))?;
            Ok(ClassifierResult::Score(ScoreResult {
                score,
                confidence: unit(answer.get("confidence"))?,
                levels: q
                    .levels
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, level)| ScoreLevel {
                        level,
                        probability: probabilities[&i.to_string()],
                    })
                    .collect(),
            }))
        }
    }
}
