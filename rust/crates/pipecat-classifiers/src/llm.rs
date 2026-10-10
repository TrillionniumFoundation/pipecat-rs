use crate::{limits, *};
use async_trait::async_trait;
use pipecat_core::{
    ChatMessage, ChatRole, LlmContextData, LlmSettings, Setting, ToolChoice, ToolSet,
};
use pipecat_services::{InferenceOptions, LlmInference};
use serde_json::{json, Map, Value};
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::Instant};

pub const DEFAULT_INSTRUCTIONS: &str = "You are a classifier. Each message describes some input and asks one or more questions about it. Every question has a name and says the shape its answer must have. Reply with a single JSON object, and nothing else: one key per question name, each holding that question's answer in the shape asked for. Probabilities and confidences are numbers from 0 to 1. For example, a yes or no question named \"voicemail\" is answered {\"voicemail\": {\"probability\": 0.97}}. A choice question named \"department\" with the options billing, support and sales is answered {\"department\": {\"choice\": \"billing\", \"probabilities\": {\"billing\": 0.92, \"support\": 0.06, \"sales\": 0.02}}}. A score question named \"mood\" on a scale of three levels is answered with a probability per level, by position, {\"mood\": {\"probabilities\": {\"0\": 0.1, \"1\": 0.7, \"2\": 0.2}}}. Several questions about one input get one object with an answer for each of them.";

#[derive(Clone)]
pub struct LlmClassifierConfig {
    pub name: String,
    /// Optional caller-supplied model label for metrics, not a provider override.
    pub model: Option<String>,
    pub instructions: String,
    pub max_tokens: Option<u64>,
    pub timeout: Duration,
    pub limits: ClassifierLimits,
}
impl Default for LlmClassifierConfig {
    fn default() -> Self {
        Self {
            name: "LlmClassifier".into(),
            model: None,
            instructions: DEFAULT_INSTRUCTIONS.into(),
            max_tokens: None,
            timeout: Duration::from_secs(10),
            limits: ClassifierLimits::default(),
        }
    }
}
impl fmt::Debug for LlmClassifierConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmClassifierConfig")
            .field("timeout", &self.timeout)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
pub struct LlmClassifier {
    llm: Arc<dyn LlmInference>,
    config: LlmClassifierConfig,
    permits: Semaphore,
}
impl fmt::Debug for LlmClassifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmClassifier")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl LlmClassifier {
    pub fn new(llm: Arc<dyn LlmInference>, config: LlmClassifierConfig) -> Result<Self> {
        config.limits.validate()?;
        config.limits.name(&config.name)?;
        if let Some(model) = &config.model {
            config.limits.name(model)?;
        }
        if !limits::duration_valid(config.timeout) {
            return Err(ClassifierError::Configuration(
                "timeout must be in (0, 300s]",
            ));
        }
        if config.instructions.len() > config.limits.max_request_bytes
            || config.max_tokens.is_some_and(|v| v == 0 || v > 1_000_000)
        {
            return Err(ClassifierError::Configuration(
                "instructions or token limit",
            ));
        }
        Ok(Self {
            permits: Semaphore::new(config.limits.max_in_flight),
            llm,
            config,
        })
    }
    /// A schema for every named answer. The current LlmInference interface has
    /// no native schema option; this schema is supplied in the prompt only.
    pub fn response_schema(&self, questions: &Questions) -> Result<Value> {
        self.config
            .limits
            .request(&Value::String(String::new()), questions)?;
        let schema = schema(questions, &self.config.limits)?;
        limits::encode(&schema, self.config.limits.max_request_bytes)?;
        Ok(schema)
    }
}
#[async_trait]
impl Classifier for LlmClassifier {
    async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        options: CallOptions,
    ) -> Result<ClassificationBatch> {
        let started = Instant::now();
        let deadline = options.deadline(self.config.timeout);
        options.check(deadline)?;
        // Admission precedes rendering/allocation; concurrent queued callers retain
        // only their borrowed input, not a second encoded request.
        options
            .run(deadline, &CancellationToken::new(), async {
                let _permit = self
                    .permits
                    .acquire()
                    .await
                    .map_err(|_| ClassifierError::Closed)?;
                self.config.limits.request(state, questions)?;
                let prompt_budget = self
                    .config
                    .limits
                    .max_request_bytes
                    .saturating_sub(self.config.instructions.len());
                let mut prompt = render(state, questions, prompt_budget)?;
                let marker = "\n\nJSON response schema (follow exactly):\n";
                if marker.len() > prompt_budget.saturating_sub(prompt.len()) {
                    return Err(ClassifierError::Limit("request bytes"));
                }
                prompt.push_str(marker);
                let schema = schema(questions, &self.config.limits)?;
                let encoded = limits::encode(&schema, prompt_budget.saturating_sub(prompt.len()))?;
                prompt.push_str(&String::from_utf8(encoded).map_err(|_| ClassifierError::State)?);
                let context = LlmContextData {
                    messages: vec![ChatMessage::new(ChatRole::User, prompt).into()],
                    tools: ToolSet::Functions(vec![]),
                    tool_choice: Some(ToolChoice::None),
                };
                let inference = InferenceOptions {
                    settings: LlmSettings {
                        system_instruction: Setting::Value(self.config.instructions.clone()),
                        max_tokens: self
                            .config
                            .max_tokens
                            .map_or(Setting::NotGiven, Setting::Value),
                        ..Default::default()
                    },
                    max_output_bytes: self.config.limits.max_response_bytes,
                };
                options.check(deadline)?;
                let reply = self
                    .llm
                    .run_inference(context, inference)
                    .await
                    .map_err(|_| ClassifierError::Inference)?
                    .unwrap_or_default();
                options.check(deadline)?;
                let answers = parse_answers(&reply, questions, &self.config.limits)?;
                options.check(deadline)?;
                Ok(ClassificationBatch {
                    answers,
                    metrics: ClassificationMetrics {
                        classifier: self.config.name.clone(),
                        model: self.config.model.clone(),
                        elapsed: started.elapsed(),
                        usage: None,
                        provenance: ProbabilityProvenance::LlmEstimate,
                    },
                })
            })
            .await
    }
}
fn object(properties: Map<String, Value>) -> Value {
    let required: Vec<_> = properties.keys().cloned().collect();
    json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
}
fn schema(questions: &Questions, limits: &ClassifierLimits) -> Result<Value> {
    // Count the derived schema tree before allocation, including required-name
    // strings and enum entries. Object-schema scaffolding contributes 5 + N
    // nodes, a number schema 2, and a choice string/enum schema 3 + N.
    let mut nodes = 5usize.saturating_add(questions.len());
    let mut depth = 6;
    for question in questions.values() {
        nodes = nodes.saturating_add(match question {
            ClassifierQuestion::YesNo(_) => 8,
            ClassifierQuestion::Choice(q) => {
                depth = 8;
                15usize.saturating_add(q.options.len().saturating_mul(4))
            }
            ClassifierQuestion::Score(q) => {
                depth = 8;
                11usize.saturating_add(q.levels.len().saturating_mul(3))
            }
        });
        if nodes > limits.max_json_nodes {
            return Err(ClassifierError::Limit("JSON nodes"));
        }
    }
    if depth > limits.max_json_depth {
        return Err(ClassifierError::Limit("JSON depth"));
    }
    Ok(object(
        questions
            .iter()
            .map(|(name, q)| {
                let properties = match q {
                    ClassifierQuestion::YesNo(_) => {
                        Map::from_iter([("probability".into(), json!({"type":"number"}))])
                    }
                    ClassifierQuestion::Choice(q) => Map::from_iter([
                        (
                            "choice".into(),
                            json!({"type":"string", "enum":q.options.keys().collect::<Vec<_>>()}),
                        ),
                        (
                            "probabilities".into(),
                            object(
                                q.options
                                    .keys()
                                    .map(|key| (key.clone(), json!({"type":"number"})))
                                    .collect(),
                            ),
                        ),
                    ]),
                    ClassifierQuestion::Score(q) => Map::from_iter([(
                        "probabilities".into(),
                        object(
                            (0..q.levels.len())
                                .map(|i| (i.to_string(), json!({"type":"number"})))
                                .collect(),
                        ),
                    )]),
                };
                (name.clone(), object(properties))
            })
            .collect(),
    ))
}
fn text(value: &Value, limit: usize) -> Result<String> {
    match value {
        Value::String(s) if s.len() <= limit => Ok(s.clone()),
        Value::String(_) => Err(ClassifierError::Limit("request bytes")),
        _ => String::from_utf8(limits::encode_pretty(value, limit)?)
            .map_err(|_| ClassifierError::State),
    }
}
fn render(state: &Value, questions: &Questions, limit: usize) -> Result<String> {
    let mut prompt = String::new();
    let mut append = |part: &str| -> Result<()> {
        if part.len() > limit.saturating_sub(prompt.len()) {
            return Err(ClassifierError::Limit("request bytes"));
        }
        prompt.push_str(part);
        Ok(())
    };
    append("Input:\n")?;
    append(&text(state, limit)?)?;
    for (name, q) in questions {
        let instructions = match q {
            ClassifierQuestion::YesNo(q) => &q.instructions,
            ClassifierQuestion::Choice(q) => &q.instructions,
            ClassifierQuestion::Score(q) => &q.instructions,
        };
        append(&format!(
            "\n\nQuestion {}: {}",
            json!(name),
            text(instructions, limit)?
        ))?;
        match q {
            ClassifierQuestion::YesNo(q) => {
                if let Some(yes) = &q.yes {
                    append(&format!("\nYes means: {}", text(yes, limit)?))?;
                }
                if let Some(no) = &q.no {
                    append(&format!("\nNo means: {}", text(no, limit)?))?;
                }
                append("\nAnswer shape: {\"probability\": <probability that the answer is yes>}")?;
            }
            ClassifierQuestion::Choice(q) => {
                append("\nOptions:")?;
                for (option, description) in &q.options {
                    append(&match description {
                        Some(v) => format!("\n- {option}: {}", text(v, limit)?),
                        None => format!("\n- {option}"),
                    })?;
                }
                append("\nAnswer shape: {\"choice\": <the option that fits best>, \"probabilities\": {<a probability per option, summing to 1>}}")?;
            }
            ClassifierQuestion::Score(q) => {
                append("\nScale, lowest first:")?;
                for (i, level) in q.levels.iter().enumerate() {
                    append(&format!("\n{i}: {}", text(level, limit)?))?;
                }
                let positions = (0..q.levels.len())
                    .map(|i| format!("\"{i}\": <probability>"))
                    .collect::<Vec<_>>()
                    .join(", ");
                append(&format!("\nAnswer shape: {{\"probabilities\": {{{positions}}}}}, one per level by position, summing to 1"))?;
            }
        }
    }
    let names = questions
        .keys()
        .map(|name| format!("{}: <answer to {}>", json!(name), json!(name)))
        .collect::<Vec<_>>()
        .join(", ");
    append(&format!(
        "\n\nReply with one JSON object of this shape: {{{names}}}"
    ))?;
    Ok(prompt)
}
fn parse_answers(
    reply: &str,
    questions: &Questions,
    limits: &ClassifierLimits,
) -> Result<BTreeMap<String, ClassifierResult>> {
    if reply.len() > limits.max_response_bytes {
        return Err(ClassifierError::Limit("response bytes"));
    }
    let mut text = reply.trim();
    if let Some(start) = text.find("```") {
        let fenced = &text[start + 3..];
        if let Some(end) = fenced.find("```") {
            text = fenced[..end]
                .strip_prefix("json")
                .unwrap_or(&fenced[..end])
                .trim();
        }
    }
    let (start, end) = (text.find('{'), text.rfind('}'));
    let (Some(start), Some(end)) = (start, end) else {
        return Err(ClassifierError::Response("not a JSON object"));
    };
    if start > end {
        return Err(ClassifierError::Response("not valid JSON"));
    }
    let data = limits.parse(&text.as_bytes()[start..=end])?;
    let answers = data
        .as_object()
        .ok_or(ClassifierError::Response("not a JSON object"))?;
    let unwrapped = questions.len() == 1 && !questions.keys().any(|key| answers.contains_key(key));
    questions
        .iter()
        .map(|(name, question)| {
            let answer = if unwrapped {
                answers
            } else {
                answers
                    .get(name)
                    .and_then(Value::as_object)
                    .ok_or(ClassifierError::Response("missing or non-object answer"))?
            };
            Ok((name.clone(), result(question, answer)?))
        })
        .collect()
}
pub(crate) fn number(value: Option<&Value>) -> Result<f64> {
    let number = match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
    .ok_or(ClassifierError::Response("missing or unusable number"))?;
    if !number.is_finite() {
        return Err(ClassifierError::Response("nonfinite number"));
    }
    Ok(number)
}
fn clamp(value: Option<&Value>) -> Result<f64> {
    match number(value) {
        Ok(n) => Ok(n.clamp(0.0, 1.0)),
        Err(ClassifierError::Response("nonfinite number")) => {
            Err(ClassifierError::Response("nonfinite number"))
        }
        Err(_) => Ok(0.0),
    }
}
fn result(question: &ClassifierQuestion, answer: &Map<String, Value>) -> Result<ClassifierResult> {
    match question {
        ClassifierQuestion::YesNo(_) => Ok(ClassifierResult::YesNo(YesNoResult {
            probability: number(answer.get("probability"))?.clamp(0.0, 1.0),
        })),
        ClassifierQuestion::Choice(q) => {
            let choice = answer
                .get("choice")
                .and_then(Value::as_str)
                .filter(|c| q.options.contains_key(*c))
                .ok_or(ClassifierError::Response("choice is not an option"))?;
            let given = answer.get("probabilities").and_then(Value::as_object);
            let probabilities: BTreeMap<_, _> = q
                .options
                .keys()
                .map(|key| Ok((key.clone(), clamp(given.and_then(|p| p.get(key)))?)))
                .collect::<Result<_>>()?;
            Ok(ClassifierResult::Choice(ChoiceResult {
                choice: choice.into(),
                confidence: probabilities[choice],
                probabilities,
            }))
        }
        ClassifierQuestion::Score(q) => {
            let given = answer.get("probabilities").and_then(Value::as_object);
            let mut probabilities: Vec<_> = (0..q.levels.len())
                .map(|i| clamp(given.and_then(|p| p.get(&i.to_string()))))
                .collect::<Result<_>>()?;
            let total: f64 = probabilities.iter().sum();
            if total <= 0.0 {
                return Err(ClassifierError::Response("no probability for any level"));
            }
            for p in &mut probabilities {
                *p /= total;
            }
            let score = probabilities
                .iter()
                .enumerate()
                .map(|(i, p)| i as f64 * p)
                .sum::<f64>()
                .clamp(0.0, (q.levels.len() - 1) as f64);
            let confidence = probabilities.iter().copied().fold(0.0, f64::max);
            Ok(ClassifierResult::Score(ScoreResult {
                score,
                confidence,
                levels: q
                    .levels
                    .iter()
                    .cloned()
                    .zip(probabilities)
                    .map(|(level, probability)| ScoreLevel { level, probability })
                    .collect(),
            }))
        }
    }
}
