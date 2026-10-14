//! Bounded, read-only projection of ordered RTVI `ui-job-group` events.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

pub const MAX_GROUPS: usize = 8;
pub const MAX_WORKERS: usize = 16;
pub const MAX_SEEN_IDS: usize = 64;
pub const MAX_ID_BYTES: usize = 128;
pub const MAX_LABEL_BYTES: usize = 512;
pub const MAX_STATUS_BYTES: usize = 256;
pub const MAX_PREVIEW_BYTES: usize = 4096;
pub const MAX_EVENT_BYTES: usize = 32 * 1024;
pub const MAX_CONTENT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Worker {
    pub name: String,
    pub at: u64,
    /// Completion is terminal even when its result was omitted for capacity.
    pub completed: bool,
    /// Any status text is accepted; None after completion means the result was omitted.
    pub status: Option<String>,
    /// Serialized JSON for literal text rendering only; never HTML or executable content.
    pub data: Option<String>,
    pub response: Option<String>,
}
impl Worker {
    fn new(name: String) -> Self {
        Self {
            name,
            at: 0,
            completed: false,
            status: None,
            data: None,
            response: None,
        }
    }
    fn content_bytes(&self) -> usize {
        self.name.len() + bytes(&self.status) + bytes(&self.data) + bytes(&self.response)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobGroup {
    pub job_id: String,
    pub label: Option<String>,
    /// Wire metadata only: this model does not expose a cancellation action.
    pub cancellable: bool,
    pub at: u64,
    pub completed: bool,
    pub workers: Vec<Worker>,
    // Separate bounded metadata ledger: omitted completions cannot resurrect.
    completed_omitted: HashSet<String>,
    seal_unknown_workers: bool,
}
impl JobGroup {
    fn content_bytes(&self) -> usize {
        self.job_id.len()
            + bytes(&self.label)
            + self
                .workers
                .iter()
                .map(Worker::content_bytes)
                .sum::<usize>()
    }
}
fn bytes(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::len)
}
fn default_true() -> bool {
    true
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    GroupStarted {
        job_id: String,
        #[serde(default)]
        workers: Option<Vec<String>>,
        #[serde(default)]
        label: Option<String>,
        #[serde(default = "default_true")]
        cancellable: bool,
        #[serde(default)]
        at: u64,
    },
    JobUpdate {
        job_id: String,
        worker_name: String,
        #[serde(default)]
        data: Option<Value>,
        #[serde(default)]
        at: u64,
    },
    JobCompleted {
        job_id: String,
        worker_name: String,
        status: String,
        #[serde(default)]
        response: Option<Value>,
        #[serde(default)]
        at: u64,
    },
    GroupCompleted {
        job_id: String,
        #[serde(default)]
        at: u64,
    },
}
fn bounded(value: &str, limit: usize, field: &str, nonempty: bool) -> Result<(), String> {
    if value.len() > limit || (nonempty && value.is_empty()) {
        return Err(format!(
            "invalid {field}: byte length outside allowed range"
        ));
    }
    Ok(())
}
fn id(value: &str) -> Result<(), String> {
    bounded(value, MAX_ID_BYTES, "identifier", true)
}
fn preview(value: Option<Value>) -> Result<Option<String>, String> {
    value
        .map(|value| {
            let text = serde_json::to_string(&value).map_err(|e| e.to_string())?;
            bounded(&text, MAX_PREVIEW_BYTES, "JSON preview", false)?;
            Ok(text)
        })
        .transpose()
}

#[derive(Default, Debug)]
pub struct JobGroups {
    epoch: Option<u64>,
    groups: Vec<JobGroup>,
    // Includes completed/evicted groups and valid starts omitted for capacity.
    seen: HashSet<String>,
    incomplete: bool,
}
impl JobGroups {
    pub fn begin(&mut self, epoch: u64) {
        *self = Self {
            epoch: Some(epoch),
            ..Self::default()
        };
    }
    pub fn close(&mut self) {
        *self = Self::default();
    }
    pub fn groups(&self) -> &[JobGroup] {
        &self.groups
    }
    pub fn is_incomplete(&self) -> bool {
        self.incomplete
    }
    pub fn retained_content_bytes(&self) -> usize {
        self.groups.iter().map(JobGroup::content_bytes).sum()
    }
    fn omit(&mut self) -> bool {
        let changed = !self.incomplete;
        self.incomplete = true;
        changed
    }
    /// Returns whether the projection (including its incomplete flag) changed.
    /// Stale sessions and other envelope types are ignored. Input validation is
    /// atomic for accepted groups. Any matching-session rejection latches
    /// `incomplete`, including errors; callers should refresh the view on Err.
    /// Valid capacity omissions also latch `incomplete`; starts consume
    /// a seen-ID slot so delayed messages cannot resurrect omitted work.
    /// `at` is metadata, never a clock for reordering or deduplication.
    pub fn apply(&mut self, epoch: u64, envelope: &Value) -> Result<bool, String> {
        if self.epoch != Some(epoch)
            || envelope.get("label").and_then(Value::as_str) != Some("rtvi-ai")
            || envelope.get("type").and_then(Value::as_str) != Some("ui-job-group")
        {
            return Ok(false);
        }
        let result = self.apply_event(envelope);
        if result.is_err() {
            self.incomplete = true;
        }
        result
    }
    fn apply_event(&mut self, envelope: &Value) -> Result<bool, String> {
        if serde_json::to_vec(envelope)
            .map_err(|e| e.to_string())?
            .len()
            > MAX_EVENT_BYTES
        {
            return Err("ui-job-group event exceeds byte limit".into());
        }
        let event: Event =
            serde_json::from_value(envelope.get("data").cloned().unwrap_or(Value::Null))
                .map_err(|e| format!("invalid ui-job-group event: {e}"))?;
        match event {
            Event::GroupStarted {
                job_id,
                workers,
                label,
                cancellable,
                at,
            } => {
                id(&job_id)?;
                if let Some(label) = &label {
                    bounded(label, MAX_LABEL_BYTES, "label", false)?;
                }
                let workers = workers.unwrap_or_default();
                if workers.len() > MAX_WORKERS {
                    return Err("too many initial workers".into());
                }
                let mut names = HashSet::new();
                for name in &workers {
                    id(name)?;
                    if !names.insert(name) {
                        return Err("duplicate initial worker name".into());
                    }
                }
                if self.seen.contains(&job_id) {
                    return Ok(false);
                }
                if self.seen.len() == MAX_SEEN_IDS {
                    return Ok(self.omit());
                }
                self.seen.insert(job_id.clone());
                let group = JobGroup {
                    job_id,
                    label,
                    cancellable,
                    at,
                    completed: false,
                    workers: workers.into_iter().map(Worker::new).collect(),
                    completed_omitted: HashSet::new(),
                    seal_unknown_workers: false,
                };
                // Prepare a candidate first: a rejected start cannot evict anything.
                let mut candidate = self.groups.clone();
                let mut evicted = false;
                if candidate.len() == MAX_GROUPS {
                    if let Some(index) = candidate.iter().position(|group| group.completed) {
                        candidate.remove(index);
                        evicted = true;
                    } else {
                        return Ok(self.omit());
                    }
                }
                candidate.push(group);
                if candidate.iter().map(JobGroup::content_bytes).sum::<usize>() > MAX_CONTENT_BYTES
                {
                    return Ok(self.omit());
                }
                self.groups = candidate;
                self.incomplete |= evicted;
                Ok(true)
            }
            Event::JobUpdate {
                job_id,
                worker_name,
                data,
                at,
            } => {
                id(&job_id)?;
                id(&worker_name)?;
                let data = preview(data)?;
                self.worker_event(&job_id, worker_name, at, data, None, None)
            }
            Event::JobCompleted {
                job_id,
                worker_name,
                status,
                response,
                at,
            } => {
                id(&job_id)?;
                id(&worker_name)?;
                bounded(&status, MAX_STATUS_BYTES, "status", false)?;
                let response = preview(response)?;
                self.worker_event(&job_id, worker_name, at, None, Some(status), response)
            }
            Event::GroupCompleted { job_id, at } => {
                id(&job_id)?;
                let Some(group) = self.groups.iter_mut().find(|g| g.job_id == job_id) else {
                    return Ok(false);
                };
                if group.completed {
                    return Ok(false);
                }
                group.completed = true;
                group.at = at;
                Ok(true)
            }
        }
    }
    fn worker_event(
        &mut self,
        job_id: &str,
        name: String,
        at: u64,
        data: Option<String>,
        status: Option<String>,
        response: Option<String>,
    ) -> Result<bool, String> {
        let Some(index) = self.groups.iter().position(|g| g.job_id == job_id) else {
            return Ok(false);
        };
        let group = &self.groups[index];
        if group.completed {
            return Ok(false);
        }
        let known_worker = group.workers.iter().any(|worker| worker.name == name);
        if !known_worker && (group.seal_unknown_workers || group.completed_omitted.contains(&name))
        {
            return Ok(false);
        }
        let mut candidate = group.clone();
        let worker_index =
            if let Some(index) = candidate.workers.iter().position(|w| w.name == name) {
                if candidate.workers[index].completed {
                    return Ok(false);
                }
                index
            } else {
                if candidate.workers.len() == MAX_WORKERS {
                    if status.is_some() {
                        self.groups[index].seal_unknown_workers = true;
                    }
                    return Ok(self.omit());
                }
                candidate.workers.push(Worker::new(name));
                candidate.workers.len() - 1
            };
        let is_completion = status.is_some();
        let worker = &mut candidate.workers[worker_index];
        worker.at = at;
        if is_completion {
            worker.completed = true;
            worker.status = status;
            worker.response = response;
        } else {
            worker.data = data;
        }
        if self.retained_content_bytes() - group.content_bytes() + candidate.content_bytes()
            > MAX_CONTENT_BYTES
        {
            // A validated completion is a fact even if its status/response cannot
            // fit. Preserve the accepted payload and seal an existing worker.
            if is_completion {
                if worker_index < group.workers.len() {
                    let worker = &mut self.groups[index].workers[worker_index];
                    worker.completed = true;
                    worker.at = at;
                    self.incomplete = true;
                    return Ok(true);
                }
                // An unseen worker can also retain its completion fact if the
                // name fits, even when its result does not.
                let mut minimal = Worker::new(candidate.workers[worker_index].name.clone());
                minimal.completed = true;
                minimal.at = at;
                if self.retained_content_bytes() + minimal.content_bytes() <= MAX_CONTENT_BYTES {
                    self.groups[index].workers.push(minimal);
                    self.incomplete = true;
                    return Ok(true);
                }
                let group = &mut self.groups[index];
                if group.completed_omitted.len() < MAX_WORKERS {
                    group.completed_omitted.insert(minimal.name);
                } else {
                    group.seal_unknown_workers = true;
                }
            }
            return Ok(self.omit());
        }
        if *group == candidate {
            return Ok(false);
        }
        self.groups[index] = candidate;
        Ok(true)
    }
}
