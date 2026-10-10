use crate::{ProxyError, ProxyOptions};
use pipecat_workers::{BusMessage, MessageData};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ForwardKind {
    Frame,
    Speak,
}
pub(crate) type JobKey = (String, String);
#[derive(Default)]
pub(crate) struct Pending {
    pub outgoing: BTreeSet<JobKey>,
    pub incoming: BTreeSet<JobKey>,
    // At most one request can trigger attachment teardown beyond the active limit.
    pub rejected_outgoing: Option<JobKey>,
}

pub(crate) fn job_id(data: &MessageData) -> Option<&str> {
    match data {
        MessageData::JobRequest { job_id, .. }
        | MessageData::JobResponse { job_id, .. }
        | MessageData::JobCancel { job_id, .. }
        | MessageData::JobUpdateRequest { job_id }
        | MessageData::JobUpdate { job_id, .. }
        | MessageData::StreamStart { job_id }
        | MessageData::StreamData { job_id, .. }
        | MessageData::StreamEnd { job_id } => Some(job_id),
        _ => None,
    }
}
fn caller(source: &str, worker: &str) -> bool {
    source == worker
        || ["/job/", "/group/"].iter().any(|suffix| {
            source
                .strip_prefix(&format!("{worker}{suffix}"))
                .is_some_and(|id| !id.is_empty())
        })
}
pub(crate) fn allowed(
    message: &BusMessage,
    options: &ProxyOptions,
    pending: &Pending,
    inbound: bool,
) -> bool {
    let (source, target, replies) = if inbound {
        (
            &options.remote_worker,
            &options.local_worker,
            &pending.outgoing,
        )
    } else {
        (
            &options.local_worker,
            &options.remote_worker,
            &pending.incoming,
        )
    };
    match &message.data {
        MessageData::JobRequest { .. }
        | MessageData::JobCancel { .. }
        | MessageData::JobUpdateRequest { .. } => {
            caller(&message.source, source) && message.target.as_ref() == Some(target)
        }
        MessageData::JobResponse { .. }
        | MessageData::JobUpdate { .. }
        | MessageData::StreamStart { .. }
        | MessageData::StreamData { .. }
        | MessageData::StreamEnd { .. } => {
            message.source == *source
                && message.target.as_ref().is_some_and(|target| {
                    replies.contains(&(target.clone(), job_id(&message.data).unwrap().into()))
                })
        }
        data => {
            message.source == *source
                && (message.target.as_ref() == Some(target)
                    || match data {
                        MessageData::Frame { .. } => {
                            options.forward_messages.contains(&ForwardKind::Frame)
                        }
                        MessageData::Speak { .. } => {
                            options.forward_messages.contains(&ForwardKind::Speak)
                        }
                        _ => false,
                    })
        }
    }
}
pub(crate) fn admit_request(
    message: &BusMessage,
    pending: &mut Pending,
    inbound: bool,
    limit: usize,
) -> Result<(), ProxyError> {
    if let MessageData::JobRequest { job_id, .. } = &message.data {
        let entries = if inbound {
            &mut pending.incoming
        } else {
            &mut pending.outgoing
        };
        if entries.len() >= limit {
            if !inbound {
                pending.rejected_outgoing = Some((message.source.clone(), job_id.clone()));
            }
            return Err(ProxyError::Capacity("pending jobs"));
        }
        if !entries.insert((message.source.clone(), job_id.clone())) {
            return Err(ProxyError::Protocol(
                "duplicate request on a live connection".into(),
            ));
        }
    }
    Ok(())
}
pub(crate) fn finish_response(message: &BusMessage, pending: &mut Pending, inbound: bool) {
    if let MessageData::JobResponse { job_id, .. } = &message.data {
        if let Some(target) = &message.target {
            let entries = if inbound {
                &mut pending.outgoing
            } else {
                &mut pending.incoming
            };
            entries.remove(&(target.clone(), job_id.clone()));
        }
    }
}
