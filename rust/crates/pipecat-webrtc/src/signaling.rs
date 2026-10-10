//! Framework-independent request handler compatible with SmallWebRTC JSON bodies.
use crate::{Error, RTCIceCandidateInit, Result, SmallWebRtcConfig, SmallWebRtcConnection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallWebRtcRequest {
    pub sdp: String,
    #[serde(rename = "type")]
    pub sdp_type: String,
    #[serde(default)]
    pub pc_id: Option<String>,
    #[serde(default)]
    pub restart_pc: bool,
    #[serde(default, alias = "requestData")]
    pub request_data: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallWebRtcAnswer {
    pub sdp: String,
    #[serde(rename = "type")]
    pub sdp_type: String,
    pub pc_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCandidate {
    pub candidate: String,
    #[serde(default, alias = "sdpMid")]
    pub sdp_mid: Option<String>,
    #[serde(default, alias = "sdpMLineIndex")]
    pub sdp_mline_index: Option<u16>,
    #[serde(default, alias = "usernameFragment")]
    pub username_fragment: Option<String>,
}
impl From<IceCandidate> for RTCIceCandidateInit {
    fn from(candidate: IceCandidate) -> Self {
        Self {
            candidate: candidate.candidate,
            sdp_mid: candidate.sdp_mid,
            sdp_mline_index: candidate.sdp_mline_index,
            username_fragment: candidate.username_fragment,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallWebRtcPatchRequest {
    pub pc_id: String,
    pub candidates: Vec<IceCandidate>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    Single,
    Multiple,
}

enum RegistryCloseState {
    Idle,
    Running(JoinHandle<std::result::Result<(), String>>),
    Finished(std::result::Result<(), String>),
}

/// Bounded peer registry for an HTTP server's POST /offer and PATCH /offer routes.
///
/// The embedding server must authenticate requests and authorize pc_id access.
/// IDs are identifiers, never bearer credentials. request_data is caller-owned.
/// Video, ESP32 SDP rewriting, and device capture are deliberately not implicit.
pub struct SmallWebRtcRequestHandler {
    config: SmallWebRtcConfig,
    mode: ConnectionMode,
    max_connections: usize,
    peers: Mutex<HashMap<String, Arc<SmallWebRtcConnection>>>,
    closed: AtomicBool,
    close_state: Mutex<RegistryCloseState>,
}
impl SmallWebRtcRequestHandler {
    pub fn new(
        config: SmallWebRtcConfig,
        mode: ConnectionMode,
        max_connections: usize,
    ) -> Result<Self> {
        if max_connections == 0 {
            return Err(Error::InvalidConfig("max_connections must be positive"));
        }
        Ok(Self {
            config,
            mode,
            max_connections,
            peers: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            close_state: Mutex::new(RegistryCloseState::Idle),
        })
    }
    /// Returns the answer and peer so the server can attach a pipeline immediately.
    pub async fn handle_offer(
        &self,
        request: SmallWebRtcRequest,
    ) -> Result<(SmallWebRtcAnswer, Arc<SmallWebRtcConnection>)> {
        if request.sdp_type != "offer" {
            return Err(Error::Signaling("request type must be offer".into()));
        }
        if request.sdp.len() > 262_144 {
            return Err(Error::Signaling("SDP exceeds 256 KiB".into()));
        }
        // Serialize registry changes so single-session admission cannot race.
        let mut peers = self.peers.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let dead = peers
            .iter()
            .filter(|(_, peer)| peer.is_cancelled())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in dead {
            if let Some(peer) = peers.remove(&id) {
                peer.close().await?;
            }
        }
        let existing = match request.pc_id.as_ref() {
            Some(id) => Some(
                peers
                    .get(id)
                    .cloned()
                    .ok_or_else(|| Error::UnknownPeer(id.clone()))?,
            ),
            None => None,
        };
        if existing.is_none()
            && ((self.mode == ConnectionMode::Single && !peers.is_empty())
                || peers.len() >= self.max_connections)
        {
            return Err(Error::ConnectionLimit);
        }
        let new_peer = existing.is_none() || request.restart_pc;
        if request.restart_pc {
            if let Some(peer) = &existing {
                peer.close().await?;
                peers.remove(peer.pc_id());
            }
        }
        let peer = if new_peer {
            Arc::new(SmallWebRtcConnection::new(self.config.clone()).await?)
        } else {
            existing.unwrap()
        };
        let answer = match peer.answer_offer(request.sdp).await {
            Ok(answer) => answer,
            Err(error) => {
                if new_peer {
                    let _ = peer.close().await;
                }
                return Err(error);
            }
        };
        let answer = SmallWebRtcAnswer {
            sdp: answer.sdp,
            sdp_type: "answer".into(),
            pc_id: peer.pc_id().into(),
        };
        peers.insert(answer.pc_id.clone(), peer.clone());
        Ok((answer, peer))
    }
    pub async fn handle_patch(&self, request: SmallWebRtcPatchRequest) -> Result<()> {
        if request.candidates.len() > 256 {
            return Err(Error::Signaling("ICE patch exceeds 256 candidates".into()));
        }
        let peer = self
            .get(&request.pc_id)
            .await
            .ok_or(Error::UnknownPeer(request.pc_id))?;
        for candidate in request.candidates {
            if candidate.candidate.len() > 8192 {
                return Err(Error::Signaling("ICE candidate exceeds 8192 bytes".into()));
            }
            peer.add_ice_candidate(candidate.into()).await?;
        }
        Ok(())
    }
    pub async fn get(&self, id: &str) -> Option<Arc<SmallWebRtcConnection>> {
        self.peers.lock().await.get(id).cloned()
    }
    pub async fn len(&self) -> usize {
        self.peers.lock().await.len()
    }
    pub async fn is_empty(&self) -> bool {
        self.peers.lock().await.is_empty()
    }
    pub async fn remove(&self, id: &str) -> Result<()> {
        let peer = self
            .peers
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| Error::UnknownPeer(id.into()))?;
        peer.close().await
    }
    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let mut state = self.close_state.lock().await;
        if matches!(*state, RegistryCloseState::Idle) {
            let peers = std::mem::take(&mut *self.peers.lock().await);
            *state = RegistryCloseState::Running(tokio::spawn(async move {
                let mut first_error = None;
                // Cancel every retained peer before the first asynchronous close.
                for peer in peers.values() {
                    peer.cancel();
                }
                for (_, peer) in peers {
                    if let Err(error) = peer.close().await {
                        first_error.get_or_insert_with(|| error.to_string());
                    }
                }
                first_error.map_or(Ok(()), Err)
            }));
        }
        if let RegistryCloseState::Running(handle) = &mut *state {
            let result = handle
                .await
                .unwrap_or_else(|error| Err(format!("registry shutdown task: {error}")));
            *state = RegistryCloseState::Finished(result);
        }
        match &*state {
            RegistryCloseState::Finished(result) => result.clone().map_err(Error::Shutdown),
            _ => unreachable!("registry shutdown has completed"),
        }
    }
}
