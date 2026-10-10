use crate::MessageEnvelope;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReceiveError {
    #[error("subscriber missed {0} messages because its bounded queue was full")]
    Lagged(u64),
    #[error("subscription state lock poisoned")]
    Poisoned,
    #[error("priority change would exceed the bounded control lane")]
    PriorityChangeFull,
}
struct Queue {
    target: Option<String>,
    native_control: bool,
    system: VecDeque<MessageEnvelope>,
    data: VecDeque<MessageEnvelope>,
    lagged: u64,
    ready: Arc<Notify>,
}
struct State {
    closed: bool,
    next_id: u64,
    subscribers: BTreeMap<u64, Queue>,
}
#[derive(Clone)]
pub(crate) struct Hub {
    state: Arc<Mutex<State>>,
    capacity: usize,
    space: Arc<Notify>,
}
/// Two finite FIFO queues; system messages are received before ordinary data.
/// Overflow is explicit (`Lagged`) and never silently evicts admitted messages.
pub struct NetworkSubscription {
    hub: Hub,
    id: u64,
    ready: Arc<Notify>,
}
impl Hub {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                closed: false,
                next_id: 0,
                subscribers: BTreeMap::new(),
            })),
            capacity,
            space: Arc::new(Notify::new()),
        }
    }
    pub fn subscribe(&self, target: Option<String>) -> Result<NetworkSubscription, ReceiveError> {
        let mut state = self.state.lock().map_err(|_| ReceiveError::Poisoned)?;
        state.next_id += 1;
        let id = state.next_id;
        let ready = Arc::new(Notify::new());
        state.subscribers.insert(
            id,
            Queue {
                target,
                native_control: false,
                system: VecDeque::new(),
                data: VecDeque::new(),
                lagged: 0,
                ready: ready.clone(),
            },
        );
        Ok(NetworkSubscription {
            hub: self.clone(),
            id,
            ready,
        })
    }
    pub fn deliver(&self, message: MessageEnvelope) -> Result<usize, ReceiveError> {
        let mut state = self.state.lock().map_err(|_| ReceiveError::Poisoned)?;
        if state.closed {
            return Ok(0);
        }
        let mut delivered = 0;
        for queue in state.subscribers.values_mut() {
            if queue.target.as_deref().is_some_and(|target| {
                message
                    .target()
                    .is_some_and(|recipient| recipient != target)
            }) {
                continue;
            }
            let destination = if is_system(queue, &message) {
                &mut queue.system
            } else {
                &mut queue.data
            };
            if destination.len() >= self.capacity {
                queue.lagged = queue.lagged.saturating_add(1);
            } else {
                destination.push_back(message.clone());
                delivered += 1;
            }
            queue.ready.notify_waiters();
        }
        Ok(delivered)
    }
    #[cfg(feature = "pgmq")]
    pub async fn deliver_wait(&self, message: MessageEnvelope) -> Result<usize, ReceiveError> {
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().map_err(|_| ReceiveError::Poisoned)?;
                if state.closed {
                    return Ok(0);
                }
                let accepts = |queue: &&Queue| {
                    !queue.target.as_deref().is_some_and(|target| {
                        message
                            .target()
                            .is_some_and(|recipient| recipient != target)
                    })
                };
                if state.subscribers.values().filter(accepts).all(|queue| {
                    if is_system(queue, &message) {
                        queue.system.len() < self.capacity
                    } else {
                        queue.data.len() < self.capacity
                    }
                }) {
                    let mut count = 0;
                    for queue in state.subscribers.values_mut() {
                        if !accepts(&&*queue) {
                            continue;
                        }
                        (if is_system(queue, &message) {
                            &mut queue.system
                        } else {
                            &mut queue.data
                        })
                        .push_back(message.clone());
                        queue.ready.notify_waiters();
                        count += 1;
                    }
                    return Ok(count);
                }
            }
            notified.await;
        }
    }
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.space.notify_waiters();
            for queue in state.subscribers.values() {
                queue.ready.notify_waiters();
            }
        }
    }
}
fn is_system(queue: &Queue, message: &MessageEnvelope) -> bool {
    message.is_system()
        || (queue.native_control
            && message
                .as_bus_message()
                .is_some_and(|message| message.data.is_system()))
}
impl NetworkSubscription {
    /// Opt into native bounded-worker control priority. The Python wire schema
    /// and raw subscriptions remain unchanged; native JobUpdateRequest can bypass
    /// blocked data admission. Existing queued messages are moved atomically.
    pub fn enable_native_control(&self) -> Result<(), ReceiveError> {
        let mut state = self.hub.state.lock().map_err(|_| ReceiveError::Poisoned)?;
        let Some(queue) = state.subscribers.get_mut(&self.id) else {
            return Ok(());
        };
        let promoted = queue
            .data
            .iter()
            .filter(|message| {
                message
                    .as_bus_message()
                    .is_some_and(|message| message.data.is_system())
            })
            .count();
        if promoted > self.hub.capacity.saturating_sub(queue.system.len()) {
            return Err(ReceiveError::PriorityChangeFull);
        }
        queue.native_control = true;
        let pending = std::mem::take(&mut queue.data);
        for message in pending {
            if is_system(queue, &message) {
                queue.system.push_back(message);
            } else {
                queue.data.push_back(message);
            }
        }
        queue.ready.notify_waiters();
        self.hub.space.notify_waiters();
        Ok(())
    }
    /// Drains admitted messages after shutdown, then returns `None`.
    pub async fn recv(&self) -> Result<Option<MessageEnvelope>, ReceiveError> {
        self.recv_class(None).await
    }
    /// Consume only the control lane; may run beside one data consumer. Multiple
    /// consumers for the same lane compete for messages without duplicating them.
    pub async fn recv_system(&self) -> Result<Option<MessageEnvelope>, ReceiveError> {
        self.recv_class(Some(true)).await
    }
    pub async fn recv_data(&self) -> Result<Option<MessageEnvelope>, ReceiveError> {
        self.recv_class(Some(false)).await
    }
    async fn recv_class(
        &self,
        system: Option<bool>,
    ) -> Result<Option<MessageEnvelope>, ReceiveError> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.hub.state.lock().map_err(|_| ReceiveError::Poisoned)?;
                let closed = state.closed;
                let Some(queue) = state.subscribers.get_mut(&self.id) else {
                    return Ok(None);
                };
                if queue.lagged != 0 {
                    return Err(ReceiveError::Lagged(std::mem::take(&mut queue.lagged)));
                }
                let message = match system {
                    Some(true) => queue.system.pop_front(),
                    Some(false) => queue.data.pop_front(),
                    None => queue.system.pop_front().or_else(|| queue.data.pop_front()),
                };
                if let Some(message) = message {
                    self.hub.space.notify_waiters();
                    return Ok(Some(message));
                }
                if closed {
                    return Ok(None);
                }
            }
            notified.await;
        }
    }
}
impl Drop for NetworkSubscription {
    fn drop(&mut self) {
        if let Ok(mut state) = self.hub.state.lock() {
            state.subscribers.remove(&self.id);
            self.hub.space.notify_waiters();
        }
    }
}
