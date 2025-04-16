use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::anyhow;
use irpc::channel::spsc;

use super::DownloadKind;

/// The channel that can be used to subscribe to progress updates.
pub type ProgressSubscriber = Arc<tokio::sync::Mutex<spsc::Sender<u64>>>;

/// Track the progress of downloads.
///
/// This struct allows to create [`ProgressSender`] structs to be passed to
/// [`crate::get::db::get_to_db`]. Each progress sender can be subscribed to by any number of
/// [`ProgressSubscriber`] channel senders, which will receive each progress update (if they have
/// capacity). Additionally, the [`ProgressTracker`] maintains a [`TransferState`] for each
/// transfer, applying each progress update to update this state. When subscribing to an already
/// running transfer, the subscriber will receive a [`DownloadProgress::InitialState`] message
/// containing the state at the time of the subscription, and then receive all further progress
/// events directly.
#[derive(Debug, Default)]
pub struct ProgressTracker {
    /// Map of shared state for each tracked download.
    running: HashMap<DownloadKind, Shared>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Track a new download with a list of initial subscribers.
    ///
    /// Note that this should only be called for *new* downloads. If a download for the `kind` is
    /// already tracked in this [`ProgressTracker`], calling `track` will replace all existing
    /// state and subscribers (equal to calling [`Self::remove`] first).
    pub fn track(
        &mut self,
        kind: DownloadKind,
        subscribers: impl IntoIterator<Item = ProgressSubscriber>,
    ) -> BroadcastProgressSender {
        let inner = Inner {
            subscribers: subscribers.into_iter().collect(),
            current: 0,
        };
        let shared = Arc::new(Mutex::new(inner));
        self.running.insert(kind, Arc::clone(&shared));
        BroadcastProgressSender { shared }
    }

    /// Subscribe to a tracked download.
    ///
    /// Will return an error if `kind` is not yet tracked.
    pub async fn subscribe(
        &mut self,
        kind: DownloadKind,
        sender: ProgressSubscriber,
    ) -> anyhow::Result<()> {
        let initial_offset = self
            .running
            .get_mut(&kind)
            .ok_or_else(|| anyhow!("state for download {kind:?} not found"))?
            .lock()
            .unwrap()
            .subscribe(sender.clone());
        let mut guard = sender.lock().await;
        guard.send(initial_offset).await?;
        Ok(())
    }

    /// Unsubscribe `sender` from `kind`.
    pub fn unsubscribe(&mut self, kind: &DownloadKind, sender: &ProgressSubscriber) {
        if let Some(shared) = self.running.get_mut(kind) {
            shared.lock().unwrap().unsubscribe(sender)
        }
    }

    /// Remove all state for a download.
    pub fn remove(&mut self, kind: &DownloadKind) {
        self.running.remove(kind);
    }
}

type Shared = Arc<Mutex<Inner>>;

#[derive(Debug)]
struct Inner {
    subscribers: Vec<ProgressSubscriber>,
    // current offset of the download
    current: u64,
}

impl Inner {
    fn subscribe(&mut self, subscriber: ProgressSubscriber) -> u64 {
        self.subscribers.push(subscriber);
        self.current
    }

    fn unsubscribe(&mut self, sender: &ProgressSubscriber) {
        self.subscribers.retain(|s| !Arc::ptr_eq(s, sender));
    }

    async fn on_progress(&mut self, progress: u64) {
        let mut dropped = Vec::new();
        for (index, sender) in self.subscribers.iter_mut().enumerate() {
            if sender.lock().await.try_send(progress).await.is_err() {
                dropped.push(index);
            }
        }
        for index in dropped.into_iter().rev() {
            self.subscribers.remove(index);
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastProgressSender {
    shared: Shared,
}
