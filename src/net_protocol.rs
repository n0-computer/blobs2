//! Adaptation of `iroh-blobs` as an `iroh` protocol.

// TODO: reduce API surface and add documentation
#![allow(missing_docs)]

use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh::{Endpoint, endpoint::Connection, protocol::ProtocolHandler};
use tokio::sync::mpsc;
use tracing::error;

use crate::{api::Store, provider::Event};

#[derive(Debug)]
pub(crate) struct BlobsInner {
    pub(crate) store: Store,
    pub(crate) endpoint: Endpoint,
    pub(crate) progress: ProgressSender,
}

pub trait LazyEvent {
    fn call(self) -> Event;
}

impl<T> LazyEvent for T
where
    T: FnOnce() -> Event,
{
    fn call(self) -> Event {
        self()
    }
}

impl LazyEvent for Event {
    fn call(self) -> Event {
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ProgressSender {
    Disabled,
    Enabled(mpsc::Sender<Event>),
}

impl ProgressSender {
    pub fn new(sender: Option<mpsc::Sender<Event>>) -> Self {
        match sender {
            Some(sender) => Self::Enabled(sender),
            None => Self::Disabled,
        }
    }

    pub fn try_send(&self, event: impl LazyEvent) {
        match self {
            Self::Enabled(sender) => {
                let value = event.call();
                sender.try_send(value).ok();
            }
            Self::Disabled => {}
        }
    }

    pub async fn send(&self, event: impl LazyEvent) {
        match self {
            Self::Enabled(sender) => {
                let value = event.call();
                if let Err(err) = sender.send(value).await {
                    error!("failed to send progress event: {:?}", err);
                }
            }
            Self::Disabled => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct Blobs {
    pub(crate) inner: Arc<BlobsInner>,
}

impl Blobs {
    pub fn new(
        store: impl AsRef<Store>,
        endpoint: Endpoint,
        progress: Option<mpsc::Sender<Event>>,
    ) -> Self {
        Self {
            inner: Arc::new(BlobsInner {
                store: store.as_ref().clone(),
                endpoint,
                progress: ProgressSender::new(progress),
            }),
        }
    }

    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }
}

impl ProtocolHandler for Blobs {
    fn accept(&self, conn: Connection) -> BoxedFuture<Result<()>> {
        let store = self.store().clone();
        let progress = self.inner.progress.clone();

        Box::pin(async move {
            crate::provider::handle_connection(conn, store, progress).await;
            Ok(())
        })
    }

    fn shutdown(&self) -> BoxedFuture<()> {
        let store = self.store().clone();
        Box::pin(async move {
            if let Err(cause) = store.shutdown().await {
                error!("error shutting down store: {:?}", cause);
            }
        })
    }
}
