//! Adaptation of `iroh-blobs` as an `iroh` protocol.

// TODO: reduce API surface and add documentation
#![allow(missing_docs)]

use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler, Endpoint};
use tracing::error;

use crate::api::Store;

#[derive(Debug)]
pub(crate) struct BlobsInner {
    pub(crate) store: Store,
    pub(crate) endpoint: Endpoint,
}

#[derive(Debug, Clone)]
pub struct Blobs {
    pub(crate) inner: Arc<BlobsInner>,
}

impl Blobs {
    pub fn new(store: impl AsRef<Store>, endpoint: Endpoint) -> Self {
        Self {
            inner: Arc::new(BlobsInner {
                store: store.as_ref().clone(),
                endpoint,
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
    fn accept(&self, conn: Connecting) -> BoxedFuture<Result<()>> {
        let store = self.store().clone();

        Box::pin(async move {
            crate::provider::handle_connection(conn.await?, store).await;
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
