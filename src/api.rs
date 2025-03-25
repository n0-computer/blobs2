//! The user facing API of the store.
use std::{io, marker::PhantomData, net::SocketAddr, ops::Deref, sync::Arc};

use bytes::Bytes;
use proto::Request;
use quic_rpc::rpc::{listen, Handler};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

use crate::{BlobFormat, Hash};
pub mod blobs;
pub mod proto;
pub mod tags;

pub(crate) type ApiSender =
    quic_rpc::ServiceSender<proto::Command, proto::Request, proto::StoreService>;

#[derive(Debug, derive_more::Display, derive_more::From, Serialize, Deserialize)]
pub enum Error {
    #[serde(with = "crate::util::serde::io_error_serde")]
    Io(io::Error),
}

impl Error {
    pub fn io(
        kind: io::ErrorKind,
        msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::Io(io::Error::new(kind, msg.into()))
    }

    pub fn other<E>(msg: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Io(io::Error::other(msg.into()))
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Store {
    sender: ApiSender,
}

impl Deref for Store {
    type Target = blobs::Blobs;

    fn deref(&self) -> &Self::Target {
        blobs::Blobs::ref_from_sender(&self.sender)
    }
}

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Tags {
    sender: ApiSender,
}

impl Store {
    pub fn tags(&self) -> &Tags {
        Tags::ref_from_sender(&self.sender)
    }

    pub fn blobs(&self) -> &blobs::Blobs {
        blobs::Blobs::ref_from_sender(&self.sender)
    }

    pub fn connect(endpoint: quinn::Endpoint, addr: SocketAddr) -> Self {
        let sender = quic_rpc::ServiceSender::Remote(endpoint, addr, PhantomData);
        Store::from_sender(sender)
    }

    pub async fn listen(self, endpoint: quinn::Endpoint) {
        let local = self.sender.local().unwrap().clone();
        let handler: Handler<Request> = Arc::new(move |req, rx, tx| {
            let local = local.clone();
            Box::pin({
                match req {
                    Request::SetTag(msg) => local.send((msg, tx)),
                    Request::CreateTag(msg) => local.send((msg, tx)),
                    Request::DeleteTags(msg) => local.send((msg, tx)),
                    Request::RenameTag(msg) => local.send((msg, tx)),
                    Request::ListTags(msg) => local.send((msg, tx)),
                    Request::ListTempTags(msg) => local.send((msg, tx)),

                    Request::ImportBytes(msg) => local.send((msg, tx)),
                    Request::ImportByteStream(msg) => local.send((msg, tx)),
                    Request::ImportBao(msg) => local.send((msg, tx, rx)),
                    Request::ImportPath(msg) => local.send((msg, tx)),
                    Request::ListBlobs(msg) => local.send((msg, tx)),
                    Request::DeleteBlobs(msg) => local.send((msg, tx)),

                    Request::ExportBao(msg) => local.send((msg, tx)),
                    Request::ExportPath(msg) => local.send((msg, tx)),

                    Request::Observe(msg) => local.send((msg, tx)),

                    Request::SyncDb(msg) => local.send((msg, tx)),
                    Request::Shutdown(msg) => local.send((msg, tx)),
                }
            })
        });
        listen::<Request>(endpoint, handler).await
    }

    pub(crate) fn from_sender(sender: ApiSender) -> Self {
        Self { sender }
    }

    pub(crate) fn ref_from_sender(sender: &ApiSender) -> &Self {
        Self::ref_cast(sender)
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Scope(u64);

impl Scope {
    pub const GLOBAL: Self = Self(0);
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "Global")
        } else {
            f.debug_tuple("Scope").field(&self.0).finish()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncDb;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown;
