//! The user facing API of the store.
//!
//! This API is both for interacting with an in-process store and for interacting
//! with a remote store via rpc calls.
use std::{io, net::SocketAddr, ops::Deref, sync::Arc};

use iroh::Endpoint;
use irpc::rpc::{Handler, listen};
use irpc_schema::schema;
use proto::{Request, ShutdownRequest, SyncDbRequest};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use tags::Tags;

pub mod blobs;
pub mod downloader;
pub mod proto;
pub mod remote;
pub mod tags;
pub use crate::{store::util::Tag, util::temp_tag::TempTag};

pub(crate) type ApiClient = irpc::Client<proto::Command, proto::Request, proto::StoreService>;

/// Error for all rpc interactions.
pub type RpcError = irpc::Error;
pub type RpcResult<T> = std::result::Result<T, RpcError>;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// Request failed due to rpc error.
    #[error("rpc error: {0}")]
    Rpc(#[from] irpc::Error),
    /// Request failed due an actual error.
    #[error("inner error: {0}")]
    Inner(#[from] Error),
}

pub type RequestResult<T> = std::result::Result<T, RequestError>;

impl From<irpc::channel::SendError> for RequestError {
    fn from(e: irpc::channel::SendError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<irpc::channel::RecvError> for RequestError {
    fn from(e: irpc::channel::RecvError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<irpc::RequestError> for RequestError {
    fn from(e: irpc::RequestError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<irpc::rpc::WriteError> for RequestError {
    fn from(e: irpc::rpc::WriteError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<io::Error> for RequestError {
    fn from(e: io::Error) -> Self {
        Self::Inner(e.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExportBaoError {
    #[error("send error: {0}")]
    Send(#[from] irpc::channel::SendError),
    #[error("recv error: {0}")]
    Recv(#[from] irpc::channel::RecvError),
    #[error("request error: {0}")]
    Request(#[from] irpc::RequestError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Inner(#[from] bao_tree::io::EncodeError),
}

impl From<ExportBaoError> for Error {
    fn from(e: ExportBaoError) -> Self {
        match e {
            ExportBaoError::Send(e) => Self::Io(e.into()),
            ExportBaoError::Recv(e) => Self::Io(e.into()),
            ExportBaoError::Request(e) => Self::Io(e.into()),
            ExportBaoError::Io(e) => Self::Io(e),
            ExportBaoError::Inner(e) => Self::Io(e.into()),
        }
    }
}

impl From<RpcError> for ExportBaoError {
    fn from(e: RpcError) -> Self {
        match e {
            RpcError::Recv(e) => Self::Recv(e),
            RpcError::Send(e) => Self::Send(e),
            RpcError::Request(e) => Self::Request(e),
            RpcError::Write(e) => Self::Io(e.into()),
        }
    }
}

pub type ExportBaoResult<T> = std::result::Result<T, ExportBaoError>;

#[derive(Debug, derive_more::Display, derive_more::From, Serialize, Deserialize)]
#[schema(Nominal)]
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

impl From<RpcError> for Error {
    fn from(e: RpcError) -> Self {
        Self::Io(e.into())
    }
}

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        match e {
            RequestError::Rpc(e) => Self::Io(e.into()),
            RequestError::Inner(e) => e,
        }
    }
}

impl From<irpc::channel::RecvError> for Error {
    fn from(e: irpc::channel::RecvError) -> Self {
        Self::Io(e.into())
    }
}

impl From<irpc::rpc::WriteError> for Error {
    fn from(e: irpc::rpc::WriteError) -> Self {
        Self::Io(e.into())
    }
}

impl From<irpc::RequestError> for Error {
    fn from(e: irpc::RequestError) -> Self {
        Self::Io(e.into())
    }
}

impl From<irpc::channel::SendError> for Error {
    fn from(e: irpc::channel::SendError) -> Self {
        Self::Io(e.into())
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

/// The main entry point for the store API.
#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Store {
    client: ApiClient,
}

impl Deref for Store {
    type Target = blobs::Blobs;

    fn deref(&self) -> &Self::Target {
        blobs::Blobs::ref_from_sender(&self.client)
    }
}

impl Store {
    /// The tags API.
    pub fn tags(&self) -> &Tags {
        Tags::ref_from_sender(&self.client)
    }

    /// The blobs API.
    pub fn blobs(&self) -> &blobs::Blobs {
        blobs::Blobs::ref_from_sender(&self.client)
    }

    /// API for getting blobs from a *single* remote node.
    pub fn remote(&self) -> &remote::Remote {
        remote::Remote::ref_from_sender(&self.client)
    }

    /// Create a downloader for more complex downloads.
    ///
    /// Unlike the other APIs, this creates an object that has internal state,
    /// so don't create it ad hoc but store it somewhere if you need it multiple
    /// times.
    pub fn downloader(&self, endpoint: &Endpoint) -> downloader::Downloader {
        downloader::Downloader::new(self, endpoint)
    }

    /// Connect to a remote store as a rpc client.
    pub fn connect(endpoint: quinn::Endpoint, addr: SocketAddr) -> Self {
        let sender = irpc::Client::quinn(endpoint, addr);
        Store::from_sender(sender)
    }

    /// Listen on a quinn endpoint for incoming rpc connections.
    pub async fn listen(self, endpoint: quinn::Endpoint) {
        let local = self.client.local().unwrap().clone();
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
                    Request::CreateTempTag(msg) => local.send((msg, tx)),

                    Request::BlobStatus(msg) => local.send((msg, tx)),

                    Request::ImportBytes(msg) => local.send((msg, tx)),
                    Request::ImportByteStream(msg) => local.send((msg, tx)),
                    Request::ImportBao(msg) => local.send((msg, tx, rx)),
                    Request::ImportPath(msg) => local.send((msg, tx)),
                    Request::ListBlobs(msg) => local.send((msg, tx)),
                    Request::DeleteBlobs(msg) => local.send((msg, tx)),
                    Request::Batch(msg) => local.send((msg, tx, rx)),

                    Request::ExportBao(msg) => local.send((msg, tx)),
                    Request::ExportRanges(msg) => local.send((msg, tx)),
                    Request::ExportPath(msg) => local.send((msg, tx)),

                    Request::Observe(msg) => local.send((msg, tx)),

                    Request::ClearProtected(msg) => local.send((msg, tx)),
                    Request::SyncDb(msg) => local.send((msg, tx)),
                    Request::Shutdown(msg) => local.send((msg, tx)),
                }
            })
        });
        listen::<Request>(endpoint, handler).await
    }

    pub async fn sync_db(&self) -> RequestResult<()> {
        let msg = SyncDbRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }

    pub async fn shutdown(&self) -> RpcResult<()> {
        let msg = ShutdownRequest;
        self.client.rpc(msg).await?;
        Ok(())
    }

    pub(crate) fn from_sender(client: ApiClient) -> Self {
        Self { client }
    }

    pub(crate) fn ref_from_sender(client: &ApiClient) -> &Self {
        Self::ref_cast(client)
    }
}
