//! The user facing API of the store.
use std::{io, marker::PhantomData, net::SocketAddr, ops::Deref, sync::Arc};

use proto::Request;
use quic_rpc::rpc::{listen, Handler};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

pub mod blobs;
pub mod proto;
pub mod tags;

pub(crate) type ApiSender =
    quic_rpc::ServiceSender<proto::Command, proto::Request, proto::StoreService>;

pub type RequestError = quic_rpc::Error;
pub type RequestResult<T> = std::result::Result<T, RequestError>;

#[derive(Debug, thiserror::Error)]
pub enum FallibleRequestError {
    #[error("rpc error: {0}")]
    Rpc(#[from] quic_rpc::Error),
    #[error("inner error: {0}")]
    Inner(#[from] Error),
}

impl From<quic_rpc::channel::SendError> for FallibleRequestError {
    fn from(e: quic_rpc::channel::SendError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<quic_rpc::channel::RecvError> for FallibleRequestError {
    fn from(e: quic_rpc::channel::RecvError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<quic_rpc::RequestError> for FallibleRequestError {
    fn from(e: quic_rpc::RequestError) -> Self {
        Self::Rpc(e.into())
    }
}

impl From<quic_rpc::rpc::WriteError> for FallibleRequestError {
    fn from(e: quic_rpc::rpc::WriteError) -> Self {
        Self::Rpc(e.into())
    }
}

pub type FallibleRequestResult<T> = std::result::Result<T, FallibleRequestError>;

#[derive(Debug, thiserror::Error)]
pub enum ExportBaoError {
    #[error("send error: {0}")]
    Send(#[from] quic_rpc::channel::SendError),
    #[error("recv error: {0}")]
    Recv(#[from] quic_rpc::channel::RecvError),
    #[error("request error: {0}")]
    Request(#[from] quic_rpc::RequestError),
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

impl From<RequestError> for ExportBaoError {
    fn from(e: RequestError) -> Self {
        match e {
            RequestError::Recv(e) => Self::Recv(e),
            RequestError::Send(e) => Self::Send(e),
            RequestError::Request(e) => Self::Request(e),
            RequestError::Write(e) => Self::Io(e.into()),
        }
    }
}

pub type ExportBaoResult<T> = std::result::Result<T, ExportBaoError>;

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

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        Self::Io(e.into())
    }
}

impl From<FallibleRequestError> for Error {
    fn from(e: FallibleRequestError) -> Self {
        match e {
            FallibleRequestError::Rpc(e) => Self::Io(e.into()),
            FallibleRequestError::Inner(e) => e,
        }
    }
}

impl From<quic_rpc::channel::RecvError> for Error {
    fn from(e: quic_rpc::channel::RecvError) -> Self {
        Self::Io(e.into())
    }
}

impl From<quic_rpc::rpc::WriteError> for Error {
    fn from(e: quic_rpc::rpc::WriteError) -> Self {
        Self::Io(e.into())
    }
}

impl From<quic_rpc::RequestError> for Error {
    fn from(e: quic_rpc::RequestError) -> Self {
        Self::Io(e.into())
    }
}

impl From<quic_rpc::channel::SendError> for Error {
    fn from(e: quic_rpc::channel::SendError) -> Self {
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
                    Request::CreateTempTag(msg) => local.send((msg, tx)),

                    Request::GetBlobStatus(msg) => local.send((msg, tx)),

                    Request::ImportBytes(msg) => local.send((msg, tx)),
                    Request::ImportByteStream(msg) => local.send((msg, tx)),
                    Request::ImportBao(msg) => local.send((msg, tx, rx)),
                    Request::ImportPath(msg) => local.send((msg, tx)),
                    Request::ListBlobs(msg) => local.send((msg, tx)),
                    Request::DeleteBlobs(msg) => local.send((msg, tx)),
                    Request::Batch(msg) => local.send((msg, tx, rx)),

                    Request::ExportBao(msg) => local.send((msg, tx)),
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

    pub(crate) fn from_sender(sender: ApiSender) -> Self {
        Self { sender }
    }

    pub(crate) fn ref_from_sender(sender: &ApiSender) -> &Self {
        Self::ref_cast(sender)
    }
}

#[derive(
    Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display,
)]
pub struct Scope(pub(crate) u64);

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
pub struct ShutdownRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClearProtected;
