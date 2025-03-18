use std::{marker::PhantomData, net::SocketAddr, ops::Deref, sync::Arc};

use bao_tree::BlockSize;
pub mod api;
mod bitfield;
pub mod fs;
mod mem;
mod proto;
mod readonly_mem;
mod test;
pub(crate) mod util;

use iroh::endpoint;
use n0_future::io;
use proto::{Request, StoreService};
use quic_rpc::rpc::{listen, Handler};
use ref_cast::RefCast;

pub use crate::hash::{BlobFormat, Hash, HashAndFormat};

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Store {
    sender: quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
}

impl Deref for Store {
    type Target = Blobs;

    fn deref(&self) -> &Self::Target {
        Blobs::ref_from_sender(&self.sender)
    }
}

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Tags {
    sender: quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
}

impl Tags {
    fn ref_from_sender(
        sender: &quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
    ) -> &Self {
        Self::ref_cast(sender)
    }
}

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Blobs {
    sender: quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
}

impl Blobs {
    fn ref_from_sender(
        sender: &quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
    ) -> &Self {
        Self::ref_cast(sender)
    }
}

impl Store {
    pub fn tags(&self) -> &Tags {
        Tags::ref_from_sender(&self.sender)
    }

    pub fn connect(endpoint: quinn::Endpoint, addr: SocketAddr) -> Self {
        let sender = quic_rpc::ServiceSender::Remote(endpoint, addr, PhantomData);
        Store::from_sender(sender)
    }

    pub async fn listen(self, endpoint: quinn::Endpoint) {
        let local = self.sender.local().unwrap().clone();
        let handler: Handler<crate::store::proto::Request> = Arc::new(move |req, rx, tx| {
            let local = local.clone();
            Box::pin(match req {
                Request::SetTag(msg) => local.send((msg, tx)),
                Request::CreateTag(msg) => local.send((msg, tx)),
                Request::DeleteTags(msg) => local.send((msg, tx)),
                Request::RenameTag(msg) => local.send((msg, tx)),
                Request::ListTags(msg) => local.send((msg, tx)),
                _ => {
                    unimplemented!()
                }
            })
        });
        listen::<crate::store::proto::Request>(endpoint, handler).await
    }

    fn from_sender(
        sender: quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
    ) -> Self {
        Self { sender }
    }

    fn ref_from_sender(
        sender: &quic_rpc::ServiceSender<proto::Command, proto::Request, StoreService>,
    ) -> &Self {
        Self::ref_cast(sender)
    }
}

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
