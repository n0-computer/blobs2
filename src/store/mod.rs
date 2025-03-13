use bao_tree::BlockSize;
pub mod api;
mod bitfield;
pub mod fs;
mod mem;
mod proto;
mod readonly_mem;
mod test;
pub(crate) mod util;

use ref_cast::RefCast;

pub use crate::hash::{BlobFormat, Hash, HashAndFormat};
use crate::util::channel::mpsc;

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Store {
    sender: mpsc::Sender<proto::Command>,
}

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Tags {
    sender: mpsc::Sender<proto::Command>,
}

impl Tags {
    pub fn ref_from_sender(sender: &mpsc::Sender<proto::Command>) -> &Self {
        Self::ref_cast(sender)
    }
}

impl Store {
    pub fn tags(&self) -> &Tags {
        Tags::ref_from_sender(&self.sender)
    }

    pub fn from_sender(sender: mpsc::Sender<proto::Command>) -> Self {
        Self { sender }
    }

    pub fn ref_from_sender(sender: &mpsc::Sender<proto::Command>) -> &Self {
        Self::ref_cast(sender)
    }
}

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
