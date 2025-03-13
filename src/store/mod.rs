use bao_tree::BlockSize;
mod api;
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

impl Store {
    pub fn from_sender(sender: mpsc::Sender<proto::Command>) -> Self {
        Self { sender }
    }

    pub fn ref_from_sender(sender: &mpsc::Sender<proto::Command>) -> &Self {
        Store::ref_cast(sender)
    }
}

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
