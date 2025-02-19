use bao_tree::BlockSize;
mod api;
mod bao_file;
mod bitfield;
mod fs;
mod hash;
mod mem;
mod proto;
mod readonly_mem;
mod test;
mod util;

pub use hash::{BlobFormat, Hash, HashAndFormat};

#[derive(Debug, Clone)]
pub struct Store {
    sender: tokio::sync::mpsc::Sender<proto::Command>,
}

impl Store {
    pub fn from_sender(sender: tokio::sync::mpsc::Sender<proto::Command>) -> Self {
        Self { sender }
    }
}

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
