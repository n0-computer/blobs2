use bao_tree::BlockSize;
pub(crate) mod util {
    pub mod sparse_mem_file;
}
mod api;
mod bitfield;
mod mem;
mod proto;
mod readonly_mem;

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
