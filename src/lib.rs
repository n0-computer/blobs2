use anyhow::Result;
use bao_tree::{blake3::Hash, io::{mixed::EncodedItem, BaoContentItem}, BlockSize, ChunkRanges};
use bitfield::BitfieldEvent;
use bytes::Bytes;

pub enum ImportProgress {
    CopyProgress { offset: u64 },
    Size { size: u64 },
    CopyDone,
    OutboardProgress { offset: u64 },
    Done { hash: Hash },
    Error { cause: anyhow::Error },
}

pub(crate) mod util {
    pub mod sparse_mem_file;
}
mod bitfield;
mod mem;

/// Trait for a blob store.
///
/// All fns take tokio sync primitives. We don't try to abstract
/// over them because they have lots of goodies like reserve that there
/// are no good existing abstractions for.
///
/// It is expected that a store will use some kind of internal tasks
/// to manage operations, since most store implementations will want to
/// perform IO.
pub trait Store {

    fn import_bao(
        &self,
        hash: Hash,
        size: u64,
        data: tokio::sync::mpsc::Receiver<BaoContentItem>,
        out: tokio::sync::oneshot::Sender<Result<()>>,
    ) -> bool;

    fn export_bao(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        out: tokio::sync::mpsc::Sender<EncodedItem>,
    ) -> bool;

    fn observe(
        &self,
        hash: Hash,
        out: tokio::sync::mpsc::Sender<BitfieldEvent>,
    ) -> bool;

    fn import_bytes(
        &self,
        bytes: Bytes,
        progress: tokio::sync::mpsc::Sender<ImportProgress>,
    ) -> bool;
}

pub type BoxStore = Box<dyn Store + Send + Sync + 'static>;

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
