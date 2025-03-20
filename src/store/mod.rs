use bao_tree::BlockSize;
pub mod fs;
mod mem;
mod readonly_mem;
mod test;
pub(crate) mod util;

pub use crate::hash::{BlobFormat, Hash, HashAndFormat};

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
