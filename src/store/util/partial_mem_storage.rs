use std::{io, num::NonZeroU64};

use bao_tree::{
    BaoTree, ChunkRanges,
    io::{BaoContentItem, sync::WriteAt},
};

use super::{
    SparseMemFile,
    observer::{BitfieldObserver, ObservableBitfield, UpdateResult},
    size_info::SizeInfo,
};
use crate::{api::blobs::Bitfield, store::IROH_BLOCK_SIZE};

/// An incomplete entry, with all the logic to keep track of the state of the entry
/// and for observing changes.
#[derive(Debug, Default)]
pub struct PartialMemStorage {
    pub(crate) data: SparseMemFile,
    pub(crate) outboard: SparseMemFile,
    pub(crate) size: SizeInfo,
    pub(crate) bitfield: ObservableBitfield,
}

impl PartialMemStorage {
    pub fn subscribe(&self) -> BitfieldObserver {
        self.bitfield.subscribe()
    }

    pub fn update(&mut self, update: Bitfield) {
        self.bitfield.update(update);
    }

    pub fn current_size(&self) -> u64 {
        self.bitfield.with_state(|x| x.size())
    }

    pub fn write_batch(
        &mut self,
        size: NonZeroU64,
        batch: &[BaoContentItem],
        ranges: &ChunkRanges,
    ) -> io::Result<UpdateResult> {
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset
                            .checked_mul(64)
                            .expect("u64 overflow multiplying to hash pair offset");
                        let outboard = &mut self.outboard;
                        let mut buf = [0u8; 64];
                        buf[..32].copy_from_slice(parent.pair.0.as_bytes());
                        buf[32..].copy_from_slice(parent.pair.1.as_bytes());
                        outboard.write_all_at(o0, &buf)?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    self.size.write(leaf.offset, size.get());
                    self.data.write_all_at(leaf.offset, leaf.data.as_ref())?;
                }
            }
        }
        Ok(self
            .bitfield
            .update(Bitfield::new(ranges.clone(), size.get())))
    }
}
