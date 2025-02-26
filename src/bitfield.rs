use std::num::NonZeroU64;

use bao_tree::{blake3, ChunkNum, ChunkRanges, ChunkRangesRef};
use tokio::sync::mpsc;
use tracing::error;

use crate::util::observer::Combine;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct UnverifiedSize(u64);

/// The size of a bao file
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BaoBlobSize {
    /// A remote side told us the size, but we have insufficient data to verify it.
    Unverified(u64),
    /// We have verified the size.
    Verified(u64),
}

fn is_validated(size: NonZeroU64, ranges: ChunkRanges) -> bool {
    let size = size.get();
    // ChunkNum::chunks will be at least 1, so this is safe.
    let last_chunk = ChunkNum::chunks(size) - 1;
    if ranges.contains(&last_chunk) {
        true
    } else {
        false
    }
}

fn is_complete(size: u64, ranges: ChunkRanges) -> bool {
    ChunkRanges::from(..ChunkNum::chunks(size)).is_subset(&ranges)
}

impl BaoBlobSize {
    /// Create a new `BaoBlobSize` with the given size and verification status.
    pub fn new(size: u64, verified: bool) -> Self {
        if verified {
            BaoBlobSize::Verified(size)
        } else {
            BaoBlobSize::Unverified(size)
        }
    }

    pub fn from_size_and_ranges(size: NonZeroU64, ranges: &ChunkRangesRef) -> BaoBlobSize {
        let size = size.get();
        // ChunkNum::chunks will be at least 1, so this is safe.
        let last_chunk = ChunkNum::chunks(size) - 1;
        if ranges.contains(&last_chunk) {
            BaoBlobSize::Verified(size)
        } else {
            BaoBlobSize::Unverified(size)
        }
    }

    pub fn from_size_and_ranges_and_hash(
        size: u64,
        ranges: &ChunkRangesRef,
        hash: &blake3::Hash,
    ) -> BaoBlobSize {
        if size == 0 {
            // Getting called with 0 is weird, but we still have to handle it.
            if hash == &EMPTY_HASH && ranges.is_empty() {
                BaoBlobSize::Verified(0)
            } else {
                BaoBlobSize::Unverified(0)
            }
        } else {
            // full_chunks will be at least 1, so this is safe.
            let last_chunk = ChunkNum::chunks(size) - 1;
            if ranges.contains(&last_chunk) {
                BaoBlobSize::Verified(size)
            } else {
                BaoBlobSize::Unverified(size)
            }
        }
    }

    /// Get just the value, no matter if it is verified or not.
    pub fn value(&self) -> u64 {
        match self {
            BaoBlobSize::Unverified(size) => *size,
            BaoBlobSize::Verified(size) => *size,
        }
    }
}

/// Knowlege about the size of a blob
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BaoBlobSizeOpt {
    /// We have a size that a peer told us about, but we don't know if it is correct
    /// It can be off at most by a factor of 2, so it is OK for things like showing
    /// a progress bar or even for an allocation size
    Unverified(u64),
    /// We know the size, and it is verified
    /// either by having the last chunk locally or by receiving a size proof from a peer
    Verified(u64),
    /// We know nothing, e.g. we have never heard of the blob
    #[default]
    Unknown,
}

impl BaoBlobSizeOpt {
    pub fn combine(
        a: BaoBlobSizeOpt,
        b: BaoBlobSizeOpt,
        ar: &ChunkRanges,
        br: &ChunkRanges,
    ) -> Self {
        match (a, b) {
            (a, BaoBlobSizeOpt::Unknown) => a,
            (BaoBlobSizeOpt::Unknown, b) => b,
            (BaoBlobSizeOpt::Verified(a), BaoBlobSizeOpt::Verified(b)) => {
                if a != b {
                    error!("mismatched verified sizes: {a} != {b}");
                }
                BaoBlobSizeOpt::Verified(a.max(b))
            }
            (a @ BaoBlobSizeOpt::Verified(_), BaoBlobSizeOpt::Unverified(_)) => a,
            (BaoBlobSizeOpt::Unverified(_), b @ BaoBlobSizeOpt::Verified(_)) => b,
            (BaoBlobSizeOpt::Unverified(a), BaoBlobSizeOpt::Unverified(b)) => {
                // todo! use ar and br
                BaoBlobSizeOpt::Unverified(a.max(b))
            }
        }
    }

    /// Get the value of the size, if known
    pub fn value(self) -> Option<u64> {
        match self {
            BaoBlobSizeOpt::Unverified(x) => Some(x),
            BaoBlobSizeOpt::Verified(x) => Some(x),
            BaoBlobSizeOpt::Unknown => None,
        }
    }

    /// Update the size information
    ///
    /// Unkown sizes are always updated
    /// Unverified sizes are updated if the new size is verified
    /// Verified sizes must never change
    pub fn update(&mut self, size: BaoBlobSizeOpt) -> anyhow::Result<()> {
        match self {
            BaoBlobSizeOpt::Verified(old) => {
                if let BaoBlobSizeOpt::Verified(new) = size {
                    if *old != new {
                        anyhow::bail!("mismatched verified sizes: {old} != {new}");
                    }
                }
            }
            BaoBlobSizeOpt::Unverified(_) => {
                if let BaoBlobSizeOpt::Verified(new) = size {
                    *self = BaoBlobSizeOpt::Verified(new);
                }
            }
            BaoBlobSizeOpt::Unknown => *self = size,
        };
        Ok(())
    }
}

impl From<BaoBlobSize> for BaoBlobSizeOpt {
    fn from(size: BaoBlobSize) -> Self {
        match size {
            BaoBlobSize::Unverified(x) => Self::Unverified(x),
            BaoBlobSize::Verified(x) => Self::Verified(x),
        }
    }
}

const EMPTY_HASH: [u8; 32] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, // af1349b9f5f9a1a6
    0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49, // a0404dea36dcc949
    0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, // 9bcb25c9adc112b7
    0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62, // cc9a93cae41f3262
];

/// An update to a bitfield
///
/// Note that removals are extremely rare, so we model them as a full new state
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct BitfieldUpdate {
    /// The ranges that were added
    pub ranges: ChunkRanges,
    /// Possible update to the size information. can this be just a u64?
    pub size: BaoBlobSizeOpt,
}

impl Combine for BitfieldUpdate {
    fn combine(self, that: Self) -> Self {
        let size = BaoBlobSizeOpt::combine(self.size, that.size, &self.ranges, &that.ranges);
        let added = self.ranges | that.ranges;
        Self {
            ranges: added,
            size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bao_blob_size() {
        let hash = blake3::hash(&[]);
        let size = BaoBlobSize::from_size_and_ranges_and_hash(0, &ChunkRanges::empty(), &hash);
        assert_eq!(size, BaoBlobSize::Verified(0));
        let hash = blake3::hash(b"a");
        let size =
            BaoBlobSize::from_size_and_ranges_and_hash(1, &ChunkRanges::from(..ChunkNum(1)), &hash);
        assert_eq!(size, BaoBlobSize::Verified(1));
        let hash = blake3::hash(&[0u8; 1024]);
        let size = BaoBlobSize::from_size_and_ranges_and_hash(
            1024,
            &ChunkRanges::from(..ChunkNum(1)),
            &hash,
        );
        assert_eq!(size, BaoBlobSize::Verified(1024));
        let hash = blake3::hash(&[0u8; 1025]);
        let size = BaoBlobSize::from_size_and_ranges_and_hash(
            1025,
            &ChunkRanges::from(..ChunkNum(1)),
            &hash,
        );
        assert_eq!(size, BaoBlobSize::Unverified(1025));
        let hash = blake3::hash(&[0u8; 1025]);
        let size = BaoBlobSize::from_size_and_ranges_and_hash(
            1025,
            &ChunkRanges::from(ChunkNum(1)..ChunkNum(2)),
            &hash,
        );
        assert_eq!(size, BaoBlobSize::Verified(1025));
    }

    #[test]
    fn test_empty_hash() {
        let hash = blake3::hash(&[]);
        assert_eq!(hash, blake3::Hash::from(EMPTY_HASH));
    }
}
