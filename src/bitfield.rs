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

pub fn is_validated(size: NonZeroU64, ranges: &ChunkRanges) -> bool {
    let size = size.get();
    // ChunkNum::chunks will be at least 1, so this is safe.
    let last_chunk = ChunkNum::chunks(size) - 1;
    if ranges.contains(&last_chunk) {
        true
    } else {
        false
    }
}

pub fn is_complete(size: NonZeroU64, ranges: &ChunkRanges) -> bool {
    let complete = ChunkRanges::from(..ChunkNum::chunks(size.get()));
    // is_subset is a bit weirdly named. This means that complete is a subset of ranges.
    complete.is_subset(&ranges)
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

pub const EMPTY_HASH: [u8; 32] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, // af1349b9f5f9a1a6
    0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49, // a0404dea36dcc949
    0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, // 9bcb25c9adc112b7
    0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62, // cc9a93cae41f3262
];

/// The state of a bitfield, or an update to a bitfield
///
/// Note that removals are extremely rare, so we model them as a full new state
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Bitfield {
    /// The ranges that were added
    pub ranges: ChunkRanges,
    /// Possible update to the size information. can this be just a u64?
    pub size: u64,
}

impl Bitfield {
    /// Special bitfield for the empty blob &[].
    ///
    /// An empty blob size can not be validated the usual way, so we need to
    /// check the hash.
    pub fn complete_empty() -> Self {
        Self {
            ranges: ChunkRanges::all(),
            size: 0,
        }
    }

    /// The upper (exclusive) bound of the bitfield
    pub fn upper_bound(&self) -> Option<ChunkNum> {
        let boundaries = self.ranges.boundaries();
        if boundaries.is_empty() {
            Some(ChunkNum(0))
        } else if boundaries.len() % 2 == 0 {
            Some(boundaries[boundaries.len() - 1].clone())
        } else {
            None
        }
    }

    pub fn is_validated(&self) -> bool {
        if let Some(size) = NonZeroU64::new(self.size) {
            is_validated(size, &self.ranges)
        } else {
            self.ranges.is_all()
        }
    }

    pub fn is_complete(&self) -> bool {
        if let Some(size) = NonZeroU64::new(self.size) {
            is_complete(size, &self.ranges)
        } else {
            self.ranges.is_all()
        }
    }
}

impl Combine for Bitfield {
    fn combine(self, that: Self) -> Self {
        // the size of the chunk with the larger last chunk wins
        let size = match (self.upper_bound(), that.upper_bound()) {
            (Some(a), Some(b)) => {
                if a < b {
                    that.size
                } else if a > b {
                    self.size
                } else {
                    self.size.max(that.size)
                }
            }
            (Some(_), None) => that.size,
            (None, Some(_)) => self.size,
            (None, None) => self.size.max(that.size),
        };
        let mut ranges = self.ranges | that.ranges;
        // for non zero sizzes, if we have all chunks, we canonicalize the ranges to all
        if let Some(size) = NonZeroU64::new(size) {
            if is_complete(size, &ranges) {
                ranges = ChunkRanges::all();
            }
        }
        Self { ranges, size }
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
