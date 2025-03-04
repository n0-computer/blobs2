use std::num::NonZeroU64;

use bao_tree::{ChunkNum, ChunkRanges};

use crate::util::{
    observer::{Combine, CombineInPlace},
    RangeSetExt,
};

pub fn is_validated(size: NonZeroU64, ranges: &ChunkRanges) -> bool {
    let size = size.get();
    // ChunkNum::chunks will be at least 1, so this is safe.
    let last_chunk = ChunkNum::chunks(size) - 1;
    ranges.contains(&last_chunk)
}

pub fn is_complete(size: NonZeroU64, ranges: &ChunkRanges) -> bool {
    let complete = ChunkRanges::from(..ChunkNum::chunks(size.get()));
    // is_subset is a bit weirdly named. This means that complete is a subset of ranges.
    complete.is_subset(&ranges)
}

/// The state of a bitfield, or an update to a bitfield
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Bitfield {
    /// The ranges that were added
    pub ranges: ChunkRanges,
    /// Possible update to the size information. can this be just a u64?
    pub size: u64,
}

impl Bitfield {
    pub fn new(mut ranges: ChunkRanges, size: u64) -> Self {
        // for zero size, we have to trust the caller
        if let Some(size) = NonZeroU64::new(size) {
            let end = ChunkNum::chunks(size.get());
            if ChunkRanges::from(..end).is_subset(&ranges) {
                // complete bitfield, canonicalize to all
                ranges = ChunkRanges::all();
            } else if ranges.contains(&(end - 1)) {
                // validated bitfield, canonicalize to open end
                ranges |= ChunkRanges::from(end..);
            }
        }
        Self { ranges, size }
    }

    /// An empty bitfield. This is the neutral element for the combine operation.
    pub fn empty() -> Self {
        Self {
            ranges: ChunkRanges::empty(),
            size: 0,
        }
    }

    /// Create a complete bitfield for the given size
    pub fn complete(size: u64) -> Self {
        Self {
            ranges: ChunkRanges::all(),
            size,
        }
    }

    /// True if the chunk corresponding to the size is included in the ranges
    pub fn is_validated(&self) -> bool {
        if let Some(size) = NonZeroU64::new(self.size) {
            is_validated(size, &self.ranges)
        } else {
            self.ranges.is_all()
        }
    }

    /// True if all chunks up to size are included in the ranges
    pub fn is_complete(&self) -> bool {
        if let Some(size) = NonZeroU64::new(self.size) {
            is_complete(size, &self.ranges)
        } else {
            self.ranges.is_all()
        }
    }
}

fn choose_size(a: &Bitfield, b: &Bitfield) -> u64 {
    match (a.ranges.upper_bound(), b.ranges.upper_bound()) {
        (Some(ac), Some(bc)) => {
            if ac < bc {
                b.size
            } else if ac > bc {
                a.size
            } else {
                a.size.max(b.size)
            }
        }
        (Some(_), None) => b.size,
        (None, Some(_)) => a.size,
        (None, None) => a.size.max(b.size),
    }
}

impl Combine for Bitfield {
    fn combine(self, that: Self) -> Self {
        // the size of the chunk with the larger last chunk wins
        let size = choose_size(&self, &that);
        let ranges = self.ranges | that.ranges;
        Self::new(ranges, size)
    }
}

impl CombineInPlace for Bitfield {
    fn combine_with(&mut self, other: Self) -> Self {
        let new = &other.ranges - &self.ranges;
        if new.is_empty() {
            return Bitfield::empty();
        }
        self.ranges.union_with(&new);
        self.size = choose_size(self, &other);
        Bitfield {
            ranges: new,
            size: self.size,
        }
    }

    fn is_neutral(&self) -> bool {
        self.ranges.is_empty() && self.size == 0
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::{ChunkNum, ChunkRanges};
    use proptest::prelude::{prop, Strategy};
    use smallvec::SmallVec;
    use test_strategy::proptest;

    use super::Bitfield;
    use crate::util::observer::{Combine, CombineInPlace};

    fn gen_chunk_ranges(max: ChunkNum, k: usize) -> impl Strategy<Value = ChunkRanges> {
        prop::collection::btree_set(0..=max.0, 0..=k).prop_map(|vec| {
            let bounds = vec.into_iter().map(ChunkNum).collect::<SmallVec<[_; 2]>>();
            ChunkRanges::new(bounds).unwrap()
        })
    }

    fn gen_bitfields(size: u64, k: usize) -> impl Strategy<Value = Bitfield> {
        (0..size).prop_flat_map(move |size| {
            let chunks = ChunkNum::full_chunks(size);
            gen_chunk_ranges(chunks, k).prop_map(move |ranges| Bitfield::new(ranges, size))
        })
    }

    fn gen_non_empty_bitfields(size: u64, k: usize) -> impl Strategy<Value = Bitfield> {
        gen_bitfields(size, k).prop_filter("non-empty", |x| !x.is_neutral())
    }

    #[proptest]
    fn test_combine_empty(#[strategy(gen_non_empty_bitfields(32768, 4))] a: Bitfield) {
        assert_eq!(a.clone().combine(Bitfield::empty()), a);
        assert_eq!(Bitfield::empty().combine(a.clone()), a);
    }

    #[proptest]
    fn test_combine_order(
        #[strategy(gen_non_empty_bitfields(32768, 4))] a: Bitfield,
        #[strategy(gen_non_empty_bitfields(32768, 4))] b: Bitfield,
    ) {
        let ab = a.clone().combine(b.clone());
        let ba = b.combine(a);
        assert_eq!(ab, ba);
    }

    #[proptest]
    fn test_complete_normalized(#[strategy(gen_non_empty_bitfields(32768, 4))] a: Bitfield) {
        if a.is_complete() {
            assert_eq!(a.ranges, ChunkRanges::all());
        }
    }
}
