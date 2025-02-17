use bao_tree::ChunkRanges;

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

impl BaoBlobSize {
    /// Create a new `BaoBlobSize` with the given size and verification status.
    pub fn new(size: u64, verified: bool) -> Self {
        if verified {
            BaoBlobSize::Verified(size)
        } else {
            BaoBlobSize::Unverified(size)
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

/// Events from observing a local bitfield
#[derive(Debug, PartialEq, Eq, derive_more::From)]
pub enum BitfieldEvent {
    /// The full state of the bitfield
    State(BitfieldState),
    /// An update to the bitfield
    Update(BitfieldUpdate),
}

/// The state of a bitfield
#[derive(Debug, PartialEq, Eq)]
pub struct BitfieldState {
    /// The ranges that are set
    pub ranges: ChunkRanges,
    /// Whatever size information is available
    pub size: BaoBlobSizeOpt,
}

/// An update to a bitfield
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BitfieldUpdate {
    /// The ranges that were added
    pub added: ChunkRanges,
    /// The ranges that were removed
    pub removed: ChunkRanges,
    /// Possible update to the size information
    pub size: BaoBlobSizeOpt,
}

impl BitfieldState {
    /// State for a completely unknown bitfield
    pub fn unknown() -> Self {
        Self {
            ranges: ChunkRanges::empty(),
            size: BaoBlobSizeOpt::Unknown,
        }
    }
}
