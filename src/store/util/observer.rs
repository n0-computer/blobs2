use std::{fmt::Debug, ops::Deref};

use irpc::channel::spsc;
use tokio::sync::watch;

pub struct BitfieldObserver {
    receiver: watch::Receiver<Bitfield>,
}

impl BitfieldObserver {
    pub fn once(value: Bitfield) -> Self {
        let (_tx, rx) = watch::channel(value);
        Self::new(rx)
    }

    pub fn new(receiver: watch::Receiver<Bitfield>) -> Self {
        Self { receiver }
    }

    /// Forward observed *values* to the given sender
    ///
    /// Returns an error if sending fails, ok if the last sender is dropped
    pub async fn forward(mut self, mut tx: spsc::Sender<Bitfield>) -> anyhow::Result<()> {
        let value = self.receiver.borrow().clone();
        tx.send(value).await?;
        loop {
            self.receiver.changed().await?;
            let value = self.receiver.borrow().clone();
            tx.send(value).await?;
        }
    }
}

use crate::api::{blobs::Bitfield, proto::bitfield::BitfieldState};

// A commutative combine trait for updates
pub trait Combine: Debug {
    fn combine(self, other: Self) -> Self;
}

#[allow(dead_code)]
pub trait CombineInPlace: Combine {
    fn combine_with(&mut self, other: Self) -> Self;
    fn is_neutral(&self) -> bool;
}

#[derive(Debug, Default)]
pub struct ObservableBitfield {
    state: watch::Sender<Bitfield>,
}

impl ObservableBitfield {
    pub fn new(state: Bitfield) -> Self {
        Self {
            state: watch::Sender::new(state),
        }
    }

    pub fn subscribe(&self) -> BitfieldObserver {
        BitfieldObserver::new(self.state.subscribe())
    }

    /// Do something with the state. This takes a lock, so don't hold it for too long.
    pub fn with_state<T>(&self, f: impl FnOnce(&Bitfield) -> T) -> T {
        f(self.state.borrow().deref())
    }

    /// Update the bitfield with a new value, and gives detailed information about the change.
    ///
    /// returns a tuple of (changed, Some((old, new))). If the bitfield changed at all, the flag
    /// is true. If there was a significant change, the old and new states are returned.
    pub fn update(&self, update: Bitfield) -> UpdateResult {
        let mut res = None;
        self.state.send_if_modified(|bitfield| {
            let s0 = bitfield.state();
            let bitfield1 = bitfield.clone().combine(update);
            let ur = if bitfield1 != *bitfield {
                let s1 = bitfield1.state();
                *bitfield = bitfield1;
                if s0 != s1 {
                    UpdateResult::MajorChange(s0, s1)
                } else {
                    UpdateResult::MinorChange(s1)
                }
            } else {
                UpdateResult::NoChange(s0)
            };
            let changed = ur.changed();
            res = Some(ur);
            changed
        });
        res.unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateResult {
    NoChange(BitfieldState),
    MinorChange(BitfieldState),
    MajorChange(BitfieldState, BitfieldState),
}

impl UpdateResult {
    pub fn new(&self) -> &BitfieldState {
        match self {
            UpdateResult::NoChange(new) => &new,
            UpdateResult::MinorChange(new) => &new,
            UpdateResult::MajorChange(_, new) => &new,
        }
    }

    /// True if this change went from non-validated to validated
    pub fn was_validated(&self) -> bool {
        match self {
            UpdateResult::NoChange(_) => false,
            UpdateResult::MinorChange(_) => false,
            UpdateResult::MajorChange(old, new) => {
                new.validated_size.is_some() && old.validated_size.is_none()
            }
        }
    }

    pub fn changed(&self) -> bool {
        match self {
            UpdateResult::NoChange(_) => false,
            UpdateResult::MinorChange(_) => true,
            UpdateResult::MajorChange(_, _) => true,
        }
    }
}
