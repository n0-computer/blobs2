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
