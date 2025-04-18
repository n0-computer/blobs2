use std::{
    fmt::{self, Debug},
    future::Future,
    ops::Deref,
};

use irpc::{
    RpcMessage,
    channel::{SendError, spsc},
};
use n0_future::{Stream, join_all};
use tokio::sync::watch;
use tracing::trace;

pub struct Observer2<T> {
    receiver: watch::Receiver<T>,
}

impl<T> Observer2<T> {
    pub fn once(value: T) -> Self {
        let (_tx, rx) = watch::channel(value);
        Self::new(rx)
    }

    pub fn new(receiver: watch::Receiver<T>) -> Self {
        Self { receiver }
    }

    /// Forward observed *values* to the given sender
    ///
    /// Returns an error if sending fails, ok if the last sender is dropped
    pub async fn forward(mut self, mut tx: spsc::Sender<T>) -> anyhow::Result<()>
    where
        T: RpcMessage + Clone,
    {
        let value = self.receiver.borrow().clone();
        tx.send(value).await?;
        loop {
            self.receiver.changed().await?;
            let value = self.receiver.borrow().clone();
            println!("[observer2] forwarding {:?}", value);
            tx.send(value).await?;
        }
    }
}

use crate::api::blobs::Bitfield;

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

    pub fn subscribe(&self) -> Observer2<Bitfield> {
        Observer2::new(self.state.subscribe())
    }

    pub fn state(&self) -> impl Deref<Target = Bitfield> + '_ {
        self.state.borrow()
    }

    pub fn update(&self, update: Bitfield) -> bool {
        println!("[observer] update outer: {:?}", update);
        let res = self.state.send_if_modified(|state| {
            let current = state.clone();
            println!("[observer] update: {:?} {:?}", current, update);
            let new_state = current.combine(update);
            if new_state != *state {
                *state = new_state;
                true
            } else {
                false
            }
        });
        println!("[observer] update outer end");
        res
    }
}
