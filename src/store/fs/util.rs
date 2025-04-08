use std::{pin::Pin, task::Poll};

use chrono::Duration;
use n0_future::{FutureExt, Stream};

use crate::util::channel::mpsc;

/// A wrapper for a tokio mpsc receiver that allows peeking at the next message.
#[derive(Debug)]
pub struct PeekableReceiver<T> {
    msg: Option<T>,
    recv: mpsc::Receiver<T>,
}

pub struct PeekableReceiverStream<'a, T> {
    inner: &'a mut PeekableReceiver<T>,
    timeout: Pin<Box<n0_future::time::Sleep>>,
}

impl<'a, T> Stream for PeekableReceiverStream<'a, T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Poll::Ready(()) = this.timeout.poll(cx) {
                return Poll::Ready(None);
            }
            if let Some(msg) = this.inner.msg.take() {
                return Poll::Ready(Some(msg));
            }
            if let Poll::Ready(msg) = this.inner.recv.poll_recv(cx) {
                return Poll::Ready(msg);
            }
        }
    }
}

#[allow(dead_code)]
impl<T> PeekableReceiver<T> {
    pub fn new(recv: mpsc::Receiver<T>) -> Self {
        Self { msg: None, recv }
    }

    /// Receive the next message.
    ///
    /// Will block if there are no messages.
    /// Returns None only if there are no more messages (sender is dropped).
    pub async fn recv(&mut self) -> Option<T> {
        if let Some(msg) = self.msg.take() {
            return Some(msg);
        }
        self.recv.recv().await
    }

    pub fn stream(&mut self, duration: std::time::Duration) -> PeekableReceiverStream<'_, T> {
        PeekableReceiverStream {
            inner: self,
            timeout: Box::pin(n0_future::time::sleep(duration)),
        }
    }

    pub async fn extract<U>(&mut self, f: impl Fn(T) -> std::result::Result<U, T>) -> Option<U> {
        let msg = self.recv().await?;
        match f(msg) {
            Ok(u) => Some(u),
            Err(msg) => {
                self.msg = Some(msg);
                None
            }
        }
    }

    /// Push back a message. This will only work if there is room for it.
    /// Otherwise, it will fail and return the message.
    pub fn push_back(&mut self, msg: T) -> std::result::Result<(), T> {
        if self.msg.is_none() {
            self.msg = Some(msg);
            Ok(())
        } else {
            Err(msg)
        }
    }
}
