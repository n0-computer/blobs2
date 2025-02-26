use std::{fmt, future::Future};

use n0_future::join_all;
use quinn::rustls::crypto::hash::Output;
use tokio::sync::mpsc;

// A commutative combine trait for updates
pub trait Combine {
    fn combine(self, other: Self) -> Self;
}

// An observer that accumulates updates
#[derive(Debug)]
pub struct Observer<U> {
    tx: mpsc::Sender<U>,
    pending_update: Option<U>,
}

impl<U> Observer<U> {
    // Create a new observer with an externally provided sender and no observers initially
    pub fn new(tx: mpsc::Sender<U>) -> Self {
        Self {
            tx,
            pending_update: None,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    // Send an update, handling queue full (no error) or remote drop scenarios with a single try_send
    pub fn send(&mut self, update: U) -> Result<ObserveSuccess, ObserverError>
    where
        U: Combine,
    {
        // If we have a pending update, combine it with the new update
        let update_to_send = match self.pending_update.take() {
            Some(pending) => pending.combine(update),
            None => update,
        };

        match self.tx.try_send(update_to_send) {
            Ok(()) => Ok(ObserveSuccess::Sent),
            Err(mpsc::error::TrySendError::Full(unsent_update)) => {
                self.pending_update = Some(unsent_update);
                Ok(ObserveSuccess::Combined) // Queue full is not an error; update is preserved
            }
            Err(mpsc::error::TrySendError::Closed(unsent_update)) => {
                self.pending_update = Some(unsent_update);
                Err(ObserverError::RemoteDropped)
            }
        }
    }

    /// Send the pending update, if any
    pub async fn finish(mut self) {
        if let Some(update) = self.pending_update.take() {
            let _ = self.tx.send(update).await;
        }
    }

    pub fn pending_update(&self) -> Option<&U> {
        self.pending_update.as_ref()
    }

    pub fn into_pending_update(self) -> Option<U> {
        self.pending_update
    }

    pub fn receiver_dropped(&self) -> impl Future<Output = ()> {
        let tx = self.tx.clone();
        async move {
            tx.closed().await;
        }
    }
}

// Concrete error type for observer failures (only RemoteDropped remains)
#[derive(Debug, PartialEq, Eq)]
pub enum ObserverError {
    RemoteDropped,
}

// Concrete error type for observer failures (only RemoteDropped remains)
#[derive(Debug, PartialEq, Eq)]
pub enum ObserveSuccess {
    Sent,
    Combined,
}

impl fmt::Display for ObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObserverError::RemoteDropped => write!(f, "Remote receiver dropped"),
        }
    }
}

impl std::error::Error for ObserverError {}

// ObserverManager struct to manage state (of type U) and observables
#[derive(Debug)]
pub struct Observable<U> {
    state: Option<U>,
    observers: Vec<Observer<U>>,
}

impl<U: Default> Default for Observable<U> {
    fn default() -> Self {
        Self::new(U::default())
    }
}

impl<U> Observable<U> {
    // Create a new observer manager with the default state and no observables
    pub fn new(state: U) -> Self {
        Self {
            state: Some(state),
            observers: Vec::new(),
        }
    }

    // Add an observable to the manager
    pub fn add_observer(&mut self, mut observer: Observer<U>)
    where
        U: Combine + Clone,
    {
        let state = self.state().clone();
        if observer.send(state).is_ok() {
            self.observers.push(observer);
        }
    }

    pub fn observer_dropped(&mut self) {
        self.observers.retain(|o| !o.is_closed());
    }

    // Update the state and send the update to all observables, retaining only live observables
    pub fn update(&mut self, update: U)
    where
        U: Combine + Clone,
    {
        // Update the state by combining with the update
        self.state = Some(
            self.state
                .take()
                .expect("State must be initialized")
                .combine(update.clone()),
        );

        // Send the update to all observables and filter out dropped ones
        self.observers
            .retain_mut(|observable| observable.send(update.clone()).is_ok());
    }

    // Get the current state (for testing or debugging)
    pub fn state(&self) -> &U {
        &self.state.as_ref().expect("State must be initialized")
    }

    // Get the list of observables (for testing or debugging)
    pub fn observables(&self) -> &[Observer<U>] {
        &self.observers
    }

    // Finish all observables (send pending update if any)
    pub async fn finish(&mut self) {
        let mut observers = std::mem::take(&mut self.observers);
        observers.retain_mut(|o| !o.is_closed());
        if !observers.is_empty() {
            join_all(observers.into_iter().map(|o| o.finish())).await;
        }
    }
}

// Tests module
#[cfg(test)]
mod tests {
    use testresult::TestResult;
    use tokio::sync::{mpsc, oneshot};

    use super::*;

    // Example update type for testing, implementing Combine and Default
    #[derive(Debug, PartialEq, Default, Clone)]
    struct Counter(u32);

    impl Combine for Counter {
        fn combine(self, other: Self) -> Self {
            Counter(self.0 + other.0) // Commutative addition
        }
    }

    impl fmt::Display for Counter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Update({})", self.0)
        }
    }

    #[tokio::test]
    async fn test_observer() -> TestResult<()> {
        // Test Observer behavior (queue full and remote drop)
        let (tx, mut rx) = mpsc::channel(2); // Small capacity to test queue full
        let mut observer = Observer::new(tx);

        // Test sending initial update
        assert_eq!(observer.send(Counter(1)), Ok(ObserveSuccess::Sent));

        // Test sending another update (queue should handle it)
        assert_eq!(observer.send(Counter(2)), Ok(ObserveSuccess::Sent));

        // Test queue full (should preserve update, no error)
        assert_eq!(observer.send(Counter(3)), Ok(ObserveSuccess::Combined));

        // Test queue full (should preserve update, no error)
        assert_eq!(observer.send(Counter(4)), Ok(ObserveSuccess::Combined));
        assert_eq!(
            observer.pending_update(),
            Some(&Counter(7)), // 3 + 4 = 7
        );

        // Spawn a task to receive updates (simulate a consumer) and return a oneshot channel for completion
        let handle = tokio::spawn(async move {
            let mut res = Counter::default();
            while let Some(update) = rx.recv().await {
                res = res.combine(update);
            }
            res
        });

        // Finish the observable (send the pending update, if any)
        observer.finish().await;

        // check the final result
        let res = handle.await?;
        assert_eq!(res, Counter(10)); // 1 + 2 + 3 + 4 = 10
        Ok(())
    }

    #[tokio::test]
    async fn test_observable() -> TestResult<()> {
        let mut obs = Observable::new(Counter(1));
        obs.update(Counter(2));
        let (tx1, mut rx1) = mpsc::channel(2);
        obs.add_observer(Observer::new(tx1));

        // first message is the initial state
        assert_eq!(rx1.recv().await, Some(Counter(3)));

        // next message is the update
        obs.update(Counter(3));
        assert_eq!(rx1.recv().await, Some(Counter(3)));

        // new observer should get the current state
        let (tx1, mut rx1) = mpsc::channel(2);
        obs.add_observer(Observer::new(tx1));
        assert_eq!(rx1.recv().await, Some(Counter(6)));

        Ok(())
    }
}
