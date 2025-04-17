//! Implementation of [`super::Getter`] used for testing.

use std::sync::RwLock;

use n0_future::future::Boxed as BoxFuture;

use super::*;
use crate::downloader;

#[derive(Default, Clone, derive_more::Debug)]
#[debug("TestingGetter")]
pub(super) struct TestingGetter(Arc<RwLock<TestingGetterInner>>);

pub(super) type RequestHandlerFn = Arc<
    dyn Fn(
            DownloadKind,
            NodeId,
            BroadcastProgressSender,
            Duration,
        ) -> BoxFuture<InternalDownloadResult>
        + Send
        + Sync
        + 'static,
>;

#[derive(Default)]
struct TestingGetterInner {
    /// How long requests take.
    request_duration: Duration,
    /// History of requests performed by the [`Getter`] and if they were successful.
    request_history: Vec<(DownloadKind, NodeId)>,
    /// Set a handler function which actually handles the requests.
    request_handler: Option<RequestHandlerFn>,
}

impl Getter for TestingGetter {
    // since for testing we don't need a real connection, just keep track of what peer is the
    // request being sent to
    type Connection = NodeId;
    type NeedsConn = GetStateNeedsConn;

    fn get(
        &mut self,
        kind: DownloadKind,
        progress_sender: BroadcastProgressSender,
    ) -> GetStartFut<Self::NeedsConn> {
        Box::pin(std::future::ready(Ok(downloader::GetOutput::NeedsConn(
            GetStateNeedsConn(self.clone(), kind, progress_sender),
        ))))
    }
}

#[derive(Debug)]
pub(super) struct GetStateNeedsConn(TestingGetter, DownloadKind, BroadcastProgressSender);

impl downloader::NeedsConn<NodeId> for GetStateNeedsConn {
    fn proceed(self, peer: NodeId) -> super::GetProceedFut {
        let GetStateNeedsConn(getter, kind, progress_sender) = self;
        let mut inner = getter.0.write().unwrap();
        inner.request_history.push((kind.clone(), peer));
        let request_duration = inner.request_duration;
        let handler = inner.request_handler.clone();
        Box::pin(async move {
            if let Some(f) = handler {
                f(kind, peer, progress_sender, request_duration).await
            } else {
                tokio::time::sleep(request_duration).await;
                Ok(Stats::default())
            }
        })
    }
}

impl TestingGetter {
    pub(super) fn set_handler(&self, handler: RequestHandlerFn) {
        self.0.write().unwrap().request_handler = Some(handler);
    }
    pub(super) fn set_request_duration(&self, request_duration: Duration) {
        self.0.write().unwrap().request_duration = request_duration;
    }
    /// Verify that the request history is as expected
    #[track_caller]
    pub(super) fn assert_history(&self, history: &[(DownloadKind, NodeId)]) {
        assert_eq!(self.0.read().unwrap().request_history, history);
    }
}
