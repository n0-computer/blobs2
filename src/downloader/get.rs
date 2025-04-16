//! [`Getter`] implementation that performs requests over [`Connection`]s.
//!
//! [`Connection`]: iroh::endpoint::Connection
use iroh::endpoint;
use tracing::error;

use super::{
    DownloadKind, FailureAction, GetOutput, GetStartFut, Getter, NeedsConn,
    progress::BroadcastProgressSender,
};
use crate::api::{Store, download::LocalInfo};

impl From<anyhow::Error> for FailureAction {
    fn from(e: anyhow::Error) -> Self {
        todo!()
        // match e {
        //     e @ GetError::NotFound(_) => FailureAction::AbortRequest(e.into()),
        //     e @ GetError::RemoteReset(_) => FailureAction::RetryLater(e.into()),
        //     e @ GetError::NoncompliantNode(_) => FailureAction::DropPeer(e.into()),
        //     e @ GetError::Io(_) => FailureAction::RetryLater(e.into()),
        //     e @ GetError::BadRequest(_) => FailureAction::AbortRequest(e.into()),
        //     // TODO: what do we want to do on local failures?
        //     e @ GetError::LocalFailure(_) => FailureAction::AbortRequest(e.into()),
        // }
    }
}

/// [`Getter`] implementation that performs requests over [`Connection`]s.
///
/// [`Connection`]: iroh::endpoint::Connection
pub(crate) struct IoGetter {
    pub store: Store,
}

#[derive(Debug)]
pub struct GetStateNeedsConn {
    pub store: Store,
    pub info: LocalInfo,
    pub progress: BroadcastProgressSender,
}

impl NeedsConn<endpoint::Connection> for GetStateNeedsConn {
    fn proceed(self, conn: endpoint::Connection) -> super::GetProceedFut {
        let store = self.store.clone();
        let local_bytes = self.info.local_bytes();
        Box::pin(async move {
            let res = store
                .download()
                .execute(
                    conn,
                    self.info.missing(),
                    Some(self.progress.add_offset(local_bytes)),
                )
                .await;
            #[cfg(feature = "metrics")]
            track_metrics(&res);
            match res {
                Ok(stats) => Ok(stats),
                Err(err) => Err(err.into()),
            }
        })
    }
}

impl Getter for IoGetter {
    type Connection = endpoint::Connection;
    type NeedsConn = GetStateNeedsConn;

    fn get(
        &mut self,
        kind: DownloadKind,
        progress: BroadcastProgressSender,
    ) -> GetStartFut<Self::NeedsConn> {
        let store = self.store.clone();
        Box::pin(async move {
            let local = store.download().local(kind).await.map_err(|e| {
                error!("failed to get local info: {}", e);
                FailureAction::AbortRequest(e)
            })?;
            Ok(if local.is_complete() {
                GetOutput::Complete(Default::default())
            } else {
                GetOutput::NeedsConn(GetStateNeedsConn {
                    store: store.clone(),
                    info: local,
                    progress,
                })
            })
        })
    }
}

#[cfg(feature = "metrics")]
fn track_metrics(res: &Result<crate::get::Stats, GetError>) {
    use iroh_metrics::{inc, inc_by};

    use crate::metrics::Metrics;
    match res {
        Ok(stats) => {
            let crate::get::Stats {
                bytes_written,
                bytes_read: _,
                elapsed,
            } = stats;

            inc!(Metrics, downloads_success);
            inc_by!(Metrics, download_bytes_total, *bytes_written);
            inc_by!(Metrics, download_time_total, elapsed.as_millis() as u64);
        }
        Err(e) => match &e {
            GetError::NotFound(_) => inc!(Metrics, downloads_notfound),
            _ => inc!(Metrics, downloads_error),
        },
    }
}
