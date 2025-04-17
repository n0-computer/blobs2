//! The low level server side API
//!
//! Note that while using this API directly is fine, the standard way
//! to provide data is to just register a [`crate::net_protocol`] protocol
//! handler with an [`iroh::Endpoint`](iroh::protocol::Router).
use std::{fmt::Debug, time::Duration};

use anyhow::{Context, Result};
use bao_tree::ChunkRanges;
use iroh::{
    NodeId,
    endpoint::{self, RecvStream, SendStream},
};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, debug_span, error, warn};

use crate::{
    Hash,
    api::{self, Store},
    hashseq::HashSeq,
    protocol::{GetRequest, RangeSpecSeq, Request},
};

/// Provider progress events, to keep track of what the provider is doing.
///
/// ClientConnected ->
///    (GetRequestReceived -> (TransferStarted -> TransferProgress*n)*n -> (TransferCompleted | TransferAborted))*n ->
/// ConnectionClosed
#[derive(Debug)]
pub enum Event {
    /// A new client connected to the provider.
    ClientConnected { connection_id: u64, node_id: NodeId },
    /// Connection closed.
    ConnectionClosed { connection_id: u64 },
    /// A new get request was received from the provider.
    GetRequestReceived {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The root hash of the request.
        hash: Hash,
        /// The exact query ranges of the request.
        ranges: RangeSpecSeq,
    },
    /// Transfer for the nth blob started.
    TransferStarted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
        index: u64,
        /// The hash of the blob. This is the hash of the request for the first blob, the child hash (index-1) for subsequent blobs.
        hash: Hash,
        /// The size of the blob. This is the full size of the blob, not the size we are sending.
        size: u64,
    },
    /// Progress of the transfer.
    TransferProgress {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
        index: u64,
        /// The end offset of the chunk that was sent.
        end_offset: u64,
    },
    /// Entire transfer completed.
    TransferCompleted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// Statistics about the transfer.
        stats: Box<TransferStats>,
    },
    /// Entire transfer aborted
    TransferAborted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// Statistics about the part of the transfer that was aborted.
        stats: Option<Box<TransferStats>>,
    },
}

/// Statistics about a successful or failed transfer.
#[derive(Debug)]
pub struct TransferStats {
    /// The number of bytes sent that are part of the payload.
    pub payload_bytes_sent: u64,
    /// The number of bytes sent that are not part of the payload.
    ///
    /// Hash pairs and the initial size header.
    pub other_bytes_sent: u64,
    /// The number of bytes read from the stream.
    ///
    /// This is the size of the request.
    pub bytes_read: u64,
    /// Total duration from reading the request to transfer completed.
    pub duration: Duration,
}

/// Read the request from the getter.
///
/// Will fail if there is an error while reading, if the reader
/// contains more data than the Request, or if no valid request is sent.
///
/// When successful, the buffer is empty after this function call.
pub async fn read_request(mut reader: RecvStream) -> Result<(Request, usize)> {
    let payload = reader
        .read_to_end(crate::protocol::MAX_MESSAGE_SIZE)
        .await?;
    let request: Request = postcard::from_bytes(&payload)?;
    Ok((request, payload.len()))
}

/// Wrapper for a [`quinn::SendStream`] with additional per request information.
#[derive(Debug)]
pub struct ProgressWriter {
    /// The quinn::SendStream to write to
    pub inner: SendStream,
    /// The connection ID from the connection
    connection_id: u64,
    /// The request ID from the recv stream
    request_id: u64,
    /// The number of bytes written that are part of the payload
    payload_bytes_sent: u64,
    /// The number of bytes written that are not part of the payload
    other_bytes_sent: u64,
    /// The number of bytes read from the stream
    bytes_read: u64,
    /// The progress sender to send events to
    progress: EventSender,
}

impl ProgressWriter {
    /// Increase the write count due to a non-payload write.
    pub fn log_other_write(&mut self, len: usize) {
        self.other_bytes_sent += len as u64;
    }

    pub async fn send_transfer_completed(&mut self) {
        self.progress
            .send(|| Event::TransferCompleted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                stats: Box::new(TransferStats {
                    payload_bytes_sent: self.payload_bytes_sent,
                    other_bytes_sent: self.other_bytes_sent,
                    bytes_read: self.bytes_read,
                    duration: Duration::ZERO,
                }),
            })
            .await;
    }

    pub async fn send_transfer_aborted(&mut self) {
        self.progress
            .send(|| Event::TransferAborted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                stats: Some(Box::new(TransferStats {
                    payload_bytes_sent: self.payload_bytes_sent,
                    other_bytes_sent: self.other_bytes_sent,
                    bytes_read: self.bytes_read,
                    duration: Duration::ZERO,
                })),
            })
            .await;
    }

    /// Increase the write count due to a payload write, and notify the progress sender.
    ///
    /// `index` is the index of the blob in the request.
    /// `offset` is the offset in the blob where the write started.
    /// `len` is the length of the write.
    pub fn notify_payload_write(&mut self, index: u64, offset: u64, len: usize) {
        self.payload_bytes_sent += len as u64;
        self.progress.try_send(|| Event::TransferProgress {
            connection_id: self.connection_id,
            request_id: self.request_id,
            index,
            end_offset: offset + len as u64,
        });
    }

    pub async fn send_request_received(&self, hash: &Hash, ranges: &RangeSpecSeq) {
        self.progress
            .send(|| Event::GetRequestReceived {
                connection_id: self.connection_id,
                request_id: self.request_id,
                hash: *hash,
                ranges: ranges.clone(),
            })
            .await;
    }

    pub async fn send_transfer_started(&self, index: u64, hash: &Hash, size: u64) {
        self.progress
            .send(|| Event::TransferStarted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                index,
                hash: *hash,
                size,
            })
            .await;
    }
}

/// Handle a single connection.
pub async fn handle_connection(
    connection: endpoint::Connection,
    store: Store,
    progress: EventSender,
) {
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id);
    async move {
        let Ok(node_id) = connection.remote_node_id() else {
            warn!("failed to get node id");
            return;
        };
        progress
            .send(Event::ClientConnected {
                connection_id,
                node_id,
            })
            .await;
        while let Ok((writer, reader)) = connection.accept_bi().await {
            // The stream ID index is used to identify this request.  Requests only arrive in
            // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
            let request_id = reader.id().index();
            let span = debug_span!("stream", stream_id = %request_id);
            let store = store.clone();
            let mut writer = ProgressWriter {
                inner: writer,
                connection_id,
                request_id,
                progress: progress.clone(),
                payload_bytes_sent: 0,
                other_bytes_sent: 0,
                bytes_read: 0,
            };
            tokio::spawn(
                async move {
                    match handle_stream(store, reader, &mut writer).await {
                        Ok(()) => {
                            writer.send_transfer_completed().await;
                        }
                        Err(err) => {
                            warn!("error: {err:#?}",);
                            writer.send_transfer_aborted().await;
                        }
                    }
                }
                .instrument(span),
            );
        }
        progress
            .send(Event::ConnectionClosed { connection_id })
            .await;
    }
    .instrument(span)
    .await
}

async fn handle_stream(
    store: Store,
    reader: RecvStream,
    writer: &mut ProgressWriter,
) -> Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let request = match read_request(reader).await {
        Ok((request, size)) => {
            writer.bytes_read += size as u64;
            request
        }
        Err(e) => {
            return Err(e);
        }
    };

    match request {
        Request::Get(request) => handle_get(store, request, writer).await,
        _ => anyhow::bail!("unsupported request: {request:?}"),
        // Request::Push(request) => handle_push(store, request, writer).await,
    }
}

/// Handle a single get request.
///
/// Requires the request, a database, and a writer.
pub async fn handle_get(
    store: Store,
    request: GetRequest,
    writer: &mut ProgressWriter,
) -> Result<()> {
    let hash = request.hash;
    debug!(%hash, "received request");

    writer.send_request_received(&hash, &request.ranges).await;
    let mut hash_seq = None;
    for (offset, ranges) in request.ranges.iter_non_empty_infinite() {
        if offset == 0 {
            send_blob(&store, offset, hash, ranges.to_chunk_ranges(), writer).await?;
        } else {
            // todo: this assumes that 1. the hashseq is complete and 2. it is
            // small enough to fit in memory.
            //
            // This should really read the hashseq from the store in chunks,
            // only where needed, so we can deal with holes and large hashseqs.
            let hash_seq = match &hash_seq {
                Some(b) => b,
                None => {
                    let bytes = store.get_bytes(hash).await?;
                    let hs = HashSeq::try_from(bytes)?;
                    hash_seq = Some(hs);
                    hash_seq.as_ref().unwrap()
                }
            };
            let o = usize::try_from(offset - 1).context("offset too large")?;
            let Some(hash) = hash_seq.get(o) else {
                break;
            };
            send_blob(&store, offset, hash, ranges.to_chunk_ranges(), writer).await?;
        }
    }

    Ok(())
}

/// Send a blob to the client.
pub(crate) async fn send_blob(
    store: &Store,
    index: u64,
    hash: Hash,
    ranges: ChunkRanges,
    writer: &mut ProgressWriter,
) -> api::Result<()> {
    Ok(store
        .export_bao(hash, ranges)
        .write_quinn_with_progress(writer, &hash, index)
        .await?)
}

/// Helper to lazyly create an [`Event`], in the case that the event creation
/// is expensive and we want to avoid it if the progress sender is disabled.
pub trait LazyEvent {
    fn call(self) -> Event;
}

impl<T> LazyEvent for T
where
    T: FnOnce() -> Event,
{
    fn call(self) -> Event {
        self()
    }
}

impl LazyEvent for Event {
    fn call(self) -> Event {
        self
    }
}

/// A sender for provider events.
#[derive(Debug, Clone)]
pub struct EventSender(EventSenderInner);

#[derive(Debug, Clone)]
enum EventSenderInner {
    Disabled,
    Enabled(mpsc::Sender<Event>),
}

impl EventSender {
    pub fn new(sender: Option<mpsc::Sender<Event>>) -> Self {
        match sender {
            Some(sender) => Self(EventSenderInner::Enabled(sender)),
            None => Self(EventSenderInner::Disabled),
        }
    }

    /// Send an ephemeral event, if the progress sender is enabled.
    ///
    /// The event will only be created if the sender is enabled.
    pub fn try_send(&self, event: impl LazyEvent) {
        match &self.0 {
            EventSenderInner::Enabled(sender) => {
                let value = event.call();
                sender.try_send(value).ok();
            }
            EventSenderInner::Disabled => {}
        }
    }

    /// Send a mandatory event, if the progress sender is enabled.
    ///
    /// The event only be created if the sender is enabled.
    pub async fn send(&self, event: impl LazyEvent) {
        match &self.0 {
            EventSenderInner::Enabled(sender) => {
                let value = event.call();
                if let Err(err) = sender.send(value).await {
                    error!("failed to send progress event: {:?}", err);
                }
            }
            EventSenderInner::Disabled => {}
        }
    }
}
