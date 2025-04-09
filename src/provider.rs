//! The server side API
use std::{fmt::Debug, time::Duration};

use anyhow::{Context, Result};
use bao_tree::ChunkRanges;
use iroh::endpoint::{self, RecvStream, SendStream};
use tracing::{debug, debug_span, warn, Instrument};

use crate::{
    api::{self, Store},
    hashseq::HashSeq,
    net_protocol::ProgressSender,
    protocol::{GetRequest, RangeSpecSeq, Request},
    store::util::Tag,
    BlobFormat, Hash,
};

/// Provider progress events, to keep track of what the provider is doing.
///
/// ClientConnected ->
///    (GetRequestReceived -> (TransferStarted -> TransferProgress*n)*n -> (TransferCompleted | TransferAborted))*n
pub enum Event {
    /// A new client connected to the provider.
    ClientConnected { connection_id: u64 },
    /// A new get request was received from the provider.
    GetRequestReceived {
        connection_id: u64,
        request_id: u64,
        hash: Hash,
        ranges: RangeSpecSeq,
    },
    /// Transfer for the nth blob started.
    TransferStarted {
        connection_id: u64,
        request_id: u64,
        index: u64,
        hash: Hash,
    },
    /// Progress of the transfer.
    TransferProgress {
        connection_id: u64,
        request_id: u64,
        index: u64,
        end_offset: u64,
    },
    /// Entire transfer completed.
    TransferCompleted {
        connection_id: u64,
        request_id: u64,
        stats: Box<TransferStats>,
    },
    /// Entire transfer aborted
    TransferAborted {
        connection_id: u64,
        request_id: u64,
        stats: Option<Box<TransferStats>>,
    },
}

pub struct TransferStats {
    pub payload_bytes_sent: u64,
    pub other_bytes_sent: u64,
    pub bytes_read: u64,
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

/// Wrapper for a quinn::SendStream with additional per request information.
#[derive(Debug)]
pub struct WrappedWriter {
    /// The quinn::SendStream to write to
    pub writer: SendStream,
    /// The connection ID from the connection
    pub connection_id: u64,
    /// The request ID from the recv stream
    pub request_id: u64,
    /// The number of bytes written that are part of the payload
    pub payload_bytes_sent: u64,
    /// The number of bytes written that are not part of the payload
    pub other_bytes_sent: u64,
    /// The number of bytes read from the stream
    pub bytes_read: u64,
    /// The progress sender to send events to
    pub progress: ProgressSender,
}

impl WrappedWriter {
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

    pub async fn send_ret_request_received(&self, hash: &Hash, ranges: &RangeSpecSeq) {
        self.progress
            .send(|| Event::GetRequestReceived {
                connection_id: self.connection_id,
                request_id: self.request_id,
                hash: *hash,
                ranges: ranges.clone(),
            })
            .await;
    }

    pub async fn send_transfer_started(&self, index: u64, hash: &Hash) {
        self.progress
            .send(|| Event::TransferStarted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                index,
                hash: *hash,
            })
            .await;
    }
}

/// Handle a single connection.
pub async fn handle_connection(
    connection: endpoint::Connection,
    store: Store,
    progress: ProgressSender,
) {
    let connection_id = connection.stable_id() as u64;
    progress
        .send(Event::ClientConnected { connection_id })
        .await;
    let span = debug_span!("connection", connection_id);
    async move {
        while let Ok((writer, reader)) = connection.accept_bi().await {
            // The stream ID index is used to identify this request.  Requests only arrive in
            // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
            let request_id = reader.id().index();
            let span = debug_span!("stream", stream_id = %request_id);
            let store = store.clone();
            let mut writer = WrappedWriter {
                writer,
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
    }
    .instrument(span)
    .await
}

async fn handle_stream(store: Store, reader: RecvStream, writer: &mut WrappedWriter) -> Result<()> {
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
    }
}

/// Handle a single get request.
///
/// Requires the request, a database, and a writer.
pub async fn handle_get(
    store: Store,
    request: GetRequest,
    writer: &mut WrappedWriter,
) -> Result<()> {
    let hash = request.hash;
    debug!(%hash, "received request");

    writer
        .send_ret_request_received(&hash, &request.ranges)
        .await;
    let mut hash_seq = None;
    for (offset, ranges) in request.ranges.iter_non_empty() {
        if offset == 0 {
            writer.send_transfer_started(offset, &hash).await;
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
                    let bytes = store.export_bytes(hash).await?;
                    let hs = HashSeq::try_from(bytes)?;
                    hash_seq = Some(hs);
                    hash_seq.as_ref().unwrap()
                }
            };
            let o = usize::try_from(offset - 1).context("offset too large")?;
            let Some(hash) = hash_seq.get(o) else {
                break;
            };
            writer.send_transfer_started(offset, &hash).await;
            send_blob(&store, offset, hash, ranges.to_chunk_ranges(), writer).await?;
        }
    }

    Ok(())
}

/// A helper struct that combines a quinn::SendStream with auxiliary information
#[derive(Debug)]
pub struct ResponseWriter {
    inner: SendStream,
    connection_id: u64,
}

impl ResponseWriter {
    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub fn request_id(&self) -> u64 {
        self.inner.id().index()
    }
}

/// Status  of a send operation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SentStatus {
    /// The requested data was sent
    Sent,
    /// The requested data was not found
    NotFound,
}

/// Send a blob to the client.
pub async fn send_blob(
    store: &Store,
    index: u64,
    hash: Hash,
    ranges: ChunkRanges,
    writer: &mut WrappedWriter,
) -> api::Result<()> {
    Ok(store
        .export_bao(hash, ranges)
        .write_quinn_with_progress(writer, index)
        .await?)
}
