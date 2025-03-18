//! The server side API
use std::{fmt::Debug, io};

use anyhow::{Context, Result};
use bao_tree::{io::EncodeError, ChunkRanges};
use iroh::endpoint::{self, RecvStream, SendStream};
use tracing::{debug, debug_span, warn, Instrument};

use crate::{
    hashseq::HashSeq,
    protocol::{GetRequest, Request},
    store::*,
    Hash,
};

/// Read the request from the getter.
///
/// Will fail if there is an error while reading, if the reader
/// contains more data than the Request, or if no valid request is sent.
///
/// When successful, the buffer is empty after this function call.
pub async fn read_request(mut reader: RecvStream) -> Result<Request> {
    let payload = reader
        .read_to_end(crate::protocol::MAX_MESSAGE_SIZE)
        .await?;
    let request: Request = postcard::from_bytes(&payload)?;
    Ok(request)
}

/// Handle a single connection.
pub async fn handle_connection(connection: endpoint::Connection, store: Store) {
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id);
    async move {
        while let Ok((writer, reader)) = connection.accept_bi().await {
            // The stream ID index is used to identify this request.  Requests only arrive in
            // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
            let request_id = reader.id().index();
            let span = debug_span!("stream", stream_id = %request_id);
            let store = store.clone();
            tokio::spawn(
                async move {
                    if let Err(err) = handle_stream(store, reader, writer).await {
                        warn!("error: {err:#?}",);
                    }
                }
                .instrument(span),
            );
        }
    }
    .instrument(span)
    .await
}

async fn handle_stream(store: Store, reader: RecvStream, writer: SendStream) -> Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let request = match read_request(reader).await {
        Ok(r) => r,
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
pub async fn handle_get(store: Store, request: GetRequest, mut writer: SendStream) -> Result<()> {
    let hash = request.hash;
    debug!(%hash, "received request");

    let mut hash_seq = None;
    for (offset, ranges) in request.ranges.iter_non_empty() {
        if offset == 0 {
            send_blob(&store, hash, ranges.to_chunk_ranges(), &mut writer).await?;
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
            send_blob(&store, hash, ranges.to_chunk_ranges(), &mut writer).await?;
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
    hash: Hash,
    ranges: ChunkRanges,
    writer: &mut SendStream,
) -> io::Result<()> {
    store
        .export_bao(hash, ranges)
        .await
        .write_quinn(writer)
        .await
}

#[allow(dead_code)]
fn encode_error_to_anyhow(err: EncodeError, hash: &Hash) -> anyhow::Error {
    match err {
        EncodeError::LeafHashMismatch(x) => anyhow::Error::from(EncodeError::LeafHashMismatch(x))
            .context(format!("hash {} offset {}", hash.to_hex(), x.to_bytes())),
        EncodeError::ParentHashMismatch(n) => {
            let r = n.chunk_range();
            anyhow::Error::from(EncodeError::ParentHashMismatch(n)).context(format!(
                "hash {} range {}..{}",
                hash.to_hex(),
                r.start.to_bytes(),
                r.end.to_bytes()
            ))
        }
        e => anyhow::Error::from(e).context(format!("hash {}", hash.to_hex())),
    }
}
