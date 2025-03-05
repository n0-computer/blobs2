//! The server side API
use core::panic;
use std::{fmt::Debug, io, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bao_tree::{
    io::{
        fsm::{encode_ranges_validated, Outboard},
        EncodeError,
    },
    ChunkRanges,
};
use futures_lite::future::Boxed as BoxFuture;
use iroh::endpoint::{self, RecvStream, SendStream};
use quinn::Chunk;
use serde::{Deserialize, Serialize};
use tracing::{debug, debug_span, info, trace, warn, Instrument};

use crate::{
    hashseq::parse_hash_seq,
    protocol::{GetRequest, RangeSpec, Request},
    store::*,
    BlobFormat, Hash,
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

/// Transfers a blob or hash sequence to the client.
///
/// The difference to [`handle_get`] is that we already have a reader for the
/// root blob and outboard.
///
/// First, it transfers the root blob. Then, if needed, it sequentially
/// transfers each individual blob data.
///
/// The transfer fail if there is an error writing to the writer or reading from
/// the database.
///
/// If a blob from the hash sequence cannot be found in the database, the
/// transfer will return with [`SentStatus::NotFound`]. If the transfer completes
/// successfully, it will return with [`SentStatus::Sent`].
// pub(crate) async fn transfer_hash_seq(
//     request: GetRequest,
//     // Store from which to fetch blobs.
//     db: &Store,
//     // Response writer, containing the quinn stream.
//     writer: &mut ResponseWriter,
//     // the collection to transfer
// ) -> Result<SentStatus> {
//     let hash = request.hash;
//     let request_id = writer.request_id();
//     let connection_id = writer.connection_id();

//     // if the request is just for the root, we don't need to deserialize the collection
//     let just_root = matches!(request.ranges.as_single(), Some((0, _)));
//     let mut c = if !just_root {
//         // parse the hash seq
//         let (stream, num_blobs) = parse_hash_seq(&mut data).await?;
//         Some(stream)
//     } else {
//         None
//     };

//     let mut prev = 0;
//     for (offset, ranges) in request.ranges.iter_non_empty() {
//         // create a tracking writer so we can get some stats for writing
//         let mut tw = writer.tracking_writer();
//         if offset == 0 {
//             debug!("writing ranges '{:?}' of sequence {}", ranges, hash);
//             // wrap the data reader in a tracking reader so we can get some stats for reading
//             let mut tracking_reader = TrackingSliceReader::new(&mut data);
//             let mut sending_reader =
//                 SendingSliceReader::new(&mut tracking_reader, &events, mk_progress);
//             // send the root
//             tw.write(outboard.tree().size().to_le_bytes().as_slice())
//                 .await?;
//             encode_ranges_validated(
//                 &mut sending_reader,
//                 &mut outboard,
//                 &ranges.to_chunk_ranges(),
//                 &mut tw,
//             )
//             .await?;
//             stats.read += tracking_reader.stats();
//             stats.send += tw.stats();
//             debug!(
//                 "finished writing ranges '{:?}' of collection {}",
//                 ranges, hash
//             );
//         } else {
//             let c = c.as_mut().context("collection parser not available")?;
//             debug!("wrtiting ranges '{:?}' of child {}", ranges, offset);
//             // skip to the next blob if there is a gap
//             if prev < offset - 1 {
//                 c.skip(offset - prev - 1).await?;
//             }
//             if let Some(hash) = c.next().await? {
//                 tokio::task::yield_now().await;
//                 let (status, size, blob_read_stats) =
//                     send_blob(db, hash, ranges, &mut tw, events.clone(), mk_progress).await?;
//                 stats.send += tw.stats();
//                 stats.read += blob_read_stats;
//                 if SentStatus::NotFound == status {
//                     writer.inner.finish()?;
//                     return Ok(status);
//                 }

//                 writer
//                     .events
//                     .send(|| Event::TransferBlobCompleted {
//                         connection_id: writer.connection_id(),
//                         request_id: writer.request_id(),
//                         hash,
//                         index: offset - 1,
//                         size,
//                     })
//                     .await;
//             } else {
//                 // nothing more we can send
//                 break;
//             }
//             prev = offset;
//         }
//     }

//     debug!("done writing");
//     Ok(SentStatus::Sent)
// }

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
            let hash_seq = match &hash_seq {
                Some(b) => b,
                None => {
                    hash_seq = Some(store.export_bytes(hash).await?);
                    hash_seq.as_ref().unwrap()
                }
            };
            let size = hash_seq.len();
            if size % 32 != 0 {
                anyhow::bail!("hash sequence size is not a multiple of 32");
            }
            let n = size / 32;
            let o = usize::try_from(offset - 1).context("offset too large")?;
            if o >= n {
                break;
            }
            let range = o * 32..(o + 1) * 32;
            let hash = Hash::from_bytes(hash_seq[range].try_into().unwrap());
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
    fn connection_id(&self) -> u64 {
        self.connection_id
    }

    fn request_id(&self) -> u64 {
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
        .export_bao(hash.into(), ranges)
        .write_quinn(writer)
        .await
}

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
