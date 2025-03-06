use std::num::NonZeroU64;

use bao_tree::ChunkRanges;
use bytes::buf;
use iroh::endpoint::Connection;
use n0_future::StreamExt;
use tokio::sync::mpsc;
use tracing::{info, trace};

use crate::{
    protocol::{GetRequest, RangeSpecSeq},
    store::Store,
    BlobFormat, Hash, HashAndFormat, IROH_BLOCK_SIZE,
};

use super::{
    fsm::{BlobContentNext, ConnectedNext, EndBlobNext},
    Stats,
};

pub async fn get_all(
    conn: Connection,
    data: impl Into<HashAndFormat>,
    store: impl AsRef<Store>,
) -> anyhow::Result<()> {
    let data = data.into();
    let store = store.as_ref();
    match data.format {
        BlobFormat::Raw => {
            get_blob_impl(conn, data.hash, store).await?;
        }
        BlobFormat::HashSeq => {
            todo!()
        }
    }
    Ok(())
}

fn get_buffer_size(size: NonZeroU64) -> usize {
    (size.get() / (IROH_BLOCK_SIZE.bytes() as u64) + 2).min(64) as usize
}

// get a single blob, taking the locally available ranges into account
async fn get_blob_impl(conn: Connection, hash: Hash, store: &Store) -> anyhow::Result<Stats> {
    trace!("get blob: {}", hash);
    let local_ranges = store
        .observe(hash)
        .next()
        .await
        .map(|x| x.ranges)
        .unwrap_or_default();
    let required_ranges = ChunkRanges::all() - local_ranges;
    if required_ranges.is_empty() {
        // we don't count the time for looking up the bitfield locally
        return Ok(Stats::default());
    }
    let request = GetRequest::new(hash, RangeSpecSeq::from_ranges([required_ranges]));
    let start = crate::get::fsm::start(conn, request);
    let next = start.next().await?;
    let ConnectedNext::StartRoot(root) = next.next().await? else {
        unreachable!()
    };
    let header = root.next();
    let (mut content, size) = header.next().await?;
    let Some(size) = NonZeroU64::new(size) else {
        return if hash == Hash::EMPTY {
            // we don't count the time for looking up the bitfield locally
            Ok(Stats::default())
        } else {
            Err(anyhow::anyhow!("invalid size for hash"))
        };
    };
    let buffer_size = get_buffer_size(size);
    trace!(%size, %buffer_size, "get blob");
    let (tx, rx) = mpsc::channel(buffer_size);
    info!("calling import_bao");
    let complete = store.import_bao(hash, size, rx);
    let write = async move {
        anyhow::Ok(loop {
            match content.next().await {
                BlobContentNext::More((next, res)) => {
                    let item = res?;
                    info!("send item {:?}", item);
                    tx.send(item).await?;
                    content = next;
                }
                BlobContentNext::Done(end) => {
                    drop(tx);
                    break end;
                }
            }
        })
    };
    let (_, end) = tokio::try_join!(complete, write)?;
    // we need to drop the sender so the other side can finish
    let EndBlobNext::Closing(closing) = end.next() else {
        unreachable!()
    };
    let stats = closing.next().await?;
    trace!(?stats, "get blob done");
    Ok(stats)
}
