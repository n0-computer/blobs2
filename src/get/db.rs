use std::{collections::{BTreeMap, HashSet}, num::NonZeroU64};

use bao_tree::{io::mixed::EncodedItem, ChunkRanges};
use iroh::endpoint::Connection;
use n0_future::StreamExt;
use tokio::sync::mpsc;
use tracing::{info, trace};

use super::{
    fsm::{AtBlobHeader, AtEndBlob, BlobContentNext, ConnectedNext, EndBlobNext},
    Stats,
};
use crate::{
    hashseq::HashSeq,
    protocol::{GetRequest, RangeSpec, RangeSpecSeq},
    store::Store,
    BlobFormat, Hash, HashAndFormat, IROH_BLOCK_SIZE,
};

/// Compute the missing data for the given hash or hash seq.
pub async fn get_missing(
    data: impl Into<HashAndFormat>,
    store: impl AsRef<Store>,
) -> anyhow::Result<RangeSpecSeq> {
    let data = data.into();
    let store = store.as_ref();
    Ok(match data.format {
        BlobFormat::Raw => get_missing_blob(data.hash, store).await?,
        BlobFormat::HashSeq => get_missing_hash_seq(data.hash, store).await?,
    })
}

pub async fn get_missing_blob(hash: Hash, store: &Store) -> anyhow::Result<RangeSpecSeq> {
    let local_ranges = store
        .observe(hash)
        .next()
        .await
        .map(|x| x.ranges)
        .expect("observe stream stopped");
    let required_ranges = ChunkRanges::all() - local_ranges;
    Ok(RangeSpecSeq::from_ranges([required_ranges]))
}

pub async fn get_missing_hash_seq(root: Hash, store: &Store) -> anyhow::Result<RangeSpecSeq> {
    let local_bitmap = store
        .observe(root)
        .next()
        .await
        .expect("observe stream stopped");
    let root_ranges = ChunkRanges::all() - local_bitmap.ranges.clone();
    let mut stream = store.export_bao(root, local_bitmap.ranges);
    let mut hashes = HashSet::new();
    hashes.insert(root);
    let mut ranges = BTreeMap::new();
    if !root_ranges.is_empty() {
        ranges.insert(0, RangeSpec::new(root_ranges));
    }
    while let Some(item) = stream.next().await {
        if let EncodedItem::Leaf(data) = item {
            let offset = data.offset / 32 + 1;
            let hs = HashSeq::try_from(data.data)?;
            for (i, hash) in hs.into_iter().enumerate() {
                if !hashes.insert(hash) {
                    continue;
                }
                let local_bitmap = store.observe(hash).next().await.unwrap();
                let missing = ChunkRanges::all() - local_bitmap.ranges;
                let missing = RangeSpec::new(missing);
                ranges.insert(offset + i as u64, missing);
            }
        }
    }
    // let required_ranges = ChunkRanges::all() - local_ranges;
    let n = ranges.keys().last().copied().unwrap_or_default();
    let ranges = (0..=n).map(|i| ranges.remove(&i).unwrap_or(RangeSpec::all()));
    Ok(RangeSpecSeq::new(ranges))
}

pub async fn get_all(
    conn: Connection,
    data: impl Into<HashAndFormat>,
    store: impl AsRef<Store>,
) -> anyhow::Result<Stats> {
    let data = data.into();
    let store = store.as_ref();
    let stats = match data.format {
        BlobFormat::Raw => get_blob_impl(conn, data.hash, store).await?,
        BlobFormat::HashSeq => get_hash_seq_impl(conn, data.hash, store).await?,
    };
    Ok(stats)
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
    let end = get_blob_ranges_impl(header, hash, store).await?;
    // we need to drop the sender so the other side can finish
    let EndBlobNext::Closing(closing) = end.next() else {
        unreachable!()
    };
    let stats = closing.next().await?;
    trace!(?stats, "get blob done");
    Ok(stats)
}

async fn get_blob_ranges_impl(
    header: AtBlobHeader,
    hash: Hash,
    store: &Store,
) -> anyhow::Result<AtEndBlob> {
    let (mut content, size) = header.next().await?;
    let Some(size) = NonZeroU64::new(size) else {
        return if hash == Hash::EMPTY {
            let end = content.drain().await?;
            Ok(end)
        } else {
            Err(anyhow::anyhow!("invalid size for hash"))
        };
    };
    let buffer_size = get_buffer_size(size);
    trace!(%size, %buffer_size, "get blob");
    let (tx, rx) = mpsc::channel(buffer_size);
    let complete = store.import_bao(hash, size, rx);
    let write = async move {
        anyhow::Ok(loop {
            match content.next().await {
                BlobContentNext::More((next, res)) => {
                    let item = res?;
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
    Ok(end)
}

// get a single blob, taking the locally available ranges into account
async fn get_hash_seq_impl(conn: Connection, root: Hash, store: &Store) -> anyhow::Result<Stats> {
    trace!("get hash seq: {}", root);
    let local_ranges = store
        .observe(root)
        .next()
        .await
        .map(|x| x.ranges)
        .unwrap_or_default();
    let required_ranges = ChunkRanges::all() - local_ranges;
    let request = GetRequest::new(root, RangeSpecSeq::from_ranges_infinite([required_ranges]));
    let start = crate::get::fsm::start(conn, request);
    let connected = start.next().await?;
    info!("Getting header");
    // read the header
    let mut next_child = match connected.next().await? {
        ConnectedNext::StartRoot(at_start_root) => {
            let header = at_start_root.next();
            let end = get_blob_ranges_impl(header, root, store).await?;
            match end.next() {
                EndBlobNext::MoreChildren(at_start_child) => Ok(at_start_child),
                EndBlobNext::Closing(at_closing) => Err(at_closing),
            }
        }
        ConnectedNext::StartChild(at_start_child) => Ok(at_start_child),
        ConnectedNext::Closing(at_closing) => Err(at_closing),
    };
    let hash_seq = store.export_bytes(root).await?;
    let Ok(hash_seq) = HashSeq::try_from(hash_seq) else {
        anyhow::bail!("invalid hash seq");
    };
    // read the rest, if any
    let at_closing = loop {
        let at_start_child = match next_child {
            Ok(at_start_child) => at_start_child,
            Err(at_closing) => break at_closing,
        };
        let Ok(offset) = usize::try_from(at_start_child.child_offset()) else {
            anyhow::bail!("hash seq offset too large");
        };
        let Some(hash) = hash_seq.get(offset) else {
            break at_start_child.finish();
        };
        info!("getting child {offset} {}", hash.fmt_short());
        let header = at_start_child.next(hash);
        let end = get_blob_ranges_impl(header, hash, store).await?;
        next_child = match end.next() {
            EndBlobNext::MoreChildren(at_start_child) => Ok(at_start_child),
            EndBlobNext::Closing(at_closing) => Err(at_closing),
        }
    };
    let stats = at_closing.next().await?;
    trace!(?stats, "get hash seq done");
    Ok(stats)
}
