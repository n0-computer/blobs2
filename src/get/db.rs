use std::{future::Future, num::NonZeroU64};

use bao_tree::{io::Leaf, ChunkRanges};
use iroh::{endpoint::Connection, Endpoint, NodeAddr};
use tracing::{info, trace};

use super::{
    fsm::{AtBlobHeader, AtEndBlob, BlobContentNext, ConnectedNext, EndBlobNext},
    Stats,
};
use crate::{
    api::{self, blobs::Blobs, Store},
    hashseq::{HashSeq, HashSeqIter},
    protocol::{GetRequest, RangeSpecSeq},
    util::channel::mpsc,
    BlobFormat, Hash, HashAndFormat, IROH_BLOCK_SIZE,
};

/// Trait to lazily get a connection
pub trait GetConnection {
    fn connection(&mut self)
        -> impl Future<Output = Result<Connection, anyhow::Error>> + Send + '_;
}

/// If we already have a connection, the impl is trivial
impl GetConnection for Connection {
    fn connection(
        &mut self,
    ) -> impl Future<Output = Result<Connection, anyhow::Error>> + Send + '_ {
        let conn = self.clone();
        async { Ok(conn) }
    }
}

pub struct Dialer {
    endpoint: Endpoint,
    addr: NodeAddr,
    conn: Option<Connection>,
}

impl Dialer {
    pub fn new(endpoint: Endpoint, addr: NodeAddr) -> Self {
        Self {
            endpoint,
            addr,
            conn: None,
        }
    }
}

impl GetConnection for Dialer {
    async fn connection(&mut self) -> Result<Connection, anyhow::Error> {
        if let Some(conn) = &self.conn {
            Ok(conn.clone())
        } else {
            let conn = self
                .endpoint
                .connect(self.addr.clone(), crate::ALPN)
                .await?;
            self.conn = Some(conn.clone());
            Ok(conn)
        }
    }
}

pub async fn get_all(
    conn: impl GetConnection,
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
async fn get_blob_impl(
    mut conn: impl GetConnection,
    hash: Hash,
    store: &Store,
) -> anyhow::Result<Stats> {
    trace!("get blob: {}", hash);
    let local_ranges = store.observe(hash).await?.ranges;
    let required_ranges = ChunkRanges::all() - local_ranges;
    if required_ranges.is_empty() {
        // we don't count the time for looking up the bitfield locally
        return Ok(Stats::default());
    }
    let request = GetRequest::new(hash, RangeSpecSeq::from_ranges([required_ranges]));
    let conn = conn.connection().await?;
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
) -> api::FallibleRequestResult<AtEndBlob> {
    let (mut content, size) = header.next().await.map_err(api::Error::other)?;
    let Some(size) = NonZeroU64::new(size) else {
        return if hash == Hash::EMPTY {
            let end = content.drain().await.map_err(api::Error::other)?;
            Ok(end)
        } else {
            Err(api::Error::other("invalid size for hash").into())
        };
    };
    let buffer_size = get_buffer_size(size);
    trace!(%size, %buffer_size, "get blob");
    let (tx, rx) = mpsc::channel(buffer_size);
    let complete = store.import_bao(hash, size, rx);
    let write = async move {
        api::FallibleRequestResult::Ok(loop {
            match content.next().await {
                BlobContentNext::More((next, res)) => {
                    let item = res.map_err(api::Error::other)?;
                    tx.send(item).await.map_err(api::Error::other)?;
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

#[derive(Debug)]
pub struct LazyHashSeq {
    blobs: Blobs,
    hash: Hash,
    current_chunk: Option<HashSeqChunk>,
}

#[derive(Debug)]
pub struct HashSeqChunk {
    /// the offset of the first hash in this chunk
    offset: u64,
    /// the hashes in this chunk
    chunk: HashSeq,
}

impl TryFrom<Leaf> for HashSeqChunk {
    type Error = anyhow::Error;

    fn try_from(leaf: Leaf) -> Result<Self, Self::Error> {
        let offset = leaf.offset;
        let chunk = HashSeq::try_from(leaf.data)?;
        Ok(Self { offset, chunk })
    }
}

impl IntoIterator for HashSeqChunk {
    type Item = Hash;
    type IntoIter = HashSeqIter;

    fn into_iter(self) -> Self::IntoIter {
        self.chunk.into_iter()
    }
}

impl HashSeqChunk {
    fn get(&self, offset: u64) -> Option<Hash> {
        let start = self.offset;
        let end = start + self.chunk.len() as u64;
        if offset >= start && offset < end {
            let o = (offset - start) as usize;
            self.chunk.get(o)
        } else {
            None
        }
    }
}

impl LazyHashSeq {
    pub fn new(blobs: Blobs, hash: Hash) -> Self {
        Self {
            blobs,
            hash,
            current_chunk: None,
        }
    }

    pub async fn get_from_offset(&mut self, offset: u64) -> anyhow::Result<Option<Hash>> {
        if offset == 0 {
            Ok(Some(self.hash))
        } else {
            self.get(offset - 1).await
        }
    }

    pub async fn get(&mut self, child_offset: u64) -> anyhow::Result<Option<Hash>> {
        // check if we have the hash in the current chunk
        if let Some(chunk) = &self.current_chunk {
            if let Some(hash) = chunk.get(child_offset) {
                return Ok(Some(hash));
            }
        }
        // load the chunk covering the offset
        let leaf = self
            .blobs
            .export_chunk(self.hash, child_offset * 32)
            .await?;
        // return the hash if it is in the chunk, otherwise we are behind the end
        let hs = HashSeqChunk::try_from(leaf)?;
        Ok(hs.get(child_offset).map(|hash| {
            self.current_chunk = Some(hs);
            hash
        }))
    }
}

pub async fn execute_request(
    store: &Store,
    conn: Connection,
    request: GetRequest,
) -> anyhow::Result<Stats> {
    let root = request.hash;
    let start = crate::get::fsm::start(conn, request);
    let connected = start.next().await?;
    trace!("Getting header");
    // read the header
    let next_child = match connected.next().await? {
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
    // read the rest, if any
    let at_closing = match next_child {
        Ok(at_start_child) => {
            let mut next_child = Ok(at_start_child);
            // let hash_seq = HashSeq::try_from(store.export_bytes(root).await?)?;
            let mut hash_seq = LazyHashSeq::new(store.blobs().clone(), root);
            loop {
                let at_start_child = match next_child {
                    Ok(at_start_child) => at_start_child,
                    Err(at_closing) => break at_closing,
                };
                let offset = at_start_child.child_offset();
                let Some(hash) = hash_seq.get(offset).await? else {
                    break at_start_child.finish();
                };
                info!("getting child {offset} {}", hash.fmt_short());
                let header = at_start_child.next(hash);
                let end = get_blob_ranges_impl(header, hash, store).await?;
                next_child = match end.next() {
                    EndBlobNext::MoreChildren(at_start_child) => Ok(at_start_child),
                    EndBlobNext::Closing(at_closing) => Err(at_closing),
                }
            }
        }
        Err(at_closing) => at_closing,
    };
    // read the rest, if any
    let stats = at_closing.next().await?;
    trace!(?stats, "get hash seq done");
    Ok(stats)
}

// get a single blob, taking the locally available ranges into account
async fn get_hash_seq_impl(
    mut conn: impl GetConnection,
    root: Hash,
    store: &Store,
) -> anyhow::Result<Stats> {
    trace!("get hash seq: {}", root);
    let local_ranges = store.observe(root).await?.ranges;
    let required_ranges = ChunkRanges::all() - local_ranges;
    let request = GetRequest::new(
        root,
        RangeSpecSeq::from_ranges_infinite([required_ranges, ChunkRanges::all()]),
    );
    let conn = conn.connection().await?;
    let start = crate::get::fsm::start(conn, request);
    let connected = start.next().await?;
    trace!("Getting header");
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
