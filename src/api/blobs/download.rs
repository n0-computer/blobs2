use genawaiter::rc::Gen;
use n0_future::StreamExt;
use quinn::Chunk;
use redb::Range;
use ref_cast::RefCast;

use crate::{api::ApiClient, get::Stats};

#[derive(Debug, RefCast)]
#[repr(transparent)]
pub struct Download {
    client: ApiClient,
}

/// Local info for a blob or hash sequence.
///
/// This can be used to get the amount of missing data, and to construct a
/// request to get the missing data.
#[derive(Debug)]
pub struct LocalInfo {
    /// The hash for which this is the local info
    root: Hash,
    /// The bitfield for the root hash
    bitfield: Bitfield,
    /// Optional - the hash sequence info if this was a request for a hash sequence
    children: Option<HashSeqLocalInfo>,
}

impl LocalInfo {
    /// Content (hash and format) of this local info
    pub fn content(&self) -> HashAndFormat {
        HashAndFormat {
            hash: self.root,
            format: if self.children.is_some() {
                BlobFormat::HashSeq
            } else {
                BlobFormat::Raw
            },
        }
    }

    /// The number of bytes we have locally
    pub fn local_bytes(&self) -> u64 {
        let mut res = self.bitfield.total_bytes();
        if let Some(children) = &self.children {
            for (_, bitfield) in children.bitfields.iter() {
                res += bitfield.total_bytes();
            }
        }
        res
    }

    /// True if the data is complete.
    ///
    /// For a blob, this is true if the blob is complete.
    /// For a hash sequence, this is true if the hash sequence is complete and
    /// all its children are complete.
    pub fn is_complete(&self) -> bool {
        if let Some(children) = self.children.as_ref() {
            // if the bitfield is complete, and we did not get an error, we know that we have a bitfield
            // for every child, so we don't need to check for holes.
            self.bitfield.is_complete() && children.bitfields.values().all(|bf| bf.is_complete())
        } else {
            self.bitfield.is_complete()
        }
    }

    /// A request to get the missing data to complete this blob or hash sequence
    pub fn missing(&self) -> GetRequest {
        let root_missing = ChunkRanges::all() - &self.bitfield.ranges;
        let ranges = if let Some(children) = self.children.as_ref() {
            if children.hash_seq.last().is_some() {
                let start = root_missing;
                let children = iter_without_gaps(&children.hash_seq).map(|(_, hash)| {
                    if let Some(hash) = hash {
                        ChunkRanges::all() - &children.bitfields.get(&hash).unwrap().ranges
                    } else {
                        ChunkRanges::all()
                    }
                });
                let end = if self.bitfield.is_validated() {
                    ChunkRanges::empty()
                } else {
                    ChunkRanges::all()
                };
                let ranges = iter::once(start).chain(children).chain(iter::once(end));
                RangeSpecSeq::from_ranges_infinite(ranges)
            } else {
                RangeSpecSeq::from_ranges_infinite([
                    self.bitfield.ranges.clone(),
                    ChunkRanges::all(),
                ])
            }
        } else {
            RangeSpecSeq::from_ranges([root_missing])
        };
        GetRequest::new(self.root, ranges)
    }
}

#[derive(Debug)]
pub struct HashSeqLocalInfo {
    /// the available part of the hash sequence
    hash_seq: Vec<(u64, Hash)>,
    /// Bitfield for every hash in the hash sequence
    bitfields: BTreeMap<Hash, Bitfield>,
}

fn iter_without_gaps<'a, T: Copy + 'a>(
    iter: impl IntoIterator<Item = &'a (u64, T)> + 'a,
) -> impl Iterator<Item = (u64, Option<T>)> + 'a {
    let mut prev = 0;
    iter.into_iter().flat_map(move |(i, hash)| {
        let start = prev + 1;
        let curr = *i;
        prev = *i;
        (start..curr)
            .map(|i| (i, None))
            .chain(std::iter::once((curr, Some(*hash))))
    })
}

impl Download {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

    fn store(&self) -> &Store {
        Store::ref_from_sender(&self.client)
    }

    /// Get the local info for a given blob or hash sequence, at the present time.
    pub async fn local(&self, content: impl Into<HashAndFormat>) -> anyhow::Result<LocalInfo> {
        let content = content.into();
        let root = content.hash;
        let bitfield = self.store().observe(root).await?;
        let children = if content.format == BlobFormat::HashSeq {
            let bao = self.store().export_bao(root, bitfield.ranges.clone());
            let mut hash_seq = Vec::new();
            let mut stream = bao.hashes_with_index();
            while let Some(item) = stream.next().await {
                hash_seq.push(item?);
            }
            let mut bitfields = BTreeMap::new();
            for (_, hash) in hash_seq.iter() {
                let bitfield = self.store().observe(*hash).await?;
                bitfields.insert(*hash, bitfield);
            }
            Some(HashSeqLocalInfo {
                hash_seq,
                bitfields,
            })
        } else {
            None
        };
        Ok(LocalInfo {
            root,
            bitfield,
            children,
        })
    }

    /// Get a blob or hash sequence from the given connection, taking the locally available
    /// ranges into account.
    ///
    /// This will return the stats of the download.
    pub async fn fetch(
        &self,
        mut conn: impl GetConnection,
        data: impl Into<HashAndFormat>,
        progress: Option<spsc::Sender<u64>>,
    ) -> anyhow::Result<Stats> {
        let content = data.into();
        let missing = self.store().missing(content).await?;
        if missing == RangeSpecSeq::empty() {
            return Ok(Default::default());
        }
        let request = GetRequest::new(content.hash, missing);
        let stats = self
            .execute_request(conn.connection().await?, request, progress)
            .await?;
        // let data = data.into();
        // let stats = match data.format {
        //     BlobFormat::Raw => fetch_blob_impl(conn, data.hash, store, &mut progress).await?,
        //     BlobFormat::HashSeq => fetch_hash_seq_impl(conn, data.hash, store, &mut progress).await?,
        // };
        Ok(stats)
    }

    /// Execute a get request *without* taking the locally available ranges into account.
    pub async fn execute_request(
        &self,
        conn: Connection,
        request: GetRequest,
        progress: Option<spsc::Sender<u64>>,
    ) -> anyhow::Result<Stats> {
        let store = self.store();
        let root = request.hash;
        let start = crate::get::fsm::start(conn, request);
        let connected = start.next().await?;
        let mut progress = DownloadProgress::new(progress);
        trace!("Getting header");
        // read the header
        let next_child = match connected.next().await? {
            ConnectedNext::StartRoot(at_start_root) => {
                let header = at_start_root.next();
                let end = get_blob_ranges_impl(header, root, store, &mut progress).await?;
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
                    trace!("getting child {offset} {}", hash.fmt_short());
                    let header = at_start_child.next(hash);
                    let end = get_blob_ranges_impl(header, hash, store, &mut progress).await?;
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
}

use std::{collections::BTreeMap, future::Future, iter, num::NonZeroU64};

use bao_tree::{io::Leaf, ChunkRanges};
use iroh::{endpoint::Connection, Endpoint, NodeAddr};
use irpc::channel::{spsc, SendError};
use tracing::{info, trace};

use super::Bitfield;
use crate::{
    api::{self, blobs::Blobs, Store},
    get::fsm::{AtBlobHeader, AtEndBlob, BlobContentNext, ConnectedNext, EndBlobNext},
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

impl GetConnection for &Connection {
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

fn get_buffer_size(size: NonZeroU64) -> usize {
    (size.get() / (IROH_BLOCK_SIZE.bytes() as u64) + 2).min(64) as usize
}

pub struct DownloadProgress {
    sender: Option<spsc::Sender<u64>>,
}

impl DownloadProgress {
    pub fn new(sender: Option<spsc::Sender<u64>>) -> Self {
        Self { sender }
    }

    pub async fn send(&mut self, total: u64) -> std::result::Result<(), SendError> {
        if let Some(sender) = self.sender.as_mut() {
            sender.try_send(total).await?;
        }
        Ok(())
    }
}

// get a single blob, taking the locally available ranges into account
async fn fetch_blob_impl(
    mut conn: impl GetConnection,
    hash: Hash,
    store: &Store,
    progress: &mut DownloadProgress,
) -> anyhow::Result<Stats> {
    trace!("get blob: {}", hash);
    let local_ranges = store.observe(hash).await?.ranges;
    let required_ranges = ChunkRanges::all() - local_ranges;
    if required_ranges.is_empty() {
        // we don't count the time for looking up the bitfield locally
        return Ok(Default::default());
    }
    let request = GetRequest::new(hash, RangeSpecSeq::from_ranges([required_ranges]));
    let conn = conn.connection().await?;
    let start = crate::get::fsm::start(conn, request);
    let next = start.next().await?;
    let ConnectedNext::StartRoot(root) = next.next().await? else {
        unreachable!()
    };
    let header = root.next();
    let end = get_blob_ranges_impl(header, hash, store, progress).await?;
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
    progress: &mut DownloadProgress,
) -> api::RequestResult<AtEndBlob> {
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
        api::RequestResult::Ok(loop {
            match content.next().await {
                BlobContentNext::More((next, res)) => {
                    let item = res.map_err(api::Error::other)?;
                    progress
                        .send(next.stats().payload_bytes_read)
                        .await
                        .map_err(api::Error::other)?;
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
    /// the offset of the first hash in this chunk, in bytes
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
    pub fn base(&self) -> u64 {
        self.offset / 32
    }

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

// get a hash seq, taking the locally available ranges into account
async fn fetch_hash_seq_impl(
    mut conn: impl GetConnection,
    root: Hash,
    store: &Store,
    progress: &mut DownloadProgress,
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
            let end = get_blob_ranges_impl(header, root, store, progress).await?;
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
        trace!("getting child {offset} {}", hash.fmt_short());
        let header = at_start_child.next(hash);
        let end = get_blob_ranges_impl(header, hash, store, progress).await?;
        next_child = match end.next() {
            EndBlobNext::MoreChildren(at_start_child) => Ok(at_start_child),
            EndBlobNext::Closing(at_closing) => Err(at_closing),
        }
    };
    let stats = at_closing.next().await?;
    trace!(?stats, "get hash seq done");
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use bao_tree::ChunkRanges;
    use testresult::TestResult;

    use crate::{
        protocol::{GetRequest, RangeSpecSeq},
        store::fs::{tests::INTERESTING_SIZES, FsStore},
        tests::{add_test_hash_seq, add_test_hash_seq_incomplete},
    };

    #[tokio::test]
    async fn test_local_info_raw() -> TestResult<()> {
        let td = tempfile::tempdir()?;
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        let blobs = store.blobs();
        let tt = blobs.add_slice(b"test").temp_tag().await?;
        let hash = *tt.hash();
        let info = blobs.download().local(hash).await?;
        assert_eq!(info.content(), hash.into());
        assert_eq!(info.bitfield.ranges, ChunkRanges::all());
        assert_eq!(info.local_bytes(), 4);
        assert_eq!(info.is_complete(), true);
        assert_eq!(info.missing(), GetRequest::new(hash, RangeSpecSeq::empty()));
        Ok(())
    }

    #[tokio::test]
    async fn test_local_info_hash_seq() -> TestResult<()> {
        let sizes = INTERESTING_SIZES;
        let total_size = sizes.iter().map(|x| *x as u64).sum::<u64>();
        let hash_seq_size = (sizes.len() as u64) * 32;
        let td = tempfile::tempdir()?;
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        {
            let content = add_test_hash_seq_incomplete(&store, &sizes).await?;
            let info = store.download().local(content).await?;
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size);
            assert_eq!(info.is_complete(), false);
            assert_eq!(
                info.missing(),
                GetRequest::new(
                    content.hash,
                    RangeSpecSeq::from_ranges([
                        ChunkRanges::empty(), // we have the hash seq itself
                        ChunkRanges::empty(), // we always have the empty blob
                        ChunkRanges::all(),   // we miss all the remaining blobs (sizes.len() - 1)
                        ChunkRanges::all(),
                        ChunkRanges::all(),
                        ChunkRanges::all(),
                        ChunkRanges::all(),
                        ChunkRanges::all(),
                        ChunkRanges::all(),
                    ])
                )
            );
        }
        {
            let content = add_test_hash_seq(&store, &sizes).await?;
            let info = store.download().local(content).await?;
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), total_size + hash_seq_size);
            assert_eq!(info.is_complete(), true);
            assert_eq!(
                info.missing(),
                GetRequest::new(content.hash, RangeSpecSeq::empty())
            );
        }
        Ok(())
    }
}
