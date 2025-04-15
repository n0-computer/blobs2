//! Download API
//! 
//! The entry point is the [`Download`] struct.
use n0_future::StreamExt;
use ref_cast::RefCast;

use crate::{api::ApiClient, get::Stats};

/// API to compute request and to download from remote nodes.
///
/// Usually you want to first find out what, if any, data you have locally.
/// This can be done using [`Download::local`], which inspects the local store
/// and returns a [`LocalInfo`].
///
/// From this you can compute various values such as the number of locally present
/// bytes. You can also compute a request to get the missing data using [`LocalInfo::missing`].
///
/// Once you have a request, you can execute it using [`Download::execute`].
/// Executing a request will store to the local store, but otherwise does not take
/// the available data into account.
///
/// If you are not interested in the details and just want your data, you can use
/// [`Download::fetch`]. This will internally do the dance described above.
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

impl fmt::Display for LocalInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{} {:?}", self.root, self.bitfield)?;
        if let Some(children) = &self.children {
            for (_, hash) in iter_without_gaps(children.hash_seq.iter()) {
                if let Some(hash) = hash {
                    let bitfield = children
                        .bitfields
                        .get(&hash)
                        .expect("missing bitfield for child");
                    writeln!(f, "  {} {:?}", hash, bitfield)?;
                } else {
                    writeln!(f, "  -")?;
                }
            }
            if !self.bitfield.is_validated() {
                writeln!(f, "  ...")?;
            }
        }
        Ok(())
    }
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

    /// Number of children in this hash sequence
    pub fn children(&self) -> Option<u64> {
        if self.children.is_some() {
            self.bitfield.validated_size().map(|x| x / 32)
        } else {
            Some(0)
        }
    }

    /// All the known sizes for blobs in this hash sequence, without gaps
    pub fn sizes(&self) -> impl Iterator<Item = Option<u64>> + '_ {
        let first = self.bitfield.validated_size();
        let children = self.children.as_ref().into_iter().flat_map(|children| {
            iter_without_gaps(&children.hash_seq).map(|(_, hash)| {
                if let Some(hash) = hash {
                    children.bitfields.get(&hash).unwrap().validated_size()
                } else {
                    None
                }
            })
        });
        iter::once(first).chain(children)
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

    /// A request that when executed will give us complete sizes for all entries of the blob or hash sequence
    pub fn complete_sizes(&self) -> GetRequest {
        let request_last_chunk = ChunkRanges::from(ChunkNum(u64::MAX)..);
        let ranges = if let Some(children) = self.children.as_ref() {
            let root_missing = ChunkRanges::all() - &self.bitfield.ranges;
            let start = root_missing;
            let end = if self.bitfield.is_validated() {
                ChunkRanges::empty()
            } else {
                request_last_chunk.clone()
            };
            let mut hashes = HashSet::new();
            let children = iter_without_gaps(&children.hash_seq).map(|(_, hash)| {
                if let Some(hash) = hash {
                    if hashes.insert(hash) {
                        let bitfield = children.bitfields.get(&hash).unwrap();
                        if bitfield.is_validated() {
                            // we have the last chunk of the blob
                            ChunkRanges::empty()
                        } else {
                            // request the last chunk of the blob
                            request_last_chunk.clone()
                        }
                    } else {
                        // we have already seen this hash, so we don't need to request it again
                        ChunkRanges::empty()
                    }
                } else {
                    request_last_chunk.clone()
                }
            });
            let ranges = iter::once(start).chain(children).chain(iter::once(end));
            RangeSpecSeq::from_ranges_infinite(ranges)
        } else if self.bitfield.is_validated() {
            RangeSpecSeq::empty()
        } else {
            // request the last chunk of the blob
            RangeSpecSeq::from_ranges([request_last_chunk])
        };
        GetRequest::new(self.root, ranges)
    }

    /// A request to get the missing data to complete this blob or hash sequence
    pub fn missing(&self) -> GetRequest {
        let root_missing = ChunkRanges::all() - &self.bitfield.ranges;
        let ranges = if let Some(children) = self.children.as_ref() {
            let mut hashes = HashSet::new();
            let start = root_missing;
            let children = iter_without_gaps(&children.hash_seq).map(|(_, hash)| {
                if let Some(hash) = hash {
                    if hashes.insert(hash) {
                        ChunkRanges::all() - &children.bitfields.get(&hash).unwrap().ranges
                    } else {
                        ChunkRanges::empty()
                    }
                } else {
                    ChunkRanges::all()
                }
            });
            // is_validated means we have the last chunk and thus also the last hash
            let end = if self.bitfield.is_validated() {
                ChunkRanges::empty()
            } else {
                ChunkRanges::all()
            };
            let ranges = iter::once(start).chain(children).chain(iter::once(end));
            RangeSpecSeq::from_ranges_infinite(ranges)
        } else {
            RangeSpecSeq::from_ranges([root_missing])
        };
        GetRequest::new(self.root, ranges)
    }
}

#[derive(Debug)]
struct HashSeqLocalInfo {
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
    /// You can provide a progress channel to get updates on the download progress. This progress
    /// is the aggregated number of downloaded payload bytes in the request.
    ///
    /// This will return the stats of the download.
    pub async fn fetch(
        &self,
        mut conn: impl GetConnection,
        data: impl Into<HashAndFormat>,
        progress: Option<spsc::Sender<u64>>,
    ) -> anyhow::Result<Stats> {
        let content = data.into();
        let local = self.local(content).await?;
        if local.is_complete() {
            return Ok(Default::default());
        }
        let request = local.missing();
        let stats = self
            .execute(conn.connection().await?, request, progress)
            .await?;
        Ok(stats)
    }

    /// Execute a get request *without* taking the locally available ranges into account.
    ///
    /// You can provide a progress channel to get updates on the download progress. This progress
    /// is the aggregated number of downloaded payload bytes in the request.
    ///
    /// This will download the data again even if the data is locally present.
    ///
    /// This will return the stats of the download.
    pub async fn execute(
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
                let hash_seq = HashSeq::try_from(store.get_bytes(root).await?)?;
                // let mut hash_seq = LazyHashSeq::new(store.blobs().clone(), root);
                loop {
                    let at_start_child = match next_child {
                        Ok(at_start_child) => at_start_child,
                        Err(at_closing) => break at_closing,
                    };
                    let offset = at_start_child.child_offset();
                    let Some(hash) = hash_seq.get(offset as usize) else {
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

use std::{
    collections::{BTreeMap, HashSet},
    fmt::{self},
    future::Future,
    iter,
    num::NonZeroU64,
};

use bao_tree::{ChunkNum, ChunkRanges, io::Leaf};
use iroh::{Endpoint, NodeAddr, endpoint::Connection};
use irpc::channel::{SendError, spsc};
use tracing::trace;

use crate::{
    BlobFormat, Hash, HashAndFormat,
    api::{self, Store, blobs::Blobs},
    get::fsm::{AtBlobHeader, AtEndBlob, BlobContentNext, ConnectedNext, EndBlobNext},
    hashseq::{HashSeq, HashSeqIter},
    protocol::{GetRequest, RangeSpecSeq},
    store::IROH_BLOCK_SIZE,
    util::channel::mpsc,
};

use super::blobs::Bitfield;

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

pub(crate) struct Dialer {
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

struct DownloadProgress {
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
pub(crate) struct LazyHashSeq {
    blobs: Blobs,
    hash: Hash,
    current_chunk: Option<HashSeqChunk>,
}

#[derive(Debug)]
pub(crate) struct HashSeqChunk {
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
        Ok(hs.get(child_offset).inspect(|_hash| {
            self.current_chunk = Some(hs);
        }))
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::{ChunkNum, ChunkRanges};
    use testresult::TestResult;

    use crate::{
        protocol::{GetRequest, RangeSpecSeq},
        store::fs::{FsStore, tests::INTERESTING_SIZES},
        tests::{add_test_hash_seq, add_test_hash_seq_incomplete},
    };

    #[tokio::test]
    async fn test_local_info_raw() -> TestResult<()> {
        let td = tempfile::tempdir()?;
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        let blobs = store.blobs();
        let tt = blobs.add_slice(b"test").temp_tag().await?;
        let hash = *tt.hash();
        let info = store.download().local(hash).await?;
        assert_eq!(info.content(), hash.into());
        assert_eq!(info.bitfield.ranges, ChunkRanges::all());
        assert_eq!(info.local_bytes(), 4);
        assert!(info.is_complete());
        assert_eq!(info.missing(), GetRequest::new(hash, RangeSpecSeq::empty()));
        Ok(())
    }

    #[tokio::test]
    async fn test_local_info_hash_seq_large() -> TestResult<()> {
        let sizes = (0..1024 + 5).collect::<Vec<_>>();
        let relevant_sizes = sizes[32 * 16..32 * 32]
            .iter()
            .map(|x| *x as u64)
            .sum::<u64>();
        let td = tempfile::tempdir()?;
        let hash_seq_ranges = ChunkRanges::from(ChunkNum(16)..ChunkNum(32));
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        {
            // only add the hash seq itself, and only the first chunk of the children
            let present = |i| {
                if i == 0 {
                    hash_seq_ranges.clone()
                } else {
                    ChunkRanges::from(..ChunkNum(1))
                }
            };
            let content = add_test_hash_seq_incomplete(&store, sizes, present).await?;
            let info = store.download().local(content).await?;
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, hash_seq_ranges);
            assert!(!info.is_complete());
            assert_eq!(info.local_bytes(), relevant_sizes + 16 * 1024);
        }

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
            // only add the hash seq itself, none of the children
            let present = |i| {
                if i == 0 {
                    ChunkRanges::all()
                } else {
                    ChunkRanges::empty()
                }
            };
            let content = add_test_hash_seq_incomplete(&store, sizes, present).await?;
            let info = store.download().local(content).await?;
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size);
            assert!(!info.is_complete());
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
            store.tags().delete_all().await?;
        }
        {
            // only add the hash seq itself, and only the first chunk of the children
            let present = |i| {
                if i == 0 {
                    ChunkRanges::all()
                } else {
                    ChunkRanges::from(..ChunkNum(1))
                }
            };
            let content = add_test_hash_seq_incomplete(&store, sizes, present).await?;
            let info = store.download().local(content).await?;
            let first_chunk_size = sizes.into_iter().map(|x| x.min(1024) as u64).sum::<u64>();
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size + first_chunk_size);
            assert!(!info.is_complete());
            assert_eq!(
                info.missing(),
                GetRequest::new(
                    content.hash,
                    RangeSpecSeq::from_ranges([
                        ChunkRanges::empty(), // we have the hash seq itself
                        ChunkRanges::empty(), // we always have the empty blob
                        ChunkRanges::empty(), // size=1
                        ChunkRanges::empty(), // size=1024
                        ChunkRanges::from(ChunkNum(1)..),
                        ChunkRanges::from(ChunkNum(1)..),
                        ChunkRanges::from(ChunkNum(1)..),
                        ChunkRanges::from(ChunkNum(1)..),
                        ChunkRanges::from(ChunkNum(1)..),
                    ])
                )
            );
        }
        {
            let content = add_test_hash_seq(&store, sizes).await?;
            let info = store.download().local(content).await?;
            assert_eq!(info.content(), content);
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), total_size + hash_seq_size);
            assert!(info.is_complete());
            assert_eq!(
                info.missing(),
                GetRequest::new(content.hash, RangeSpecSeq::empty())
            );
        }
        Ok(())
    }
}
