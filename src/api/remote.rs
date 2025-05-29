//! Download API
//!
//! The entry point is the [`Download`] struct.
use genawaiter::sync::{Co, Gen};
use n0_future::{Stream, StreamExt, TryFutureExt, io};
use quinn::SendStream;
use ref_cast::RefCast;

use super::blobs::Bitfield;
use crate::{
    api::ApiClient,
    get::{GetError, GetResult, Stats, fsm::DecodeError},
    protocol::{GetManyRequest, ObserveItem, ObserveRequest, PushRequest, Request},
    provider::{EventSender, ProgressWriter, StreamContext},
    util::sink::{Drain, Sink},
};

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
pub struct Remote {
    client: ApiClient,
}

/// Local info for a blob or hash sequence.
///
/// This can be used to get the amount of missing data, and to construct a
/// request to get the missing data.
#[derive(Debug)]
pub struct LocalInfo {
    /// The hash for which this is the local info
    request: Arc<GetRequest>,
    /// The bitfield for the root hash
    bitfield: Bitfield,
    /// Optional - the hash sequence info if this was a request for a hash sequence
    children: Option<NonRawLocalInfo>,
}

impl LocalInfo {
    /// The number of bytes we have locally
    pub fn local_bytes(&self) -> u64 {
        let Some(root_requested) = self.requested_root_ranges() else {
            // empty request requests 0 bytes
            return 0;
        };
        let mut local = self.bitfield.clone();
        local.ranges.intersection_with(root_requested);
        let mut res = local.total_bytes();
        if let Some(children) = &self.children {
            let Some(max_local_index) = children.hash_seq.keys().next_back() else {
                // no children
                return res;
            };
            for (offset, ranges) in self.request.ranges.iter_non_empty_infinite() {
                if offset == 0 {
                    // skip the root hash
                    continue;
                }
                let child = offset - 1;
                if child > *max_local_index {
                    // we are done
                    break;
                }
                let Some(hash) = children.hash_seq.get(&child) else {
                    continue;
                };
                let bitfield = &children.bitfields[hash];
                let mut local = bitfield.clone();
                local.ranges.intersection_with(ranges);
                res += local.total_bytes();
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

    /// The requested root ranges.
    ///
    /// This will return None if the request is empty, and an empty CHunkRanges
    /// if no ranges were requested for the root hash.
    fn requested_root_ranges(&self) -> Option<&ChunkRanges> {
        self.request.ranges.iter().next()
    }

    /// True if the data is complete.
    ///
    /// For a blob, this is true if the blob is complete.
    /// For a hash sequence, this is true if the hash sequence is complete and
    /// all its children are complete.
    pub fn is_complete(&self) -> bool {
        let Some(root_requested) = self.requested_root_ranges() else {
            // empty request is complete
            return true;
        };
        if !self.bitfield.ranges.is_superset(root_requested) {
            return false;
        }
        if let Some(children) = self.children.as_ref() {
            let mut iter = self.request.ranges.iter_non_empty_infinite();
            let max_child = self.bitfield.validated_size().map(|x| x / 32);
            loop {
                let Some((offset, range)) = iter.next() else {
                    break;
                };
                if offset == 0 {
                    // skip the root hash
                    continue;
                }
                let child = offset - 1;
                if let Some(hash) = children.hash_seq.get(&child) {
                    let bitfield = &children.bitfields[hash];
                    if !bitfield.ranges.is_superset(range) {
                        // we don't have the requested ranges
                        return false;
                    }
                } else {
                    if let Some(max_child) = max_child {
                        if child >= max_child {
                            // reading after the end of the request
                            return true;
                        }
                    }
                    return false;
                }
            }
        }
        true
    }

    /// A request to get the missing data to complete this request
    pub fn missing(&self) -> GetRequest {
        let Some(root_requested) = self.requested_root_ranges() else {
            // empty request is complete
            return GetRequest::new(self.request.hash, ChunkRangesSeq::empty());
        };
        let mut builder = GetRequest::builder().root(root_requested - &self.bitfield.ranges);

        let Some(children) = self.children.as_ref() else {
            return builder.build(self.request.hash);
        };
        let mut iter = self.request.ranges.iter_non_empty_infinite();
        let max_local = children
            .hash_seq
            .keys()
            .next_back()
            .map(|x| *x + 1)
            .unwrap_or_default();
        let max_offset = self.bitfield.validated_size().map(|x| x / 32);
        loop {
            let Some((offset, requested)) = iter.next() else {
                break;
            };
            if offset == 0 {
                // skip the root hash
                continue;
            }
            let child = offset - 1;
            let missing = match children.hash_seq.get(&child) {
                Some(hash) => requested.difference(&children.bitfields[hash].ranges),
                None => requested.clone(),
            };
            builder = builder.child(child, missing);
            if offset >= max_local {
                // we can't do anything clever anymore
                break;
            }
        }
        loop {
            let Some((offset, requested)) = iter.next() else {
                return builder.build(self.request.hash);
            };
            if offset == 0 {
                // skip the root hash
                continue;
            }
            let child = offset - 1;
            if let Some(max_offset) = &max_offset {
                if child >= *max_offset {
                    return builder.build(self.request.hash);
                }
                builder = builder.child(child, requested.clone());
            } else {
                builder = builder.child(child, requested.clone());
                if iter.is_at_end() {
                    if iter.next().is_none() {
                        return builder.build(self.request.hash);
                    } else {
                        return builder.build_open(self.request.hash);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct NonRawLocalInfo {
    /// the available and relevant part of the hash sequence
    hash_seq: BTreeMap<u64, Hash>,
    /// For each hash relevant to the request, the local bitfield and the ranges
    /// that were requested.
    bitfields: BTreeMap<Hash, Bitfield>,
}

// fn iter_without_gaps<'a, T: Copy + 'a>(
//     iter: impl IntoIterator<Item = &'a (u64, T)> + 'a,
// ) -> impl Iterator<Item = (u64, Option<T>)> + 'a {
//     let mut prev = 0;
//     iter.into_iter().flat_map(move |(i, hash)| {
//         let start = prev + 1;
//         let curr = *i;
//         prev = *i;
//         (start..curr)
//             .map(|i| (i, None))
//             .chain(std::iter::once((curr, Some(*hash))))
//     })
// }

impl Remote {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

    fn store(&self) -> &Store {
        Store::ref_from_sender(&self.client)
    }

    pub async fn local_for_request(
        &self,
        request: impl Into<Arc<GetRequest>>,
    ) -> anyhow::Result<LocalInfo> {
        let request = request.into();
        let root = request.hash;
        let bitfield = self.store().observe(root).await?;
        let children = if !request.ranges.is_blob() {
            let bao = self.store().export_bao(root, bitfield.ranges.clone());
            let mut by_index = BTreeMap::new();
            let mut stream = bao.hashes_with_index();
            while let Some(item) = stream.next().await {
                let (index, hash) = item?;
                by_index.insert(index, hash);
            }
            let mut bitfields = BTreeMap::new();
            let mut hash_seq = BTreeMap::new();
            let max = by_index.last_key_value().map(|(k, _)| *k + 1).unwrap_or(0);
            for (index, _) in request.ranges.iter_non_empty_infinite() {
                if index == 0 {
                    // skip the root hash
                    continue;
                }
                let child = index - 1;
                if child > max {
                    // we are done
                    break;
                }
                let Some(hash) = by_index.get(&child) else {
                    // we don't have the hash, so we can't store the bitfield
                    continue;
                };
                let bitfield = self.store().observe(*hash).await?;
                bitfields.insert(*hash, bitfield);
                hash_seq.insert(child, *hash);
            }
            Some(NonRawLocalInfo {
                hash_seq,
                bitfields,
            })
        } else {
            None
        };
        Ok(LocalInfo {
            request: request.clone(),
            bitfield,
            children,
        })
    }

    /// Get the local info for a given blob or hash sequence, at the present time.
    pub async fn local(&self, content: impl Into<HashAndFormat>) -> anyhow::Result<LocalInfo> {
        let request = GetRequest::from(content.into());
        self.local_for_request(request).await
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
        content: impl Into<HashAndFormat>,
        progress: impl Sink<u64, Error = io::Error>,
    ) -> anyhow::Result<Stats> {
        let content = content.into();
        let local = self.local(content).await?;
        if local.is_complete() {
            return Ok(Default::default());
        }
        let request = local.missing();
        let conn = conn.connection().await?;
        let stats = self.execute_with_opts(conn, request, progress).await?;
        Ok(stats)
    }

    pub fn observe(
        &self,
        conn: Connection,
        request: ObserveRequest,
    ) -> impl Stream<Item = io::Result<Bitfield>> + 'static {
        Gen::new(|co| async move {
            if let Err(cause) = Self::observe_impl(conn, request, &co).await {
                co.yield_(Err(cause)).await
            }
        })
    }

    async fn observe_impl(
        conn: Connection,
        request: ObserveRequest,
        co: &Co<io::Result<Bitfield>>,
    ) -> io::Result<()> {
        let hash = request.hash;
        debug!(%hash, "observing");
        let (mut send, mut recv) = conn.open_bi().await?;
        // write the request. Unlike for reading, we can just serialize it sync using postcard.
        write_observe_request(request, &mut send).await?;
        send.finish()?;
        loop {
            let msg = ObserveItem::read_async(&mut recv).await?;
            co.yield_(Ok(Bitfield::from(&msg))).await;
        }
    }

    /// Push the given blob or hash sequence to a remote node.
    ///
    /// Note that many nodes will reject push requests. Also, this is an experimental feature for now.
    pub async fn execute_push(
        &self,
        conn: Connection,
        request: PushRequest,
        _progress: Option<spsc::Sender<u64>>,
    ) -> anyhow::Result<Stats> {
        let hash = request.hash;
        debug!(%hash, "pushing");
        let (send, mut recv) = conn.open_bi().await?;
        let mut writer = ProgressWriter {
            inner: send,
            context: StreamContext {
                connection_id: conn.stable_id() as u64,
                request_id: recv.id().into(),
                payload_bytes_sent: 0,
                other_bytes_sent: 0,
                bytes_read: 0,
                progress: EventSender::new(None),
            },
        };
        // we are not going to need this!
        recv.stop(0u32.into())?;
        // write the request. Unlike for reading, we can just serialize it sync using postcard.
        let request = write_push_request(request, &mut writer).await?;
        let mut request_ranges = request.ranges.iter_infinite();
        let root = request.hash;
        let root_ranges = request_ranges.next().expect("infinite iterator");
        if !root_ranges.is_empty() {
            self.store()
                .export_bao(root, root_ranges.clone())
                .write_quinn_with_progress(&mut writer, &root, 0)
                .await?;
        }
        if request.ranges.is_blob() {
            // we are done
            writer.inner.finish()?;
            return Ok(Default::default());
        }
        let hash_seq = self.store().get_bytes(root).await?;
        let hash_seq = HashSeq::try_from(hash_seq)?;
        for (child, (child_hash, child_ranges)) in
            hash_seq.into_iter().zip(request_ranges).enumerate()
        {
            if !child_ranges.is_empty() {
                self.store()
                    .export_bao(child_hash, child_ranges.clone())
                    .write_quinn_with_progress(&mut writer, &child_hash, (child + 1) as u64)
                    .await?;
            }
        }
        writer.inner.finish()?;
        Ok(Default::default())
    }

    pub async fn execute(&self, conn: Connection, request: GetRequest) -> GetResult<Stats> {
        self.execute_with_opts(conn, request, Drain).await
    }

    /// Execute a get request *without* taking the locally available ranges into account.
    ///
    /// You can provide a progress channel to get updates on the download progress. This progress
    /// is the aggregated number of downloaded payload bytes in the request.
    ///
    /// This will download the data again even if the data is locally present.
    ///
    /// This will return the stats of the download.
    pub async fn execute_with_opts(
        &self,
        conn: Connection,
        request: GetRequest,
        mut progress: impl Sink<u64, Error = io::Error>,
    ) -> GetResult<Stats> {
        let store = self.store();
        let root = request.hash;
        let start = crate::get::fsm::start(conn, request, Default::default());
        let connected = start.next().await?;
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
                let hash_seq = HashSeq::try_from(
                    store
                        .get_bytes(root)
                        .await
                        .map_err(|e| GetError::LocalFailure(e.into()))?,
                )
                .map_err(GetError::BadRequest)?;
                // let mut hash_seq = LazyHashSeq::new(store.blobs().clone(), root);
                loop {
                    let at_start_child = match next_child {
                        Ok(at_start_child) => at_start_child,
                        Err(at_closing) => break at_closing,
                    };
                    let offset = at_start_child.offset() - 1;
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

    /// Execute a get request *without* taking the locally available ranges into account.
    ///
    /// You can provide a progress channel to get updates on the download progress. This progress
    /// is the aggregated number of downloaded payload bytes in the request.
    ///
    /// This will download the data again even if the data is locally present.
    ///
    /// This will return the stats of the download.
    pub async fn execute_get_many(
        &self,
        conn: Connection,
        request: GetManyRequest,
        mut progress: impl Sink<u64, Error = io::Error>,
    ) -> GetResult<Stats> {
        let store = self.store();
        let hash_seq = request.hashes.iter().copied().collect::<HashSeq>();
        let next_child = crate::get::fsm::start_get_many(conn, request, Default::default()).await?;
        // read all children.
        let at_closing = match next_child {
            Ok(at_start_child) => {
                let mut next_child = Ok(at_start_child);
                loop {
                    let at_start_child = match next_child {
                        Ok(at_start_child) => at_start_child,
                        Err(at_closing) => break at_closing,
                    };
                    let offset = at_start_child.offset();
                    println!("offset {offset}");
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

/// Failures for a get operation
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    /// Network or IO operation failed.
    #[error("Unable to open bidi stream")]
    Connection(#[from] iroh::endpoint::ConnectionError),
    #[error("Unable to read from the remote")]
    Read(#[from] iroh::endpoint::ReadError),
    #[error("Error sending the request")]
    Send(#[from] crate::get::fsm::ConnectedNextError),
    #[error("Unable to read size")]
    Size(#[from] crate::get::fsm::AtBlobHeaderNextError),
    #[error("Error while decoding the data")]
    Decode(#[from] crate::get::fsm::DecodeError),
    #[error("Internal error while reading the hash sequence")]
    ExportBao(#[from] api::ExportBaoError),
    #[error("Hash sequence has an invalid length")]
    InvalidHashSeq(#[source] anyhow::Error),
    #[error("Internal error importing the data")]
    ImportBao(#[source] crate::api::RequestError),
    #[error("Error sending download progress - receiver closed")]
    SendDownloadProgress(#[from] irpc::channel::SendError),
    #[error("Internal error importing the data")]
    MpscSend(#[from] tokio::sync::mpsc::error::SendError<BaoContentItem>),
}

use std::{collections::BTreeMap, future::Future, num::NonZeroU64, sync::Arc};

use bao_tree::{
    ChunkNum, ChunkRanges,
    io::{BaoContentItem, Leaf},
};
use iroh::{Endpoint, NodeAddr, endpoint::Connection};
use irpc::channel::spsc;
use tracing::{debug, trace};

use crate::{
    Hash, HashAndFormat,
    api::{self, Store, blobs::Blobs},
    get::fsm::{AtBlobHeader, AtEndBlob, BlobContentNext, ConnectedNext, EndBlobNext},
    hashseq::{HashSeq, HashSeqIter},
    protocol::{ChunkRangesSeq, GetRequest},
    store::IROH_BLOCK_SIZE,
    util::channel::mpsc,
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

#[allow(dead_code)]
pub(crate) struct Dialer {
    endpoint: Endpoint,
    addr: NodeAddr,
    conn: Option<Connection>,
}

impl Dialer {
    #[allow(dead_code)]
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

async fn get_blob_ranges_impl(
    header: AtBlobHeader,
    hash: Hash,
    store: &Store,
    mut progress: impl Sink<u64, Error = io::Error>,
) -> GetResult<AtEndBlob> {
    let (mut content, size) = header.next().await?;
    let Some(size) = NonZeroU64::new(size) else {
        return if hash == Hash::EMPTY {
            let end = content.drain().await?;
            Ok(end)
        } else {
            Err(DecodeError::LeafHashMismatch(ChunkNum(0)).into())
        };
    };
    let buffer_size = get_buffer_size(size);
    trace!(%size, %buffer_size, "get blob");
    let (tx, rx) = mpsc::channel(buffer_size);
    let complete = store
        .import_bao(hash, size, rx)
        .into_future()
        .map_err(|e| GetError::LocalFailure(e.into()));
    let write = async move {
        GetResult::Ok(loop {
            match content.next().await {
                BlobContentNext::More((next, res)) => {
                    let item = res?;
                    progress
                        .send(next.stats().payload_bytes_read)
                        .await
                        .map_err(|e| GetError::LocalFailure(e.into()))?;
                    tx.send(item.into()).await?;
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

    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn new(blobs: Blobs, hash: Hash) -> Self {
        Self {
            blobs,
            hash,
            current_chunk: None,
        }
    }

    #[allow(dead_code)]
    pub async fn get_from_offset(&mut self, offset: u64) -> anyhow::Result<Option<Hash>> {
        if offset == 0 {
            Ok(Some(self.hash))
        } else {
            self.get(offset - 1).await
        }
    }

    #[allow(dead_code)]
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

async fn write_push_request(
    requst: PushRequest,
    stream: &mut ProgressWriter,
) -> anyhow::Result<PushRequest> {
    let request = Request::Push(requst);
    let request_bytes = postcard::to_allocvec(&request)?;
    stream.inner.write_all(&request_bytes).await?;
    stream.context.payload_bytes_sent += request_bytes.len() as u64;
    let Request::Push(request) = request else {
        unreachable!()
    };
    Ok(request)
}

async fn write_observe_request(requst: ObserveRequest, stream: &mut SendStream) -> io::Result<()> {
    let request = Request::Observe(requst);
    let request_bytes = postcard::to_allocvec(&request)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    stream.write_all(&request_bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bao_tree::{ChunkNum, ChunkRanges};
    use testresult::TestResult;

    use crate::{
        protocol::{ChunkRangesSeq, GetRequest},
        store::fs::{FsStore, tests::INTERESTING_SIZES},
        tests::{add_test_hash_seq, add_test_hash_seq_incomplete},
        util::ChunkRangesExt,
    };

    #[tokio::test]
    async fn test_local_info_raw() -> TestResult<()> {
        let td = tempfile::tempdir()?;
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        let blobs = store.blobs();
        let tt = blobs.add_slice(b"test").temp_tag().await?;
        let hash = *tt.hash();
        let info = store.remote().local(hash).await?;
        assert_eq!(info.bitfield.ranges, ChunkRanges::all());
        assert_eq!(info.local_bytes(), 4);
        assert!(info.is_complete());
        assert_eq!(
            info.missing(),
            GetRequest::new(hash, ChunkRangesSeq::empty())
        );
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
        let hash_seq_ranges = ChunkRanges::chunks(16..32);
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
            let info = store.remote().local(content).await?;
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
            let info = store.remote().local(content).await?;
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size);
            assert!(!info.is_complete());
            assert_eq!(
                info.missing(),
                GetRequest::new(
                    content.hash,
                    ChunkRangesSeq::from_ranges([
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
            let info = store.remote().local(content).await?;
            let first_chunk_size = sizes.into_iter().map(|x| x.min(1024) as u64).sum::<u64>();
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size + first_chunk_size);
            assert!(!info.is_complete());
            assert_eq!(
                info.missing(),
                GetRequest::new(
                    content.hash,
                    ChunkRangesSeq::from_ranges([
                        ChunkRanges::empty(), // we have the hash seq itself
                        ChunkRanges::empty(), // we always have the empty blob
                        ChunkRanges::empty(), // size=1
                        ChunkRanges::empty(), // size=1024
                        ChunkRanges::chunks(1..),
                        ChunkRanges::chunks(1..),
                        ChunkRanges::chunks(1..),
                        ChunkRanges::chunks(1..),
                        ChunkRanges::chunks(1..),
                    ])
                )
            );
        }
        {
            let content = add_test_hash_seq(&store, sizes).await?;
            let info = store.remote().local(content).await?;
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), total_size + hash_seq_size);
            assert!(info.is_complete());
            assert_eq!(
                info.missing(),
                GetRequest::new(content.hash, ChunkRangesSeq::empty())
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_local_info_complex_request() -> TestResult<()> {
        let sizes = INTERESTING_SIZES;
        let hash_seq_size = (sizes.len() as u64) * 32;
        let td = tempfile::tempdir()?;
        let store = FsStore::load(td.path().join("blobs.db")).await?;
        // only add the hash seq itself, and only the first chunk of the children
        let present = |i| {
            if i == 0 {
                ChunkRanges::all()
            } else {
                ChunkRanges::chunks(..2)
            }
        };
        let content = add_test_hash_seq_incomplete(&store, sizes, present).await?;
        {
            let request: GetRequest = GetRequest::builder()
                .root(ChunkRanges::all())
                .build(content.hash);
            let info = store.remote().local_for_request(request).await?;
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size);
            assert!(info.is_complete());
        }
        {
            let request: GetRequest = GetRequest::builder()
                .root(ChunkRanges::all())
                .next(ChunkRanges::all())
                .build(content.hash);
            let info = store.remote().local_for_request(request).await?;
            let expected_child_sizes = sizes
                .into_iter()
                .take(1)
                .map(|x| 1024.min(x as u64))
                .sum::<u64>();
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size + expected_child_sizes);
            assert!(info.is_complete());
        }
        {
            let request: GetRequest = GetRequest::builder()
                .root(ChunkRanges::all())
                .next(ChunkRanges::all())
                .next(ChunkRanges::all())
                .build(content.hash);
            let info = store.remote().local_for_request(request).await?;
            let expected_child_sizes = sizes
                .into_iter()
                .take(2)
                .map(|x| 1024.min(x as u64))
                .sum::<u64>();
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size + expected_child_sizes);
            assert!(info.is_complete());
        }
        {
            let request: GetRequest = GetRequest::builder()
                .root(ChunkRanges::all())
                .next(ChunkRanges::chunk(0))
                .build_open(content.hash);
            let info = store.remote().local_for_request(request).await?;
            let expected_child_sizes = sizes.into_iter().map(|x| 1024.min(x as u64)).sum::<u64>();
            assert_eq!(info.bitfield.ranges, ChunkRanges::all());
            assert_eq!(info.local_bytes(), hash_seq_size + expected_child_sizes);
            assert!(info.is_complete());
        }
        Ok(())
    }
}
