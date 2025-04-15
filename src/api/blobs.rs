//! Blobs API
//!
//! This API is for local interactions with the blob store, such as importing
//! and exporting blobs, observing the bitfield of a blob, and deleting blobs.
//!
//! The main entry point is the [`Blobs`] struct.
use std::{
    future::{Future, IntoFuture},
    io,
    num::NonZeroU64,
    path::{Path, PathBuf},
    pin::Pin,
};
use bao_tree::{
    BaoTree, ChunkNum, ChunkRanges,
    io::{
        BaoContentItem, Leaf,
        fsm::{ResponseDecoder, ResponseDecoderNext},
    },
};
use bytes::Bytes;
use download::{Download, HashSeqChunk};
use genawaiter::sync::Gen;
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use irpc::{
    Request,
    channel::{oneshot, spsc},
};
use n0_future::{Stream, StreamExt};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tracing::trace;

use super::{
    proto::{
        BatchResponse, BlobStatusRequest, ClearProtectedRequest, CreateTempTagRequest, ExportBaoRequest, ImportBaoRequest, ImportByteStreamRequest, ImportBytesRequest, ImportPathRequest, ListRequest, Scope
    }, tags::TagInfo, ApiClient, RequestResult, Tags
};
use crate::{
    BlobFormat, Hash, HashAndFormat,
    api::proto::BatchRequest,
    provider::ProgressWriter,
    store::{IROH_BLOCK_SIZE, util::observer::Aggregator},
    util::temp_tag::TempTag,
};
pub use bao_tree::io::mixed::EncodedItem;
pub mod download;
use ref_cast::RefCast;

// Public reexports from the proto module.
//
// Due to the fact that the proto module is hidden from docs by default,
// these will appear in the docs as if they were declared here.
pub use super::proto::{
    BlobDeleteRequest as DeleteOptions, ExportBaoRequest as ExportBaoOptions,
    ObserveRequest as ObserveOptions, ExportPathRequest as ExportOptions,
    ImportBaoRequest as ImportBaoOptions,
    ImportMode, ExportMode, ImportProgress, ExportProgress,
    BlobStatus,
    Bitfield,
};

#[derive(Debug)]
pub struct AddBytesOptions {
    pub data: Bytes,
    pub format: BlobFormat,
}

impl<T: Into<Bytes>> From<(T, BlobFormat)> for AddBytesOptions {
    fn from(item: (T, BlobFormat)) -> Self {
        let (data, format) = item;
        Self {
            data: data.into(),
            format,
        }
    }
}

#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Blobs {
    client: ApiClient,
}

impl Blobs {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

    pub fn download(&self) -> &Download {
        Download::ref_from_sender(&self.client)
    }

    pub async fn batch(&self) -> super::RpcResult<Batch<'_>> {
        let msg = BatchRequest;
        trace!("{msg:?}");
        let (rx, tx) = match self.client.request().await? {
            Request::Local(c) => {
                let (tx, rx) = oneshot::channel();
                let (out_tx, out_rx) = spsc::channel(32);
                c.send((msg, tx, out_rx)).await?;
                (rx, out_tx)
            }
            Request::Remote(r) => {
                let (tx, rx) = r.write(msg).await?;
                (rx.into(), tx.into())
            }
        };
        let scope = rx.await?;

        Ok(Batch {
            scope,
            blobs: self,
            _tx: tx,
        })
    }

    pub async fn delete_with_opts(&self, options: DeleteOptions) -> RequestResult<()> {
        trace!("{options:?}");
        self.client.rpc(options).await??;
        Ok(())
    }

    pub async fn delete(
        &self,
        hashes: impl IntoIterator<Item = impl Into<Hash>>,
    ) -> RequestResult<()> {
        self.delete_with_opts(DeleteOptions {
            hashes: hashes.into_iter().map(Into::into).collect(),
            force: false,
        })
        .await
    }

    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> ImportResult {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    pub fn add_bytes(&self, data: impl Into<bytes::Bytes>) -> ImportResult {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> ImportResult {
        let options = options.into();
        let request = ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(request)
    }

    fn add_bytes_impl(&self, options: ImportBytesRequest) -> ImportResult {
        trace!("{options:?}");
        let request = self.client.request();
        ImportResult::new(self, async move {
            let rx = match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(options).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> ImportResult {
        let options = options.into();
        self.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: Scope::GLOBAL,
        })
    }

    fn add_path_with_opts_impl(&self, options: ImportPathRequest) -> ImportResult {
        trace!("{:?}", options);
        let request = self.client.request();
        ImportResult::new(self, async move {
            Ok(match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn add_path(&self, path: impl AsRef<Path>) -> ImportResult {
        self.add_path_with_opts(AddPathOptions {
            path: path.as_ref().to_owned(),
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
        })
    }

    pub async fn add_stream(
        &self,
        data: impl Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    ) -> ImportResult {
        self.add_stream_impl(Box::pin(data)).await
    }

    async fn add_stream_impl(
        &self,
        data: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>,
    ) -> ImportResult {
        let data = data
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let inner = ImportByteStreamRequest {
            data,
            format: crate::BlobFormat::Raw,
            scope: Scope::default(),
        };
        let request = self.client.request();
        ImportResult::new(self, async move {
            let rx = match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((inner, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(inner).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn export_bao_with_opts(&self, options: ExportBaoOptions) -> ExportBaoResult {
        trace!("{options:?}");
        let request = self.client.request();
        ExportBaoResult::new(async move {
            Ok(match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn export_bao(&self, hash: impl Into<Hash>, ranges: impl Into<ChunkRanges>) -> ExportBaoResult {
        self.export_bao_with_opts(ExportBaoRequest {
            hash: hash.into(),
            ranges: ranges.into(),
        })
    }

    /// Export a single chunk from the given hash, at the given offset.
    pub async fn export_chunk(
        &self,
        hash: impl Into<Hash>,
        offset: u64,
    ) -> super::ExportBaoResult<Leaf> {
        let base = ChunkNum::full_chunks(offset);
        let ranges = ChunkRanges::from(base..base + 1);
        let mut stream = self.export_bao(hash, ranges).stream();
        while let Some(item) = stream.next().await {
            match item {
                EncodedItem::Leaf(leaf) => return Ok(leaf),
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Err(io::Error::other("unexpected end of stream").into())
    }

    /// Get the entire blob into a Bytes
    ///
    /// This will run out of memory when called for very large blobs, so be careful!
    pub async fn get_bytes(&self, hash: impl Into<Hash>) -> super::ExportBaoResult<Bytes> {
        self.export_bao(hash.into(), ChunkRanges::all())
            .data_to_bytes()
            .await
    }

    /// Observe the bitfield of the given hash.
    pub fn observe(&self, hash: impl Into<Hash>) -> ObserveResult {
        self.observe_with_opts(ObserveOptions { hash: hash.into() })
    }

    pub fn observe_with_opts(&self, options: ObserveOptions) -> ObserveResult {
        trace!("{:?}", options);
        if options.hash == Hash::EMPTY {
            return ObserveResult::new(async move {
                let (mut tx, rx) = spsc::channel(1);
                tx.send(Bitfield::complete(0)).await.ok();
                Ok(rx)
            });
        }
        let (tx, rx) = spsc::channel(32);
        let request = self.client.request();
        ObserveResult::new(async move {
            let rx = match request.await? {
                Request::Local(c) => {
                    c.send((options, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(options).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn export_with_opts(&self, options: ExportOptions) -> ExportResult {
        trace!("{:?}", options);
        let request = self.client.request();
        ExportResult::new(async move {
            Ok(match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn export(&self, hash: impl Into<Hash>, target: impl AsRef<Path>) -> ExportResult {
        let options = ExportOptions {
            hash: hash.into(),
            mode: ExportMode::Copy,
            target: target.as_ref().to_owned(),
        };
        self.export_with_opts(options)
    }

    #[cfg_attr(not(feature = "proto-docs"), doc(hidden))]
    pub fn import_bao_with_opts(
        &self,
        mut data: spsc::Receiver<BaoContentItem>,
        options: ImportBaoOptions,
    ) -> ImportBaoResult {
        trace!("{:?}", options);
        let request = self.client.request();
        ImportBaoResult::new(async move {
            let rx = match request.await? {
                Request::Local(c) => {
                    let (tx, rx) = oneshot::channel();
                    c.send((options, tx, data)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (tx, rx) = r.write(options).await?;
                    let mut tx: spsc::Sender<_> = tx.into();
                    while let Some(item) = data.recv().await? {
                        tx.send(item).await?;
                    }
                    rx.into()
                }
            };
            rx.await??;
            Ok(())
        })
    }

    #[cfg_attr(not(feature = "proto-docs"), doc(hidden))]
    async fn import_bao_reader<R: AsyncStreamReader>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> RequestResult<R> {
        let (mut tx, rx) = spsc::channel(32);
        let size = u64::from_le_bytes(reader.read::<8>().await.map_err(super::Error::other)?);
        let Some(size) = NonZeroU64::new(size) else {
            return if hash == Hash::EMPTY {
                Ok(reader)
            } else {
                Err(super::Error::other("invalid size for hash").into())
            };
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash.into(), ranges, tree, reader);
        let inner = ImportBaoRequest { hash, size };
        let fut = self.import_bao_with_opts(rx, inner);
        let driver = async move {
            let reader = loop {
                match decoder.next().await {
                    ResponseDecoderNext::More((rest, item)) => {
                        tx.send(item?).await?;
                        decoder = rest;
                    }
                    ResponseDecoderNext::Done(reader) => break reader,
                };
            };
            drop(tx);
            io::Result::Ok(reader)
        };
        let (reader, res) = tokio::join!(driver, fut);
        res?;
        Ok(reader?)
    }

    /// Import BaoContentItems from a stream.
    ///
    /// The store assumes that these are already verified and in the correct order.
    #[cfg_attr(not(feature = "proto-docs"), doc(hidden))]
    pub fn import_bao(
        &self,
        hash: impl Into<Hash>,
        size: NonZeroU64,
        data: mpsc::Receiver<BaoContentItem>,
    ) -> ImportBaoResult {
        let options = ImportBaoRequest {
            hash: hash.into(),
            size,
        };
        // todo: we must expose the second future
        self.import_bao_with_opts(data.into(), options)
    }

    #[cfg_attr(not(feature = "proto-docs"), doc(hidden))]
    pub async fn import_bao_quinn(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        stream: &mut quinn::RecvStream,
    ) -> RequestResult<()> {
        let reader = TokioStreamReader::new(stream);
        self.import_bao_reader(hash, ranges, reader).await?;
        Ok(())
    }

    #[cfg_attr(not(feature = "proto-docs"), doc(hidden))]
    pub async fn import_bao_bytes(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        data: impl Into<Bytes>,
    ) -> RequestResult<()> {
        self.import_bao_reader(hash, ranges, data.into()).await?;
        Ok(())
    }

    pub fn list(&self) -> BlobsListResult {
        let msg = ListRequest;
        let req = self.client.request();
        BlobsListResult::new(async move {
            Ok(match req.await? {
                Request::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((msg, tx)).await?;
                    rx
                }
                Request::Remote(r) => {
                    let (_, rx) = r.write(msg).await?;
                    rx.into()
                }
            })
        })
    }

    pub async fn status(&self, hash: impl Into<Hash>) -> super::RpcResult<BlobStatus> {
        let hash = hash.into();
        let msg = BlobStatusRequest { hash };
        self.client.rpc(msg).await
    }

    pub async fn has(&self, hash: impl Into<Hash>) -> super::RpcResult<bool> {
        match self.status(hash).await? {
            BlobStatus::Complete { .. } => Ok(true),
            _ => Ok(false),
        }
    }

    pub(crate) async fn clear_protected(&self) -> RequestResult<()> {
        let msg = ClearProtectedRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }
}

pub struct Batch<'a> {
    scope: Scope,
    blobs: &'a Blobs,
    _tx: spsc::Sender<BatchResponse>,
}

impl<'a> Batch<'a> {
    pub fn add_bytes(&self, data: impl Into<Bytes>) -> ImportResult {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        self.blobs.add_bytes_impl(options)
    }

    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> ImportResult {
        let options = options.into();
        self.blobs.add_bytes_impl(ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: self.scope,
        })
    }

    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> ImportResult {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        self.blobs.add_bytes_impl(options)
    }

    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> ImportResult {
        let options = options.into();
        self.blobs.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: self.scope,
        })
    }

    pub async fn temp_tag(&self, value: impl Into<HashAndFormat>) -> super::RpcResult<TempTag> {
        let value = value.into();
        let msg = CreateTempTagRequest {
            scope: self.scope,
            value,
        };
        self.blobs.client.rpc(msg).await
    }
}

#[derive(Debug)]
pub struct AddPathOptions {
    pub path: PathBuf,
    pub format: BlobFormat,
    pub mode: ImportMode,
}

pub struct ImportResult<'a> {
    blobs: &'a Blobs,
    inner: n0_future::future::Boxed<super::RpcResult<spsc::Receiver<ImportProgress>>>,
}

impl<'a> ImportResult<'a> {
    fn new(
        blobs: &'a Blobs,
        fut: impl Future<Output = super::RpcResult<spsc::Receiver<ImportProgress>>> + Send + 'static,
    ) -> Self {
        Self {
            blobs,
            inner: Box::pin(fut),
        }
    }

    pub async fn temp_tag(self) -> RequestResult<TempTag> {
        let mut rx = self.inner.await?;
        loop {
            match rx.recv().await {
                Ok(Some(ImportProgress::Done(tt))) => break Ok(tt),
                Ok(Some(ImportProgress::Error(cause))) => {
                    trace!("got explicit error: {:?}", cause);
                    break Err(super::Error::other(cause).into());
                }
                Err(cause) => {
                    trace!("error receiving import progress: {:?}", cause);
                    return Err(cause.into());
                }
                _ => {}
            }
        }
    }

    pub async fn with_named_tag(self, name: impl AsRef<[u8]>) -> RequestResult<HashAndFormat> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let haf = *tt.hash_and_format();
        let tags = Tags::ref_from_sender(&blobs.client);
        tags.set(name, *tt.hash_and_format()).await?;
        drop(tt);
        Ok(haf)
    }

    pub async fn with_tag(self) -> RequestResult<TagInfo> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let hash = *tt.hash();
        let format = tt.format();
        let tags = Tags::ref_from_sender(&blobs.client);
        let name = tags.create(*tt.hash_and_format()).await?;
        drop(tt);
        Ok(TagInfo { name, hash, format })
    }

    pub async fn stream(self) -> RequestResult<impl Stream<Item = ImportProgress>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

/// An observe result. Awaiting this will return the current state.
///
/// Calling [`ObserveResult::stream`] will return a stream of updates, where
/// the first item is the current state and subsequent items are updates.
///
/// Calling [`ObserveResult::aggregated`] will return a stream of states,
/// where each state is the current state at the time of the update.
pub struct ObserveResult {
    inner: n0_future::future::Boxed<super::RpcResult<spsc::Receiver<Bitfield>>>,
}

impl IntoFuture for ObserveResult {
    type Output = RequestResult<Bitfield>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut rx = self.inner.await?;
            match rx.recv().await? {
                Some(bitfield) => Ok(bitfield),
                None => Err(super::Error::other("unexpected end of stream").into()),
            }
        })
    }
}

impl ObserveResult {
    fn new(
        fut: impl Future<Output = super::RpcResult<spsc::Receiver<Bitfield>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn aggregated(self) -> super::RpcResult<Aggregator<Bitfield>> {
        let rx = self.inner.await?.try_into().unwrap();
        Ok(Aggregator::new(rx))
    }

    /// Returns an infinite stream of bitfields. The first bitfield is the
    /// current state, and the following bitfields are updates.
    ///
    /// Once a blob is complete, there will be no more updates.
    pub async fn stream(self) -> super::RpcResult<impl Stream<Item = Bitfield>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

pub struct ExportResult {
    inner: n0_future::future::Boxed<super::RpcResult<spsc::Receiver<ExportProgress>>>,
}

impl ExportResult {
    fn new(
        fut: impl Future<Output = super::RpcResult<spsc::Receiver<ExportProgress>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn stream(self) -> impl Stream<Item = ExportProgress> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(e) => {
                    co.yield_(ExportProgress::Error(e.into())).await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }

    pub async fn finish(self) -> RequestResult<u64> {
        let mut rx = self.inner.await?;
        let mut size = None;
        loop {
            match rx.recv().await? {
                Some(ExportProgress::Done) => break,
                Some(ExportProgress::Size(s)) => size = Some(s),
                Some(ExportProgress::Error(cause)) => return Err(cause.into()),
                _ => {}
            }
        }
        if let Some(size) = size {
            Ok(size)
        } else {
            Err(super::Error::other("unexpected end of stream").into())
        }
    }
}

/// Result of importing a stream of bao items.
///
/// This future will resolve once the import is complete, but *must* be polled even before!
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ImportBaoResult {
    inner: n0_future::future::Boxed<RequestResult<()>>,
}

impl ImportBaoResult {
    fn new(fut: impl Future<Output = RequestResult<()>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }
}

impl IntoFuture for ImportBaoResult {
    type Output = RequestResult<()>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.inner)
    }
}
pub struct BlobsListResult {
    inner: n0_future::future::Boxed<super::RpcResult<spsc::Receiver<super::Result<Hash>>>>,
}

impl BlobsListResult {
    fn new(
        fut: impl Future<Output = super::RpcResult<spsc::Receiver<super::Result<Hash>>>>
        + Send
        + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn hashes(self) -> RequestResult<Vec<Hash>> {
        let mut rx: spsc::Receiver<Result<Hash, super::Error>> = self.inner.await?;
        let mut hashes = Vec::new();
        while let Some(item) = rx.recv().await? {
            hashes.push(item?);
        }
        Ok(hashes)
    }

    pub async fn stream(self) -> super::RpcResult<impl Stream<Item = super::Result<Hash>>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

pub struct ExportBaoResult {
    inner: n0_future::future::Boxed<super::RpcResult<spsc::Receiver<EncodedItem>>>,
}

impl ExportBaoResult {
    fn new(
        fut: impl Future<Output = super::RpcResult<spsc::Receiver<EncodedItem>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Interprets this blob as a hash sequence and returns a stream of hashes.
    ///
    /// Errors will be reported, but the iterator will nevertheless continue.
    /// If you get an error despite having asked for ranges that should be present,
    /// this means that the data is corrupted. It can still make sense to continue
    /// to get all non-corrupted sections.
    pub fn hashes_with_index(
        self,
    ) -> impl Stream<Item = std::result::Result<(u64, Hash), anyhow::Error>> {
        let mut stream = self.stream();
        Gen::new(|co| async move {
            while let Some(item) = stream.next().await {
                let leaf = match item {
                    EncodedItem::Leaf(leaf) => leaf,
                    EncodedItem::Error(e) => {
                        co.yield_(Err(e.into())).await;
                        continue;
                    }
                    _ => continue,
                };
                let slice = match HashSeqChunk::try_from(leaf) {
                    Ok(slice) => slice,
                    Err(e) => {
                        co.yield_(Err(e)).await;
                        continue;
                    }
                };
                let offset = slice.base();
                for (o, hash) in slice.into_iter().enumerate() {
                    co.yield_(Ok((offset + o as u64, hash))).await;
                }
            }
        })
    }

    /// Same as [`Self::hashes_with_index`], but without the indexes.
    pub fn hashes(self) -> impl Stream<Item = std::result::Result<Hash, anyhow::Error>> {
        self.hashes_with_index().map(|x| x.map(|(_, hash)| hash))
    }

    pub async fn bao_to_vec(self) -> RequestResult<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            println!("item: {:?}", item);
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    pub async fn data_to_bytes(self) -> super::ExportBaoResult<Bytes> {
        let mut rx = self.inner.await?;
        let mut data = Vec::new();
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Leaf(leaf) => {
                    data.push(leaf.data);
                }
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        if data.len() == 1 {
            Ok(data.pop().unwrap())
        } else {
            let mut out = Vec::new();
            for item in data {
                out.extend_from_slice(&item);
            }
            Ok(out.into())
        }
    }

    pub async fn data_to_vec(self) -> super::ExportBaoResult<Vec<u8>> {
        let mut rx = self.inner.await?;
        let mut data = Vec::new();
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Leaf(leaf) => {
                    data.extend_from_slice(&leaf.data);
                }
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(data)
    }

    pub async fn write_quinn(self, target: &mut quinn::SendStream) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    target.write_u64_le(size).await?;
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    target
                        .write_all(&data)
                        .await
                        .map_err(|e| super::ExportBaoError::Io(e.into()))?;
                }
                EncodedItem::Leaf(leaf) => {
                    target
                        .write_chunk(leaf.data)
                        .await
                        .map_err(|e| super::ExportBaoError::Io(e.into()))?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

    /// Write quinn variant that also feeds a progress writer.
    pub(crate) async fn write_quinn_with_progress(
        self,
        writer: &mut ProgressWriter,
        hash: &Hash,
        index: u64,
    ) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    writer.send_transfer_started(index, hash, size).await;
                    writer.inner.write_u64_le(size).await?;
                    writer.log_other_write(8);
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    writer
                        .inner
                        .write_all(&data)
                        .await
                        .map_err(|e| super::ExportBaoError::Io(e.into()))?;
                    writer.log_other_write(64);
                }
                EncodedItem::Leaf(leaf) => {
                    let len = leaf.data.len();
                    writer
                        .inner
                        .write_chunk(leaf.data)
                        .await
                        .map_err(|e| super::ExportBaoError::Io(e.into()))?;
                    writer.notify_payload_write(index, leaf.offset, len);
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

    pub fn into_byte_stream(self) -> impl Stream<Item = super::Result<Bytes>> {
        self.stream().filter_map(|item| match item {
            EncodedItem::Size(size) => {
                let size = size.to_le_bytes().to_vec().into();
                Some(Ok(size))
            }
            EncodedItem::Parent(parent) => {
                let mut data = vec![0u8; 64];
                data[..32].copy_from_slice(parent.pair.0.as_bytes());
                data[32..].copy_from_slice(parent.pair.1.as_bytes());
                Some(Ok(data.into()))
            }
            EncodedItem::Leaf(leaf) => Some(Ok(leaf.data)),
            EncodedItem::Done => None,
            EncodedItem::Error(cause) => Some(Err(super::Error::other(cause))),
        })
    }

    pub fn stream(self) -> impl Stream<Item = EncodedItem> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(cause) => {
                    co.yield_(EncodedItem::Error(io::Error::other(cause).into()))
                        .await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }
}
