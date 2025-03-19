//! The user facing API of the store.
use std::{
    future::{Future, IntoFuture},
    io,
    num::NonZeroU64,
    path::Path,
    pin::Pin,
};

use bao_tree::{
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        mixed::EncodedItem,
        BaoContentItem,
    },
    BaoTree, ChunkRanges,
};
use bytes::Bytes;
use genawaiter::sync::Gen;
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use n0_future::{Stream, StreamExt};
use quic_rpc::{
    channel::{oneshot, spsc},
    ServiceRequest,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::trace;

use super::{BlobFormat, Blobs};
use crate::{
    store::{
        api,
        bitfield::Bitfield,
        proto::*,
        util::{observer::Aggregator, SliceInfoExt},
        IROH_BLOCK_SIZE,
    },
    util::channel::mpsc,
    Hash,
};

#[derive(Debug, derive_more::Display, derive_more::From, Serialize, Deserialize)]
pub enum Error {
    #[serde(with = "crate::util::serde::io_error_serde")]
    Io(io::Error),
}

impl Error {
    pub fn io(
        kind: io::ErrorKind,
        msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::Io(io::Error::new(kind, msg.into()))
    }

    pub fn other<E>(msg: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Io(io::Error::other(msg.into()))
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub mod tags {
    use std::{
        io,
        ops::{Bound, RangeBounds},
    };

    use n0_future::{Stream, StreamExt};
    use quic_rpc::{channel::oneshot, ServiceRequest};
    use serde::{Deserialize, Serialize};

    use super::super::Tags;
    use crate::{
        store::{api, util::Tag},
        BlobFormat, Hash, HashAndFormat,
    };

    /// Information about a tag.
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct TagInfo {
        /// Name of the tag
        pub name: Tag,
        /// Format of the data
        pub format: BlobFormat,
        /// Hash of the data
        pub hash: Hash,
    }

    impl TagInfo {
        /// Create a new tag info.
        pub fn new(name: impl AsRef<[u8]>, value: impl Into<HashAndFormat>) -> Self {
            let name = name.as_ref();
            let value = value.into();
            Self {
                name: Tag::from(name),
                hash: value.hash,
                format: value.format,
            }
        }

        /// Get the hash and format of the tag.
        pub fn hash_and_format(&self) -> HashAndFormat {
            HashAndFormat {
                hash: self.hash,
                format: self.format,
            }
        }
    }

    /// Options for a list operation.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ListTags {
        /// List tags to hash seqs
        pub hash_seq: bool,
        /// List tags to raw blobs
        pub raw: bool,
        /// Optional from tag (inclusive)
        pub from: Option<Tag>,
        /// Optional to tag (exclusive)
        pub to: Option<Tag>,
    }

    impl ListTags {
        /// List a range of tags
        pub fn range<R, E>(range: R) -> Self
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            let (from, to) = tags_from_range(range);
            Self {
                from,
                to,
                raw: true,
                hash_seq: true,
            }
        }

        /// List tags with a prefix
        pub fn prefix(prefix: &[u8]) -> Self {
            let from = Tag::from(prefix);
            let to = from.next_prefix();
            Self {
                raw: true,
                hash_seq: true,
                from: Some(from),
                to,
            }
        }

        /// List a single tag
        pub fn single(name: &[u8]) -> Self {
            let from = Tag::from(name);
            Self {
                to: Some(from.successor()),
                from: Some(from),
                raw: true,
                hash_seq: true,
            }
        }

        /// List all tags
        pub fn all() -> Self {
            Self {
                raw: true,
                hash_seq: true,
                from: None,
                to: None,
            }
        }

        /// List raw tags
        pub fn raw() -> Self {
            Self {
                raw: true,
                hash_seq: false,
                from: None,
                to: None,
            }
        }

        /// List hash seq tags
        pub fn hash_seq() -> Self {
            Self {
                raw: false,
                hash_seq: true,
                from: None,
                to: None,
            }
        }
    }

    /// Rename a tag atomically
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Rename {
        /// Old tag name
        pub from: Tag,
        /// New tag name
        pub to: Tag,
    }

    /// Options for a delete operation.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Delete {
        /// Optional from tag (inclusive)
        pub from: Option<Tag>,
        /// Optional to tag (exclusive)
        pub to: Option<Tag>,
    }

    impl Delete {
        /// Delete a single tag
        pub fn single(name: &[u8]) -> Self {
            let name = Tag::from(name);
            Self {
                to: Some(name.successor()),
                from: Some(name),
            }
        }

        /// Delete a range of tags
        pub fn range<R, E>(range: R) -> Self
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            let (from, to) = tags_from_range(range);
            Self { from, to }
        }

        /// Delete tags with a prefix
        pub fn prefix(prefix: &[u8]) -> Self {
            let from = Tag::from(prefix);
            let to = from.next_prefix();
            Self {
                from: Some(from),
                to,
            }
        }
    }

    fn tags_from_range<R, E>(range: R) -> (Option<Tag>, Option<Tag>)
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let from = match range.start_bound() {
            Bound::Included(start) => Some(Tag::from(start.as_ref())),
            Bound::Excluded(start) => Some(Tag::from(start.as_ref()).successor()),
            Bound::Unbounded => None,
        };
        let to = match range.end_bound() {
            Bound::Included(end) => Some(Tag::from(end.as_ref()).successor()),
            Bound::Excluded(end) => Some(Tag::from(end.as_ref())),
            Bound::Unbounded => None,
        };
        (from, to)
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct SetTag {
        pub name: Tag,
        pub value: HashAndFormat,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CreateTag {
        pub content: HashAndFormat,
    }

    impl Tags {
        /// List all tags with options.
        ///
        /// This is the most flexible way to list tags. All the other list methods are just convenience
        /// methods that call this one with the appropriate options.
        pub async fn list_with_opts(
            &self,
            options: ListTags,
        ) -> super::Result<impl Stream<Item = super::Result<TagInfo>>> {
            let (tx, rx) = oneshot::channel();
            let rx = match self.sender.request().await? {
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
                ServiceRequest::Local(l) => {
                    l.send((options, tx)).await?;
                    rx
                }
            };
            let res = rx.await?;
            Ok(futures_lite::stream::iter(res))
        }

        /// Get the value of a single tag
        pub async fn get(&self, name: impl AsRef<[u8]>) -> super::Result<Option<TagInfo>> {
            let mut stream = self.list_with_opts(ListTags::single(name.as_ref())).await?;
            stream.next().await.transpose()
        }

        pub async fn set_with_opts(&self, msg: SetTag) -> super::Result<()> {
            let rx = match self.sender.request().await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = oneshot::channel();
                    c.send((msg, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(msg).await?;
                    rx.into()
                }
            };
            Ok(rx.await??)
        }

        pub async fn set(
            &self,
            name: impl AsRef<[u8]>,
            value: impl Into<HashAndFormat>,
        ) -> super::Result<()> {
            self.set_with_opts(SetTag {
                name: Tag::from(name.as_ref()),
                value: value.into(),
            })
            .await
        }

        /// List a range of tags
        pub async fn list_range<R, E>(
            &self,
            range: R,
        ) -> super::Result<impl Stream<Item = super::Result<TagInfo>>>
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            self.list_with_opts(ListTags::range(range)).await
        }

        /// Lists all tags with the given prefix.
        pub async fn list_prefix(
            &self,
            prefix: impl AsRef<[u8]>,
        ) -> super::Result<impl Stream<Item = super::Result<TagInfo>>> {
            self.list_with_opts(ListTags::prefix(prefix.as_ref())).await
        }

        /// Lists all tags.
        pub async fn list(&self) -> super::Result<impl Stream<Item = super::Result<TagInfo>>> {
            self.list_with_opts(ListTags::all()).await
        }

        /// Lists all tags with a hash_seq format.
        pub async fn list_hash_seq(
            &self,
        ) -> super::Result<impl Stream<Item = super::Result<TagInfo>>> {
            self.list_with_opts(ListTags::hash_seq()).await
        }

        /// Deletes a tag.
        pub async fn delete_with_opts(&self, options: Delete) -> api::Result<()> {
            let rx = match self.sender.request().await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = quic_rpc::channel::oneshot::channel();
                    c.send((options, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
            };
            rx.await.map_err(|_e| io::Error::other("error"))?
        }

        /// Deletes a tag.
        pub async fn delete(&self, name: impl AsRef<[u8]>) -> api::Result<()> {
            self.delete_with_opts(Delete::single(name.as_ref())).await
        }

        /// Deletes a range of tags.
        pub async fn delete_range<R, E>(&self, range: R) -> api::Result<()>
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            self.delete_with_opts(Delete::range(range)).await
        }

        /// Delete all tags with the given prefix.
        pub async fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> api::Result<()> {
            self.delete_with_opts(Delete::prefix(prefix.as_ref())).await
        }

        /// Delete all tags. Use with care. After this, all data will be garbage collected.
        pub async fn delete_all(&self) -> api::Result<()> {
            self.delete_with_opts(Delete {
                from: None,
                to: None,
            })
            .await
        }

        /// Rename a tag atomically
        ///
        /// If the tag does not exist, this will return an error.
        pub async fn rename_with_opts(&self, options: Rename) -> api::Result<()> {
            let rx = match self.sender.request().await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = quic_rpc::channel::oneshot::channel();
                    c.send((options, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
            };
            rx.await?
        }

        /// Rename a tag atomically
        ///
        /// If the tag does not exist, this will return an error.
        pub async fn rename(
            &self,
            from: impl AsRef<[u8]>,
            to: impl AsRef<[u8]>,
        ) -> api::Result<()> {
            self.rename_with_opts(Rename {
                from: Tag::from(from.as_ref()),
                to: Tag::from(to.as_ref()),
            })
            .await
        }
    }
}

impl Blobs {
    pub fn import_bytes(&self, data: impl Into<bytes::Bytes>) -> ImportResult {
        self.import_bytes_impl(data.into())
    }

    fn import_bytes_impl(&self, data: bytes::Bytes) -> ImportResult {
        trace!(
            "import_bytes size={} addr={}",
            data.len(),
            data.addr_short()
        );
        let inner = ImportBytes {
            data,
            format: crate::BlobFormat::Raw,
        };
        let request = self.sender.request();
        ImportResult::new(async move {
            let rx = match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((inner, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(inner).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn import_path_with_opts(&self, options: ImportPath) -> ImportResult {
        let request = self.sender.request();
        ImportResult::new(async move {
            Ok(match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn import_path(&self, path: impl AsRef<Path>) -> ImportResult {
        self.import_path_with_opts(ImportPath {
            path: path.as_ref().to_owned(),
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
        })
    }

    pub async fn import_byte_stream(
        &self,
        data: impl Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    ) -> ImportResult {
        self.import_byte_stream_impl(Box::pin(data)).await
    }

    async fn import_byte_stream_impl(
        &self,
        data: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>,
    ) -> ImportResult {
        let data = data
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let inner = ImportByteStream {
            data,
            format: crate::BlobFormat::Raw,
        };
        let request = self.sender.request();
        ImportResult::new(async move {
            let rx = match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((inner, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(inner).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn export_bao_with_opts(&self, options: ExportBao) -> ExportBaoResult {
        let request = self.sender.request();
        ExportBaoResult::new(async move {
            Ok(match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn export_bao(&self, hash: impl Into<Hash>, ranges: ChunkRanges) -> ExportBaoResult {
        self.export_bao_with_opts(ExportBao {
            hash: hash.into(),
            ranges,
        })
    }

    /// Helper to just export the entire blob into a Bytes
    pub async fn export_bytes(&self, hash: impl Into<Hash>) -> api::Result<Bytes> {
        self.export_bao(hash.into(), ChunkRanges::all())
            .data_to_bytes()
            .await
    }

    /// Observe the bitfield of the given hash.
    pub fn observe(&self, hash: impl Into<Hash>) -> ObserveResult {
        self.observe_with_opts(Observe { hash: hash.into() })
    }

    pub fn observe_with_opts(&self, options: Observe) -> ObserveResult {
        let Observe { hash } = options;
        if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
            return ObserveResult::new(async move {
                let (mut tx, rx) = spsc::channel(1);
                tx.send(Bitfield::complete(0)).await.ok();
                Ok(rx)
            });
        }
        let (tx, rx) = spsc::channel(32);
        let request = self.sender.request();
        ObserveResult::new(async move {
            let rx = match request.await? {
                ServiceRequest::Local(c) => {
                    c.send((Observe { hash }, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(Observe { hash }).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    pub fn export_path_with_opts(&self, options: ExportPath) -> ExportPathResult {
        let request = self.sender.request();
        ExportPathResult::new(async move {
            Ok(match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((options, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(options).await?;
                    rx.into()
                }
            })
        })
    }

    pub fn export_path(&self, hash: Hash, target: impl AsRef<Path>) -> ExportPathResult {
        let options = ExportPath {
            hash,
            mode: ExportMode::Copy,
            target: target.as_ref().to_owned(),
        };
        self.export_path_with_opts(options)
    }

    pub fn import_bao_with_opts(
        &self,
        mut data: spsc::Receiver<BaoContentItem>,
        options: ImportBao,
    ) -> ImportBaoResult {
        let request = self.sender.request();
        ImportBaoResult::new(async move {
            let rx = match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = oneshot::channel();
                    c.send((options, tx, data)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, tx) = r.write(options).await?;
                    let mut tx: spsc::Sender<_> = tx.into();
                    while let Some(item) = data.recv().await? {
                        tx.send(item).await.map_err(|e| api::Error::other(e))?;
                    }
                    rx.into()
                }
            };
            Ok(rx.await??)
        })
    }

    // todo: export_path_with_opts
    async fn import_bao_reader<R: AsyncStreamReader>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> api::Result<R> {
        let (mut tx, rx) = spsc::channel(32);
        let size = u64::from_le_bytes(reader.read::<8>().await?);
        let Some(size) = NonZeroU64::new(size) else {
            return if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
                Ok(reader)
            } else {
                Err(api::Error::other("invalid size for hash"))
            };
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash.into(), ranges, tree, reader);
        let inner = ImportBao { hash, size };
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
    pub fn import_bao(
        &self,
        hash: impl Into<Hash>,
        size: NonZeroU64,
        data: mpsc::Receiver<BaoContentItem>,
    ) -> ImportBaoResult {
        let options = ImportBao {
            hash: hash.into(),
            size,
        };
        // todo: we must expose the second future
        self.import_bao_with_opts(data.into(), options)
    }

    pub async fn import_bao_quinn(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        stream: &mut quinn::RecvStream,
    ) -> api::Result<()> {
        let reader = TokioStreamReader::new(stream);
        self.import_bao_reader(hash, ranges, reader).await?;
        Ok(())
    }

    pub async fn import_bao_bytes(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        data: Bytes,
    ) -> api::Result<()> {
        self.import_bao_reader(hash, ranges, data).await?;
        Ok(())
    }

    pub async fn sync_db(&self) -> api::Result<()> {
        let rx = match self.sender.request().await? {
            ServiceRequest::Local(c) => {
                let (tx, rx) = oneshot::channel();
                c.send((SyncDb, tx)).await?;
                rx
            }
            ServiceRequest::Remote(r) => {
                let (rx, _) = r.write(SyncDb).await?;
                rx.into()
            }
        };
        rx.await??;
        Ok(())
    }

    pub async fn shutdown(&self) -> api::Result<()> {
        let msg = Shutdown;
        let rx = match self.sender.request().await? {
            ServiceRequest::Local(c) => {
                let (tx, rx) = quic_rpc::channel::oneshot::channel();
                c.send((msg, tx)).await?;
                rx
            }
            ServiceRequest::Remote(r) => {
                let (rx, _) = r.write(msg).await?;
                rx.into()
            }
        };
        rx.await?;
        Ok(())
    }
}

pub struct ImportResult {
    inner: n0_future::future::Boxed<io::Result<spsc::Receiver<ImportProgress>>>,
}

impl ImportResult {
    fn new(
        fut: impl Future<Output = io::Result<spsc::Receiver<ImportProgress>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn hash(self) -> io::Result<Hash> {
        let mut rx = self.inner.await?;
        loop {
            match rx.recv().await? {
                Some(ImportProgress::Done { hash }) => break Ok(hash),
                Some(ImportProgress::Error { cause }) => break Err(cause),
                _ => {}
            }
        }
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
    inner: n0_future::future::Boxed<api::Result<spsc::Receiver<Bitfield>>>,
}

impl IntoFuture for ObserveResult {
    type Output = api::Result<Bitfield>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut rx = self.inner.await?;
            match rx.recv().await? {
                Some(bitfield) => Ok(bitfield),
                None => Err(io::Error::other("unexpected end of stream").into()),
            }
        })
    }
}

impl ObserveResult {
    fn new(
        fut: impl Future<Output = api::Result<spsc::Receiver<Bitfield>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn aggregated(self) -> api::Result<Aggregator<Bitfield>> {
        let rx = self.inner.await?.try_into().unwrap();
        Ok(Aggregator::new(rx))
    }

    /// Returns an infinite stream of bitfields. The first bitfield is the
    /// current state, and the following bitfields are updates.
    ///
    /// Once a blob is complete, there will be no more updates.
    pub async fn stream(self) -> api::Result<impl Stream<Item = Bitfield>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

pub struct ExportPathResult {
    inner: n0_future::future::Boxed<io::Result<spsc::Receiver<ExportProgress>>>,
}

impl ExportPathResult {
    fn new(
        fut: impl Future<Output = io::Result<spsc::Receiver<ExportProgress>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn finish(self) -> api::Result<()> {
        let mut rx = self.inner.await?;
        loop {
            match rx.recv().await? {
                Some(ExportProgress::Done) => break Ok(()),
                Some(ExportProgress::Error { cause }) => break Err(cause),
                _ => {}
            }
        }
    }
}

/// Result of importing a stream of bao items.
///
/// This future will resolve once the import is complete, but *must* be polled even before!
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ImportBaoResult {
    inner: n0_future::future::Boxed<api::Result<()>>,
}

impl ImportBaoResult {
    fn new(fut: impl Future<Output = api::Result<()>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }
}

impl IntoFuture for ImportBaoResult {
    type Output = api::Result<()>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.inner.await })
    }
}

pub struct ExportBaoResult {
    inner: n0_future::future::Boxed<api::Result<spsc::Receiver<EncodedItem>>>,
}

impl ExportBaoResult {
    fn new(
        fut: impl Future<Output = api::Result<spsc::Receiver<EncodedItem>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn bao_to_vec(self) -> api::Result<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    pub async fn data_to_bytes(self) -> api::Result<Bytes> {
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
                EncodedItem::Error(cause) => return Err(api::Error::other(cause)),
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

    pub async fn data_to_vec(self) -> api::Result<Vec<u8>> {
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
                EncodedItem::Error(cause) => return Err(api::Error::other(cause)),
            }
        }
        Ok(data)
    }

    pub async fn write_quinn(self, target: &mut quinn::SendStream) -> api::Result<()> {
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
                    target.write_all(&data).await.map_err(api::Error::other)?;
                }
                EncodedItem::Leaf(leaf) => {
                    target
                        .write_chunk(leaf.data)
                        .await
                        .map_err(api::Error::other)?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(api::Error::other(cause)),
            }
        }
        Ok(())
    }

    pub fn into_byte_stream(self) -> impl Stream<Item = api::Result<Bytes>> {
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
            EncodedItem::Error(cause) => Some(Err(api::Error::other(cause))),
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
