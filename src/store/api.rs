//! The user facing API of the store.
use std::{
    future::Future,
    io,
    num::NonZeroU64,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
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
    get::request,
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

    use n0_future::{Stream, StreamExt, TryFutureExt};
    use quic_rpc::{channel::oneshot, ServiceRequest};
    use serde::{Deserialize, Serialize};

    use super::super::Tags;
    use crate::{
        store::{
            api,
            proto::{Rename, SetTag},
            util::Tag,
        },
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

    /// Options for a delete operation.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DeleteTags {
        /// Optional from tag (inclusive)
        pub from: Option<Tag>,
        /// Optional to tag (exclusive)
        pub to: Option<Tag>,
    }

    impl DeleteTags {
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
        pub async fn delete_with_opts(&self, options: DeleteTags) -> api::Result<()> {
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
            self.delete_with_opts(DeleteTags::single(name.as_ref()))
                .await
        }

        /// Deletes a range of tags.
        pub async fn delete_range<R, E>(&self, range: R) -> api::Result<()>
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            self.delete_with_opts(DeleteTags::range(range)).await
        }

        /// Delete all tags with the given prefix.
        pub async fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> api::Result<()> {
            self.delete_with_opts(DeleteTags::prefix(prefix.as_ref()))
                .await
        }

        /// Delete all tags. Use with care. After this, all data will be garbage collected.
        pub async fn delete_all(&self) -> api::Result<()> {
            self.delete_with_opts(DeleteTags {
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

    pub fn import_path(&self, path: impl AsRef<Path>) -> ImportResult {
        let inner = ImportPath {
            path: path.as_ref().to_owned(),
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
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

    pub fn export_bao(&self, hash: Hash, ranges: ChunkRanges) -> ExportBaoResult {
        let inner = ExportBao { hash, ranges };
        let request = self.sender.request();
        ExportBaoResult::new(async move {
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
            io::Result::Ok(rx)
        })
    }

    /// Helper to just export the entire blob into a Bytes
    pub async fn export_bytes(&self, hash: impl Into<Hash>) -> io::Result<Bytes> {
        self.export_bao(hash.into(), ChunkRanges::all())
            .data_to_bytes()
            .await
    }

    /// Observe the bitfield of the given hash.
    ///
    /// Returns an infinite stream of bitfields. The first bitfield is the
    /// current state, and the following bitfields are updates.
    ///
    /// Once a blob is complete, there will be no more updates.
    pub async fn observe(&self, hash: impl Into<Hash>) -> ObserveResult {
        let hash = hash.into();
        if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
            let (mut tx, rx) = spsc::channel(1);
            tx.send(Bitfield::complete(0)).await.ok();
            return ObserveResult {
                rx: rx.try_into().unwrap(),
            };
        }
        let (tx, rx) = spsc::channel(32);
        self.sender
            .local()
            .unwrap()
            .send((Observe { hash }, tx))
            .await
            .ok();
        ObserveResult {
            rx: rx.try_into().unwrap(),
        }
    }

    pub fn export_path(&self, hash: Hash, target: impl AsRef<Path>) -> ExportPathResult {
        let cmd = ExportPath {
            hash,
            mode: ExportMode::Copy,
            target: target.as_ref().to_owned(),
        };
        let request = self.sender.request();
        ExportPathResult::new(async move {
            let rx = match request.await? {
                ServiceRequest::Local(c) => {
                    let (tx, rx) = spsc::channel(32);
                    c.send((cmd, tx)).await?;
                    rx
                }
                ServiceRequest::Remote(r) => {
                    let (rx, _) = r.write(cmd).await?;
                    rx.into()
                }
            };
            Ok(rx)
        })
    }

    // todo: export_path_with_opts
    async fn import_bao_reader<R: AsyncStreamReader>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> anyhow::Result<R> {
        let (mut tx, rx) = spsc::channel(32);
        let (out_tx, out_rx) = oneshot::channel();
        let size = u64::from_le_bytes(reader.read::<8>().await?);
        let Some(size) = NonZeroU64::new(size) else {
            return if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
                Ok(reader)
            } else {
                Err(anyhow::anyhow!("invalid size for hash"))
            };
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash.into(), ranges, tree, reader);
        let inner = ImportBao { hash, size };
        self.sender
            .local()
            .unwrap()
            .send((inner, out_tx, rx))
            .await?;
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
        out_rx.await??;
        Ok(reader)
    }

    /// Import BaoContentItems from a stream.
    ///
    /// The store assumes that these are already verified and in the correct order.
    pub async fn import_bao(
        &self,
        hash: impl Into<Hash>,
        size: NonZeroU64,
        data: mpsc::Receiver<BaoContentItem>,
    ) -> anyhow::Result<()> {
        let hash = hash.into();
        let (tx, rx) = oneshot::channel();
        let msg = ImportBao { hash, size };
        self.sender.local().unwrap().send((msg, tx, data)).await?;
        rx.await??;
        Ok(())
    }

    pub async fn import_bao_quinn(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        stream: &mut quinn::RecvStream,
    ) -> anyhow::Result<()> {
        let reader = TokioStreamReader::new(stream);
        self.import_bao_reader(hash, ranges, reader).await?;
        Ok(())
    }

    pub async fn import_bao_bytes(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        data: Bytes,
    ) -> anyhow::Result<()> {
        self.import_bao_reader(hash, ranges, data).await?;
        Ok(())
    }

    pub async fn sync_db(&self) -> anyhow::Result<()> {
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

    pub async fn shutdown(&self) -> anyhow::Result<()> {
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

/// An infinite stream of bitfields, where the first is the current state
/// and all following are updates. Once a blob is complete, there will be no
/// more updates.
pub struct ObserveResult {
    rx: tokio::sync::mpsc::Receiver<Bitfield>,
}

impl ObserveResult {
    pub fn aggregated(self) -> Aggregator<Bitfield> {
        Aggregator::new(self.rx)
    }
}

impl Stream for ObserveResult {
    type Item = Bitfield;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            _ => Poll::Pending,
        }
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

pub struct ExportBaoResult {
    inner: n0_future::future::Boxed<io::Result<spsc::Receiver<EncodedItem>>>,
}

impl ExportBaoResult {
    fn new(
        fut: impl Future<Output = io::Result<spsc::Receiver<EncodedItem>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    pub async fn bao_to_vec(self) -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    pub async fn data_to_bytes(self) -> io::Result<Bytes> {
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
                EncodedItem::Error(cause) => return Err(io::Error::from(cause)),
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

    pub async fn data_to_vec(self) -> io::Result<Vec<u8>> {
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
                EncodedItem::Error(cause) => return Err(io::Error::from(cause)),
            }
        }
        Ok(data)
    }

    pub async fn write_quinn(self, target: &mut quinn::SendStream) -> io::Result<()> {
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
                    target.write_all(&data).await?;
                }
                EncodedItem::Leaf(leaf) => {
                    target.write_chunk(leaf.data).await?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(io::Error::from(cause)),
            }
        }
        Ok(())
    }

    pub fn into_byte_stream(self) -> impl Stream<Item = io::Result<Bytes>> {
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
            EncodedItem::Error(cause) => Some(Err(io::Error::from(cause))),
        })
    }

    pub fn stream(self) -> impl Stream<Item = EncodedItem> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(cause) => {
                    co.yield_(EncodedItem::Error(cause.into())).await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }
}
