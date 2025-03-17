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
        BaoContentItem, EncodeError,
    },
    BaoTree, ChunkRanges,
};
use bytes::Bytes;
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use n0_future::{Stream, StreamExt};
use tokio::io::AsyncWriteExt;
use tracing::trace;

use super::Blobs;
use crate::{
    store::{
        bitfield::Bitfield,
        proto::*,
        util::{
            observer::{Aggregator, Observer},
            SliceInfoExt,
        },
        IROH_BLOCK_SIZE,
    },
    util::channel::{mpsc, oneshot},
    Hash,
};

pub mod tags {
    use std::ops::{Bound, RangeBounds};

    use anyhow::Result;
    use n0_future::{Stream, StreamExt};
    use serde::{Deserialize, Serialize};

    use super::super::Tags;
    use crate::{
        store::{
            proto::{DeleteTags, ListTags, RenameOptions, RenameTag, SetTag, SetTagOptions},
            util::Tag,
        },
        util::channel::oneshot,
        BlobFormat, Hash, HashAndFormat,
    };

    /// Information about a tag.
    #[derive(Debug, PartialEq, Eq)]
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
    pub struct ListOptions {
        /// List tags to hash seqs
        pub hash_seq: bool,
        /// List tags to raw blobs
        pub raw: bool,
        /// Optional from tag (inclusive)
        pub from: Option<Tag>,
        /// Optional to tag (exclusive)
        pub to: Option<Tag>,
    }

    impl ListOptions {
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
    #[derive(Debug, Clone)]
    pub struct DeleteOptions {
        /// Optional from tag (inclusive)
        pub from: Option<Tag>,
        /// Optional to tag (exclusive)
        pub to: Option<Tag>,
    }

    impl DeleteOptions {
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
            options: ListOptions,
        ) -> Result<impl Stream<Item = Result<TagInfo>>> {
            let (tx, rx) = oneshot::channel();
            self.sender.send(ListTags { options, tx }.into()).await?;
            let res = rx.await?;
            Ok(futures_lite::stream::iter(res))
        }

        /// Get the value of a single tag
        pub async fn get(&self, name: impl AsRef<[u8]>) -> Result<Option<TagInfo>> {
            let mut stream = self
                .list_with_opts(ListOptions::single(name.as_ref()))
                .await?;
            stream.next().await.transpose()
        }

        pub async fn set_with_opts(&self, options: SetTagOptions) -> Result<()> {
            let (tx, rx) = oneshot::channel();
            self.sender.send(SetTag { options, tx }.into()).await?;
            rx.await?
        }

        pub async fn set(
            &self,
            name: impl AsRef<[u8]>,
            value: impl Into<HashAndFormat>,
        ) -> anyhow::Result<()> {
            self.set_with_opts(SetTagOptions {
                name: Tag::from(name.as_ref()),
                value: value.into(),
            })
            .await
        }

        /// List a range of tags
        pub async fn list_range<R, E>(
            &self,
            range: R,
        ) -> Result<impl Stream<Item = Result<TagInfo>>>
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            self.list_with_opts(ListOptions::range(range)).await
        }

        /// Lists all tags with the given prefix.
        pub async fn list_prefix(
            &self,
            prefix: impl AsRef<[u8]>,
        ) -> Result<impl Stream<Item = Result<TagInfo>>> {
            self.list_with_opts(ListOptions::prefix(prefix.as_ref()))
                .await
        }

        /// Lists all tags.
        pub async fn list(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
            self.list_with_opts(ListOptions::all()).await
        }

        /// Lists all tags with a hash_seq format.
        pub async fn list_hash_seq(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
            self.list_with_opts(ListOptions::hash_seq()).await
        }

        /// Deletes a tag.
        pub async fn delete_with_opts(&self, options: DeleteOptions) -> Result<()> {
            let (tx, rx) = oneshot::channel();
            self.sender.send(DeleteTags { options, tx }.into()).await?;
            rx.await?
        }

        /// Deletes a tag.
        pub async fn delete(&self, name: impl AsRef<[u8]>) -> Result<()> {
            self.delete_with_opts(DeleteOptions::single(name.as_ref()))
                .await
        }

        /// Deletes a range of tags.
        pub async fn delete_range<R, E>(&self, range: R) -> Result<()>
        where
            R: RangeBounds<E>,
            E: AsRef<[u8]>,
        {
            self.delete_with_opts(DeleteOptions::range(range)).await
        }

        /// Delete all tags with the given prefix.
        pub async fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<()> {
            self.delete_with_opts(DeleteOptions::prefix(prefix.as_ref()))
                .await
        }

        /// Delete all tags. Use with care. After this, all data will be garbage collected.
        pub async fn delete_all(&self) -> Result<()> {
            self.delete_with_opts(DeleteOptions {
                from: None,
                to: None,
            })
            .await
        }

        /// Rename a tag atomically
        ///
        /// If the tag does not exist, this will return an error.
        pub async fn rename_with_opts(&self, options: RenameOptions) -> Result<()> {
            let (tx, rx) = oneshot::channel();
            self.sender.send(RenameTag { options, tx }.into()).await?;
            rx.await?
        }

        /// Rename a tag atomically
        ///
        /// If the tag does not exist, this will return an error.
        pub async fn rename(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Result<()> {
            self.rename_with_opts(RenameOptions {
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
        let (tx, rx) = mpsc::channel(32);
        self.sender.try_send(ImportBytes { data, tx }.into()).ok();
        ImportResult { rx }
    }

    pub fn import_path(&self, path: impl AsRef<Path>) -> ImportResult {
        let (tx, rx) = mpsc::channel(32);
        self.sender
            .try_send(
                ImportPath {
                    path: path.as_ref().to_owned(),
                    tx,
                    mode: ImportMode::Copy,
                    format: crate::BlobFormat::Raw,
                }
                .into(),
            )
            .ok();
        ImportResult { rx }
    }

    pub fn import_byte_stream(
        &self,
        data: impl Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    ) -> ImportResult {
        self.import_byte_stream_impl(Box::pin(data))
    }

    fn import_byte_stream_impl(
        &self,
        data: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>,
    ) -> ImportResult {
        let (tx, rx) = mpsc::channel(32);
        self.sender
            .try_send(ImportByteStream { data, tx }.into())
            .ok();
        ImportResult { rx }
    }

    pub fn export_bao(&self, hash: Hash, ranges: ChunkRanges) -> ExportBaoResult {
        let (tx, rx) = mpsc::channel(32);
        self.sender
            .try_send(ExportBao { hash, ranges, tx }.into())
            .ok();
        ExportBaoResult { rx }
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
    pub fn observe(&self, hash: impl Into<Hash>) -> ObserveResult {
        let hash = hash.into();
        if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
            let (tx, rx) = mpsc::channel(1);
            tx.try_send(Bitfield::complete(0)).ok();
            return ObserveResult { rx };
        }
        let (tx, rx) = mpsc::channel(32);
        self.sender
            .try_send(
                Observe {
                    hash,
                    out: Observer::new(tx),
                }
                .into(),
            )
            .ok();
        ObserveResult { rx }
    }

    pub fn export_path(&self, hash: Hash, target: impl AsRef<Path>) -> ExportPathResult {
        let (tx, rx) = mpsc::channel(32);
        self.sender
            .try_send(
                ExportPath {
                    hash,
                    mode: ExportMode::Copy,
                    target: target.as_ref().to_owned(),
                    out: tx,
                }
                .into(),
            )
            .ok();
        ExportPathResult { rx }
    }

    // todo: export_path_with_opts
    async fn import_bao_reader<R: AsyncStreamReader>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> anyhow::Result<R> {
        let (tx, rx) = mpsc::channel(32);
        let (out, out_receiver) = oneshot::channel();
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
        self.sender.try_send(
            ImportBao {
                hash,
                size,
                rx,
                tx: out,
            }
            .into(),
        )?;
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
        out_receiver.await??;
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
        self.sender
            .send(
                ImportBao {
                    hash,
                    size,
                    rx: data,
                    tx,
                }
                .into(),
            )
            .await?;
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
        let (tx, rx) = oneshot::channel();
        self.sender.send(SyncDb { tx }.into()).await?;
        rx.await??;
        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Shutdown { tx }.into()).await?;
        rx.await?;
        Ok(())
    }
}

pub struct ImportResult {
    rx: mpsc::Receiver<ImportProgress>,
}

impl Future for ImportResult {
    type Output = anyhow::Result<Hash>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(ImportProgress::Done { hash })) => break Poll::Ready(Ok(hash)),
                Poll::Ready(Some(ImportProgress::Error { cause })) => {
                    break Poll::Ready(Err(anyhow::anyhow!(
                        "import task ended unexpectedly {}",
                        cause
                    )))
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => {
                    break Poll::Ready(Err(anyhow::anyhow!("import task ended unexpectedly")))
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

impl Stream for ImportResult {
    type Item = anyhow::Result<ImportProgress>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(ImportProgress::Error { cause })) => Poll::Ready(Some(Err(cause))),
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An infinite stream of bitfields, where the first is the current state
/// and all following are updates. Once a blob is complete, there will be no
/// more updates.
pub struct ObserveResult {
    rx: mpsc::Receiver<Bitfield>,
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
    rx: mpsc::Receiver<ExportProgress>,
}

impl Stream for ExportPathResult {
    type Item = ExportProgress;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Future for ExportPathResult {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(ExportProgress::Done)) => break Poll::Ready(Ok(())),
                Poll::Ready(Some(ExportProgress::Error { cause })) => {
                    break Poll::Ready(Err(cause))
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => {
                    break Poll::Ready(Err(anyhow::anyhow!("export task ended unexpectedly")))
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

pub struct ExportBaoResult {
    rx: mpsc::Receiver<EncodedItem>,
}

impl ExportBaoResult {
    pub async fn bao_to_vec(self) -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    pub async fn data_to_bytes(mut self) -> io::Result<Bytes> {
        let mut data = Vec::new();
        while let Some(item) = self.rx.recv().await {
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

    pub async fn data_to_vec(mut self) -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        while let Some(item) = self.rx.recv().await {
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

    pub async fn write_quinn(mut self, target: &mut quinn::SendStream) -> io::Result<()> {
        while let Some(item) = self.rx.recv().await {
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
        self.filter_map(|item| match item {
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
}

impl Future for ExportBaoResult {
    type Output = Result<(), EncodeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(EncodedItem::Done)) => break Poll::Ready(Ok(())),
                Poll::Ready(Some(EncodedItem::Error(cause))) => break Poll::Ready(Err(cause)),
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => {
                    break Poll::Ready(Err(EncodeError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "export task ended unexpectedly",
                    ))))
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

impl Stream for ExportBaoResult {
    type Item = EncodedItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
