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
    blake3::Hash,
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        mixed::EncodedItem,
        EncodeError,
    },
    BaoTree, ChunkRanges,
};
use bytes::Bytes;
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use n0_future::{Stream, StreamExt};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tracing::trace;

use crate::{
    bitfield::Bitfield,
    proto::*,
    util::{
        observer::{Aggregator, Observer},
        SliceInfoExt, Tag,
    },
    HashAndFormat, Store, IROH_BLOCK_SIZE,
};

impl Store {
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
        ImportResult { rx: rx }
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
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(ImportByteStream { data, tx }.into())
            .ok();
        ImportResult { rx }
    }

    pub fn export_bao(&self, hash: Hash, ranges: ChunkRanges) -> ExportBaoResult {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(ExportBao { hash, ranges, tx }.into())
            .ok();
        ExportBaoResult { rx }
    }

    /// Helper to just export the entire blob into a Bytes
    pub async fn export_bytes(&self, hash: impl Into<Hash>) -> io::Result<Bytes> {
        self.export_bao(hash.into(), ChunkRanges::all())
            .data_to_vec()
            .await
            .map(Bytes::from)
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
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Bitfield::complete(0)).ok();
            return ObserveResult { rx };
        }
        let (tx, rx) = tokio::sync::mpsc::channel(32);
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
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(
                ExportPath {
                    hash,
                    target: target.as_ref().to_owned(),
                    out: tx,
                }
                .into(),
            )
            .ok();
        ExportPathResult { rx }
    }

    /// todo: export_path_with_opts

    async fn import_bao_reader(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut stream: impl AsyncStreamReader,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let (out, out_receiver) = tokio::sync::oneshot::channel();
        let size = u64::from_le_bytes(stream.read::<8>().await?);
        let Some(size) = NonZeroU64::new(size) else {
            // todo: drain stream here?
            if hash.as_bytes() == crate::Hash::EMPTY.as_bytes() {
                return Ok(());
            } else {
                return Err(anyhow::anyhow!("invalid size for hash"));
            }
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash, ranges, tree, stream);
        self.sender.try_send(
            ImportBao {
                hash,
                size,
                rx,
                tx: out,
            }
            .into(),
        )?;
        loop {
            match decoder.next().await {
                ResponseDecoderNext::More((rest, item)) => {
                    tx.send(item?).await?;
                    decoder = rest;
                }
                ResponseDecoderNext::Done(_) => break,
            };
        }
        drop(tx);
        out_receiver.await??;
        Ok(())
    }

    pub async fn import_bao_quinn(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        stream: &mut quinn::RecvStream,
    ) -> anyhow::Result<()> {
        let reader = TokioStreamReader::new(stream);
        self.import_bao_reader(hash, ranges, reader).await
    }

    pub async fn import_bao_bytes(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        data: Bytes,
    ) -> anyhow::Result<()> {
        self.import_bao_reader(hash, ranges, data).await
    }

    pub async fn set_tag(&self, tag: Tag, value: HashAndFormat) -> anyhow::Result<()> {
        self.set_tag_impl(tag, Some(value)).await
    }

    async fn set_tag_impl(&self, tag: Tag, value: Option<HashAndFormat>) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(SetTag { tag, value, tx }.into()).await?;
        rx.await??;
        Ok(())
    }

    pub async fn sync_db(&self) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(SyncDb { tx }.into()).await?;
        rx.await??;
        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Shutdown { tx }.into()).await?;
        rx.await?;
        Ok(())
    }
}

pub struct ImportResult {
    rx: tokio::sync::mpsc::Receiver<ImportProgress>,
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
    rx: tokio::sync::mpsc::Receiver<ExportProgress>,
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
    rx: tokio::sync::mpsc::Receiver<EncodedItem>,
}

impl ExportBaoResult {
    pub async fn bao_to_vec(self) -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.to_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
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

    pub async fn write_quinn(mut self, mut target: quinn::SendStream) -> io::Result<()> {
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

    pub fn to_byte_stream(self) -> impl Stream<Item = io::Result<Bytes>> {
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
