use std::{
    collections::HashMap,
    future::Future,
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use bao_tree::{
    blake3::{self, Hash},
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        mixed::{traverse_ranges_validated, EncodedItem},
        outboard::PreOrderMemOutboard,
        sync::{Outboard, ReadAt, WriteAt},
        BaoContentItem, EncodeError,
    },
    BaoTree, ChunkNum, ChunkRanges, TreeNode,
};
use bytes::Bytes;
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use n0_future::{stream, Stream, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{error::TrySendError, OwnedPermit},
    task::JoinSet,
};

use crate::{
    bitfield::{BaoBlobSizeOpt, BitfieldEvent, BitfieldState, BitfieldUpdate},
    util::sparse_mem_file::SparseMemFile,
    ExportProgress, ImportProgress, IROH_BLOCK_SIZE,
};

/// Keep track of the most precise size we know of.
///
/// When in memory, we don't have to write the size for every chunk to a separate
/// slot, but can just keep the best one.
#[derive(Debug, Default)]
pub struct SizeInfo {
    pub offset: u64,
    pub size: u64,
}

impl SizeInfo {
    /// Create a new size info for a complete file of size `size`.
    pub(crate) fn complete(size: u64) -> Self {
        let mask = (1 << IROH_BLOCK_SIZE.chunk_log()) - 1;
        // offset of the last bao chunk in a file of size `size`
        let last_chunk_offset = size & mask;
        Self {
            offset: last_chunk_offset,
            size,
        }
    }

    /// Write a size at the given offset. The size at the highest offset is going to be kept.
    fn write(&mut self, offset: u64, size: u64) {
        // >= instead of > because we want to be able to update size 0, the initial value.
        if offset >= self.offset {
            self.offset = offset;
            self.size = size;
        }
    }

    /// The current size, representing the most correct size we know.
    pub fn current_size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug)]
struct ImportBao {
    hash: Hash,
    size: u64,
    data: tokio::sync::mpsc::Receiver<BaoContentItem>,
    out: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug)]
struct Observe {
    hash: Hash,
    out: tokio::sync::mpsc::Sender<BitfieldEvent>,
}

#[derive(Debug)]
struct ImportBytes {
    data: Bytes,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
}

#[derive(Debug)]
struct ExportBao {
    hash: Hash,
    ranges: ChunkRanges,
    out: tokio::sync::mpsc::Sender<EncodedItem>,
}

#[derive(Debug)]
struct ExportPath {
    hash: Hash,
    target: PathBuf,
    out: tokio::sync::mpsc::Sender<ExportProgress>,
}

type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

struct ImportByteStream {
    data: BoxedByteStream,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
}

impl std::fmt::Debug for ImportByteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportByteStream").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ImportPath {
    path: PathBuf,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
}

#[derive(Debug, derive_more::From)]
enum Command {
    ImportBao(ImportBao),
    ExportBao(ExportBao),
    Observe(Observe),
    ImportBytes(ImportBytes),
    ImportByteStream(ImportByteStream),
    ImportPath(ImportPath),
    ExportPath(ExportPath),
}

#[derive(Debug, Clone)]
pub struct Store {
    sender: tokio::sync::mpsc::Sender<Command>,
}

impl Store {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        tokio::spawn(
            Actor {
                commands: receiver,
                bao_tasks: JoinSet::new(),
                import_tasks: JoinSet::new(),
                state: State {
                    data: HashMap::new(),
                },
            }
            .run(),
        );
        Self { sender }
    }

    pub fn import_bytes(&self, data: bytes::Bytes) -> ImportResult {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(ImportBytes { data, out: sender }.into())
            .ok();
        ImportResult { receiver }
    }

    pub fn import_byte_stream(
        &self,
        data: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>,
    ) -> ImportResult {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(ImportByteStream { data, out: sender }.into())
            .ok();
        ImportResult { receiver }
    }

    pub fn export_bao(&self, hash: Hash, ranges: ChunkRanges) -> ExportBaoResult {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(
                ExportBao {
                    hash,
                    ranges,
                    out: sender,
                }
                .into(),
            )
            .ok();
        ExportBaoResult { receiver }
    }

    pub fn observe(&self, hash: Hash) -> ObserveResult {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        self.sender
            .try_send(Observe { hash, out: sender }.into())
            .ok();
        ObserveResult { receiver }
    }

    async fn import_bao_reader(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut stream: impl AsyncStreamReader,
    ) -> anyhow::Result<()> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let (out, out_receiver) = tokio::sync::oneshot::channel();
        let size = u64::from_le_bytes(stream.read::<8>().await?);
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash, ranges, tree, stream);
        self.sender.try_send(
            ImportBao {
                hash,
                size,
                data: receiver,
                out,
            }
            .into(),
        )?;
        loop {
            match decoder.next().await {
                ResponseDecoderNext::More((rest, item)) => {
                    sender.send(item?).await?;
                    decoder = rest;
                }
                ResponseDecoderNext::Done(_) => break,
            };
        }
        drop(sender);
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
}

pub struct ImportResult {
    receiver: tokio::sync::mpsc::Receiver<ImportProgress>,
}

impl Future for ImportResult {
    type Output = anyhow::Result<Hash>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(ImportProgress::Done { hash })) => break Poll::Ready(Ok(hash)),
                Poll::Ready(Some(ImportProgress::Error { cause })) => {
                    break Poll::Ready(Err(cause))
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
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(ImportProgress::Error { cause })) => Poll::Ready(Some(Err(cause))),
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct ObserveResult {
    receiver: tokio::sync::mpsc::Receiver<BitfieldEvent>,
}

impl Stream for ObserveResult {
    type Item = BitfieldEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct ExportBaoResult {
    receiver: tokio::sync::mpsc::Receiver<EncodedItem>,
}

impl ExportBaoResult {
    pub async fn to_vec(self) -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.to_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    pub async fn write_quinn(mut self, mut target: quinn::SendStream) -> io::Result<()> {
        while let Some(item) = self.receiver.recv().await {
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
            match self.receiver.poll_recv(cx) {
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
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl super::Store for Store {
    fn import_bao(
        &self,
        hash: Hash,
        size: u64,
        data: tokio::sync::mpsc::Receiver<BaoContentItem>,
        out: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    ) -> bool {
        self.sender
            .try_send(
                ImportBao {
                    hash,
                    size,
                    data,
                    out,
                }
                .into(),
            )
            .is_ok()
    }

    fn export_bao(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        out: tokio::sync::mpsc::Sender<EncodedItem>,
    ) -> bool {
        self.sender
            .try_send(ExportBao { hash, ranges, out }.into())
            .is_ok()
    }

    fn observe(&self, hash: Hash, out: tokio::sync::mpsc::Sender<BitfieldEvent>) -> bool {
        self.sender.try_send(Observe { hash, out }.into()).is_ok()
    }

    fn import_bytes(
        &self,
        data: bytes::Bytes,
        out: tokio::sync::mpsc::Sender<crate::ImportProgress>,
    ) -> bool {
        self.sender
            .try_send(ImportBytes { data, out }.into())
            .is_ok()
    }
}

struct Actor {
    commands: tokio::sync::mpsc::Receiver<Command>,
    bao_tasks: JoinSet<()>,
    import_tasks: JoinSet<anyhow::Result<ImportEntry>>,
    state: State,
}

impl Actor {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
                    match cmd {
                        Command::ImportBao(ImportBao { hash, size, data, out}) => {
                            let entry = self.state.data.entry(hash).or_default();
                            self.bao_tasks.spawn(import_bao_task(entry.clone(), size, data, out));
                        }
                        Command::Observe(Observe { hash, out }) => {
                            let entry = self.state.data.entry(hash).or_default();
                            let mut entry = entry.write().unwrap();
                            if out.try_send(BitfieldState {
                                ranges: entry.bitfield.clone(),
                                size: BaoBlobSizeOpt::Unverified(entry.size.current_size()),
                            }.into()).is_ok() {
                                entry.observers.push(out);
                            }
                        }
                        Command::ImportBytes(ImportBytes { data, out }) => {
                            self.import_tasks.spawn(import_bytes_task(data, out));
                        }
                        Command::ImportByteStream(ImportByteStream { data, out }) => {
                            self.import_tasks.spawn(import_byte_stream_task(data, out));
                        }
                        Command::ImportPath(ImportPath { path, out }) => {
                            self.import_tasks.spawn(import_path_task(path, out));
                        }
                        Command::ExportBao(ExportBao { hash, ranges, out }) => {
                            let entry = self.state.data.entry(hash).or_default();
                            self.bao_tasks.spawn(export_bao_task(hash, entry.clone(), ranges, out));
                        }
                        Command::ExportPath(ExportPath { hash, target, out }) => {
                            let entry = self.state.data.get(&hash).cloned();
                            self.bao_tasks.spawn(export_path_task(entry, target, out));
                        }
                    }
                }
                Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
                    let import_data = match res {
                        Ok(Ok(entry)) => {
                            entry
                        },
                        Ok(Err(e)) => {
                            tracing::error!("import failed: {e}");
                            continue;
                        },
                        Err(e) => {
                            tracing::error!("import task failed: {e}");
                            continue;
                        }
                    };
                    let hash = import_data.outboard.root();
                    let entry = self.state.data.entry(hash).or_default();
                    let mut entry = entry.write().unwrap();
                    let size = import_data.data.len() as u64;
                    entry.size = SizeInfo::complete(size);
                    entry.data = SparseMemFile::from(import_data.data.to_vec());
                    entry.outboard = SparseMemFile::from(import_data.outboard.data);
                    import_data.out.send(ImportProgress::Done { hash });
                    if entry.observers.is_empty() {
                        continue;
                    }
                    let added = ChunkRanges::from(.. ChunkNum::full_chunks(size));
                    let added = &added - &entry.bitfield;
                    // todo: also trigger event when verification status changes?
                    // is that even needed? A verification status change can only happen
                    // when there is also a bitmap change.
                    if added.is_empty() {
                        continue;
                    }
                    let update = BitfieldUpdate {
                        added,
                        removed: ChunkRanges::empty(),
                        size: BaoBlobSizeOpt::Verified(size),
                    };
                    entry.observers.retain(|sender| {
                        sender.try_send(BitfieldEvent::Update(update.clone())).is_ok()
                    });
                }
                Some(res) = self.bao_tasks.join_next(), if !self.bao_tasks.is_empty() => {
                    if let Err(e) = res {
                        tracing::error!("task failed: {e}");
                    }
                }
            }
        }
    }
}

async fn import_bao_task(
    entry: Arc<RwLock<HashData>>,
    size: u64,
    mut stream: tokio::sync::mpsc::Receiver<BaoContentItem>,
    out: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
) {
    entry.write().unwrap().size.write(0, size);
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    while let Some(item) = stream.recv().await {
        let mut entry = entry.write().unwrap();
        match item {
            BaoContentItem::Parent(parent) => {
                if let Some(offset) = tree.pre_order_offset(parent.node) {
                    let mut pair = [0u8; 64];
                    pair[..32].copy_from_slice(parent.pair.0.as_bytes());
                    pair[32..].copy_from_slice(parent.pair.1.as_bytes());
                    entry
                        .outboard
                        .write_at(offset * 64, &pair)
                        .expect("writing to mem can never fail");
                }
            }
            BaoContentItem::Leaf(leaf) => {
                let start = leaf.offset;
                let end = start + (leaf.data.len() as u64);
                entry
                    .data
                    .write_at(start, &leaf.data)
                    .expect("writing to mem can never fail");
                if entry.observers.is_empty() {
                    continue;
                }
                let added = ChunkRanges::from(ChunkNum::chunks(start)..ChunkNum::full_chunks(end));
                let added = &added - &entry.bitfield;
                if added.is_empty() {
                    continue;
                }
                let update = BitfieldUpdate {
                    added,
                    removed: ChunkRanges::empty(),
                    size: BaoBlobSizeOpt::Unverified(size),
                };
                entry.observers.retain(|sender| {
                    sender
                        .try_send(BitfieldEvent::Update(update.clone()))
                        .is_ok()
                });
            }
        }
    }
    out.send(Ok(())).ok();
}

async fn export_bao_task(
    hash: Hash,
    entry: Arc<RwLock<HashData>>,
    ranges: ChunkRanges,
    sender: tokio::sync::mpsc::Sender<EncodedItem>,
) {
    let size = entry.read().unwrap().size.current_size();
    let data = ExportData {
        data: entry.clone(),
    };
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let outboard = ExportOutboard {
        hash,
        tree,
        data: entry.clone(),
    };
    traverse_ranges_validated(data, outboard, &ranges, &sender).await
}

async fn import_bytes_task(
    data: Bytes,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    out.send(ImportProgress::Size {
        size: data.len() as u64,
    })
    .await?;
    out.send(ImportProgress::CopyDone).await?;
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let out = out.reserve_owned().await?;
    Ok(ImportEntry {
        data,
        outboard,
        out,
    })
}

async fn import_byte_stream_task(
    mut data: BoxedByteStream,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let mut res = Vec::new();
    while let Some(item) = data.next().await {
        let item = item?;
        res.extend_from_slice(&item);
        match out.try_send(ImportProgress::CopyProgress {
            offset: res.len() as u64,
        }) {
            Ok(()) => (),
            Err(e @ TrySendError::Closed(_)) => return Err(e)?,
            Err(TrySendError::Full(_)) => continue,
        }
    }
    import_bytes_task(res.into(), out).await
}

async fn import_path_task(
    path: PathBuf,
    out: tokio::sync::mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let mut res = Vec::new();
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = [0u8; 1024 * 64];
    loop {
        let size = file.read(&mut buf).await?;
        if size == 0 {
            break;
        }
        res.extend_from_slice(&buf[..size]);
        out.send(ImportProgress::CopyProgress {
            offset: res.len() as u64,
        })
        .await?;
    }
    import_bytes_task(res.into(), out).await
}

async fn export_path_task(
    entry: Option<Arc<RwLock<HashData>>>,
    target: PathBuf,
    out: tokio::sync::mpsc::Sender<ExportProgress>,
) {
    let Some(entry) = entry else {
        out.send(ExportProgress::Error {
            cause: anyhow::anyhow!("hash not found"),
        })
        .await
        .ok();
        return;
    };
    match export_path_impl(entry, target, &out).await {
        Ok(()) => out.send(ExportProgress::Done).await.ok(),
        Err(e) => out.send(ExportProgress::Error { cause: e }).await.ok(),
    };
}

async fn export_path_impl(
    entry: Arc<RwLock<HashData>>,
    target: PathBuf,
    out: &tokio::sync::mpsc::Sender<ExportProgress>,
) -> anyhow::Result<()> {
    let mut file = std::fs::File::create(&target)?;
    let size = entry.read().unwrap().size.current_size();
    out.send(ExportProgress::Size { size }).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        entry.read().unwrap().data.read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        match out.try_send(ExportProgress::CopyProgress {
            offset: offset as u64,
        }) {
            Ok(()) => (),
            Err(e @ TrySendError::Closed(_)) => return Err(e.into()),
            Err(TrySendError::Full(_)) => continue,
        }
    }
    Ok(())
}

struct ImportEntry {
    data: Bytes,
    outboard: PreOrderMemOutboard,
    out: OwnedPermit<ImportProgress>,
}

struct ExportOutboard {
    hash: Hash,
    tree: BaoTree,
    data: Arc<RwLock<HashData>>,
}

struct ExportData {
    data: Arc<RwLock<HashData>>,
}

impl ReadAt for ExportData {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let entry = self.data.read().unwrap();
        entry.data.read_at(offset, buf)
    }
}

impl Outboard for ExportOutboard {
    fn root(&self) -> Hash {
        self.hash
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return Ok(None);
        };
        let mut buf = [0u8; 64];
        let size = self
            .data
            .read()
            .unwrap()
            .outboard
            .read_at(offset * 64, &mut buf)?;
        if size != 64 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
        }
        let left: [u8; 32] = buf[..32].try_into().unwrap();
        let right: [u8; 32] = buf[32..].try_into().unwrap();
        Ok(Some((left.into(), right.into())))
    }
}

struct State {
    data: HashMap<Hash, Arc<RwLock<HashData>>>,
}

#[derive(Debug)]
struct HashData {
    data: SparseMemFile,
    outboard: SparseMemFile,
    size: SizeInfo,
    bitfield: ChunkRanges,
    observers: Vec<tokio::sync::mpsc::Sender<BitfieldEvent>>,
}

impl Default for HashData {
    fn default() -> Self {
        Self {
            data: Default::default(),
            outboard: Default::default(),
            size: SizeInfo::default(),
            bitfield: ChunkRanges::empty(),
            observers: Vec::new(),
        }
    }
}

fn print_outboard(hashes: &[u8]) {
    assert!(hashes.len() % 64 == 0);
    for chunk in hashes.chunks(64) {
        let left: [u8; 32] = chunk[..32].try_into().unwrap();
        let right: [u8; 32] = chunk[32..].try_into().unwrap();
        let left = blake3::Hash::from(left);
        let right = blake3::Hash::from(right);
        println!("l: {:?}, r: {:?}", left, right);
    }
}

#[cfg(test)]
mod tests {
    use n0_future::StreamExt;
    use testresult::TestResult;

    use super::*;

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        let store = Store::new();
        let hash = store.import_bytes(vec![0u8; 1024 * 64].into()).await?;
        println!("hash: {:?}", hash);
        let mut stream = store.export_bao(hash, ChunkRanges::all());
        while let Some(item) = stream.next().await {
            println!("item: {:?}", item);
        }
        let stream = store.export_bao(hash, ChunkRanges::all());
        let exported = stream.to_vec().await?;

        let store2 = Store::new();
        let mut or = store2.observe(hash);
        tokio::spawn(async move {
            while let Some(event) = or.next().await {
                println!("event: {:?}", event);
            }
        });
        store2
            .import_bao_bytes(hash, ChunkRanges::all(), exported.clone().into())
            .await?;

        let exported2 = store2.export_bao(hash, ChunkRanges::all()).to_vec().await?;
        assert_eq!(exported, exported2);

        Ok(())
    }
}
