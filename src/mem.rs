use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Write},
    num::NonZeroU64,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use bao_tree::{
    blake3::{self, Hash},
    io::{
        mixed::{traverse_ranges_validated, EncodedItem},
        outboard::PreOrderMemOutboard,
        sync::{Outboard, ReadAt, WriteAt},
        BaoContentItem,
    },
    BaoTree, ChunkNum, ChunkRanges, TreeNode,
};
use bytes::Bytes;
use n0_future::{future::yield_now, StreamExt};
use tokio::{
    io::AsyncReadExt,
    sync::mpsc::{self, error::TrySendError, OwnedPermit},
    task::{JoinError, JoinSet},
};
use tracing::error;

use crate::{
    bitfield::{BaoBlobSize, BaoBlobSizeOpt, BitfieldEvent, BitfieldState, BitfieldUpdate},
    proto::*,
    util::{SparseMemFile, Tag},
    HashAndFormat, Store, IROH_BLOCK_SIZE,
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

impl Store {
    pub fn memory() -> Self {
        let (sender, receiver) = mpsc::channel(32);
        tokio::spawn(
            Actor {
                commands: receiver,
                unit_tasks: JoinSet::new(),
                import_tasks: JoinSet::new(),
                state: State {
                    data: HashMap::new(),
                    tags: BTreeMap::new(),
                },
            }
            .run(),
        );
        Self::from_sender(sender)
    }
}

struct Actor {
    commands: mpsc::Receiver<Command>,
    unit_tasks: JoinSet<()>,
    import_tasks: JoinSet<anyhow::Result<ImportEntry>>,
    state: State,
}

impl Actor {
    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::ImportBao(ImportBao {
                hash,
                size,
                data,
                out,
            }) => {
                let entry = self.state.data.entry(hash).or_default();
                self.unit_tasks
                    .spawn(import_bao_task(entry.clone(), size, data, out));
            }
            Command::Observe(Observe { hash, out }) => {
                let entry = self.state.data.entry(hash).or_default();
                let mut entry = entry.write().unwrap();
                if out
                    .try_send(
                        BitfieldState {
                            ranges: entry.bitfield().clone(),
                            size: entry.size().into(),
                        }
                        .into(),
                    )
                    .is_ok()
                {
                    entry.observers().push(out);
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
                self.unit_tasks
                    .spawn(export_bao_task(hash, entry.clone(), ranges, out));
            }
            Command::ExportPath(ExportPath { hash, target, out }) => {
                let entry = self.state.data.get(&hash).cloned();
                self.unit_tasks.spawn(export_path_task(entry, target, out));
            }
            Command::Tags(Tags { tx: out }) => {
                let items = self
                    .state
                    .tags
                    .iter()
                    .map(|(tag, hash)| (tag.clone(), hash.clone()))
                    .collect::<Vec<_>>();
                out.send(Ok(items)).ok();
            }
            Command::SetTag(SetTag {
                tag,
                value,
                tx: out,
            }) => {
                if let Some(value) = value {
                    self.state.tags.insert(tag, value);
                } else {
                    self.state.tags.remove(&tag);
                }
                out.send(Ok(())).ok();
            }
            Command::CreateTag(CreateTag { hash, tx }) => {
                let tag = Tag::auto(SystemTime::now(), |tag| self.state.tags.contains_key(tag));
                self.state.tags.insert(tag.clone(), hash);
                tx.send(Ok(tag)).ok();
            }
        }
    }

    fn finish_import(&mut self, res: Result<anyhow::Result<ImportEntry>, JoinError>) {
        let import_data = match res {
            Ok(Ok(entry)) => entry,
            Ok(Err(e)) => {
                tracing::error!("import failed: {e}");
                return;
            }
            Err(e) => {
                if e.is_cancelled() {
                    tracing::warn!("import task failed: {e}");
                } else {
                    tracing::error!("import task panicked: {e}");
                }
                return;
            }
        };
        let hash = import_data.outboard.root();
        let entry = self.state.data.entry(hash).or_default();
        let mut entry = entry.write().unwrap();
        let size = import_data.data.len() as u64;
        let old_bitfield = entry.bitfield().clone();
        import_data.out.send(ImportProgress::Done { hash });
        *entry = CompleteEntry::new(import_data.data, import_data.outboard.data.into()).into();
        if entry.observers().is_empty() {
            return;
        }
        let added = ChunkRanges::from(..ChunkNum::full_chunks(size));
        let added = &added - old_bitfield;
        // todo: also trigger event when verification status changes?
        // is that even needed? A verification status change can only happen
        // when there is also a bitmap change.
        if added.is_empty() {
            return;
        }
        let update = BitfieldUpdate {
            added,
            removed: ChunkRanges::empty(),
            size: BaoBlobSizeOpt::Verified(size),
        };
        entry.observers().retain(|sender| {
            sender
                .try_send(BitfieldEvent::Update(update.clone()))
                .is_ok()
        });
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        // last sender has been dropped.
                        // exit immediately.
                        break;
                    };
                    self.handle_command(cmd);
                }
                Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
                    self.finish_import(res);
                }
                Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
                    self.log_unit_task(res);
                }
            }
        }
    }
}

async fn import_bao_task(
    entry: Arc<RwLock<Entry>>,
    size: u64,
    mut stream: mpsc::Receiver<BaoContentItem>,
    out: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
) {
    if let Some(entry) = entry.write().unwrap().incomplete_mut() {
        entry.size.write(0, size);
    }
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    while let Some(item) = stream.recv().await {
        let mut guard = entry.write().unwrap();
        let Some(entry) = guard.incomplete_mut() else {
            // entry was completed somewhere else, no need to write
            break;
        };
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
                entry.bitfield |= added.clone();
                let update = BitfieldUpdate {
                    added,
                    removed: ChunkRanges::empty(),
                    size: BaoBlobSize::from_size_and_ranges(
                        NonZeroU64::try_from(size).unwrap(),
                        &entry.bitfield,
                    )
                    .into(),
                };
                entry.observers.retain(|sender| {
                    sender
                        .try_send(BitfieldEvent::Update(update.clone()))
                        .is_ok()
                });
                if entry.bitfield == ChunkRanges::from(..ChunkNum::chunks(size)) {
                    let data = std::mem::take(&mut entry.data);
                    let outboard = std::mem::take(&mut entry.outboard);
                    let data: Bytes = <Vec<u8>>::try_from(data).unwrap().into();
                    let outboard: Bytes = <Vec<u8>>::try_from(outboard).unwrap().into();
                    *guard = CompleteEntry::new(data, outboard).into();
                }
            }
        }
    }
    out.send(Ok(())).ok();
}

async fn export_bao_task(
    hash: Hash,
    entry: Arc<RwLock<Entry>>,
    ranges: ChunkRanges,
    sender: mpsc::Sender<EncodedItem>,
) {
    let size = entry.read().unwrap().size().value();
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
    out: mpsc::Sender<ImportProgress>,
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
    out: mpsc::Sender<ImportProgress>,
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
    out: mpsc::Sender<ImportProgress>,
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
    entry: Option<Arc<RwLock<Entry>>>,
    target: PathBuf,
    out: mpsc::Sender<ExportProgress>,
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
    entry: Arc<RwLock<Entry>>,
    target: PathBuf,
    out: &mpsc::Sender<ExportProgress>,
) -> anyhow::Result<()> {
    // todo: for partial entries make sure to only write the part that is actually present
    let mut file = std::fs::File::create(&target)?;
    let size = entry.read().unwrap().size().value();
    out.send(ExportProgress::Size { size }).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        entry.read().unwrap().data().read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        match out.try_send(ExportProgress::CopyProgress {
            offset: offset as u64,
        }) {
            Ok(()) => (),
            Err(e @ TrySendError::Closed(_)) => return Err(e.into()),
            Err(TrySendError::Full(_)) => continue,
        }
        yield_now().await;
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
    data: Arc<RwLock<Entry>>,
}

struct ExportData {
    data: Arc<RwLock<Entry>>,
}

impl ReadAt for ExportData {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let entry = self.data.read().unwrap();
        entry.data().read_at(offset, buf)
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
            .outboard()
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
    data: HashMap<Hash, Arc<RwLock<Entry>>>,
    tags: BTreeMap<Tag, HashAndFormat>,
}

#[derive(Debug, derive_more::From)]
enum Entry {
    Incomplete(IncompleteEntry),
    Complete(CompleteEntry),
}

impl Default for Entry {
    fn default() -> Self {
        Self::Incomplete(Default::default())
    }
}

impl Entry {
    fn data(&self) -> &[u8] {
        match self {
            Self::Incomplete(entry) => entry.data.as_ref(),
            Self::Complete(entry) => &entry.data,
        }
    }

    fn outboard(&self) -> &[u8] {
        match self {
            Self::Incomplete(entry) => entry.outboard.as_ref(),
            Self::Complete(entry) => &entry.outboard,
        }
    }

    fn size(&self) -> BaoBlobSize {
        match self {
            Self::Incomplete(entry) => entry.size(),
            Self::Complete(entry) => entry.size(),
        }
    }

    fn bitfield(&self) -> &ChunkRanges {
        match self {
            Self::Incomplete(entry) => entry.bitfield(),
            Self::Complete(entry) => entry.bitfield(),
        }
    }

    fn incomplete_mut(&mut self) -> Option<&mut IncompleteEntry> {
        match self {
            Self::Incomplete(entry) => Some(entry),
            Self::Complete(_) => None,
        }
    }

    fn observers(&mut self) -> &mut Vec<mpsc::Sender<BitfieldEvent>> {
        match self {
            Self::Incomplete(entry) => &mut entry.observers,
            Self::Complete(entry) => &mut entry.observers,
        }
    }
}

/// An incomplete entry, with all the logic to keep track of the state of the entry
/// and for observing changes.
#[derive(Debug)]
pub(crate) struct IncompleteEntry {
    pub(crate) data: SparseMemFile,
    pub(crate) outboard: SparseMemFile,
    pub(crate) size: SizeInfo,
    bitfield: ChunkRanges,
    observers: Vec<mpsc::Sender<BitfieldEvent>>,
}

impl IncompleteEntry {
    pub fn size(&self) -> BaoBlobSize {
        let size = self.size.current_size();
        if let Some(size) = NonZeroU64::new(size) {
            return BaoBlobSize::from_size_and_ranges(size, &self.bitfield);
        } else {
            return BaoBlobSize::Verified(size);
        }
    }

    fn bitfield(&self) -> &ChunkRanges {
        &self.bitfield
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompleteEntry {
    data: Bytes,
    outboard: Bytes,
    bitfield: ChunkRanges,
    // these observers observe nothing, this is just to keep them alive
    observers: Vec<mpsc::Sender<BitfieldEvent>>,
}

impl CompleteEntry {
    pub fn create(data: Bytes) -> (blake3::Hash, Self) {
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = outboard.root();
        let outboard = outboard.data.into();
        let entry = Self::new(data, outboard);
        (hash, entry)
    }

    pub fn new(data: Bytes, outboard: Bytes) -> Self {
        let size = data.len() as u64;
        let bitfield = ChunkRanges::from(..ChunkNum::chunks(size));
        Self {
            data,
            outboard,
            bitfield,
            observers: Vec::new(),
        }
    }

    pub fn size(&self) -> BaoBlobSize {
        let size = self.data.len() as u64;
        BaoBlobSize::Verified(size)
    }

    pub fn bitfield(&self) -> &ChunkRanges {
        &self.bitfield
    }

    pub fn data(&self) -> impl AsRef<[u8]> + '_ {
        self.data.as_ref()
    }

    pub fn outboard(&self) -> impl AsRef<[u8]> + '_ {
        self.outboard.as_ref()
    }

    pub fn observers(&mut self) -> &mut Vec<mpsc::Sender<BitfieldEvent>> {
        &mut self.observers
    }
}

impl Default for IncompleteEntry {
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
        let store = Store::memory();
        let hash = store.import_bytes(vec![0u8; 1024 * 64].into()).await?;
        println!("hash: {:?}", hash);
        let mut stream = store.export_bao(hash, ChunkRanges::all());
        while let Some(item) = stream.next().await {
            println!("item: {:?}", item);
        }
        let stream = store.export_bao(hash, ChunkRanges::all());
        let exported = stream.to_vec().await?;

        let store2 = Store::memory();
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
