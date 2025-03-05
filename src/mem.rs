use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Write},
    num::NonZeroU64,
    ops::DerefMut,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use bao_tree::{
    blake3::{self, Hash},
    io::{
        mixed::{traverse_ranges_validated, EncodedItem, ReadBytesAt},
        outboard::PreOrderMemOutboard,
        sync::{Outboard, ReadAt, WriteAt},
        BaoContentItem,
    },
    BaoTree, ChunkNum, ChunkRanges, ChunkRangesRef, TreeNode,
};
use bytes::Bytes;
use n0_future::{future::yield_now, StreamExt};
use tokio::{
    io::AsyncReadExt,
    sync::mpsc::{self, OwnedPermit},
    task::{JoinError, JoinSet},
};
use tracing::{error, instrument};

use crate::{
    bitfield::Bitfield,
    proto::*,
    util::{
        observer::{Observable, Observer},
        SenderProgressExt, SparseMemFile, Tag,
    },
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
    fn handle_command(&mut self, cmd: Command) -> Option<Shutdown> {
        match cmd {
            Command::ImportBao(ImportBao {
                hash,
                size,
                rx: data,
                tx: out,
            }) => {
                let entry = self.state.data.entry(hash).or_default();
                self.unit_tasks
                    .spawn(import_bao_task(entry.clone(), size, data, out));
            }
            Command::Observe(Observe { hash, out }) => {
                let entry = self.state.data.entry(hash).or_default();
                let mut entry = entry.write().unwrap();
                entry.add_observer(out);
            }
            Command::ImportBytes(ImportBytes { data, tx: out }) => {
                self.import_tasks.spawn(import_bytes(data, out));
            }
            Command::ImportByteStream(ImportByteStream { data, tx: out }) => {
                self.import_tasks.spawn(import_byte_stream(data, out));
            }
            Command::ImportPath(cmd) => {
                self.import_tasks.spawn(import_path(cmd));
            }
            Command::ExportBao(ExportBao {
                hash,
                ranges,
                tx: out,
            }) => {
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
            Command::SyncDb(SyncDb { tx }) => {
                tx.send(Ok(())).ok();
            }
            Command::Shutdown(cmd) => {
                return Some(cmd);
            }
        }
        None
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
        let size = import_data.data.len() as u64;
        let entry = self.state.data.entry(hash).or_default();
        let mut entry = entry.write().unwrap();
        let Entry::Partial(incomplete) = entry.deref_mut() else {
            import_data.out.send(ImportProgress::Done { hash });
            return;
        };
        incomplete.update(Bitfield::complete(size));
        import_data.out.send(ImportProgress::Done { hash });
        *entry = CompleteStorage::new(import_data.data, import_data.outboard.data.into()).into();
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    pub async fn run(mut self) {
        let shutdown = loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        // last sender has been dropped.
                        // exit immediately.
                        break None;
                    };
                    if let Some(cmd) = self.handle_command(cmd) {
                        break Some(cmd);
                    }
                }
                Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
                    self.finish_import(res);
                }
                Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
                    self.log_unit_task(res);
                }
            }
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.tx.send(());
        }
    }
}

async fn import_bao_task(
    entry: Arc<RwLock<Entry>>,
    size: NonZeroU64,
    mut stream: mpsc::Receiver<BaoContentItem>,
    tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
) {
    let size = size.get();
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
                let added = ChunkRanges::from(ChunkNum::chunks(start)..ChunkNum::full_chunks(end));
                let added = &added - &entry.bitfield.state().ranges;
                if added.is_empty() {
                    continue;
                }
                let update = Bitfield::new(added, size);
                entry.bitfield.update(update);
                if entry.bitfield.state().ranges == ChunkRanges::from(..ChunkNum::chunks(size)) {
                    let data = std::mem::take(&mut entry.data);
                    let outboard = std::mem::take(&mut entry.outboard);
                    let data: Bytes = <Vec<u8>>::try_from(data).unwrap().into();
                    let outboard: Bytes = <Vec<u8>>::try_from(outboard).unwrap().into();
                    *guard = CompleteStorage::new(data, outboard).into();
                }
            }
        }
    }
    tx.send(Ok(())).ok();
}

async fn export_bao_task(
    hash: Hash,
    entry: Arc<RwLock<Entry>>,
    ranges: ChunkRanges,
    sender: mpsc::Sender<EncodedItem>,
) {
    let size = entry.read().unwrap().size();
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

async fn import_bytes(
    data: Bytes,
    tx: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    tx.send(ImportProgress::Size {
        size: data.len() as u64,
    })
    .await?;
    tx.send(ImportProgress::CopyDone).await?;
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let out = tx.reserve_owned().await?;
    Ok(ImportEntry {
        data,
        outboard,
        out,
    })
}

async fn import_byte_stream(
    mut data: BoxedByteStream,
    tx: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let mut res = Vec::new();
    while let Some(item) = data.next().await {
        let item = item?;
        res.extend_from_slice(&item);
        tx.send_progress(ImportProgress::CopyProgress {
            offset: res.len() as u64,
        })?;
    }
    import_bytes(res.into(), tx).await
}

#[instrument(skip_all, fields(path = %cmd.path.display()))]
async fn import_path(cmd: ImportPath) -> anyhow::Result<ImportEntry> {
    let ImportPath { path, tx, .. } = cmd;
    let mut res = Vec::new();
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = [0u8; 1024 * 64];
    loop {
        let size = file.read(&mut buf).await?;
        if size == 0 {
            break;
        }
        res.extend_from_slice(&buf[..size]);
        tx.send(ImportProgress::CopyProgress {
            offset: res.len() as u64,
        })
        .await?;
    }
    import_bytes(res.into(), tx).await
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
    let size = entry.read().unwrap().size();
    out.send(ExportProgress::Size { size }).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        entry.read().unwrap().data().read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        out.send_progress(ExportProgress::CopyProgress {
            offset: offset as u64,
        })?;
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

impl ReadBytesAt for ExportData {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        let entry = self.data.read().unwrap();
        entry.data().read_bytes_at(offset, size)
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
    Partial(PartialMemStorage),
    Complete(CompleteStorage),
}

impl Default for Entry {
    fn default() -> Self {
        Self::Partial(Default::default())
    }
}

impl Entry {
    fn ranges(&self) -> &ChunkRangesRef {
        match self {
            Self::Partial(entry) => &entry.bitfield.state().ranges,
            Self::Complete(_) => &ChunkRangesRef::single(&ChunkNum(0)),
        }
    }

    fn add_observer(&mut self, out: Observer<Bitfield>) {
        match self {
            Self::Partial(entry) => entry.add_observer(out),
            Self::Complete(entry) => entry.add_observer(out),
        }
    }

    fn data(&self) -> &[u8] {
        match self {
            Self::Partial(entry) => entry.data.as_ref(),
            Self::Complete(entry) => &entry.data,
        }
    }

    fn outboard(&self) -> &[u8] {
        match self {
            Self::Partial(entry) => entry.outboard.as_ref(),
            Self::Complete(entry) => &entry.outboard,
        }
    }

    fn size(&self) -> u64 {
        match self {
            Self::Partial(entry) => entry.current_size(),
            Self::Complete(entry) => entry.size(),
        }
    }

    fn incomplete_mut(&mut self) -> Option<&mut PartialMemStorage> {
        match self {
            Self::Partial(entry) => Some(entry),
            Self::Complete(_) => None,
        }
    }
}

/// An incomplete entry, with all the logic to keep track of the state of the entry
/// and for observing changes.
#[derive(Debug, Default)]
pub(crate) struct PartialMemStorage {
    pub(crate) data: SparseMemFile,
    pub(crate) outboard: SparseMemFile,
    pub(crate) size: SizeInfo,
    pub(crate) bitfield: Observable<Bitfield>,
}

impl PartialMemStorage {
    pub fn add_observer(&mut self, out: Observer<Bitfield>) {
        self.bitfield.add_observer(out);
    }

    pub fn bitfield(&self) -> &Observable<Bitfield> {
        &self.bitfield
    }

    pub fn update(&mut self, update: Bitfield) {
        self.bitfield.update(update);
    }

    pub fn current_size(&self) -> u64 {
        self.bitfield.state().size
    }

    pub(super) fn write_batch(
        &mut self,
        size: NonZeroU64,
        batch: &[BaoContentItem],
        ranges: &ChunkRanges,
    ) -> io::Result<()> {
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset
                            .checked_mul(64)
                            .expect("u64 overflow multiplying to hash pair offset");
                        let o1 = o0.checked_add(32).expect("u64 overflow");
                        let outboard = &mut self.outboard;
                        outboard.write_all_at(o0, parent.pair.0.as_bytes().as_slice())?;
                        outboard.write_all_at(o1, parent.pair.1.as_bytes().as_slice())?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    self.size.write(leaf.offset, size.get());
                    self.data.write_all_at(leaf.offset, leaf.data.as_ref())?;
                }
            }
        }
        let update = Bitfield::new(ranges.clone(), size.get());
        self.bitfield.update(update);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CompleteStorage {
    pub(crate) data: Bytes,
    pub(crate) outboard: Bytes,
}

impl CompleteStorage {
    pub fn create(data: Bytes) -> (blake3::Hash, Self) {
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = outboard.root();
        let outboard = outboard.data.into();
        let entry = Self::new(data, outboard);
        (hash, entry)
    }

    pub fn add_observer(&mut self, mut out: Observer<Bitfield>) {
        let state = Bitfield::complete(self.size());
        out.send(state).ok();
    }

    pub fn new(data: Bytes, outboard: Bytes) -> Self {
        Self { data, outboard }
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn data(&self) -> impl AsRef<[u8]> + '_ {
        self.data.as_ref()
    }

    pub fn outboard(&self) -> impl AsRef<[u8]> + '_ {
        self.outboard.as_ref()
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
        let hash = store.import_bytes(vec![0u8; 1024 * 64]).await?;
        println!("hash: {:?}", hash);
        let mut stream = store.export_bao(hash, ChunkRanges::all());
        while let Some(item) = stream.next().await {
            println!("item: {:?}", item);
        }
        let stream = store.export_bao(hash, ChunkRanges::all());
        let exported = stream.bao_to_vec().await?;

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

        let exported2 = store2
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        assert_eq!(exported, exported2);

        Ok(())
    }
}
