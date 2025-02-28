use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Write,
    num::NonZeroU64,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::Context;
use bao_tree::{
    io::{mixed::traverse_ranges_validated, sync::ReadAt, BaoContentItem, Leaf},
    ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use entry_state::{DataLocation, OutboardLocation};
use import::ImportSource;
use n0_future::{future::yield_now, io};
use nested_enum_utils::enum_conversions;
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinSet},
};
use tracing::{error, trace, warn};

use crate::{
    bao_file::{BaoFileConfig, BaoFileHandle, BaoFileHandleWeak, BaoFileStorage, HandleChange},
    bitfield::is_validated,
    util::{MemOrFile, SenderProgressExt},
    Hash,
};

mod entry_state;
mod import;
mod meta;
mod options;
mod util;
use entry_state::EntryState;
use import::{import_byte_stream_task, import_bytes_task, import_path_task, ImportEntry};
use options::Options;

use super::{proto::*, Store};

/// Create a 16 byte unique ID.
fn new_uuid() -> [u8; 16] {
    use rand::Rng;
    rand::rng().random::<[u8; 16]>()
}

/// Create temp file name based on a 16 byte UUID.
fn temp_name() -> String {
    format!("{}.temp", hex::encode(new_uuid()))
}

#[derive(Debug)]
#[enum_conversions()]
pub enum InternalCommand {
    Dump(meta::Dump),
    FinishImport(ImportEntry),
}

/// Context needed by most tasks
#[derive(Debug)]
struct TaskContext {
    // Store options such as paths and inline thresholds, in an Arc to cheaply share with tasks.
    options: Arc<Options>,
    // Bao file configuration.
    config: Arc<BaoFileConfig>,
    // Metadata database, basically a mpsc sender with some extra functionality.
    db: meta::Db,
    // Handle to send internal commands
    internal_cmd_tx: mpsc::Sender<InternalCommand>,
    // Epoch counter to provide a total order for databse operations
    epoch: AtomicU64,
}

#[derive(Debug)]
struct Actor {
    // Context that can be cheaply shared with tasks.
    context: Arc<TaskContext>,
    // Receiver for incoming user commands.
    cmd_rx: mpsc::Receiver<Command>,
    // Receiver for incoming file store specific commands.
    fs_cmd_rx: mpsc::Receiver<InternalCommand>,
    // Tasks for import and export operations.
    tasks: JoinSet<()>,
    // handles
    handles: HashMap<Hash, Slot>,
    // our private tokio runtime. It has to live somewhere.
    rt: Option<tokio::runtime::Runtime>,
}

/// Wraps a slot and the task context.
/// 
/// This contains everything a hash-specific task should need.
struct LazyHandle {
    slot: Slot,
    context: Arc<TaskContext>,
}

impl LazyHandle {
    pub fn db(&self) -> &meta::Db {
        &self.context.db
    }

    pub async fn get_or_create(&self, hash: impl Into<Hash>) -> anyhow::Result<BaoFileHandle> {
        let hash = hash.into();
        self.slot
            .get_or_create(|| async {
                let res = self
                    .context
                    .db
                    .get(hash)
                    .await
                    .context("failed to get entry")?;
                match res {
                    Some(state) => open_bao_file(
                        &hash.into(),
                        state,
                        self.context.options.clone(),
                        self.context.config.clone(),
                    ),
                    None => Ok(BaoFileHandle::incomplete_mem(
                        self.context.config.clone(),
                        hash.into(),
                    )),
                }
            })
            .await
    }
}

fn open_bao_file(
    hash: &Hash,
    state: EntryState<Bytes>,
    options: Arc<Options>,
    config: Arc<BaoFileConfig>,
) -> anyhow::Result<BaoFileHandle> {
    Ok(match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let data_location = match data_location {
                DataLocation::Inline(data) => MemOrFile::Mem(data),
                DataLocation::Owned(size) => {
                    let path = options.path.owned_data_path(&hash);
                    let file = fs::File::open(&path)?;
                    MemOrFile::File((file, size))
                }
                DataLocation::External(paths, size) => {
                    let Some(path) = paths.into_iter().next() else {
                        anyhow::bail!("no external data path");
                    };
                    let file = fs::File::open(&path)?;
                    MemOrFile::File((file, size))
                }
            };
            let outboard_location = match outboard_location {
                OutboardLocation::NotNeeded => MemOrFile::Mem(Bytes::new()),
                OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                OutboardLocation::Owned => {
                    let path = options.path.owned_outboard_path(hash);
                    let file = fs::File::open(&path)?;
                    let size = file.metadata()?.len();
                    MemOrFile::File((file, size))
                }
            };
            BaoFileHandle::complete(config, *hash, data_location, outboard_location)
        }
        EntryState::Partial { .. } => BaoFileHandle::incomplete_file(config, *hash)?,
    })
}

/// An entry for each hash, containing a weak reference to a BaoFileHandle
/// wrapped in a tokio mutex so handle creation is sequential.
#[derive(Debug, Clone, Default)]
struct Slot(Arc<tokio::sync::Mutex<Option<BaoFileHandleWeak>>>);

impl Slot {
    /// Get the handle if it exists and is still alive.
    pub async fn get(&self) -> Option<BaoFileHandle> {
        let slot = self.0.lock().await;
        slot.as_ref().and_then(|weak| weak.upgrade())
    }

    /// Get the handle if it exists and is still alive, otherwise load it from the database.
    /// If there is nothing in the database, create a new in-memory handle.
    ///
    /// `make` will be called if the a live handle does not exist.
    pub async fn get_or_create<F, Fut>(&self, make: F) -> anyhow::Result<BaoFileHandle>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<BaoFileHandle>>,
    {
        let mut slot = self.0.lock().await;
        if let Some(weak) = &*slot {
            if let Some(handle) = weak.upgrade() {
                return Ok(handle);
            }
        }
        let handle = make().await;
        if let Ok(handle) = &handle {
            *slot = Some(handle.downgrade());
        }
        handle
    }
}

impl Actor {
    fn db(&self) -> &meta::Db {
        &self.context.db
    }

    fn options(&self) -> &Arc<Options> {
        &self.context.options
    }

    fn context(&self) -> Arc<TaskContext> {
        self.context.clone()
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::SyncDb(cmd) => {
                self.db().send(cmd.into()).await.ok();
            }
            Command::CreateTag(cmd) => {
                self.db().send(cmd.into()).await.ok();
            }
            Command::SetTag(cmd) => {
                self.db().send(cmd.into()).await.ok();
            }
            Command::Tags(Tags { tx }) => {
                self.db()
                    .send(
                        meta::Tags {
                            filter: Box::new(|_, k, v| Some((k.value(), v.value()))),
                            tx,
                        }
                        .into(),
                    )
                    .await
                    .ok();
            }
            Command::ImportBytes(cmd) => {
                self.tasks.spawn(import_bytes_task(cmd, self.context()));
            }
            Command::ImportByteStream(cmd) => {
                self.tasks
                    .spawn(import_byte_stream_task(cmd, self.context()));
            }
            Command::ImportPath(cmd) => {
                self.tasks.spawn(import_path_task(cmd, self.context()));
            }
            Command::ExportPath(cmd) => {
                let handle = self.get_handle(cmd.hash);
                self.tasks.spawn(export_path_task(cmd, handle));
            }
            Command::ExportBao(cmd) => {
                let handle = self.get_handle(cmd.hash);
                self.tasks.spawn(export_bao_task(cmd, handle));
            }
            Command::ImportBao(cmd) => {
                let handle = self.get_handle(cmd.hash);
                self.tasks.spawn(import_bao_task(cmd, handle));
            }
            Command::Observe(cmd) => {
                let handle = self.get_handle(cmd.hash);
                self.tasks.spawn(observe_task(cmd, handle));
            }
            _ => {}
        }
    }

    fn get_handle(&mut self, hash: impl Into<Hash>) -> LazyHandle {
        LazyHandle {
            slot: self.handles.entry(hash.into()).or_default().clone(),
            context: self.context.clone(),
        }
    }

    async fn handle_fs_command(&mut self, cmd: InternalCommand) {
        match cmd {
            InternalCommand::Dump(cmd) => {
                self.db().send(cmd.into()).await.ok();
            }
            InternalCommand::FinishImport(cmd) => {
                let handle = self.get_handle(cmd.hash);
                self.finish_import(cmd).await;
            }
        }
    }

    fn log_task_result(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    async fn finish_import(&mut self, import_data: ImportEntry) {
        let hash: crate::Hash = import_data.hash.into();
        let handle = self.get_handle(hash);
        // convert the import source to a data location and drop the open files
        let data_location = match import_data.source {
            ImportSource::Memory(data) => DataLocation::Inline(data),
            ImportSource::External(path, _file, size) => DataLocation::External(vec![path], size),
            ImportSource::TempFile(path, _file, size) => {
                // this will always work on any unix, but on windows there might be an issue if the target file is open!
                // possibly open with FILE_SHARE_DELETE on windows?
                let target = self.options().path.owned_data_path(&hash);
                trace!(
                    "moving temp file to owned data location: {} -> {}",
                    path.display(),
                    target.display()
                );
                if let Err(cause) = std::fs::rename(path, target) {
                    error!("failed to move temp file to owned data location: {cause}");
                }
                DataLocation::Owned(size)
            }
        };
        let outboard_location = match import_data.outboard {
            MemOrFile::Mem(bytes) if bytes.is_empty() => OutboardLocation::NotNeeded,
            MemOrFile::Mem(bytes) => OutboardLocation::Inline(bytes),
            MemOrFile::File(path) => {
                // the same caveat as above applies here
                let target = self.options().path.owned_outboard_path(&hash);
                trace!(
                    "moving temp file to owned outboard location: {} -> {}",
                    path.display(),
                    target.display()
                );
                if let Err(cause) = std::fs::rename(path, target) {
                    error!("failed to move temp file to owned outboard location: {cause}");
                }
                OutboardLocation::Owned
            }
        };
        let state = EntryState::Complete {
            data_location,
            outboard_location,
        };
        self.db()
            .send(
                meta::Update {
                    hash,
                    state,
                    epoch: 0,
                    tx: None,
                }
                .into(),
            )
            .await
            .ok();
        let hash = hash.into();
        import_data.out.send(ImportProgress::Done { hash });
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
                    trace!("{cmd:?}");
                    self.handle_command(cmd).await;
                }
                Some(cmd) = self.fs_cmd_rx.recv() => {
                    trace!("{cmd:?}");
                    self.handle_fs_command(cmd).await;
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    self.log_task_result(res);
                }
            }
        }
    }

    async fn new(
        db_path: PathBuf,
        rt: tokio::runtime::Runtime,
        cmd_rx: mpsc::Receiver<Command>,
        fs_commands_rx: mpsc::Receiver<InternalCommand>,
        fs_commands_tx: mpsc::Sender<InternalCommand>,
        options: Arc<Options>,
    ) -> anyhow::Result<Self> {
        trace!(
            "creating data directory: {}",
            options.path.data_path.display()
        );
        fs::create_dir_all(&options.path.data_path)?;
        trace!(
            "creating temp directory: {}",
            options.path.temp_path.display()
        );
        fs::create_dir_all(&options.path.temp_path)?;
        trace!(
            "creating parent directory for db file{}",
            db_path.parent().unwrap().display()
        );
        fs::create_dir_all(db_path.parent().unwrap())?;
        let (db_send, db_recv) = mpsc::channel(100);
        let db_actor = meta::Actor::new(db_path, db_recv)?;
        // the bao file config is just things from the options packed together and
        // wrapped in an arc.
        let config = Arc::new(BaoFileConfig::new(
            Arc::new(options.path.data_path.clone()),
            options.inline.max_data_inlined as usize,
        ));
        let slot_context = Arc::new(TaskContext {
            options,
            config,
            db: meta::Db::new(db_send),
            internal_cmd_tx: fs_commands_tx,
            epoch: AtomicU64::new(0),
        });
        rt.spawn(db_actor.run());
        Ok(Self {
            context: slot_context,
            cmd_rx,
            fs_cmd_rx: fs_commands_rx,
            tasks: JoinSet::new(),
            handles: Default::default(),
            rt: Some(rt),
        })
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

async fn import_bao_task(cmd: ImportBao, handle: LazyHandle) {
    let meta = handle.db().clone();
    let ImportBao {
        size,
        data,
        out,
        hash,
    } = cmd;
    match handle.get_or_create(hash).await {
        Ok(handle) => {
            let res = import_bao_impl(size, data, handle, meta).await;
            out.send(res).ok();
        }
        Err(cause) => {
            let cause = anyhow::anyhow!("failed to open file: {cause}");
            out.send(Err(cause)).ok();
        }
    }
}

fn chunk_range(leaf: &Leaf) -> ChunkRanges {
    let start = ChunkNum::chunks(leaf.offset);
    let end = ChunkNum::chunks(leaf.offset + leaf.data.len() as u64);
    (start..end).into()
}

async fn import_bao_impl(
    size: NonZeroU64,
    mut data: mpsc::Receiver<BaoContentItem>,
    handle: BaoFileHandle,
    db: meta::Db,
) -> anyhow::Result<()> {
    let mut batch = Vec::<BaoContentItem>::new();
    let mut ranges = ChunkRanges::empty();
    while let Some(item) = data.recv().await {
        // if the batch is not empty, the last item is a leaf and the current item is a parent, write the batch
        if !batch.is_empty() && batch[batch.len() - 1].is_leaf() && item.is_parent() {
            let res = handle.write_batch(size, &batch, &ranges)?;
            // create the metadata entry for the new file, if needed
            if let HandleChange::MemToFile = res {
                let hash = handle.hash();
                let epoch = 0; // not needed here, since creating a partial entry won't delete files
                let state = EntryState::Partial { size: None };
                db.send(
                    meta::Update {
                        hash,
                        epoch,
                        state,
                        tx: None,
                    }
                    .into(),
                )
                .await
                .ok();
            }
            batch.clear();
            ranges = ChunkRanges::empty();
        }
        if let BaoContentItem::Leaf(leaf) = &item {
            let leaf_range = chunk_range(leaf);
            if is_validated(size, &leaf_range) {
                anyhow::ensure!(
                    size.get() == leaf.offset + leaf.data.len() as u64,
                    "invalid leaf"
                );
            }
            ranges |= leaf_range;
        }
        batch.push(item);
    }
    if !batch.is_empty() {
        handle.write_batch(size, &batch, &ranges)?;
    }
    Ok(())
}

async fn observe_task(cmd: Observe, handle: LazyHandle) {
    let Ok(handle) = handle.get_or_create(cmd.hash).await else {
        return;
    };
    let receiver_dropped = cmd.out.receiver_dropped();
    handle.add_observer(cmd.out);
    // this keeps the handle alive until the observer is dropped
    receiver_dropped.await;
}

async fn export_bao_task(cmd: ExportBao, handle: LazyHandle) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    match handle.get_or_create(cmd.hash).await {
        Ok(handle) => {
            if let Err(cause) = export_bao_impl(cmd, handle).await {
                send.send(EncodedItem::Error(bao_tree::io::EncodeError::Io(
                    io::Error::new(io::ErrorKind::Other, cause),
                )));
            }
        }
        Err(cause) => {
            let cause = anyhow::anyhow!("failed to open file: {cause}");
            send.send(EncodedItem::Error(bao_tree::io::EncodeError::Io(
                io::Error::new(io::ErrorKind::Other, cause),
            )));
        }
    }
}

async fn export_bao_impl(cmd: ExportBao, handle: BaoFileHandle) -> anyhow::Result<()> {
    let ExportBao { ranges, out, hash } = cmd;
    anyhow::ensure!(handle.hash() == Hash::from(hash), "hash mismatch");
    let data = handle.data_reader();
    let outboard = handle.outboard()?;
    traverse_ranges_validated(data, outboard, &ranges, &out).await;
    Ok(())
}

async fn export_path_task(cmd: ExportPath, handle: LazyHandle) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    match handle.get_or_create(cmd.hash).await {
        Ok(handle) => {
            if let Err(cause) = export_path_impl(cmd, handle).await {
                send.send(ExportProgress::Error { cause });
            }
        }
        Err(cause) => {
            let cause = anyhow::anyhow!("failed to open file: {cause}");
            send.send(ExportProgress::Error { cause });
        }
    }
}

async fn export_path_impl(cmd: ExportPath, handle: BaoFileHandle) -> anyhow::Result<()> {
    let data = {
        let guard = handle.storage.read().unwrap();
        let BaoFileStorage::Complete(complete) = guard.deref() else {
            anyhow::bail!("not a complete file");
        };
        match &complete.data {
            MemOrFile::Mem(data) => MemOrFile::Mem(data.clone()),
            MemOrFile::File((file, size)) => {
                let file = file.try_clone()?;
                MemOrFile::File((file, *size))
            }
        }
    };
    let mut target = fs::File::create(&cmd.target)?;
    match data {
        MemOrFile::Mem(data) => {
            target.write_all(&data)?;
        }
        MemOrFile::File((file, size)) => {
            let mut offset = 0;
            let mut buf = vec![0u8; 1024 * 1024];
            while offset < size {
                let remaining = buf.len().min((size - offset) as usize);
                let buf = &mut buf[..remaining];
                file.read_exact_at(offset, buf)?;
                target.write_all(buf)?;
                cmd.out
                    .send_progress(ExportProgress::CopyProgress { offset })?;
                yield_now().await;
                offset += buf.len() as u64;
            }
        }
    }
    cmd.out.send(ExportProgress::Done).await?;
    Ok(())
}

impl Store {
    /// Load or create a new store.
    pub async fn load_redb(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = root.as_ref();
        let db_path = path.join("blobs.db");
        let options = Options::new(path);
        Self::load_redb_with_opts(db_path, options).await
    }

    /// Load or create a new store with custom options.
    pub async fn load_redb_with_opts(path: PathBuf, options: Options) -> anyhow::Result<Self> {
        let (this, _debug) = Self::load_redb_with_opts_inner(path, options).await?;
        Ok(this)
    }

    /// Load or create a new store with custom options, returning an additional sender for file store specific commands.
    pub async fn load_redb_with_opts_inner(
        path: PathBuf,
        options: Options,
    ) -> anyhow::Result<(Self, DbStore)> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("iroh-blob-store")
            .enable_time()
            .build()?;
        let handle = rt.handle().clone();
        let (commands_tx, commands_rx) = mpsc::channel(100);
        let (fs_commands_tx, fs_commands_rx) = mpsc::channel(100);
        let actor = handle
            .spawn(Actor::new(
                path.join("blobs.db"),
                rt,
                commands_rx,
                fs_commands_rx,
                fs_commands_tx.clone(),
                Arc::new(options),
            ))
            .await??;
        handle.spawn(actor.run());
        Ok((
            Self::from_sender(commands_tx),
            DbStore::from_sender(fs_commands_tx),
        ))
    }
}

pub struct DbStore {
    db: mpsc::Sender<InternalCommand>,
}

impl DbStore {
    fn from_sender(db: mpsc::Sender<InternalCommand>) -> Self {
        Self { db }
    }

    pub async fn dump(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db.send(meta::Dump { tx }.into()).await?;
        rx.await??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bao_tree::{blake3, io::outboard::PreOrderMemOutboard, ChunkRanges};
    use n0_future::StreamExt;
    use testresult::TestResult;

    use super::*;
    use crate::{
        util::{
            observer::{Aggregator, Combine},
            Tag,
        },
        HashAndFormat, IROH_BLOCK_SIZE,
    };

    fn create_n0_bao(data: &[u8], ranges: &ChunkRanges) -> anyhow::Result<(blake3::Hash, Vec<u8>)> {
        let outboard = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
        let mut encoded = Vec::new();
        let size = data.len() as u64;
        encoded.extend_from_slice(&size.to_le_bytes());
        bao_tree::io::sync::encode_ranges_validated(&data, &outboard, ranges, &mut encoded)?;
        Ok((outboard.root, encoded))
    }

    #[tokio::test]
    async fn observe() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let (store, debug) = Store::load_redb_with_opts_inner(db_dir, options).await?;
        let sizes = [
            0,
            1,
            1024,
            1024 * 16 - 1,
            1024 * 16,
            1024 * 16 + 1,
            1024 * 1024,
            1024 * 1024 * 8,
        ];
        for size in sizes {
            let data = vec![1u8; size];
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            let obs = store.observe(hash);
            let task = tokio::spawn(async move {
                let mut agg = obs.aggregated();
                while let Some(delta) = agg.next().await {
                    let state = agg.state();
                    println!("{:?} complete={}", state, state.is_complete());
                    if state.is_complete() {
                        break;
                    }
                }
            });
            store.import_bao_bytes(hash, ranges, bao.into()).await?;
            task.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let (store, debug) = Store::load_redb_with_opts_inner(db_dir, options).await?;
        let haf = HashAndFormat::raw(Hash::from([0u8; 32]));
        store.set_tag(Tag::from("test"), haf).await?;
        store.set_tag(Tag::from("boo"), haf).await?;
        store.set_tag(Tag::from("bar"), haf).await?;
        let sizes = [
            0,
            1,
            1024,
            1024 * 16 - 1,
            1024 * 16,
            1024 * 16 + 1,
            1024 * 1024,
            1024 * 1024 * 8,
        ];
        let mut hashes = Vec::new();
        let mut data_by_hash = HashMap::new();
        let mut bao_by_hash = HashMap::new();
        for size in sizes {
            let data = vec![0u8; size];
            let data = Bytes::from(data);
            let hash = store.import_bytes(data.clone()).await?;
            data_by_hash.insert(hash, data);
            hashes.push(hash);
        }
        store.sync_db().await?;
        for hash in &hashes {
            let path = testdir.path().join(format!("{hash}.txt"));
            store.export_file(*hash, path).await?;
        }
        for hash in &hashes {
            let data = store
                .export_bao(*hash, ChunkRanges::all())
                .data_to_vec()
                .await
                .unwrap();
            assert_eq!(data, data_by_hash[hash].to_vec());
            let bao = store
                .export_bao(*hash, ChunkRanges::all())
                .bao_to_vec()
                .await
                .unwrap();
            bao_by_hash.insert(*hash, bao);
        }
        debug.dump().await?;

        for size in sizes {
            let data = vec![1u8; size];
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            store.import_bao_bytes(hash, ranges, bao.into()).await?;
        }

        for (hash, bao_tree) in bao_by_hash {
            // let mut reader = Cursor::new(bao_tree);
            // let size = reader.read_u64_le().await?;
            // let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
            // let ranges = ChunkRanges::all();
            // let mut decoder = DecodeResponseIter::new(hash, tree, reader, &ranges);
            // while let Some(item) = decoder.next() {
            //     let item = item?;
            // }
            // store.import_bao_bytes(hash, ChunkRanges::all(), bao_tree.into()).await?;
        }
        Ok(())
    }
}
