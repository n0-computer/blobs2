//! # File based blob store.
//!
//! General design:
//!
//! The file store consists of two actors.
//!
//! # The main actor
//!
//! The purpose of the main actor is to handle user commands and own a map of
//! handles for hashes that are currently being worked on.
//!
//! It also owns tasks for ongoing import and export operations, as well as the
//! database actor.
//!
//! Handling a command almost always involves either forwarding it to the
//! database actor or creating a hash context and spawning a task.
//!
//! # The database actor
//!
//! The database actor is responsible for storing metadata about each hash,
//! as well as inlined data and outboard data for small files.
//!
//! In addition to the metadata, the database actor also stores tags.
//!
//! # Tasks
//!
//! Tasks do not return a result. They are responsible for sending an error
//! to the requester if possible. Otherwise, just dropping the sender will
//! also fail the receiver, but without a descriptive error message.
//!
//! Tasks are usually implemented as an impl fn that does return a result,
//! and a wrapper (named `..._task`) that just forwards the error, if any.
//!
//! That way you can use `?` syntax in the task implementation. The impl fns
//! are also easier to test.
//!
//! # Context
//!
//! The main actor holds a [`TaskContext`] that is needed for almost all tasks,
//! such as the config and a way to interact with the database.
//!
//! For tasks that are specific to a hash, a [`HashContext`] combines the task
//! context with a slot from the table of the main actor that can be used
//! to obtain an unqiue handle for the hash.
use std::{
    collections::HashMap,
    fmt, fs,
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
use tracing::{error, info, instrument, trace};

use crate::store::{
    bitfield::is_validated,
    util::{FixedSize, MemOrFile, SenderProgressExt, ValueOrPoisioned},
    Hash,
};
mod bao_file;
use bao_file::{BaoFileHandle, BaoFileHandleWeak, BaoFileStorage};
mod entry_state;
mod import;
mod meta;
mod options;
pub(crate) mod util;
use entry_state::EntryState;
use import::{import_byte_stream, import_bytes, import_path, ImportEntry};
use options::Options;

use super::{proto::*, Store};

/// Create a 16 byte unique ID.
fn new_uuid() -> [u8; 16] {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    bytes
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
pub(crate) struct TaskContext {
    // Store options such as paths and inline thresholds, in an Arc to cheaply share with tasks.
    pub options: Arc<Options>,
    // Metadata database, basically a mpsc sender with some extra functionality.
    pub db: meta::Db,
    // Handle to send internal commands
    pub internal_cmd_tx: mpsc::Sender<InternalCommand>,
    // Epoch counter to provide a total order for databse operations
    pub epoch: AtomicU64,
    /// The file handle for the empty hash.
    pub empty: BaoFileHandle,
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
    rt: RtWrapper,
}

/// Wraps a slot and the task context.
///
/// This contains everything a hash-specific task should need.
struct HashContext {
    slot: Slot,
    ctx: Arc<TaskContext>,
}

impl HashContext {
    pub fn db(&self) -> &meta::Db {
        &self.ctx.db
    }

    pub fn options(&self) -> &Arc<Options> {
        &self.ctx.options
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, Option<BaoFileHandleWeak>> {
        self.slot.0.lock().await
    }

    /// Update the entry state in the database, and wait for completion.
    pub async fn update(
        &self,
        hash: impl Into<Hash>,
        state: EntryState<Bytes>,
    ) -> anyhow::Result<()> {
        let hash = hash.into();
        let epoch = self
            .ctx
            .epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.db()
            .send(
                meta::Update {
                    hash,
                    epoch,
                    state,
                    tx: Some(tx),
                }
                .into(),
            )
            .await?;
        rx.await??;
        Ok(())
    }

    /// Update the entry state in the database, and wait for completion.
    pub async fn set(&self, hash: impl Into<Hash>, state: EntryState<Bytes>) -> anyhow::Result<()> {
        let hash = hash.into();
        let epoch = self
            .ctx
            .epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.db()
            .send(
                meta::Set {
                    hash,
                    epoch,
                    state,
                    tx,
                }
                .into(),
            )
            .await?;
        rx.await??;
        Ok(())
    }

    pub async fn get_or_create(&self, hash: impl Into<Hash>) -> anyhow::Result<BaoFileHandle> {
        let hash = hash.into();
        if hash == Hash::EMPTY {
            return Ok(self.ctx.empty.clone());
        }
        let res = self
            .slot
            .get_or_create(|| async {
                let res = self.db().get(hash).await.context("failed to get entry")?;
                match res {
                    Some(state) => open_bao_file(&hash.into(), state, self.ctx.options.clone()),
                    None => Ok(BaoFileHandle::new_partial_mem(
                        hash.into(),
                        self.ctx.options.clone(),
                    )),
                }
            })
            .await;
        trace!("{res:?}");
        res
    }
}

fn open_bao_file(
    hash: &Hash,
    state: EntryState<Bytes>,
    options: Arc<Options>,
) -> anyhow::Result<BaoFileHandle> {
    Ok(match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let data = match data_location {
                DataLocation::Inline(data) => MemOrFile::Mem(data),
                DataLocation::Owned(size) => {
                    let path = options.path.owned_data_path(&hash);
                    let file = fs::File::open(&path)?;
                    MemOrFile::File(FixedSize::new(file, size))
                }
                DataLocation::External(paths, size) => {
                    let Some(path) = paths.into_iter().next() else {
                        anyhow::bail!("no external data path");
                    };
                    let file = fs::File::open(&path)?;
                    MemOrFile::File(FixedSize::new(file, size))
                }
            };
            let outboard = match outboard_location {
                OutboardLocation::NotNeeded => MemOrFile::Mem(Bytes::new()),
                OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                OutboardLocation::Owned => {
                    let path = options.path.owned_outboard_path(hash);
                    let file = fs::File::open(&path)?;
                    MemOrFile::File(file)
                }
            };
            BaoFileHandle::new_complete(*hash, data, outboard)
        }
        EntryState::Partial { .. } => BaoFileHandle::new_partial_file(*hash, options.clone())?,
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

    fn context(&self) -> Arc<TaskContext> {
        self.context.clone()
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::SyncDb(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::Shutdown(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::CreateTag(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::SetTag(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::Tags(cmd) => {
                trace!("{cmd:?}");
                let Tags { tx } = cmd;
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
                trace!("{cmd:?}");
                self.tasks.spawn(import_bytes(cmd, self.context()));
            }
            Command::ImportByteStream(cmd) => {
                trace!("{cmd:?}");
                self.tasks.spawn(import_byte_stream(cmd, self.context()));
            }
            Command::ImportPath(cmd) => {
                trace!("{cmd:?}");
                self.tasks.spawn(import_path(cmd, self.context()));
            }
            Command::ExportPath(cmd) => {
                trace!("{cmd:?}");
                let ctx = self.hash_context(cmd.hash);
                self.tasks.spawn(export_path(cmd, ctx));
            }
            Command::ExportBao(cmd) => {
                trace!("{cmd:?}");
                let ctx = self.hash_context(cmd.hash);
                self.tasks.spawn(export_bao(cmd, ctx));
            }
            Command::ImportBao(cmd) => {
                trace!("{cmd:?}");
                let ctx = self.hash_context(cmd.hash);
                self.tasks.spawn(import_bao(cmd, ctx));
            }
            Command::Observe(cmd) => {
                trace!("{cmd:?}");
                let ctx = self.hash_context(cmd.hash);
                self.tasks.spawn(observe(cmd, ctx));
            }
        }
    }

    /// Create a hash context for a given hash.
    fn hash_context(&mut self, hash: impl Into<Hash>) -> HashContext {
        let hash = hash.into();
        HashContext {
            slot: self.handles.entry(hash.into()).or_default().clone(),
            ctx: self.context.clone(),
        }
    }

    async fn handle_fs_command(&mut self, cmd: InternalCommand) {
        match cmd {
            InternalCommand::Dump(cmd) => {
                self.db().send(cmd.into()).await.ok();
            }
            InternalCommand::FinishImport(cmd) => {
                if cmd.hash.as_bytes() == Hash::EMPTY.as_bytes() {
                    cmd.tx
                        .send(ImportProgress::Done { hash: cmd.hash })
                        .await
                        .ok();
                }
                let handle = self.hash_context(cmd.hash);
                self.tasks.spawn(finish_import(cmd, handle));
            }
        }
    }

    fn log_task_result(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
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
        rt: RtWrapper,
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
        let slot_context = Arc::new(TaskContext {
            options,
            db: meta::Db::new(db_send),
            internal_cmd_tx: fs_commands_tx,
            epoch: AtomicU64::new(0),
            empty: BaoFileHandle::new_complete(
                Hash::EMPTY,
                MemOrFile::Mem(Bytes::new()),
                MemOrFile::Mem(Bytes::new()),
            ),
        });
        rt.spawn(db_actor.run());
        Ok(Self {
            context: slot_context,
            cmd_rx,
            fs_cmd_rx: fs_commands_rx,
            tasks: JoinSet::new(),
            handles: Default::default(),
            rt,
        })
    }
}

struct RtWrapper(Option<tokio::runtime::Runtime>);

impl From<tokio::runtime::Runtime> for RtWrapper {
    fn from(rt: tokio::runtime::Runtime) -> Self {
        Self(Some(rt))
    }
}

impl fmt::Debug for RtWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ValueOrPoisioned(self.0.as_ref()).fmt(f)
    }
}

impl Deref for RtWrapper {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Drop for RtWrapper {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            trace!("dropping tokio runtime");
            tokio::task::block_in_place(|| {
                drop(rt);
            });
            trace!("dropped tokio runtime");
        }
    }
}

#[instrument(skip_all, fields(hash = %cmd.hash_short()))]
async fn finish_import(cmd: ImportEntry, ctx: HashContext) {
    let Ok(send) = cmd.tx.clone().reserve_owned().await else {
        return;
    };
    let hash = cmd.hash;
    let res = match finish_import_impl(cmd, ctx).await {
        Ok(()) => ImportProgress::Done { hash },
        Err(cause) => ImportProgress::Error { cause },
    };
    send.send(res);
}

async fn finish_import_impl(import_data: ImportEntry, ctx: HashContext) -> anyhow::Result<()> {
    let options = ctx.options();
    match &import_data.source {
        ImportSource::Memory(data) => {
            debug_assert!(options.is_inlined_data(data.len() as u64));
        }
        ImportSource::External(_, _, size) => {
            debug_assert!(!options.is_inlined_data(*size));
        }
        ImportSource::TempFile(_, _, size) => {
            debug_assert!(!options.is_inlined_data(*size));
        }
    }
    let hash: crate::Hash = import_data.hash.into();
    let guard = ctx.lock().await;
    let handle = guard.as_ref().and_then(|x| x.upgrade());
    // if I do have an existing handle, I have to possibly deal with observers.
    // if I don't have an existing handle, there are 2 cases:
    //   the entry exists in the db, but we don't have a handle
    //   the entry does not exist at all.
    // convert the import source to a data location and drop the open files
    let data_location = match import_data.source {
        ImportSource::Memory(data) => DataLocation::Inline(data),
        ImportSource::External(path, _file, size) => DataLocation::External(vec![path], size),
        ImportSource::TempFile(path, _file, size) => {
            // this will always work on any unix, but on windows there might be an issue if the target file is open!
            // possibly open with FILE_SHARE_DELETE on windows?
            let target = ctx.options().path.owned_data_path(&hash);
            trace!(
                "moving temp file to owned data location: {} -> {}",
                path.display(),
                target.display()
            );
            if let Err(cause) = fs::rename(&path, &target) {
                error!(
                    "failed to move temp file {} to owned data location {}: {cause}",
                    path.display(),
                    target.display()
                );
            }
            DataLocation::Owned(size)
        }
    };
    let outboard_location = match import_data.outboard {
        MemOrFile::Mem(bytes) if bytes.is_empty() => OutboardLocation::NotNeeded,
        MemOrFile::Mem(bytes) => OutboardLocation::Inline(bytes),
        MemOrFile::File(path) => {
            // the same caveat as above applies here
            let target = ctx.options().path.owned_outboard_path(&hash);
            trace!(
                "moving temp file to owned outboard location: {} -> {}",
                path.display(),
                target.display()
            );
            if let Err(cause) = fs::rename(&path, &target) {
                error!(
                    "failed to move temp file {} to owned outboard location {}: {cause}",
                    path.display(),
                    target.display()
                );
            }
            OutboardLocation::Owned
        }
    };
    if let Some(handle) = handle {
        let data = match &data_location {
            DataLocation::Inline(data) => MemOrFile::Mem(data.clone()),
            DataLocation::Owned(size) => {
                let path = ctx.options().path.owned_data_path(&hash);
                let file = fs::File::open(&path)?;
                MemOrFile::File(FixedSize::new(file, *size))
            }
            DataLocation::External(paths, size) => {
                let Some(path) = paths.into_iter().next() else {
                    anyhow::bail!("no external data path");
                };
                let file = fs::File::open(&path)?;
                MemOrFile::File(FixedSize::new(file, *size))
            }
        };
        let outboard = match &outboard_location {
            OutboardLocation::NotNeeded => MemOrFile::Mem(Bytes::new()),
            OutboardLocation::Inline(data) => MemOrFile::Mem(data.clone()),
            OutboardLocation::Owned => {
                let path = ctx.options().path.owned_outboard_path(&hash);
                let file = fs::File::open(&path)?;
                MemOrFile::File(file)
            }
        };
        handle.complete(data, outboard);
    }
    let state = EntryState::Complete {
        data_location: data_location,
        outboard_location: outboard_location,
    };
    ctx.update(hash, state).await?;
    Ok(())
}

#[instrument(skip_all, fields(hash = %cmd.hash_short()))]
async fn import_bao(cmd: ImportBao, ctx: HashContext) {
    trace!("{cmd:?}");
    let ImportBao { size, rx, tx, hash } = cmd;
    let res = match ctx.get_or_create(hash).await {
        Ok(handle) => import_bao_impl(size, rx, handle, ctx).await,
        Err(cause) => Err(cause),
    };
    trace!("{res:?}");
    tx.send(res).ok();
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
    ctx: HashContext,
) -> anyhow::Result<()> {
    trace!(
        "importing bao: {} {} bytes",
        handle.hash().fmt_short(),
        size
    );
    let mut batch = Vec::<BaoContentItem>::new();
    let mut ranges = ChunkRanges::empty();
    while let Some(item) = data.recv().await {
        // if the batch is not empty, the last item is a leaf and the current item is a parent, write the batch
        if !batch.is_empty() && batch[batch.len() - 1].is_leaf() && item.is_parent() {
            handle.write_batch(size, &batch, &ranges, &ctx.ctx).await?;
            batch.clear();
            ranges = ChunkRanges::empty();
        }
        if let BaoContentItem::Leaf(leaf) = &item {
            let leaf_range = chunk_range(leaf);
            if is_validated(size, &leaf_range) {
                anyhow::ensure!(
                    size.get() == leaf.offset + leaf.data.len() as u64,
                    "invalid size"
                );
            }
            ranges |= leaf_range;
        }
        batch.push(item);
    }
    if !batch.is_empty() {
        handle.write_batch(size, &batch, &ranges, &ctx.ctx).await?;
    }
    Ok(())
}

#[instrument(skip_all, fields(hash = %cmd.hash_short()))]
async fn observe(cmd: Observe, ctx: HashContext) {
    let Ok(handle) = ctx.get_or_create(cmd.hash).await else {
        return;
    };
    let receiver_dropped = cmd.out.receiver_dropped();
    handle.add_observer(cmd.out);
    // this keeps the handle alive until the observer is dropped
    receiver_dropped.await;
    // give the handle a chance to clean up the dropped observer
    handle.observer_dropped();
}

#[instrument(skip_all, fields(hash = %cmd.hash_short()))]
async fn export_bao(cmd: ExportBao, ctx: HashContext) {
    let Ok(send) = cmd.tx.clone().reserve_owned().await else {
        return;
    };
    match ctx.get_or_create(cmd.hash).await {
        Ok(handle) => {
            if let Err(cause) = export_bao_impl(cmd, handle).await {
                send.send(
                    bao_tree::io::EncodeError::Io(io::Error::new(io::ErrorKind::Other, cause))
                        .into(),
                );
            }
        }
        Err(cause) => {
            let cause = anyhow::anyhow!("failed to open file: {cause}");
            send.send(
                bao_tree::io::EncodeError::Io(io::Error::new(io::ErrorKind::Other, cause)).into(),
            );
        }
    }
}

async fn export_bao_impl(cmd: ExportBao, handle: BaoFileHandle) -> anyhow::Result<()> {
    let ExportBao {
        ranges,
        tx: out,
        hash,
    } = cmd;
    trace!(
        "exporting bao: {hash} {ranges:?} size={}",
        handle.current_size()?
    );
    debug_assert!(handle.hash() == Hash::from(hash), "hash mismatch");
    let data = handle.data_reader();
    let outboard = handle.outboard()?;
    traverse_ranges_validated(data, outboard, &ranges, &out).await;
    Ok(())
}

#[instrument(skip_all, fields(hash = %cmd.hash_short()))]
async fn export_path(cmd: ExportPath, ctx: HashContext) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    match ctx.get_or_create(cmd.hash).await {
        Ok(handle) => {
            if let Err(cause) = export_path_impl(cmd, handle, ctx).await {
                send.send(cause.into());
            }
        }
        Err(cause) => {
            let cause = anyhow::anyhow!("failed to open file: {cause}");
            send.send(cause.into());
        }
    }
}

async fn export_path_impl(
    cmd: ExportPath,
    handle: BaoFileHandle,
    ctx: HashContext,
) -> anyhow::Result<()> {
    let (data, outboard_location) = {
        let guard = handle.storage.read().unwrap();
        let Some(BaoFileStorage::Complete(complete)) = guard.deref() else {
            anyhow::bail!("not a complete file");
        };
        let data = {
            match &complete.data {
                MemOrFile::Mem(data) => MemOrFile::Mem(data.clone()),
                MemOrFile::File(file) => {
                    let file = file.try_clone()?;
                    MemOrFile::File(file)
                }
            }
        };
        let outboard = match &complete.outboard {
            MemOrFile::Mem(data) => OutboardLocation::inline(data.clone()),
            MemOrFile::File(_) => OutboardLocation::Owned,
        };
        (data, outboard)
    };
    match data {
        MemOrFile::Mem(data) => {
            let mut target = fs::File::create(&cmd.target)?;
            target.write_all(&data)?;
        }
        MemOrFile::File(file) => match cmd.mode {
            ExportMode::Copy => {
                let mut target = fs::File::create(&cmd.target)?;
                copy_with_progress(&file, file.size, &mut target, &cmd.out).await?;
            }
            ExportMode::TryReference => {
                let hash = handle.hash();
                let owned_path = ctx.options().path.owned_data_path(&hash);
                match std::fs::rename(&owned_path, &cmd.target) {
                    Ok(()) => {}
                    Err(cause) => {
                        const ERR_CROSS: i32 = 18;
                        if cause.raw_os_error() == Some(ERR_CROSS) {
                            let mut target = fs::File::create(&cmd.target)?;
                            let size = file.size;
                            copy_with_progress(&file, size, &mut target, &cmd.out).await?;
                        } else {
                            anyhow::bail!("failed to move file: {cause}");
                        }
                    }
                }
                ctx.set(
                    hash,
                    EntryState::Complete {
                        data_location: DataLocation::External(vec![cmd.target], file.size),
                        outboard_location,
                    },
                )
                .await?;
            }
        },
    }
    cmd.out.send(ExportProgress::Done).await?;
    Ok(())
}

async fn copy_with_progress(
    file: impl ReadAt,
    size: u64,
    target: &mut impl Write,
    out: &mpsc::Sender<ExportProgress>,
) -> anyhow::Result<()> {
    let mut offset = 0;
    let mut buf = vec![0u8; 1024 * 1024];
    while offset < size {
        let remaining = buf.len().min((size - offset) as usize);
        let buf: &mut [u8] = &mut buf[..remaining];
        file.read_exact_at(offset, buf)?;
        target.write_all(buf)?;
        out.send_progress(ExportProgress::CopyProgress { offset })?;
        yield_now().await;
        offset += buf.len() as u64;
    }
    Ok(())
}

impl FsStore {
    /// Load or create a new store.
    pub async fn load(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = root.as_ref();
        let db_path = path.join("blobs.db");
        let options = Options::new(path);
        Self::load_with_opts(db_path, options).await
    }

    /// Load or create a new store with custom options, returning an additional sender for file store specific commands.
    pub async fn load_with_opts(path: PathBuf, options: Options) -> anyhow::Result<FsStore> {
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
                rt.into(),
                commands_rx,
                fs_commands_rx,
                fs_commands_tx.clone(),
                Arc::new(options),
            ))
            .await??;
        handle.spawn(actor.run());
        Ok(FsStore::new(commands_tx, fs_commands_tx))
    }
}

pub struct FsStore {
    sender: mpsc::Sender<Command>,
    db: mpsc::Sender<InternalCommand>,
}

impl Deref for FsStore {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        Store::ref_from_sender(&self.sender)
    }
}

impl AsRef<Store> for FsStore {
    fn as_ref(&self) -> &Store {
        self.deref()
    }
}

impl FsStore {
    fn new(sender: mpsc::Sender<Command>, db: mpsc::Sender<InternalCommand>) -> Self {
        Self { sender, db }
    }

    pub async fn dump(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db.send(meta::Dump { tx }.into()).await?;
        rx.await??;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use core::panic;
    use std::{collections::HashMap, io::Read, u64};

    use bao_tree::{
        blake3,
        io::{outboard::PreOrderMemOutboard, round_up_to_chunks, round_up_to_chunks_groups},
        BlockSize, ChunkRanges,
    };
    use n0_future::{stream, SinkExt, Stream, StreamExt};
    use testresult::TestResult;
    use walkdir::WalkDir;

    use super::*;
    use crate::store::{
        bitfield::Bitfield,
        util::{observer::Aggregator, read_checksummed, SliceInfoExt, Tag},
        HashAndFormat, IROH_BLOCK_SIZE,
    };

    /// Interesting sizes for testing.
    pub const INTERESTING_SIZES: [usize; 8] = [
        0,               // annoying corner case - always present, handled by the api
        1,               // less than 1 chunk, data inline, outboard not needed
        1024,            // exactly 1 chunk, data inline, outboard not needed
        1024 * 16 - 1,   // less than 1 chunk group, data inline, outboard not needed
        1024 * 16,       // exactly 1 chunk group, data inline, outboard not needed
        1024 * 16 + 1,   // data file, outboard inline (just 1 hash pair)
        1024 * 1024,     // data file, outboard inline (many hash pairs)
        1024 * 1024 * 8, // data file, outboard file
    ];

    /// Create n0 flavoured bao. Note that this can be used to request ranges below a chunk group size,
    /// which can not be exported via bao because we don't store hashes below the chunk group level.
    fn create_n0_bao(data: &[u8], ranges: &ChunkRanges) -> anyhow::Result<(blake3::Hash, Vec<u8>)> {
        let outboard = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
        let mut encoded = Vec::new();
        let size = data.len() as u64;
        encoded.extend_from_slice(&size.to_le_bytes());
        bao_tree::io::sync::encode_ranges_validated(&data, &outboard, ranges, &mut encoded)?;
        Ok((outboard.root, encoded))
    }

    fn round_up_request(size: u64, ranges: &ChunkRanges) -> ChunkRanges {
        let last_chunk = ChunkNum::chunks(size);
        let data_range = ChunkRanges::from(..last_chunk);
        let ranges = if !data_range.intersects(ranges) && !ranges.is_empty() {
            if last_chunk == 0 {
                ChunkRanges::all()
            } else {
                ChunkRanges::from(last_chunk - 1..)
            }
        } else {
            ranges.clone()
        };
        round_up_to_chunks_groups(ranges, IROH_BLOCK_SIZE)
    }

    fn create_n0_bao_full(
        data: &[u8],
        ranges: &ChunkRanges,
    ) -> anyhow::Result<(blake3::Hash, ChunkRanges, Vec<u8>)> {
        let ranges = round_up_request(data.len() as u64, ranges);
        let (hash, encoded) = create_n0_bao(data, &ranges)?;
        Ok((hash, ranges, encoded))
    }

    #[tokio::test]
    async fn observe() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir, options).await?;
        let sizes = INTERESTING_SIZES;
        for size in sizes {
            let data = test_data(size);
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            let obs = store.observe(hash);
            let task = tokio::spawn(async move {
                let mut agg = obs.aggregated();
                while let Some(_delta) = agg.next().await {
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

    async fn import_path_observe() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir, options).await?;
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
        Ok(())
    }

    /// Generate test data for size n.
    ///
    /// We don't really care about the content, since we assume blake3 works.
    /// The only thing it should not be is all zeros, since that is what you
    /// will get for a gap.
    pub fn test_data(n: usize) -> Bytes {
        let mut res = Vec::with_capacity(n);
        // Using uppercase A-Z (65-90), 26 possible characters
        for i in 0..n {
            // Change character every 1024 bytes
            let block_num = i / 1024;
            // Map to uppercase A-Z range (65-90)
            let ascii_val = 65 + (block_num % 26) as u8;
            res.push(ascii_val);
        }
        Bytes::from(res)
    }

    async fn await_completion(mut obs: Aggregator<Bitfield>) {
        while let Some(_) = obs.next().await {
            if obs.state().is_complete() {
                break;
            }
        }
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    async fn test_import_byte_stream() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = blake3::hash(&expected);
            let stream = bytes_to_stream(expected.clone(), 1023);
            let obs = store.observe(expected_hash).aggregated();
            let actual_hash = store.import_byte_stream(stream).await?;
            assert_eq!(expected_hash, actual_hash);
            // we must at some point see completion, otherwise the test will hang
            await_completion(obs).await;
            let actual = store.export_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
        }
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    async fn test_import_bytes() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(&db_dir).await?;
        let sizes = INTERESTING_SIZES;
        println!("{}", Options::new(&db_dir).is_inlined_data(16385));
        for size in sizes {
            let expected = test_data(size);
            let expected_hash = blake3::hash(&expected);
            let obs = store.observe(expected_hash).aggregated();
            let actual_hash = store.import_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, actual_hash);
            // we must at some point see completion, otherwise the test will hang
            await_completion(obs).await;
            let actual = store.export_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
        }
        store.shutdown().await?;
        dump_dir_full(db_dir)?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    // #[ignore = "flaky. I need a reliable way to keep the handle alive"]
    async fn test_roundtrip_bytes_small() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES
            .into_iter()
            .filter(|x| *x != 0 && *x <= IROH_BLOCK_SIZE.bytes())
        {
            let expected = test_data(size);
            let expected_hash = blake3::hash(&expected);
            let obs = store.observe(expected_hash).aggregated();
            let actual_hash = store.import_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, actual_hash);
            let actual = store.export_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
            assert_eq!(
                &expected.addr(),
                &actual.addr(),
                "address mismatch for size {}",
                size
            );
            // we must at some point see completion, otherwise the test will hang
            // keep the handle alive by observing until the end, otherwise the handle
            // will change and the bytes won't be the same instance anymore
            await_completion(obs).await;
        }
        store.shutdown().await?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    async fn test_import_path() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = blake3::hash(&expected);
            let path = testdir.path().join(format!("in-{}", size));
            fs::write(&path, &expected)?;
            let obs = store.observe(expected_hash).aggregated();
            let actual_hash = store.import_path(&path).await?;
            assert_eq!(expected_hash, actual_hash);
            // we must at some point see completion, otherwise the test will hang
            await_completion(obs).await;
            let actual = store.export_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual, "size={size}");
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    async fn test_export_path() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = blake3::hash(&expected);
            let actual_hash = store.import_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, actual_hash);
            let out_path = testdir.path().join(format!("out-{}", size));
            store.export_path(expected_hash, &out_path).await?;
            let actual = fs::read(&out_path)?;
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    #[tokio::test]
    async fn import_bao_persistence_full() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = vec![0u8; size];
                let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
                let data = Bytes::from(encoded);
                store
                    .import_bao_bytes(hash, ChunkRanges::all(), data)
                    .await?;
            }
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let expected = vec![0u8; size];
                let hash = blake3::hash(&expected);
                let actual = store
                    .export_bao(hash, ChunkRanges::all())
                    .data_to_vec()
                    .await?;
                assert_eq!(&expected, &actual);
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn import_bao_persistence_just_size() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = [
            0,               // always there
            1,               // will become complete due to last chunk
            1024,            // will become complete due to last chunk
            1024 * 16 - 1,   // will become complete due to rounding up of chunks
            1024 * 16,       // will become complete due to rounding up of chunks
            1024 * 16 + 1,   // will remain incomplete as file, needs outboard
            1024 * 1024,     // will remain incomplete as file, needs outboard
            1024 * 1024 * 8, // will remain incomplete as file, needs file outboard
        ];
        let db_dir = testdir.path().join("db");
        let just_size = ChunkRanges::from(ChunkNum(u64::MAX)..);
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, expected) = create_n0_bao_full(&data, &just_size)?;
                let actual = match store.export_bao(hash, ranges).bao_to_vec().await {
                    Ok(actual) => actual,
                    Err(cause) => panic!("failed to export size={size}: {cause}"),
                };
                assert_eq!(&expected, &actual);
            }
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    #[tokio::test]
    async fn import_bao_persistence_two_stages() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = [
            0,               // always there
            1,               // will become complete due to last chunk
            1024,            // will become complete due to last chunk
            1024 * 16 - 1,   // will become complete due to rounding up of chunks
            1024 * 16,       // will become complete due to rounding up of chunks
            1024 * 16 + 1,   // will remain incomplete as file, needs outboard
            1024 * 1024,     // will remain incomplete as file, needs outboard
            1024 * 1024 * 8, // will remain incomplete as file, needs file outboard
        ];
        let db_dir = testdir.path().join("db");
        let just_size = ChunkRanges::from(ChunkNum(u64::MAX)..);
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let remaining = ChunkRanges::all() - round_up_request(size as u64, &just_size);
                if remaining.is_empty() {
                    continue;
                }
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &remaining)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        // check if the data is complete
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, expected) = create_n0_bao_full(&data, &ChunkRanges::all())?;
                let actual = match store.export_bao(hash, ranges).bao_to_vec().await {
                    Ok(actual) => actual,
                    Err(cause) => panic!("failed to export size={size}: {cause}"),
                };
                assert_eq!(&expected, &actual);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    fn just_size() -> ChunkRanges {
        ChunkRanges::from(ChunkNum(u64::MAX)..)
    }

    #[tokio::test]
    async fn import_bao_persistence_observe() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = [
            0,               // always there
            1,               // will become complete due to last chunk
            1024,            // will become complete due to last chunk
            1024 * 16 - 1,   // will become complete due to rounding up of chunks
            1024 * 16,       // will become complete due to rounding up of chunks
            1024 * 16 + 1,   // will remain incomplete as file, needs outboard
            1024 * 1024,     // will remain incomplete as file, needs outboard
            1024 * 1024 * 8, // will remain incomplete as file, needs file outboard
        ];
        let db_dir = testdir.path().join("db");
        let just_size = just_size();
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let expected_ranges = round_up_request(size as u64, &just_size);
                let data = test_data(size);
                let hash = blake3::hash(&data);
                let mut stream = store.observe(hash).aggregated();
                let Some(_) = stream.next().await else {
                    panic!("no update");
                };
                assert_eq!(stream.state().ranges, expected_ranges);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn import_bao_persistence_recover() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = [
            0,               // always there
            1,               // will become complete due to last chunk
            1024,            // will become complete due to last chunk
            1024 * 16 - 1,   // will become complete due to rounding up of chunks
            1024 * 16,       // will become complete due to rounding up of chunks
            1024 * 16 + 1,   // will remain incomplete as file, needs outboard
            1024 * 1024,     // will remain incomplete as file, needs outboard
            1024 * 1024 * 8, // will remain incomplete as file, needs file outboard
        ];
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let just_size = just_size();
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load_with_opts(db_dir.clone(), options.clone()).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        delete_rec(testdir.path(), "bitfield")?;
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load_with_opts(db_dir.clone(), options.clone()).await?;
            for size in sizes {
                let expected_ranges = round_up_request(size as u64, &just_size);
                let data = test_data(size);
                let hash = blake3::hash(&data);
                let mut stream = store.observe(hash).aggregated();
                let Some(_) = stream.next().await else {
                    panic!("no update");
                };
                println!(
                    "expected={:?} actual={:?}",
                    expected_ranges,
                    stream.state().ranges
                );
                // assert_eq!(stream.state().ranges, expected_ranges, "size={}", size);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn import_bytes_persistence_full() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let data = Bytes::from(data);
                store.import_bytes(data.clone()).await?;
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let expected = test_data(size);
                let hash = blake3::hash(&expected);
                let Ok(actual) = store
                    .export_bao(hash, ChunkRanges::all())
                    .data_to_vec()
                    .await
                else {
                    panic!("failed to export size={size}");
                };
                assert_eq!(&expected, &actual, "size={}", size);
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        let haf = HashAndFormat::raw(Hash::from([0u8; 32]));
        store.set_tag(Tag::from("test"), haf).await?;
        store.set_tag(Tag::from("boo"), haf).await?;
        store.set_tag(Tag::from("bar"), haf).await?;
        let sizes = INTERESTING_SIZES;
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
            store.export_path(*hash, path).await?;
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
        store.dump().await?;

        for size in sizes {
            let data = test_data(size);
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            store.import_bao_bytes(hash, ranges, bao.into()).await?;
        }

        for (_hash, _bao_tree) in bao_by_hash {
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

    pub fn delete_rec(root_dir: impl AsRef<Path>, extension: &str) -> Result<(), std::io::Error> {
        // Remove leading dot if present, so we have just the extension
        let ext = extension.trim_start_matches('.').to_lowercase();

        for entry in WalkDir::new(root_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            if path.is_file() {
                if let Some(file_ext) = path.extension() {
                    if file_ext.to_string_lossy().to_lowercase() == ext {
                        println!("Deleting: {}", path.display());
                        fs::remove_file(path)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn dump_dir(path: impl AsRef<Path>) -> io::Result<()> {
        let mut entries: Vec<_> = WalkDir::new(&path)
            .into_iter()
            .filter_map(Result::ok) // Skip errors
            .collect();

        // Sort by path (name at each depth)
        entries.sort_by(|a, b| a.path().cmp(b.path()));

        for entry in entries {
            let depth = entry.depth();
            let indent = "  ".repeat(depth); // Two spaces per level
            let name = entry.file_name().to_string_lossy();
            let size = entry.metadata()?.len(); // Size in bytes

            if entry.file_type().is_file() {
                println!("{}{} ({} bytes)", indent, name, size);
            } else if entry.file_type().is_dir() {
                println!("{}{}/", indent, name);
            }
        }
        Ok(())
    }

    pub fn dump_dir_full(path: impl AsRef<Path>) -> io::Result<()> {
        let mut entries: Vec<_> = WalkDir::new(&path)
            .into_iter()
            .filter_map(Result::ok) // Skip errors
            .collect();

        // Sort by path (name at each depth)
        entries.sort_by(|a, b| a.path().cmp(b.path()));

        for entry in entries {
            let depth = entry.depth();
            let indent = "  ".repeat(depth);
            let name = entry.file_name().to_string_lossy();

            if entry.file_type().is_dir() {
                println!("{}{}/", indent, name);
            } else if entry.file_type().is_file() {
                let size = entry.metadata()?.len();
                println!("{}{} ({} bytes)", indent, name, size);

                // Dump depending on file type
                let path = entry.path();
                if name.ends_with(".data") {
                    print!("{}  ", indent);
                    dump_file(path, 1024 * 16)?;
                } else if name.ends_with(".obao4") {
                    print!("{}  ", indent);
                    dump_file(path, 64)?;
                } else if name.ends_with(".sizes4") {
                    print!("{}  ", indent);
                    dump_file(path, 8)?;
                } else if name.ends_with(".bitfield") {
                    match read_checksummed::<Bitfield>(path) {
                        Ok(bitfield) => {
                            println!("{}  bitfield: {:?}", indent, bitfield);
                        }
                        Err(cause) => {
                            println!("{}  bitfield: error: {cause}", indent);
                        }
                    }
                } else {
                    continue; // Skip content dump for other files
                };
            }
        }
        Ok(())
    }

    pub fn dump_file<P: AsRef<Path>>(path: P, chunk_size: u64) -> io::Result<()> {
        let bits = file_bits(path, chunk_size)?;
        println!("{}", print_bitfield_ansi(bits));
        Ok(())
    }

    pub fn file_bits(path: impl AsRef<Path>, chunk_size: u64) -> io::Result<Vec<bool>> {
        let file = fs::File::open(&path)?;
        let file_size = file.metadata()?.len();
        let mut buffer = vec![0u8; chunk_size as usize];
        let mut bits = Vec::new();

        let mut offset = 0u64;
        while offset < file_size {
            let remaining = file_size - offset;
            let current_chunk_size = chunk_size.min(remaining);

            let mut chunk = &mut buffer[..current_chunk_size as usize];
            file.read_exact_at(offset, &mut chunk)?;

            let has_non_zero = chunk.iter().any(|&byte| byte != 0);
            bits.push(has_non_zero);

            offset += current_chunk_size;
        }

        Ok(bits)
    }

    fn print_bitfield(bits: impl IntoIterator<Item = bool>) -> String {
        bits.into_iter()
            .map(|bit| if bit { '#' } else { '_' })
            .collect()
    }

    fn print_bitfield_ansi(bits: impl IntoIterator<Item = bool>) -> String {
        let mut result = String::new();
        let mut iter = bits.into_iter();

        while let Some(b1) = iter.next() {
            let b2 = iter.next();

            // ANSI color codes
            let white_fg = "\x1b[97m"; // bright white foreground
            let reset = "\x1b[0m"; // reset all attributes
            let gray_bg = "\x1b[100m"; // bright black (gray) background
            let black_bg = "\x1b[40m"; // black background

            let colored_char = match (b1, b2) {
                (true, Some(true)) => format!("{}{}{}", white_fg, '█', reset), // 11 - solid white on default background
                (true, Some(false)) => format!("{}{}{}{}", gray_bg, white_fg, '▌', reset), // 10 - left half white on gray background
                (false, Some(true)) => format!("{}{}{}{}", gray_bg, white_fg, '▐', reset), // 01 - right half white on gray background
                (false, Some(false)) => format!("{}{}{}{}", gray_bg, white_fg, ' ', reset), // 00 - space with gray background
                (true, None) => format!("{}{}{}{}", black_bg, white_fg, '▌', reset), // 1 (pad 0) - left half white on black background
                (false, None) => format!("{}{}{}{}", black_bg, white_fg, ' ', reset), // 0 (pad 0) - space with black background
            };

            result.push_str(&colored_char);
        }

        // Ensure we end with a reset code to prevent color bleeding
        result.push_str("\x1b[0m");
        result
    }

    fn bytes_to_stream(
        bytes: Bytes,
        chunk_size: usize,
    ) -> impl Stream<Item = io::Result<Bytes>> + 'static {
        assert!(chunk_size > 0, "Chunk size must be greater than 0");
        stream::unfold((bytes, 0), move |(bytes, offset)| async move {
            if offset >= bytes.len() {
                None
            } else {
                let chunk_len = chunk_size.min(bytes.len() - offset);
                let chunk = bytes.slice(offset..offset + chunk_len);
                Some((Ok(chunk), (bytes, offset + chunk_len)))
            }
        })
    }
}
