use std::{
    any,
    collections::BTreeMap,
    f32::consts::E,
    fs,
    io::Write,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use bao_tree::{
    io::{
        mixed::traverse_ranges_validated, outboard::PreOrderOutboard, sync::ReadAt, BaoContentItem,
        Leaf,
    },
    BaoTree, ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use entry_state::{DataLocation, OutboardLocation};
use import::ImportSource;
use meta::GetResult;
use n0_future::{future::yield_now, io};
use nested_enum_utils::enum_conversions;
use tokio::{
    sync::{mpsc, mpsc::OwnedPermit, oneshot},
    task::{JoinError, JoinSet},
};
use tracing::{error, trace, warn};

use crate::{
    bao_file::{BaoFileConfig, BaoFileHandle, BaoFileHandleWeak, BaoFileStorage, HandleChange},
    util::{MemOrFile, SenderProgressExt},
    Hash, IROH_BLOCK_SIZE,
};

mod entry_state;
mod import;
mod meta;
mod options;
mod util;
use entry_state::EntryState;
use import::{import_byte_stream_task, import_bytes_task, import_path_task, ImportEntry};
use options::{Options, PathOptions};

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
pub enum FsCommand {
    Dump(meta::Dump),
}

#[derive(Debug)]
#[enum_conversions()]
enum WaitingTask {
    ExportPath(ExportPath),
    ExportBao(ExportBao),
    ImportBao(ImportBao),
    Observe(Observe),
}

#[derive(Debug)]
struct Actor {
    // Store options such as paths and inline thresholds, in an Arc to cheaply share with tasks.
    options: Arc<Options>,
    // Receiver for incoming user commands.
    commands: mpsc::Receiver<Command>,
    // Receiver for incoming file store specific commands.
    fs_commands: mpsc::Receiver<FsCommand>,
    get_results_tx: mpsc::Sender<meta::GetResult>,
    db_answers_rx: mpsc::Receiver<meta::GetResult>,
    // Tasks that are not expected to return a result. These are mainly bao import and export tasks.
    unit_tasks: JoinSet<()>,
    // Import tasks that are expected to return an import entry that is to be integrated with the store data.
    import_tasks: JoinSet<Option<ImportEntry>>,
    // Database actor that handles meta data.
    db: mpsc::Sender<meta::Command>,
    // our private tokio runtime. It has to live somewhere.
    rt: Option<tokio::runtime::Runtime>,
    // monotonic counter
    epoch: u64,
    // handles
    handles: BTreeMap<Hash, BaoFileHandleWeak>,
    waiting: BTreeMap<Hash, Vec<WaitingTask>>,
    config: Arc<BaoFileConfig>,
}

impl Actor {
    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::CreateTag(CreateTag { hash, tx }) => {
                self.db.send(meta::CreateTag { hash, tx }.into()).await.ok();
            }
            Command::SetTag(SetTag { tag, value, tx }) => {
                self.db
                    .send(meta::SetTag { tag, value, tx }.into())
                    .await
                    .ok();
            }
            Command::Tags(Tags { tx }) => {
                self.db
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
                self.import_tasks
                    .spawn(import_bytes_task(cmd, self.options.clone()));
            }
            Command::ImportByteStream(cmd) => {
                self.import_tasks
                    .spawn(import_byte_stream_task(cmd, self.options.clone()));
            }
            Command::ImportPath(cmd) => {
                self.import_tasks
                    .spawn(import_path_task(cmd, self.options.clone()));
            }
            Command::ExportPath(cmd) => {
                let hash: crate::Hash = cmd.hash.into();
                let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) else {
                    let tx = self.get_results_tx.clone().reserve_owned().await.unwrap();
                    self.db.send(meta::Get { hash, tx }.into()).await.ok();
                    self.waiting.entry(hash).or_default().push(cmd.into());
                    return;
                };
                self.unit_tasks.spawn(export_path_task(cmd, Ok(handle)));
            }
            Command::ExportBao(cmd) => {
                let hash: crate::Hash = cmd.hash.into();
                let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) else {
                    let tx = self.get_results_tx.clone().reserve_owned().await.unwrap();
                    self.db.send(meta::Get { hash, tx }.into()).await.ok();
                    self.waiting.entry(hash).or_default().push(cmd.into());
                    return;
                };
                self.unit_tasks.spawn(export_bao_task(cmd, Ok(handle)));
            }
            Command::ImportBao(cmd) => {
                let hash: crate::Hash = cmd.hash.into();
                let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) else {
                    let tx = self.get_results_tx.clone().reserve_owned().await.unwrap();
                    self.db.send(meta::Get { hash, tx }.into()).await.ok();
                    self.waiting.entry(hash).or_default().push(cmd.into());
                    return;
                };
                let sender = self.db.clone();
                self.unit_tasks
                    .spawn(import_bao_task(cmd, Ok(handle), sender));
            }
            Command::Observe(cmd) => {
                let hash: crate::Hash = cmd.hash.into();
                let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) else {
                    let tx = self.get_results_tx.clone().reserve_owned().await.unwrap();
                    self.db.send(meta::Get { hash, tx }.into()).await.ok();
                    self.waiting.entry(hash).or_default().push(cmd.into());
                    return;
                };
            }
            Command::SyncDb(cmd) => {
                self.db.send(cmd.into()).await.ok();
            }
            _ => {}
        }
    }

    async fn handle_fs_command(&mut self, cmd: FsCommand) {
        match cmd {
            FsCommand::Dump(cmd) => {
                self.db.send(cmd.into()).await.ok();
            }
        }
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    async fn finish_import(&mut self, res: Result<Option<ImportEntry>, JoinError>) {
        let import_data = match res {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                warn!("import failed");
                return;
            }
            Err(e) => {
                if e.is_cancelled() {
                    warn!("import task failed: {e}");
                } else {
                    error!("import task panicked: {e}");
                }
                return;
            }
        };
        let hash: crate::Hash = import_data.hash.into();
        // convert the import source to a data location and drop the open files
        let data_location = match import_data.source {
            ImportSource::Memory(data) => DataLocation::Inline(data),
            ImportSource::External(path, _file, size) => DataLocation::External(vec![path], size),
            ImportSource::TempFile(path, _file, size) => {
                // this will always work on any unix, but on windows there might be an issue if the target file is open!
                // possibly open with FILE_SHARE_DELETE on windows?
                let target = self.options.path.owned_data_path(&hash);
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
                let target = self.options.path.owned_outboard_path(&hash);
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
        self.db
            .send(
                meta::Update {
                    hash,
                    state,
                    epoch: self.epoch,
                    tx: None,
                }
                .into(),
            )
            .await
            .ok();
        let hash = hash.into();
        import_data.out.send(ImportProgress::Done { hash });
    }

    fn open_bao_file(
        &self,
        hash: &Hash,
        state: EntryState<Bytes>,
    ) -> anyhow::Result<BaoFileHandle> {
        let config = self.config.clone();
        Ok(match state {
            EntryState::Complete {
                data_location,
                outboard_location,
            } => {
                let data_location = match data_location {
                    DataLocation::Inline(data) => MemOrFile::Mem(data),
                    DataLocation::Owned(size) => {
                        let path = self.options.path.owned_data_path(&hash);
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File((file, size))
                    }
                    DataLocation::External(paths, size) => {
                        let Some(path) = paths.into_iter().next() else {
                            anyhow::bail!("no external data path");
                        };
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File((file, size))
                    }
                };
                let outboard_location = match outboard_location {
                    OutboardLocation::NotNeeded => MemOrFile::Mem(Bytes::new()),
                    OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                    OutboardLocation::Owned => {
                        let path = self.options.path.owned_outboard_path(hash);
                        let file = std::fs::File::open(&path)?;
                        let size = file.metadata()?.len();
                        MemOrFile::File((file, size))
                    }
                };
                BaoFileHandle::complete(config, *hash, data_location, outboard_location)
            }
            EntryState::Partial { .. } => BaoFileHandle::incomplete_file(config, *hash)?,
        })
    }

    async fn handle_db_answer(&mut self, res: GetResult) {
        let hash: crate::Hash = res.hash.into();
        let config = self.config.clone();
        let handle = match self.handles.get(&hash).and_then(|weak| weak.upgrade()) {
            Some(handle) => Ok(handle),
            None => {
                let handle = match res.state {
                    Ok(Some(entry)) => self.open_bao_file(&hash, entry),
                    Ok(None) => Ok(BaoFileHandle::incomplete_mem(config, hash)),
                    Err(cause) => Err(cause),
                };
                if let Ok(handle) = &handle {
                    self.handles.insert(hash, handle.downgrade());
                }
                handle.map_err(Arc::new)
            }
        };
        let tasks = self.waiting.remove(&hash).unwrap_or_default();
        for task in tasks {
            match task {
                WaitingTask::ExportPath(cmd) => {
                    self.unit_tasks.spawn(export_path_task(cmd, handle.clone()));
                }
                WaitingTask::ExportBao(cmd) => {
                    self.unit_tasks.spawn(export_bao_task(cmd, handle.clone()));
                }
                WaitingTask::ImportBao(cmd) => {
                    let sender = self.db.clone();
                    self.unit_tasks
                        .spawn(import_bao_task(cmd, handle.clone(), sender));
                }
                WaitingTask::Observe(cmd) => {
                    self.unit_tasks.spawn(observe_task(cmd, handle.clone()));
                }
            }
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
                    trace!("{cmd:?}");
                    self.handle_command(cmd).await;
                }
                Some(res) = self.db_answers_rx.recv() => {
                    self.handle_db_answer(res).await;
                }
                Some(cmd) = self.fs_commands.recv(), if !self.fs_commands.is_closed() => {
                    trace!("{cmd:?}");
                    self.handle_fs_command(cmd).await;
                }
                Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
                    self.finish_import(res).await;
                }
                Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
                    self.log_unit_task(res);
                }
            }
        }
    }

    async fn new(
        db_path: PathBuf,
        rt: tokio::runtime::Runtime,
        commands: mpsc::Receiver<Command>,
        fs_commands: mpsc::Receiver<FsCommand>,
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
        let (get_results_tx, get_results_rx) = mpsc::channel(100);
        let db_actor = meta::Actor::new(db_path, db_recv)?;
        let config = Arc::new(BaoFileConfig::new(
            Arc::new(options.path.data_path.clone()),
            options.inline.max_data_inlined as usize,
            None,
        ));
        rt.spawn(db_actor.run());
        Ok(Self {
            config,
            options,
            commands,
            fs_commands,
            get_results_tx,
            db_answers_rx: get_results_rx,
            unit_tasks: JoinSet::new(),
            import_tasks: JoinSet::new(),
            db: db_send,
            epoch: 0,
            handles: BTreeMap::new(),
            waiting: BTreeMap::new(),
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

async fn import_bao_task(
    cmd: ImportBao,
    handle: std::result::Result<BaoFileHandle, Arc<anyhow::Error>>,
    sender: mpsc::Sender<meta::Command>,
) {
    let ImportBao {
        size, data, out, ..
    } = cmd;
    match handle {
        Ok(handle) => {
            let res = import_bao_impl(size, data, handle, sender).await;
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
    let end = ChunkNum::full_chunks(leaf.offset + leaf.data.len() as u64);
    (start..end).into()
}

async fn import_bao_impl(
    size: u64,
    mut data: mpsc::Receiver<BaoContentItem>,
    handle: BaoFileHandle,
    sender: mpsc::Sender<meta::Command>,
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
                sender
                    .send(
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
            ranges |= chunk_range(leaf);
        }
        batch.push(item);
    }
    if !batch.is_empty() {
        handle.write_batch(size, &batch, &ranges)?;
    }
    Ok(())
}

async fn observe_task(
    cmd: Observe,
    handle: std::result::Result<BaoFileHandle, Arc<anyhow::Error>>,
) {
    let Observe { out, .. } = cmd;
    if let Ok(handle) = handle {
        observe_impl(handle, out).await;
    }
}

async fn observe_impl(handle: BaoFileHandle, out: mpsc::Sender<BitfieldEvent>) {
    handle.add_observer(out);
}

async fn export_bao_task(
    cmd: ExportBao,
    handle: std::result::Result<BaoFileHandle, Arc<anyhow::Error>>,
) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    match handle {
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

async fn export_path_task(
    cmd: ExportPath,
    handle: std::result::Result<BaoFileHandle, Arc<anyhow::Error>>,
) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    match handle {
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
    db: mpsc::Sender<FsCommand>,
}

impl DbStore {
    fn from_sender(db: mpsc::Sender<FsCommand>) -> Self {
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
    use quinn::Chunk;
    use testresult::TestResult;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{util::Tag, HashAndFormat};

    fn create_n0_bao(data: &[u8], ranges: &ChunkRanges) -> anyhow::Result<(blake3::Hash, Vec<u8>)> {
        let outboard = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
        let mut encoded = Vec::new();
        let size = data.len() as u64;
        encoded.extend_from_slice(&size.to_le_bytes());
        bao_tree::io::sync::encode_ranges_validated(&data, &outboard, ranges, &mut encoded)?;
        Ok((outboard.root, encoded))
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
