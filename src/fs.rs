use crate::bao_file::BaoFileConfig;
use crate::bao_file::BaoFileHandle;
use crate::bao_file::BaoFileHandleWeak;
use crate::bao_file::BaoFileStorage;
use crate::util::MemOrFile;
use crate::util::SenderProgressExt;
use crate::Hash;
use bao_tree::io::sync::ReadAt;
use bytes::Bytes;
use entry_state::DataLocation;
use entry_state::OutboardLocation;
use import::ImportSource;
use meta::GetResult;
use n0_future::future::yield_now;
use nested_enum_utils::enum_conversions;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinSet;
use tracing::error;
use tracing::trace;
use tracing::warn;

mod entry_state;
mod import;
mod meta;
mod options;
mod util;
use options::{Options, PathOptions};

use entry_state::EntryState;
use import::{import_byte_stream_task, import_bytes_task, import_path_task, ImportEntry};

use super::proto::*;
use super::Store;

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
struct Actor {
    // Store options such as paths and inline thresholds, in an Arc to cheaply share with tasks.
    options: Arc<Options>,
    // Receiver for incoming user commands.
    commands: mpsc::Receiver<Command>,
    // Receiver for incoming file store specific commands.
    fs_commands: mpsc::Receiver<FsCommand>,
    get_results_tx: mpsc::Sender<meta::GetResult>,
    get_results_rx: mpsc::Receiver<meta::GetResult>,
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
    waiting: BTreeMap<Hash, Vec<ExportPath>>,
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
                    self.waiting.entry(hash).or_default().push(cmd);
                    return;
                };
                self.unit_tasks.spawn(export_path_task(cmd, handle));
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

    fn handle_get_result(&mut self, res: GetResult) {
        let hash: crate::Hash = res.hash.into();
        let config = Arc::new(BaoFileConfig::new(
            Arc::new(self.options.path.data_path.clone()),
            self.options.inline.max_data_inlined as usize,
            None,
        ));
        let handle = match self.handles.get(&hash).and_then(|weak| weak.upgrade()) {
            Some(handle) => handle,
            None => {
                println!("opening files from handle!");
                let handle = match res.state {
                    Ok(Some(EntryState::Complete {
                        data_location,
                        outboard_location,
                    })) => {
                        let data_location = match data_location {
                            DataLocation::Inline(data) => MemOrFile::Mem(data),
                            DataLocation::Owned(size) => {
                                let path = self.options.path.owned_data_path(&hash);
                                let file = std::fs::File::open(&path).unwrap();
                                MemOrFile::File((file, size))
                            }
                            DataLocation::External(paths, size) => {
                                let path = paths.into_iter().next().unwrap();
                                let file = std::fs::File::open(&path).unwrap();
                                MemOrFile::File((file, size))
                            }
                        };
                        let outboard_location = match outboard_location {
                            OutboardLocation::NotNeeded => MemOrFile::Mem(Bytes::new()),
                            OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                            OutboardLocation::Owned => {
                                let path = self.options.path.owned_outboard_path(&hash);
                                let file = std::fs::File::open(&path).unwrap();
                                let size = file.metadata().unwrap().len();
                                MemOrFile::File((file, size))
                            }
                        };
                        BaoFileHandle::complete(config, hash, data_location, outboard_location)
                    }
                    Ok(Some(EntryState::Partial { size })) => {
                        BaoFileHandle::incomplete_file(config, hash).unwrap()
                    }
                    Ok(None) => BaoFileHandle::incomplete_mem(config, hash),
                    Err(_cause) => {
                        panic!()
                    }
                };
                println!("OPENED files from handle!");
                self.handles.insert(hash, handle.downgrade());
                handle
            }
        };
        let tasks = self.waiting.remove(&hash).unwrap_or_default();
        for task in tasks {
            self.unit_tasks
                .spawn(export_path_task(task, handle.clone()));
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
                Some(res) = self.get_results_rx.recv() => {
                    self.handle_get_result(res);
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
        rt.spawn(db_actor.run());
        Ok(Self {
            options,
            commands,
            fs_commands,
            get_results_tx,
            get_results_rx,
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

async fn export_path_task(cmd: ExportPath, handle: BaoFileHandle) {
    let Ok(send) = cmd.out.clone().reserve_owned().await else {
        return;
    };
    if let Err(cause) = export_path_impl(cmd, handle).await {
        send.send(ExportProgress::Error { cause });
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
    use crate::{fs::meta::Dump, test, util::Tag, HashAndFormat};

    use super::*;
    use testresult::TestResult;
    use tokio::sync::oneshot;

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
        for size in sizes {
            let data = vec![0u8; size];
            let res = store.import_bytes(data.into()).await?;
            hashes.push(res);
        }
        store.sync_db().await?;
        for hash in hashes {
            let path = testdir.path().join(format!("{hash}.txt"));
            store.export_file(hash, path).await?;
        }
        debug.dump().await?;
        Ok(())
    }
}
