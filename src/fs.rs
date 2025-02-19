use crate::bao_file::BaoFileConfig;
use crate::bao_file::BaoFileStorage;
use crate::util::MemOrFile;
use crate::util::SenderProgressExt;
use crate::Hash;
use bytes::Bytes;
use n0_future::future::yield_now;
use n0_future::StreamExt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinSet;
use tracing::error;
use tracing::trace;

mod entry_state;
mod meta;
mod options;
mod util;
use options::{Options, PathOptions};

use entry_state::EntryState;

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

/// The inner part of a bao file handle.
#[derive(Debug)]
pub struct BaoFileHandleInner {
    pub(crate) storage: RwLock<BaoFileStorage>,
    config: Arc<BaoFileConfig>,
    hash: Hash,
}

/// A cheaply cloneable handle to a bao file, including the hash and the configuration.
#[derive(Debug, Clone, derive_more::Deref)]
pub struct BaoFileHandle(Arc<BaoFileHandleInner>);

struct Actor {
    options: Arc<Options>,
    commands: mpsc::Receiver<Command>,
    unit_tasks: JoinSet<()>,
    import_tasks: JoinSet<anyhow::Result<ImportEntry>>,
    db: mpsc::Sender<meta::Command>,
    rt: tokio::runtime::Runtime,
}

impl Actor {
    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::CreateTag(CreateTag { hash, tx }) => {
                self.db.send(meta::CreateTag { hash, tx }.into()).await.ok();
            }
            Command::SetTag(SetTag {
                tag,
                value,
                tx: out,
            }) => {
                self.db
                    .send(
                        meta::SetTag {
                            tag,
                            value,
                            tx: out,
                        }
                        .into(),
                    )
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
            Command::ImportBytes(ImportBytes { data, out }) => {
                self.import_tasks
                    .spawn(import_bytes_task(data, self.options.clone(), out));
            }
            Command::ImportByteStream(ImportByteStream { data, out }) => {
                self.import_tasks
                    .spawn(import_byte_stream_task(data, self.options.clone(), out));
            }
            Command::ImportPath(ImportPath { path, out }) => {
                self.import_tasks
                    .spawn(import_path_task(path, self.options.clone(), out));
            }
            _ => {}
        }
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
                    self.handle_command(cmd).await;
                }
                Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
                    self.log_unit_task(res);
                }
                else => {
                    break;
                }
            }
        }
    }

    async fn new(
        db_path: PathBuf,
        rt: tokio::runtime::Runtime,
        receiver: mpsc::Receiver<Command>,
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
        rt.spawn(db_actor.run());
        Ok(Self {
            options,
            commands: receiver,
            unit_tasks: JoinSet::new(),
            import_tasks: JoinSet::new(),
            db: db_send,
            rt,
        })
    }
}

impl Store {
    /// Load or create a new store.
    pub async fn load_redb(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = root.as_ref();
        let db_path = path.join("blobs.db");
        let options = Options {
            path: PathOptions::new(path),
            inline: Default::default(),
            batch: Default::default(),
        };
        Self::load_redb_with_opts(db_path, options).await
    }

    /// Load or create a new store with custom options.
    pub async fn load_redb_with_opts(path: PathBuf, options: Options) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("iroh-blob-store")
            .enable_time()
            .build()?;
        let handle = rt.handle().clone();
        let (sender, receiver) = mpsc::channel(100);
        let actor = handle
            .spawn(Actor::new(path, rt, receiver, Arc::new(options)))
            .await??;
        handle.spawn(actor.run());
        Ok(Self::from_sender(sender))
    }
}

#[derive(derive_more::Debug)]
pub(crate) enum ImportSource {
    TempFile((PathBuf, File)),
    External(PathBuf),
    Memory(#[debug(skip)] Bytes),
}

impl ImportSource {
    fn content(&self) -> MemOrFile<&[u8], &Path> {
        match self {
            Self::TempFile((path, file)) => MemOrFile::File(path.as_path()),
            Self::External(path) => MemOrFile::File(path.as_path()),
            Self::Memory(data) => MemOrFile::Mem(data.as_ref()),
        }
    }

    fn len(&self) -> io::Result<u64> {
        match self {
            Self::TempFile((path, file)) => std::fs::metadata(path).map(|m| m.len()),
            Self::External(path) => std::fs::metadata(path).map(|m| m.len()),
            Self::Memory(data) => Ok(data.len() as u64),
        }
    }
}

struct ImportEntry {
    source: ImportSource,
}

async fn import_bytes_task(
    data: Bytes,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    import_byte_stream_task(Box::pin(n0_future::stream::once(Ok(data))), options, out).await
}

async fn import_byte_stream_task(
    mut stream: BoxedByteStream,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let mut size = 0;
    let mut data = Vec::new();
    let mut disk = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let new_size = size + chunk.len() as u64;
        if new_size > options.inline.max_data_inlined {
            let temp_path = options.path.temp_file_name();
            let mut file = fs::File::create(&temp_path)?;
            file.write_all(&data)?;
            file.write_all(&chunk)?;
            data.clear();
            disk = Some((file, temp_path));
            break;
        } else {
            data.extend_from_slice(&chunk);
            size = new_size;
        }
        // todo: don't send progress for every chunk if the chunks are small?
        out.send(ImportProgress::CopyProgress { offset: size })
            .await?;
    }
    if let Some((mut file, temp_path)) = disk {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)?;
            size += chunk.len() as u64;
            out.send(ImportProgress::CopyProgress { offset: size })
                .await?;
        }
        out.send(ImportProgress::CopyDone).await?;
        Ok(ImportEntry {
            source: ImportSource::TempFile((temp_path, file)),
        })
    } else {
        out.send(ImportProgress::CopyDone).await?;
        Ok(ImportEntry {
            source: ImportSource::Memory(data.into()),
        })
    }
}

async fn import_path_task(
    path: PathBuf,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    if !path.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "path must be absolute").into());
    }
    if !path.is_file() && !path.is_symlink() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "path is not a file or symlink").into(),
        );
    }

    let size = path.metadata()?.len();
    out.send_progress(ImportProgress::Size { size })?;
    if size <= options.inline.max_data_inlined {
        let data = std::fs::read(path)?;
        out.send_progress(ImportProgress::CopyDone)?;
        Ok(ImportEntry {
            source: ImportSource::Memory(data.into()),
        })
    } else {
        let temp_path = options.path.temp_file_name();
        // copy from path to temp_path in increments of 64k and send progress
        let mut file = File::create(&temp_path)?;
        let mut source = File::open(&path)?;
        let mut buffer = [0u8; 64 * 1024];
        let mut offset = 0;
        while let Ok(n) = source.read(&mut buffer) {
            if n == 0 {
                break;
            }
            file.write_all(&buffer[..n])?;
            offset += n as u64;
            out.send_progress(ImportProgress::CopyProgress { offset })?;
            yield_now().await;
        }
        out.send_progress(ImportProgress::CopyDone)?;
        Ok(ImportEntry {
            source: ImportSource::TempFile((temp_path, file)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        let store = Store::load_redb("test").await?;
        Ok(())
    }
}
