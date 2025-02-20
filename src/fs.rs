use crate::bao_file::BaoFileConfig;
use crate::bao_file::BaoFileStorage;
use crate::util::MemOrFile;
use crate::util::ProgressReader;
use crate::util::SenderProgressExt;
use crate::Hash;
use crate::IROH_BLOCK_SIZE;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::WriteAt;
use bao_tree::BaoTree;
use bytes::Bytes;
use entry_state::OutboardLocation;
use meta::raw_outboard_size;
use n0_future::future::yield_now;
use n0_future::io::Cursor;
use n0_future::StreamExt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
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
    import_tasks: JoinSet<Option<ImportEntry>>,
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
            Command::ImportPath(cmd) => {
                self.import_tasks
                    .spawn(import_path_task(cmd, self.options.clone()));
            }
            _ => {}
        }
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            error!("task failed: {e}");
        }
    }

    fn finish_import(&mut self, res: Result<Option<ImportEntry>, JoinError>) {
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
                Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
                    self.finish_import(res);
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
