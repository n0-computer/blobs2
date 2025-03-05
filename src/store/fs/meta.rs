//! The metadata database
use std::{
    io,
    ops::{Deref, DerefMut},
    path::PathBuf,
    time::SystemTime,
};

use bao_tree::BaoTree;
use bytes::Bytes;
use redb::{Database, DatabaseError, ReadableTable};
use tokio::sync::{mpsc, oneshot};
mod proto;
pub use proto::*;
mod tables;
use tables::{BaoFilePart, DeleteSet, ReadOnlyTables, ReadableTables, Tables};
use tracing::{debug, info, trace};

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    util::PeekableReceiver,
};
use crate::store::{proto::Shutdown, util::Tag, Hash, IROH_BLOCK_SIZE};

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("inconsistent database state: {0}")]
    Inconsistent(String),
    #[error("error during database migration: {0}")]
    Migration(#[source] anyhow::Error),
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        match e {
            ActorError::Io(e) => e,
            e => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

pub type ActorResult<T> = Result<T, ActorError>;

#[derive(Debug, Clone)]
pub struct Db {
    pub sender: mpsc::Sender<Command>,
}

impl Db {
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Get the entry state for a hash, if any.
    pub async fn get(&self, hash: Hash) -> anyhow::Result<Option<EntryState<Bytes>>> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Get { hash, tx }.into()).await?;
        let res = rx.await?;
        Ok(res.state?)
    }

    /// Send a command. This exists so the main actor can directly forward commands.
    ///
    /// This will fail only if the database actor is dead. In that case the main
    /// actor should probably also shut down.
    pub async fn send(&self, cmd: Command) -> anyhow::Result<()> {
        self.sender.send(cmd).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Actor {
    db: redb::Database,
    cmds: PeekableReceiver<Command>,
}

impl Actor {
    pub fn new(db_path: PathBuf, cmds: mpsc::Receiver<Command>) -> anyhow::Result<Self> {
        trace!(
            "creating or opening meta database at {:?}",
            db_path.display()
        );
        let db = match redb::Database::create(db_path) {
            Ok(db) => db,
            Err(DatabaseError::UpgradeRequired(1)) => {
                return Err(anyhow::anyhow!("migration from v1 no longer supported"))
            }
            Err(err) => return Err(err.into()),
        };
        let tx = db.begin_write()?;
        let mut ds = DeleteSet::default();
        Tables::new(&tx, &mut ds)?;
        tx.commit()?;
        let cmds = PeekableReceiver::new(cmds);
        Ok(Self { db, cmds })
    }

    fn get(tables: &impl ReadableTables, cmd: Get) -> ActorResult<()> {
        let Get { hash, tx } = cmd;
        let Some(entry) = tables.blobs().get(hash)? else {
            tx.send(GetResult { state: Ok(None) }).ok();
            return Ok(());
        };
        let entry = entry.value();
        let entry = match entry {
            EntryState::Complete {
                data_location,
                outboard_location,
            } => {
                let data_location = load_data(tables, data_location, &hash)?;
                let outboard_location = load_outboard(tables, outboard_location, &hash)?;
                EntryState::Complete {
                    data_location,
                    outboard_location,
                }
            }
            EntryState::Partial { size } => EntryState::Partial { size },
        };
        tx.send(GetResult {
            state: Ok(Some(entry)),
        })
        .ok();
        Ok(())
    }

    fn dump(tables: &impl ReadableTables, cmd: Dump) -> ActorResult<()> {
        trace!("dumping database");
        for e in tables.blobs().iter()? {
            let (k, v) = e?;
            let k = k.value();
            let v = v.value();
            println!("blobs: {} -> {:?}", k.to_hex(), v);
        }
        for e in tables.tags().iter()? {
            let (k, v) = e?;
            let k = k.value();
            let v = v.value();
            println!("tags: {} -> {:?}", k, v);
        }
        for e in tables.inline_data().iter()? {
            let (k, v) = e?;
            let k = k.value();
            let v = v.value();
            println!("inline_data: {} -> {:?}", k.to_hex(), v.len());
        }
        for e in tables.inline_outboard().iter()? {
            let (k, v) = e?;
            let k = k.value();
            let v = v.value();
            println!("inline_outboard: {} -> {:?}", k.to_hex(), v.len());
        }
        cmd.tx.send(Ok(())).ok();
        Ok(())
    }

    fn tags(tables: &impl ReadableTables, cmd: Tags) -> ActorResult<()> {
        let Tags { filter, tx } = cmd;
        let mut res = Vec::new();
        let mut index = 0u64;
        #[allow(clippy::explicit_counter_loop)]
        for item in tables.tags().iter()? {
            match item {
                Ok((k, v)) => {
                    if let Some(item) = filter(index, k, v) {
                        res.push(Ok(item));
                    }
                }
                Err(e) => {
                    res.push(Err(anyhow::Error::from(e)));
                }
            }
            index += 1;
        }
        let res = res.into_iter().collect::<Result<Vec<_>, _>>();
        tx.send(res).ok();
        Ok(())
    }

    fn blobs(tables: &impl ReadableTables, cmd: Blobs) -> ActorResult<()> {
        let Blobs { filter, tx } = cmd;
        let mut res = Vec::new();
        let mut index = 0u64;
        #[allow(clippy::explicit_counter_loop)]
        for item in tables.blobs().iter()? {
            match item {
                Ok((k, v)) => {
                    if let Some(item) = filter(index, k, v) {
                        res.push(Ok(item));
                    }
                }
                Err(e) => {
                    res.push(Err(e));
                }
            }
            index += 1;
        }
        tx.send(Ok(res)).ok();
        Ok(())
    }

    fn handle_readonly(tables: &impl ReadableTables, cmd: ReadOnlyCommand) -> ActorResult<()> {
        match cmd {
            ReadOnlyCommand::Get(cmd) => Self::get(tables, cmd),
            ReadOnlyCommand::Dump(cmd) => Self::dump(tables, cmd),
            ReadOnlyCommand::Tags(cmd) => Self::tags(tables, cmd),
            ReadOnlyCommand::Blobs(cmd) => Self::blobs(tables, cmd),
        }
    }

    fn update(tables: &mut Tables, cmd: Update) -> ActorResult<()> {
        let Update {
            hash, state, tx, ..
        } = cmd;
        trace!("updating hash {} to {}", hash.to_hex(), state.fmt_short());
        let old_entry_opt = tables.blobs.get(hash)?.map(|e| e.value());
        let (state, data, outboard): (_, Option<Bytes>, Option<Bytes>) = match state {
            EntryState::Complete {
                data_location,
                outboard_location,
            } => {
                let (data_location, data) = data_location.split_inline_data();
                let (outboard_location, outboard) = outboard_location.split_inline_data();
                (
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    },
                    data,
                    outboard,
                )
            }
            EntryState::Partial { size } => (EntryState::Partial { size }, None, None),
        };
        let state = match old_entry_opt {
            Some(old_entry) => old_entry.union(state)?,
            None => state,
        };
        tables.blobs.insert(hash, state)?;
        if let Some(data) = data {
            tables.inline_data.insert(hash, data.as_ref())?;
        }
        if let Some(outboard) = outboard {
            tables.inline_outboard.insert(hash, outboard.as_ref())?;
        }
        if let Some(tx) = tx {
            tx.send(Ok(())).ok();
        }
        Ok(())
    }

    fn delete(tables: &mut Tables, cmd: Delete) -> ActorResult<()> {
        let Delete { epoch, hashes, .. } = cmd;
        for hash in hashes {
            if let Some(entry) = tables.blobs.remove(hash)? {
                match entry.value() {
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    } => {
                        match data_location {
                            DataLocation::Inline(_) => {
                                tables.inline_data.remove(hash)?;
                            }
                            DataLocation::Owned(_) => {
                                // mark the data for deletion
                                tables.delete_after_commit.insert(hash, [BaoFilePart::Data]);
                            }
                            DataLocation::External(_, _) => {}
                        }
                        match outboard_location {
                            OutboardLocation::Inline(_) => {
                                tables.inline_outboard.remove(hash)?;
                            }
                            OutboardLocation::Owned => {
                                // mark the outboard for deletion
                                tables
                                    .delete_after_commit
                                    .insert(hash, [BaoFilePart::Outboard]);
                            }
                            OutboardLocation::NotNeeded => {}
                        }
                    }
                    EntryState::Partial { .. } => {
                        // mark all parts for deletion
                        tables.delete_after_commit.insert(
                            hash,
                            [BaoFilePart::Outboard, BaoFilePart::Data, BaoFilePart::Sizes],
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn set_tag(tables: &mut Tables, cmd: SetTag) -> ActorResult<()> {
        let SetTag { tag, value, tx } = cmd;
        let res = match value {
            Some(value) => tables.tags.insert(tag, value).map(|_| ()),
            None => tables.tags.remove(tag).map(|_| ()),
        };
        tx.send(res.map_err(anyhow::Error::from)).ok();
        Ok(())
    }

    fn create_tag(tables: &mut Tables, cmd: CreateTag) -> ActorResult<()> {
        let CreateTag { hash, tx } = cmd;
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), hash)?;
            tag
        };
        tx.send(Ok(tag.clone())).ok();
        Ok(())
    }

    fn handle_readwrite(tables: &mut Tables, cmd: ReadWriteCommand) -> ActorResult<()> {
        match cmd {
            ReadWriteCommand::Update(cmd) => Self::update(tables, cmd),
            ReadWriteCommand::Delete(cmd) => Self::delete(tables, cmd),
            ReadWriteCommand::SetTag(cmd) => Self::set_tag(tables, cmd),
            ReadWriteCommand::CreateTag(cmd) => Self::create_tag(tables, cmd),
        }
    }

    fn sync_db(_db: &mut Database, sync: SyncDb) -> ActorResult<()> {
        let SyncDb { tx } = sync;
        // nothing to do here, since for a toplevel cmd we are outside a write transaction
        tx.send(Ok(())).ok();
        Ok(())
    }

    fn handle_toplevel(db: &mut Database, cmd: TopLevelCommand) -> ActorResult<Option<Shutdown>> {
        Ok(match cmd {
            TopLevelCommand::SyncDb(cmd) => {
                Self::sync_db(db, cmd)?;
                None
            }
            TopLevelCommand::Shutdown(cmd) => {
                // nothing to do here, since the database will be dropped
                Some(cmd)
            }
        })
    }

    pub async fn run(mut self) -> ActorResult<()> {
        let mut db = DbWrapper::from(self.db);
        let shutdown = loop {
            let Some(cmd) = self.cmds.recv().await else {
                break None;
            };
            match cmd {
                Command::TopLevel(cmd) => {
                    trace!("{cmd:?}");
                    if let Some(shutdown) = Self::handle_toplevel(&mut db, cmd)? {
                        break Some(shutdown);
                    }
                }
                Command::ReadOnly(cmd) => {
                    trace!("{cmd:?}");
                    let tx = db.begin_read()?;
                    let tables = ReadOnlyTables::new(&tx)?;
                    Self::handle_readonly(&tables, cmd)?;
                }
                Command::ReadWrite(cmd) => {
                    trace!("{cmd:?}");
                    let mut delete_set = DeleteSet::default();
                    let tx = db.begin_write()?;
                    let mut tables = Tables::new(&tx, &mut delete_set)?;
                    Self::handle_readwrite(&mut tables, cmd)?;
                    drop(tables);
                    tx.commit()?;
                }
            }
        };
        if let Some(shutdown) = shutdown {
            drop(db);
            shutdown.tx.send(()).ok();
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DbWrapper(Option<Database>);

impl Deref for DbWrapper {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("database not open")
    }
}

impl DerefMut for DbWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("database not open")
    }
}

impl From<Database> for DbWrapper {
    fn from(db: Database) -> Self {
        Self(Some(db))
    }
}

impl Drop for DbWrapper {
    fn drop(&mut self) {
        if let Some(db) = self.0.take() {
            debug!("closing database");
            drop(db);
            debug!("database closed");
        }
    }
}

fn load_data(
    tables: &impl ReadableTables,
    location: DataLocation<(), u64>,
    hash: &Hash,
) -> ActorResult<DataLocation<Bytes, u64>> {
    Ok(match location {
        DataLocation::Inline(()) => {
            let Some(data) = tables.inline_data().get(hash)? else {
                return Err(ActorError::Inconsistent(format!(
                    "inconsistent database state: {} should have inline data but does not",
                    hash.to_hex()
                )));
            };
            DataLocation::Inline(Bytes::copy_from_slice(data.value()))
        }
        DataLocation::Owned(data_size) => DataLocation::Owned(data_size),
        DataLocation::External(paths, data_size) => DataLocation::External(paths, data_size),
    })
}

fn load_outboard(
    tables: &impl ReadableTables,
    location: OutboardLocation,
    hash: &Hash,
) -> ActorResult<OutboardLocation<Bytes>> {
    Ok(match location {
        OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
        OutboardLocation::Inline(_) => {
            let Some(outboard) = tables.inline_outboard().get(hash)? else {
                return Err(ActorError::Inconsistent(format!(
                    "inconsistent database state: {} should have inline outboard but does not",
                    hash.to_hex()
                )));
            };
            OutboardLocation::Inline(Bytes::copy_from_slice(outboard.value()))
        }
        OutboardLocation::Owned => OutboardLocation::Owned,
    })
}

pub(crate) fn raw_outboard_size(size: u64) -> u64 {
    BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size()
}
