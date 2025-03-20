//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    io,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    time::{Instant, SystemTime},
};

use bao_tree::BaoTree;
use bytes::Bytes;
use redb::{Database, DatabaseError, ReadableTable};

use crate::{
    store::api::{
        self,
        tags::{self, CreateTag, Delete, ListTags, SetTag, TagInfo},
    },
    util::channel::{mpsc, oneshot},
};
mod proto;
pub use proto::*;
mod tables;
use tables::{ReadOnlyTables, ReadableTables, Tables};
use tracing::{debug, error, info_span, trace};

use super::{
    delete_set::DeleteHandle,
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::BatchOptions,
    util::PeekableReceiver,
    BaoFilePart,
};
use crate::store::{proto::ShutdownMsg, util::Tag, Hash, IROH_BLOCK_SIZE};

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[allow(clippy::large_enum_variant)]
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
            e => io::Error::other(e),
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
        res.state
    }

    /// Send a command. This exists so the main actor can directly forward commands.
    ///
    /// This will fail only if the database actor is dead. In that case the main
    /// actor should probably also shut down.
    pub async fn send(&self, cmd: Command) -> io::Result<()> {
        self.sender
            .send(cmd)
            .await
            .map_err(|_e| io::Error::other("actor down"))?;
        Ok(())
    }
}

impl Get {
    fn handle(self, tables: &impl ReadableTables) -> ActorResult<()> {
        trace!("{self:?}");
        let Get { hash, tx } = self;
        let Some(entry) = tables.blobs().get(hash)? else {
            tx.send(GetResult { state: Ok(None) });
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
        });
        Ok(())
    }
}

impl Dump {
    fn handle(self, tables: &impl ReadableTables) -> ActorResult<()> {
        trace!("{self:?}");
        let cmd = self;
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
        cmd.tx.send(Ok(()));
        Ok(())
    }
}

async fn handle_list_tags(msg: ListTagsMsg, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{msg:?}");
    let ListTagsMsg {
        inner:
            ListTags {
                from,
                to,
                raw,
                hash_seq,
            },
        tx,
        ..
    } = msg;
    let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
    let mut res = Vec::new();
    for item in tables.tags().range((from, to))? {
        match item {
            Ok((k, v)) => {
                let v = v.value();
                if raw && v.format.is_raw() || hash_seq && v.format.is_hash_seq() {
                    let info = TagInfo {
                        name: k.value(),
                        hash: v.hash,
                        format: v.format,
                    };
                    res.push(crate::store::api::Result::Ok(info));
                }
            }
            Err(e) => {
                res.push(Err(crate::store::api::Error::other(e)));
            }
        }
    }
    tx.send(res).await.ok();
    Ok(())
}

impl Blobs {
    fn handle(self, tables: &impl ReadableTables) -> ActorResult<()> {
        let Blobs { filter, tx } = self;
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
        tx.send(Ok(res));
        Ok(())
    }
}

impl Update {
    fn handle(self, tables: &mut Tables) -> ActorResult<()> {
        trace!("{self:?}");
        let Update {
            hash, state, tx, ..
        } = self;
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
            Some(old) => {
                let partial_to_complete = old.is_partial() && state.is_complete();
                let res = EntryState::union(old, state)?;
                if partial_to_complete {
                    tables
                        .ftx
                        .delete(hash, [BaoFilePart::Sizes, BaoFilePart::Bitfield]);
                }
                res
            }
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
            tx.send(Ok(()));
        }
        Ok(())
    }
}

impl Set {
    fn handle(self, tables: &mut Tables) -> ActorResult<()> {
        trace!("{self:?}");
        let Set {
            state, hash, tx, ..
        } = self;
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
        tables.blobs.insert(hash, state)?;
        if let Some(data) = data {
            tables.inline_data.insert(hash, data.as_ref())?;
        }
        if let Some(outboard) = outboard {
            tables.inline_outboard.insert(hash, outboard.as_ref())?;
        }
        tx.send(Ok(()));
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum TxnNum {
    Read(u64),
    Write(u64),
    TopLevel(u64),
}

impl std::fmt::Debug for TxnNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnNum::Read(n) => write!(f, "r{}", n),
            TxnNum::Write(n) => write!(f, "w{}", n),
            TxnNum::TopLevel(n) => write!(f, "t{}", n),
        }
    }
}

#[derive(Debug)]
pub struct Actor {
    db: redb::Database,
    cmds: PeekableReceiver<Command>,
    ds: DeleteHandle,
    options: BatchOptions,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: mpsc::Receiver<Command>,
        mut ds: DeleteHandle,
        options: BatchOptions,
    ) -> anyhow::Result<Self> {
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
        let ftx = ds.begin_write();
        Tables::new(&tx, &ftx)?;
        tx.commit()?;
        drop(ftx);
        let cmds = PeekableReceiver::new(cmds);
        Ok(Self {
            db,
            cmds,
            ds,
            options,
        })
    }

    async fn handle_readonly(
        tables: &impl ReadableTables,
        cmd: ReadOnlyCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: &cmd.parent_span(),
            "txn",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadOnlyCommand::Get(cmd) => cmd.handle(tables),
            ReadOnlyCommand::Dump(cmd) => cmd.handle(tables),
            ReadOnlyCommand::Blobs(cmd) => cmd.handle(tables),
            ReadOnlyCommand::ListTags(cmd) => handle_list_tags(cmd, tables).await,
        }
    }

    async fn delete(tables: &mut Tables<'_>, cmd: DeleteBlobs) -> ActorResult<()> {
        let DeleteBlobs { hashes, .. } = cmd;
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
                                tables.ftx.delete(hash, [BaoFilePart::Data]);
                            }
                            DataLocation::External(_, _) => {}
                        }
                        match outboard_location {
                            OutboardLocation::Inline(_) => {
                                tables.inline_outboard.remove(hash)?;
                            }
                            OutboardLocation::Owned => {
                                // mark the outboard for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Outboard]);
                            }
                            OutboardLocation::NotNeeded => {}
                        }
                    }
                    EntryState::Partial { .. } => {
                        tables.ftx.delete(
                            hash,
                            [
                                BaoFilePart::Outboard,
                                BaoFilePart::Data,
                                BaoFilePart::Sizes,
                                BaoFilePart::Bitfield,
                            ],
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn set_tag(tables: &mut Tables<'_>, cmd: SetTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SetTagMsg {
            inner: SetTag { name: tag, value },
            tx,
            ..
        } = cmd;
        let res = tables.tags.insert(tag, value).map(|_| ());
        tx.send(res.map_err(|_| crate::store::api::Error::other("storage")))
            .await
            .ok();
        Ok(())
    }

    async fn create_tag(tables: &mut Tables<'_>, cmd: CreateTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let CreateTagMsg {
            inner: CreateTag { content: hash },
            tx,
            ..
        } = cmd;
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), hash)?;
            tag
        };
        tx.send(Ok(tag.clone())).await.ok();
        Ok(())
    }

    async fn delete_tags(tables: &mut Tables<'_>, cmd: DeleteTagsMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let DeleteTagsMsg {
            inner: Delete { from, to },
            tx,
            ..
        } = cmd;
        let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let removing = tables.tags.extract_from_if((from, to), |_, _| true)?;
        // drain the iterator to actually remove the tags
        for res in removing {
            res?;
        }
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn rename_tag(tables: &mut Tables<'_>, cmd: RenameTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let RenameTagMsg {
            inner: tags::Rename { from, to },
            tx,
            ..
        } = cmd;
        let value = match tables.tags.remove(from)? {
            Some(value) => value.value(),
            None => {
                tx.send(Err(api::Error::io(
                    io::ErrorKind::NotFound,
                    "tag not found",
                )))
                .await
                .ok();
                return Ok(());
            }
        };
        tables.tags.insert(to, value)?;
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_readwrite(
        tables: &mut Tables<'_>,
        cmd: ReadWriteCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: &cmd.parent_span(),
            "txn",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadWriteCommand::Update(cmd) => cmd.handle(tables),
            ReadWriteCommand::Set(cmd) => cmd.handle(tables),
            ReadWriteCommand::Delete(cmd) => Self::delete(tables, cmd).await,
            ReadWriteCommand::SetTag(cmd) => Self::set_tag(tables, cmd).await,
            ReadWriteCommand::CreateTag(cmd) => Self::create_tag(tables, cmd).await,
            ReadWriteCommand::DeleteTags(cmd) => Self::delete_tags(tables, cmd).await,
            ReadWriteCommand::RenameTag(cmd) => Self::rename_tag(tables, cmd).await,
            ReadWriteCommand::ProcessExit(cmd) => {
                std::process::exit(cmd.code);
            }
        }
    }

    async fn handle_non_toplevel(
        tables: &mut Tables<'_>,
        cmd: NonTopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        match cmd {
            NonTopLevelCommand::ReadOnly(cmd) => Self::handle_readonly(tables, cmd, op).await,
            NonTopLevelCommand::ReadWrite(cmd) => Self::handle_readwrite(tables, cmd, op).await,
        }
    }

    async fn sync_db(_db: &mut Database, cmd: SyncDbMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SyncDbMsg { tx, .. } = cmd;
        // nothing to do here, since for a toplevel cmd we are outside a write transaction
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_toplevel(
        db: &mut Database,
        cmd: TopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<Option<ShutdownMsg>> {
        let span = info_span!(
            parent: &cmd.parent_span(),
            "txn",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        Ok(match cmd {
            TopLevelCommand::SyncDb(cmd) => {
                Self::sync_db(db, cmd).await?;
                None
            }
            TopLevelCommand::Shutdown(cmd) => {
                trace!("{cmd:?}");
                // nothing to do here, since the database will be dropped
                Some(cmd)
            }
        })
    }

    pub async fn run(mut self) -> ActorResult<()> {
        let mut db = DbWrapper::from(self.db);
        let options = &self.options;
        let mut op = 0u64;
        let shutdown = loop {
            op += 1;
            let Some(cmd) = self.cmds.recv().await else {
                break None;
            };
            match cmd {
                Command::TopLevel(cmd) => {
                    let op = TxnNum::TopLevel(op);
                    if let Some(shutdown) = Self::handle_toplevel(&mut db, cmd, op).await? {
                        break Some(shutdown);
                    }
                }
                Command::ReadOnly(cmd) => {
                    let op = TxnNum::Read(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let tx = db.begin_read()?;
                    let tables = ReadOnlyTables::new(&tx)?;
                    let t0 = Instant::now();
                    let mut n = 0;
                    while let Some(cmd) = self.cmds.extract(Command::read_only).await {
                        Self::handle_readonly(&tables, cmd, op).await?;
                        n += 1;
                        if t0.elapsed() > options.max_read_duration {
                            break;
                        }
                        if n >= options.max_read_batch {
                            break;
                        }
                    }
                }
                Command::ReadWrite(cmd) => {
                    let op = TxnNum::Write(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let ftx = self.ds.begin_write();
                    let tx = db.begin_write()?;
                    let mut tables = Tables::new(&tx, &ftx)?;
                    let t0 = Instant::now();
                    let mut n = 0;
                    while let Some(cmd) = self.cmds.extract(Command::non_top_level).await {
                        Self::handle_non_toplevel(&mut tables, cmd, op).await?;
                        n += 1;
                        if t0.elapsed() > options.max_write_duration {
                            break;
                        }
                        if n >= options.max_write_batch {
                            break;
                        }
                    }
                    drop(tables);
                    tx.commit()?;
                    ftx.commit();
                }
            }
        };
        if let Some(shutdown) = shutdown {
            drop(db);
            shutdown.tx.send(()).await.ok();
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
