//! Protocol for the metadata database.
use std::fmt;

use bytes::Bytes;
use nested_enum_utils::enum_conversions;
use redb::{AccessGuard, StorageError};

use super::ActorResult;
pub use crate::store::proto::SyncDb;
use crate::{
    store::{
        fs::entry_state::EntryState,
        proto::Shutdown,
        util::{Tag, DD},
        Hash, HashAndFormat,
    },
    util::channel::oneshot,
};

/// Get the entry state for a hash.
///
/// This will read from the blobs table and enrich the result with the content
/// of the inline data and inline outboard tables if necessary.
pub struct Get {
    pub hash: Hash,
    pub tx: oneshot::Sender<GetResult>,
}

impl fmt::Debug for Get {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Get")
            .field("hash", &DD(self.hash))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct GetResult {
    pub state: anyhow::Result<Option<EntryState<Bytes>>>,
}

/// Get the entry state for a hash.
///
/// This will read from the blobs table and enrich the result with the content
/// of the inline data and inline outboard tables if necessary.
#[derive(Debug)]
pub struct Dump {
    pub tx: oneshot::Sender<anyhow::Result<()>>,
}

pub struct Update {
    pub epoch: u64,
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    /// do I need this? Optional?
    pub tx: Option<oneshot::Sender<ActorResult<()>>>,
}

impl fmt::Debug for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("epoch", &self.epoch)
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .field("tx", &self.tx.is_some())
            .finish()
    }
}

pub struct Set {
    pub epoch: u64,
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    pub tx: oneshot::Sender<ActorResult<()>>,
}

impl fmt::Debug for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Set")
            .field("epoch", &self.epoch)
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct Delete {
    pub epoch: u64,
    pub hashes: Vec<Hash>,
    pub tx: oneshot::Sender<ActorResult<()>>,
}

/// Predicate for filtering entries in a redb table.
pub(crate) type FilterPredicate<K, V> =
    Box<dyn Fn(u64, AccessGuard<K>, AccessGuard<V>) -> Option<(K, V)> + Send + Sync>;

/// Bulk query method: get entries from the blobs table
#[derive(derive_more::Debug)]
pub struct Blobs {
    #[debug(skip)]
    pub filter: FilterPredicate<Hash, EntryState>,
    #[allow(clippy::type_complexity)]
    pub tx:
        oneshot::Sender<ActorResult<Vec<std::result::Result<(Hash, EntryState), StorageError>>>>,
}

/// Bulk query method: get the entire tags table    
#[derive(derive_more::Debug)]
pub struct Tags {
    #[debug(skip)]
    pub filter: FilterPredicate<Tag, HashAndFormat>,
    #[allow(clippy::type_complexity)]
    pub tx: oneshot::Sender<anyhow::Result<Vec<(Tag, HashAndFormat)>>>,
}

/// Modification method: create a new unique tag and set it to a value.
pub use crate::store::proto::CreateTag;
/// Modification method: set a tag to a value, or remove it.
pub use crate::store::proto::SetTag;

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadOnlyCommand {
    Get(Get),
    Dump(Dump),
    Tags(Tags),
    Blobs(Blobs),
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadWriteCommand {
    Update(Update),
    Set(Set),
    Delete(Delete),
    SetTag(SetTag),
    CreateTag(CreateTag),
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum TopLevelCommand {
    SyncDb(SyncDb),
    Shutdown(Shutdown),
}

#[enum_conversions()]
pub enum Command {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
    TopLevel(TopLevelCommand),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(cmd) => cmd.fmt(f),
            Self::ReadWrite(cmd) => cmd.fmt(f),
            Self::TopLevel(cmd) => cmd.fmt(f),
        }
    }
}
