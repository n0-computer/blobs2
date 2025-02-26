//! Protocol for the metadata database.
use bytes::Bytes;
use n0_future::io;
use nested_enum_utils::enum_conversions;
use redb::{AccessGuard, StorageError};
use tokio::sync::{mpsc, oneshot};

use super::ActorResult;
pub use crate::proto::SyncDb;
use crate::{fs::entry_state::EntryState, util::Tag, Hash, HashAndFormat};

/// Get the entry state for a hash.
///
/// This will read from the blobs table and enrich the result with the content
/// of the inline data and inline outboard tables if necessary.
#[derive(Debug)]
pub struct Get {
    pub hash: Hash,
    pub tx: mpsc::OwnedPermit<GetResult>,
}

#[derive(Debug)]
pub struct GetResult {
    pub hash: Hash,
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

#[derive(Debug)]
pub struct Update {
    pub epoch: u64,
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    /// do I need this? Optional?
    pub tx: Option<oneshot::Sender<ActorResult<()>>>,
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

/// Modification method: set a tag to a value, or remove it.
#[derive(derive_more::Debug)]
pub struct SetTag {
    pub tag: Tag,
    pub value: Option<HashAndFormat>,
    #[debug(skip)]
    pub tx: oneshot::Sender<anyhow::Result<()>>,
}

/// Modification method: create a new unique tag and set it to a value.
#[derive(Debug)]
pub struct CreateTag {
    pub hash: HashAndFormat,
    pub tx: oneshot::Sender<anyhow::Result<Tag>>,
}

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
    Merge(Update),
    Delete(Delete),
    SetTag(SetTag),
    CreateTag(CreateTag),
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum TopLevelCommand {
    SyncDb(SyncDb),
}

#[derive(Debug)]
#[enum_conversions()]
pub enum Command {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
    TopLevel(TopLevelCommand),
}
