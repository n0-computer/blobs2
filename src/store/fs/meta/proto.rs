//! Protocol for the metadata database.
use std::fmt;

use bytes::Bytes;
use nested_enum_utils::enum_conversions;
use redb::{AccessGuard, StorageError};

use super::ActorResult;
pub use crate::store::proto::SyncDbMsg;
use crate::{
    store::{
        fs::entry_state::EntryState,
        proto::{ProcessExit, ShutdownMsg},
        util::DD,
    },
    util::channel::oneshot,
    Hash,
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
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    /// do I need this? Optional?
    pub tx: Option<oneshot::Sender<ActorResult<()>>>,
}

impl fmt::Debug for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .field("tx", &self.tx.is_some())
            .finish()
    }
}

pub struct Set {
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    pub tx: oneshot::Sender<ActorResult<()>>,
}

impl fmt::Debug for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Set")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct DeleteBlobs {
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

/// Modification method: create a new unique tag and set it to a value.
pub use crate::store::proto::CreateTagMsg;
/// Modification method: remove a range of tags.
pub use crate::store::proto::DeleteTagsMsg;
/// Read method: list a range of tags.
pub use crate::store::proto::ListTagsMsg;
/// Modification method: rename a tag.
pub use crate::store::proto::RenameTagMsg;
/// Modification method: set a tag to a value, or remove it.
pub use crate::store::proto::SetTagMsg;

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadOnlyCommand {
    Get(Get),
    Dump(Dump),
    ListTags(ListTagsMsg),
    Blobs(Blobs),
}

impl ReadOnlyCommand {
    pub fn parent_span(&self) -> tracing::Span {
        self.parent_span_opt()
            .cloned()
            .unwrap_or_else(|| tracing::Span::current())
    }

    pub fn parent_span_opt(&self) -> Option<&tracing::Span> {
        match self {
            Self::Get(_) => None,
            Self::Dump(_) => None,
            Self::ListTags(x) => x.parent_span_opt(),
            Self::Blobs(_) => None,
        }
    }
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadWriteCommand {
    Update(Update),
    Set(Set),
    Delete(DeleteBlobs),
    SetTag(SetTagMsg),
    DeleteTags(DeleteTagsMsg),
    RenameTag(RenameTagMsg),
    CreateTag(CreateTagMsg),
    ProcessExit(ProcessExit),
}

impl ReadWriteCommand {
    pub fn parent_span(&self) -> tracing::Span {
        self.parent_span_opt()
            .cloned()
            .unwrap_or_else(|| tracing::Span::current())
    }

    pub fn parent_span_opt(&self) -> Option<&tracing::Span> {
        match self {
            Self::Update(x) => None,
            Self::Set(x) => None,
            Self::Delete(x) => None,
            Self::SetTag(x) => x.parent_span_opt(),
            Self::DeleteTags(x) => x.parent_span_opt(),
            Self::RenameTag(x) => x.parent_span_opt(),
            Self::CreateTag(x) => x.parent_span_opt(),
            Self::ProcessExit(_) => None,
        }
    }
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum TopLevelCommand {
    SyncDb(SyncDbMsg),
    Shutdown(ShutdownMsg),
}

impl TopLevelCommand {
    pub fn parent_span(&self) -> tracing::Span {
        self.parent_span_opt()
            .cloned()
            .unwrap_or_else(|| tracing::Span::current())
    }

    pub fn parent_span_opt(&self) -> Option<&tracing::Span> {
        match self {
            Self::SyncDb(x) => x.parent_span_opt(),
            Self::Shutdown(x) => x.parent_span_opt(),
        }
    }
}

#[enum_conversions()]
pub enum Command {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
    TopLevel(TopLevelCommand),
}

impl Command {
    pub fn non_top_level(self) -> std::result::Result<NonTopLevelCommand, Self> {
        match self {
            Self::ReadOnly(cmd) => Ok(NonTopLevelCommand::ReadOnly(cmd)),
            Self::ReadWrite(cmd) => Ok(NonTopLevelCommand::ReadWrite(cmd)),
            _ => Err(self),
        }
    }

    pub fn read_only(self) -> std::result::Result<ReadOnlyCommand, Self> {
        match self {
            Self::ReadOnly(cmd) => Ok(cmd),
            _ => Err(self),
        }
    }
}

#[derive(Debug)]
#[enum_conversions()]
pub enum NonTopLevelCommand {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
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
