//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Command`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
use core::fmt;
use std::{fmt::Debug, io, num::NonZeroU64, path::PathBuf, pin::Pin};

use arrayvec::ArrayString;
pub use bao_tree::io::mixed::EncodedItem;
use bao_tree::{io::BaoContentItem, ChunkRanges};
use bytes::Bytes;
use n0_future::Stream;
use quic_rpc::WithChannels;
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::{
    api::tags::{DeleteTags, ListTags, TagInfo},
    util::DD,
};
use crate::{
    store::{
        bitfield::Bitfield,
        util::{observer::Observer, Tag},
        BlobFormat, HashAndFormat,
    },
    Hash,
};
use quic_rpc::channel::{spsc, oneshot};

pub trait HashSpecific {
    fn hash(&self) -> Hash;

    fn hash_short(&self) -> ArrayString<10> {
        self.hash().fmt_short()
    }
}

/// Import bao encoded data for the given hash with the iroh block size.
///
/// The result is just a single item, indicating if a write error occurred.
/// To observe the incoming data more granularly, use the `Observe` command
/// concurrently.
#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBao {
    pub hash: Hash,
    pub size: NonZeroU64,
}

pub type ImportBaoMsg = WithChannels<ImportBao, StoreService>;

impl HashSpecific for ImportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type ShutdownMsg = WithChannels<Shutdown, StoreService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Observe {
    pub hash: Hash,
}

/// Observe the bitfield of the given hash.
pub struct ObserveMsg {
    pub inner: Observe,
    pub tx: Observer<Bitfield>,
}

impl fmt::Debug for ObserveMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observe")
            .field("hash", &DD(self.inner.hash))
            .finish_non_exhaustive()
    }
}

impl HashSpecific for ObserveMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

/// Import the given bytes.
#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBytes {
    pub data: Bytes,
    pub format: BlobFormat,
}

pub type ImportBytesMsg = WithChannels<ImportBytes, StoreService>;

/// Export the given ranges in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
#[derive(Debug)]
pub struct ExportBao {
    pub hash: Hash,
    pub ranges: ChunkRanges,
}

pub type ExportBaoMsg = WithChannels<ExportBao, StoreService>;

impl HashSpecific for ExportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

/// Export a file to a target path.
///
/// For an incomplete file, the size might be truncated and gaps will be filled
/// with zeros. If possible, a store implementation should try to write as a
/// sparse file.

#[derive(Debug)]
pub struct ExportPath {
    pub hash: Hash,
    pub mode: ExportMode,
    pub target: PathBuf,
}

pub type ExportPathMsg = WithChannels<ExportPath, StoreService>;

impl HashSpecific for ExportPathMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportByteStream {
    pub format: BlobFormat,
    pub data: Vec<Bytes>,
}

pub type ImportByteStreamMsg = WithChannels<ImportByteStream, StoreService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportPath {
    pub path: PathBuf,
    pub mode: ImportMode,
    pub format: BlobFormat,
}

pub type ImportPathMsg = WithChannels<ImportPath, StoreService>;

pub type ListTagsMsg = WithChannels<ListTags, StoreService>;
/// Rename a tag atomically
#[derive(Debug, Serialize, Deserialize)]
pub struct Rename {
    /// Old tag name
    pub from: Tag,
    /// New tag name
    pub to: Tag,
}

pub type RenameTagMsg = WithChannels<Rename, StoreService>;

pub type DeleteTagsMsg = WithChannels<DeleteTags, StoreService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SetTag {
    pub name: Tag,
    pub value: HashAndFormat,
}

pub type SetTagMsg = WithChannels<SetTag, StoreService>;

/// Debug tool to exit the process in the middle of a write transaction, for testing.
#[derive(Debug)]
pub struct ProcessExit {
    pub code: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTag {
    pub content: HashAndFormat,
}

pub type CreateTagMsg = WithChannels<CreateTag, StoreService>;

impl HashSpecific for CreateTagMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.content.hash
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncDb;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown;

pub type SyncDbMsg = WithChannels<SyncDb, StoreService>;

#[derive(Debug, Clone)]
pub struct StoreService;
impl quic_rpc::Service for StoreService {}

#[allow(dead_code)]
#[rpc_requests(StoreService, Command2)]
#[derive(Debug)]
pub enum Request {
    #[rpc(rx = spsc::Receiver<BaoContentItem>, tx = oneshot::Sender<io::Result<()>>)]
    ImportBao(ImportBao),
    #[rpc(tx = spsc::Sender<EncodedItem>)]
    ExportBao(ExportBao),
    #[rpc(tx = spsc::Sender<Bitfield>)]
    Observe(Observe),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportBytes(ImportBytes),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportByteStream(ImportByteStream),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportPath(ImportPath),
    #[rpc(tx = spsc::Sender<ExportProgress>)]
    ExportPath(ExportPath),
    #[rpc(tx = oneshot::Sender<Vec<anyhow::Result<TagInfo>>>)]
    ListTags(ListTags),
    #[rpc(tx = oneshot::Sender<io::Result<()>>)]
    SetTag(SetTag),
    #[rpc(tx = oneshot::Sender<io::Result<()>>)]
    DeleteTags(DeleteTags),
    #[rpc(tx = oneshot::Sender<io::Result<()>>)]
    RenameTag(Rename),
    #[rpc(tx = oneshot::Sender<io::Result<Tag>>)]
    CreateTag(CreateTag),
    #[rpc(tx = oneshot::Sender<io::Result<()>>)]
    SyncDb(SyncDb),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(Shutdown),
}

#[derive(Debug, derive_more::From)]
pub enum Command {
    ImportBao(ImportBaoMsg),
    ExportBao(ExportBaoMsg),

    Observe(ObserveMsg),
    ImportBytes(ImportBytesMsg),
    ImportByteStream(ImportByteStreamMsg),
    ImportPath(ImportPathMsg),
    ExportPath(ExportPathMsg),

    ListTags(ListTagsMsg),
    RenameTag(RenameTagMsg),
    DeleteTags(DeleteTagsMsg),
    SetTag(SetTagMsg),
    CreateTag(CreateTagMsg),

    SyncDb(SyncDbMsg),
    Shutdown(ShutdownMsg),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ImportProgress {
    CopyProgress {
        offset: u64,
    },
    Size {
        size: u64,
    },
    CopyDone,
    OutboardProgress {
        offset: u64,
    },
    Done {
        hash: Hash,
    },
    Error {
        #[serde(with = "crate::util::serde::io_error_serde")]
        cause: io::Error,
    },
}

impl From<io::Error> for ImportProgress {
    fn from(e: io::Error) -> Self {
        Self::Error { cause: e }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExportProgress {
    Size {
        size: u64,
    },
    CopyProgress {
        offset: u64,
    },
    Done,
    Error {
        #[serde(with = "crate::util::serde::io_error_serde")]
        cause: io::Error,
    },
}

impl From<io::Error> for ExportProgress {
    fn from(e: io::Error) -> Self {
        Self::Error { cause: e }
    }
}

/// The import mode describes how files will be imported.
///
/// This is a hint to the import trait method. For some implementations, this
/// does not make any sense. E.g. an in memory implementation will always have
/// to copy the file into memory. Also, a disk based implementation might choose
/// to copy small files even if the mode is `Reference`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ImportMode {
    /// This mode will copy the file into the database before hashing.
    ///
    /// This is the safe default because the file can not be accidentally modified
    /// after it has been imported.
    #[default]
    Copy,
    /// This mode will try to reference the file in place and assume it is unchanged after import.
    ///
    /// This has a large performance and storage benefit, but it is less safe since
    /// the file might be modified after it has been imported.
    ///
    /// Stores are allowed to ignore this mode and always copy the file, e.g.
    /// if the file is very small or if the store does not support referencing files.
    TryReference,
}

/// The import mode describes how files will be imported.
///
/// This is a hint to the import trait method. For some implementations, this
/// does not make any sense. E.g. an in memory implementation will always have
/// to copy the file into memory. Also, a disk based implementation might choose
/// to copy small files even if the mode is `Reference`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum ExportMode {
    /// This mode will copy the file to the target directory.
    ///
    /// This is the safe default because the file can not be accidentally modified
    /// after it has been exported.
    #[default]
    Copy,
    /// This mode will try to move the file to the target directory and then reference it from
    /// the database.
    ///
    /// This has a large performance and storage benefit, but it is less safe since
    /// the file might be modified in the target directory after it has been exported.
    ///
    /// Stores are allowed to ignore this mode and always copy the file, e.g.
    /// if the file is very small or if the store does not support referencing files.
    TryReference,
}
