//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Command`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
use std::{fmt::Debug, io, num::NonZeroU64, path::PathBuf, pin::Pin};

use arrayvec::ArrayString;
pub use bao_tree::io::mixed::EncodedItem;
use bao_tree::{io::BaoContentItem, ChunkRanges};
use bytes::Bytes;
use n0_future::Stream;
use quic_rpc::{
    channel::{oneshot, spsc},
    WithChannels,
};
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::api::{
    self,
    tags::{self, TagInfo},
    ExportMode, ExportProgress, ImportMode, ImportProgress,
};
use crate::{
    store::{bitfield::Bitfield, util::Tag, BlobFormat},
    Hash,
};

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

pub type ObserveMsg = WithChannels<Observe, StoreService>;

impl HashSpecific for ObserveMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
pub struct Batch(u64);

/// Import the given bytes.
#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBytes {
    pub data: Bytes,
    pub format: BlobFormat,
    pub batch: Batch,
}

pub type ImportBytesMsg = WithChannels<ImportBytes, StoreService>;

/// Export the given ranges in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportBao {
    pub hash: Hash,
    #[serde(with = "crate::util::serde::chunk_ranges_serde")]
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

#[derive(Debug, Serialize, Deserialize)]
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
    pub batch: Batch,
}

pub type ImportByteStreamMsg = WithChannels<ImportByteStream, StoreService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportPath {
    pub path: PathBuf,
    pub mode: ImportMode,
    pub format: BlobFormat,
    pub batch: Batch,
}

pub type ImportPathMsg = WithChannels<ImportPath, StoreService>;

pub type ListTagsMsg = WithChannels<tags::ListTags, StoreService>;

pub type RenameTagMsg = WithChannels<tags::Rename, StoreService>;

pub type DeleteTagsMsg = WithChannels<tags::Delete, StoreService>;

pub type SetTagMsg = WithChannels<tags::SetTag, StoreService>;

/// Debug tool to exit the process in the middle of a write transaction, for testing.
#[derive(Debug)]
pub struct ProcessExit {
    pub code: i32,
}

pub type CreateTagMsg = WithChannels<tags::CreateTag, StoreService>;

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

#[rpc_requests(StoreService, Command)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    #[rpc(rx = spsc::Receiver<BaoContentItem>, tx = oneshot::Sender<api::Result<()>>)]
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
    #[rpc(tx = oneshot::Sender<Vec<api::Result<TagInfo>>>)]
    ListTags(tags::ListTags),
    #[rpc(tx = oneshot::Sender<api::Result<()>>)]
    SetTag(tags::SetTag),
    #[rpc(tx = oneshot::Sender<api::Result<()>>)]
    DeleteTags(tags::Delete),
    #[rpc(tx = oneshot::Sender<api::Result<()>>)]
    RenameTag(tags::Rename),
    #[rpc(tx = oneshot::Sender<api::Result<Tag>>)]
    CreateTag(tags::CreateTag),
    #[rpc(tx = oneshot::Sender<api::Result<()>>)]
    SyncDb(SyncDb),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(Shutdown),
}

#[allow(dead_code)]
#[derive(Debug, derive_more::From)]
pub enum Command2 {
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
