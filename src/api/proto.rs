#![cfg_attr(not(feature = "proto-docs"), doc(hidden))]
//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Request`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
//!
//! Each command consists of a serializable request message and channels for updates
//! and responses. The enum containing the full requests is [`Command`]. These are the
//! commands you will have to handle in a store actor handler.
//!
//! This crate provides a file system based store implementation, [`crate::store::fs::FsStore`],
//! as well as a mutable in-memory store and an immutable in-memory store.
//!
//! The file system store is quite complex and optimized, so to get started take a look at
//! the much simpler memory store.
use std::{
    fmt::Debug,
    io,
    num::NonZeroU64,
    ops::{Bound, RangeBounds},
    path::PathBuf,
    pin::Pin,
};

use arrayvec::ArrayString;
use bao_tree::{ChunkRanges, io::BaoContentItem};
use bytes::Bytes;
use irpc::channel::{oneshot, spsc};
use irpc_derive::rpc_requests;
use n0_future::Stream;
use serde::{Deserialize, Serialize};

use super::{
    blobs::{
        self, Bitfield, BlobStatus, EncodedItem, ExportMode, ExportProgress, ImportMode,
        ImportProgress, Scope,
    },
    tags::{self, TagInfo},
};
use crate::{BlobFormat, Hash, HashAndFormat, store::util::Tag, util::temp_tag::TempTag};

pub(crate) trait HashSpecific {
    fn hash(&self) -> Hash;

    fn hash_short(&self) -> ArrayString<10> {
        self.hash().fmt_short()
    }
}

impl HashSpecific for ImportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ObserveMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ExportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ExportPathMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

impl HashSpecific for CreateTagMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.value.hash
    }
}

#[derive(Debug, Clone)]
pub struct StoreService;
impl irpc::Service for StoreService {}

#[rpc_requests(StoreService, message = Command, alias = "Msg")]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    #[rpc(tx = spsc::Sender<super::Result<Hash>>)]
    ListBlobs(ListRequest),
    #[rpc(tx = oneshot::Sender<Scope>, rx = spsc::Receiver<BatchResponse>)]
    Batch(BatchRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteBlobs(BlobDeleteRequest),
    #[rpc(rx = spsc::Receiver<BaoContentItem>, tx = oneshot::Sender<super::Result<()>>)]
    ImportBao(ImportBaoRequest),
    #[rpc(tx = spsc::Sender<EncodedItem>)]
    ExportBao(ExportBaoRequest),
    #[rpc(tx = spsc::Sender<Bitfield>)]
    Observe(ObserveRequest),
    #[rpc(tx = oneshot::Sender<BlobStatus>)]
    GetBlobStatus(BlobStatusRequest),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportBytes(ImportBytesRequest),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportByteStream(ImportByteStreamRequest),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportPath(ImportPathRequest),
    #[rpc(tx = spsc::Sender<ExportProgress>)]
    ExportPath(ExportPathRequest),
    #[rpc(tx = oneshot::Sender<Vec<super::Result<TagInfo>>>)]
    ListTags(ListTagsRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SetTag(SetTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteTags(TagsDeleteRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    RenameTag(RenameTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<Tag>>)]
    CreateTag(CreateTagRequest),
    #[rpc(tx = oneshot::Sender<Vec<HashAndFormat>>)]
    ListTempTags(ListTempTagsRequest),
    #[rpc(tx = oneshot::Sender<TempTag>)]
    CreateTempTag(CreateTempTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SyncDb(SyncDbRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(ShutdownRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    ClearProtected(ClearProtectedRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncDbRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShutdownRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClearProtectedRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobStatusRequest {
    pub hash: Hash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest;

#[derive(Debug, Serialize, Deserialize)]
pub enum BatchResponse {
    Drop(HashAndFormat),
    Ping,
}

/// Options for force deletion of blobs.
#[derive(Debug, Serialize, Deserialize)]
pub struct BlobDeleteRequest {
    pub hashes: Vec<Hash>,
    pub force: bool,
}

/// Import the given bytes.
#[derive(Serialize, Deserialize)]
pub struct ImportBytesRequest {
    pub data: Bytes,
    pub format: BlobFormat,
    pub scope: Scope,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportPathRequest {
    pub path: PathBuf,
    pub mode: ImportMode,
    pub format: BlobFormat,
    pub scope: Scope,
}

/// Import bao encoded data for the given hash with the iroh block size.
///
/// The result is just a single item, indicating if a write error occurred.
/// To observe the incoming data more granularly, use the `Observe` command
/// concurrently.
#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBaoRequest {
    pub hash: Hash,
    pub size: NonZeroU64,
}

/// Observe the local bitfield of the given hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct ObserveRequest {
    pub hash: Hash,
}

/// Export the given ranges in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportBaoRequest {
    pub hash: Hash,
    pub ranges: ChunkRanges,
}

/// Export a file to a target path.
///
/// For an incomplete file, the size might be truncated and gaps will be filled
/// with zeros. If possible, a store implementation should try to write as a
/// sparse file.

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportPathRequest {
    pub hash: Hash,
    pub mode: ExportMode,
    pub target: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportByteStreamRequest {
    pub format: BlobFormat,
    pub data: Vec<Bytes>,
    pub scope: Scope,
}

/// Options for a list operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTagsRequest {
    /// List tags to hash seqs
    pub hash_seq: bool,
    /// List tags to raw blobs
    pub raw: bool,
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl ListTagsRequest {
    /// List a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self {
            from,
            to,
            raw: true,
            hash_seq: true,
        }
    }

    /// List tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            raw: true,
            hash_seq: true,
            from: Some(from),
            to,
        }
    }

    /// List a single tag
    pub fn single(name: &[u8]) -> Self {
        let from = Tag::from(name);
        Self {
            to: Some(from.successor()),
            from: Some(from),
            raw: true,
            hash_seq: true,
        }
    }

    /// List all tags
    pub fn all() -> Self {
        Self {
            raw: true,
            hash_seq: true,
            from: None,
            to: None,
        }
    }

    /// List raw tags
    pub fn raw() -> Self {
        Self {
            raw: true,
            hash_seq: false,
            from: None,
            to: None,
        }
    }

    /// List hash seq tags
    pub fn hash_seq() -> Self {
        Self {
            raw: false,
            hash_seq: true,
            from: None,
            to: None,
        }
    }
}

pub(crate) fn tags_from_range<R, E>(range: R) -> (Option<Tag>, Option<Tag>)
where
    R: RangeBounds<E>,
    E: AsRef<[u8]>,
{
    let from = match range.start_bound() {
        Bound::Included(start) => Some(Tag::from(start.as_ref())),
        Bound::Excluded(start) => Some(Tag::from(start.as_ref()).successor()),
        Bound::Unbounded => None,
    };
    let to = match range.end_bound() {
        Bound::Included(end) => Some(Tag::from(end.as_ref()).successor()),
        Bound::Excluded(end) => Some(Tag::from(end.as_ref())),
        Bound::Unbounded => None,
    };
    (from, to)
}


/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTempTagRequest {
    pub scope: Scope,
    pub value: HashAndFormat,
}

/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTempTagsRequest;

/// Rename a tag atomically
#[derive(Debug, Serialize, Deserialize)]
pub struct RenameTagRequest {
    /// Old tag name
    pub from: Tag,
    /// New tag name
    pub to: Tag,
}

/// Options for a delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagsDeleteRequest {
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl TagsDeleteRequest {
    /// Delete a single tag
    pub fn single(name: &[u8]) -> Self {
        let name = Tag::from(name);
        Self {
            to: Some(name.successor()),
            from: Some(name),
        }
    }

    /// Delete a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self { from, to }
    }

    /// Delete tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            from: Some(from),
            to,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetTagRequest {
    pub name: Tag,
    pub value: HashAndFormat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTagRequest {
    pub value: HashAndFormat,
}
