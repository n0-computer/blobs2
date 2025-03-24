//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Command`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
use std::{fmt::Debug, io, pin::Pin};

use arrayvec::ArrayString;
pub use bao_tree::io::mixed::EncodedItem;
use bao_tree::io::BaoContentItem;
use bytes::Bytes;
use n0_future::Stream;
use quic_rpc::{
    channel::{oneshot, spsc},
    WithChannels,
};
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::{
    blobs::{
        self, Bitfield, ExportBao, ExportPath, ExportProgress, ImportBao, ImportByteStream,
        ImportBytes, ImportPath, ImportProgress, ListBlobs, Observe,
    },
    tags::{self, TagInfo},
    Shutdown, SyncDb,
};
use crate::{store::util::Tag, Hash, HashAndFormat};

pub trait HashSpecific {
    fn hash(&self) -> Hash;

    fn hash_short(&self) -> ArrayString<10> {
        self.hash().fmt_short()
    }
}

pub type ImportBaoMsg = WithChannels<ImportBao, StoreService>;

impl HashSpecific for ImportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type ShutdownMsg = WithChannels<Shutdown, StoreService>;

pub type ObserveMsg = WithChannels<Observe, StoreService>;

impl HashSpecific for ObserveMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type ImportBytesMsg = WithChannels<ImportBytes, StoreService>;

pub type ExportBaoMsg = WithChannels<ExportBao, StoreService>;

impl HashSpecific for ExportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type ExportPathMsg = WithChannels<ExportPath, StoreService>;

impl HashSpecific for ExportPathMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

pub type ImportByteStreamMsg = WithChannels<ImportByteStream, StoreService>;

pub type ImportPathMsg = WithChannels<ImportPath, StoreService>;

pub type ListTagsMsg = WithChannels<tags::ListTags, StoreService>;

pub type RenameTagMsg = WithChannels<tags::Rename, StoreService>;

pub type DeleteTagsMsg = WithChannels<tags::Delete, StoreService>;

pub type DeleteBlobsMsg = WithChannels<blobs::DeleteBlobs, StoreService>;

pub type SetTagMsg = WithChannels<tags::SetTag, StoreService>;

/// Debug tool to exit the process in the middle of a write transaction, for testing.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessExit {
    pub code: i32,
}

pub type CreateTagMsg = WithChannels<tags::CreateTag, StoreService>;

impl HashSpecific for CreateTagMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.content.hash
    }
}

pub type ListBlobsMsg = WithChannels<ListBlobs, StoreService>;

pub type SyncDbMsg = WithChannels<SyncDb, StoreService>;

#[derive(Debug, Clone)]
pub struct StoreService;
impl quic_rpc::Service for StoreService {}

#[rpc_requests(StoreService, Command)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    #[rpc(tx = spsc::Sender<super::Result<Hash>>)]
    ListBlobs(blobs::ListBlobs),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteBlobs(blobs::DeleteBlobs),
    #[rpc(rx = spsc::Receiver<BaoContentItem>, tx = oneshot::Sender<super::Result<()>>)]
    ImportBao(blobs::ImportBao),
    #[rpc(tx = spsc::Sender<EncodedItem>)]
    ExportBao(blobs::ExportBao),
    #[rpc(tx = spsc::Sender<Bitfield>)]
    Observe(blobs::Observe),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportBytes(blobs::ImportBytes),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportByteStream(blobs::ImportByteStream),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportPath(blobs::ImportPath),
    #[rpc(tx = spsc::Sender<ExportProgress>)]
    ExportPath(blobs::ExportPath),
    #[rpc(tx = oneshot::Sender<Vec<super::Result<TagInfo>>>)]
    ListTags(tags::ListTags),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SetTag(tags::SetTag),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteTags(tags::Delete),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    RenameTag(tags::Rename),
    #[rpc(tx = oneshot::Sender<super::Result<Tag>>)]
    CreateTag(tags::CreateTag),
    #[rpc(tx = oneshot::Sender<Vec<HashAndFormat>>)]
    ListTempTags(tags::TempTags),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
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
