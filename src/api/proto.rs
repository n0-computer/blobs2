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
        self, BatchResponse, Bitfield, BlobStatus, ExportBaoRequest, ExportPath, ExportProgress, ImportBaoRequest, ImportByteStream, ImportBytesRequest, ImportPath, ImportProgress, ListRequest, ObserveRequest
    }, tags::{self, TagInfo}, ClearProtected, Scope, ShutdownRequest, SyncDb
};
use crate::{store::util::Tag, Hash, HashAndFormat};

pub trait HashSpecific {
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

pub type ExportPathMsg = WithChannels<ExportPath, StoreService>;

impl HashSpecific for ExportPathMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

pub type ImportBaoMsg = WithChannels<ImportBaoRequest, StoreService>;
pub type ShutdownMsg = WithChannels<ShutdownRequest, StoreService>;
pub type ObserveMsg = WithChannels<ObserveRequest, StoreService>;
pub type ImportBytesMsg = WithChannels<ImportBytesRequest, StoreService>;
pub type ExportBaoMsg = WithChannels<ExportBaoRequest, StoreService>;
pub type ImportByteStreamMsg = WithChannels<ImportByteStream, StoreService>;
pub type ImportPathMsg = WithChannels<ImportPath, StoreService>;
pub type ListTagsMsg = WithChannels<tags::ListTags, StoreService>;
pub type RenameTagMsg = WithChannels<tags::Rename, StoreService>;
pub type DeleteTagsMsg = WithChannels<tags::Delete, StoreService>;
pub type DeleteBlobsMsg = WithChannels<blobs::DeleteRequest, StoreService>;
pub type SetTagMsg = WithChannels<tags::SetTag, StoreService>;
pub type ClearProtectedMsg = WithChannels<ClearProtected, StoreService>;
pub type BlobStatusMsg = WithChannels<blobs::BlobStatusRequest, StoreService>;
pub type BatchMsg = WithChannels<blobs::BatchRequest, StoreService>;
pub type CreateTagMsg = WithChannels<tags::CreateTagRequest, StoreService>;
pub type ListBlobsMsg = WithChannels<blobs::ListRequest, StoreService>;
pub type SyncDbMsg = WithChannels<SyncDb, StoreService>;

impl HashSpecific for CreateTagMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.content.hash
    }
}

#[derive(Debug, Clone)]
pub struct StoreService;
impl quic_rpc::Service for StoreService {}

#[rpc_requests(StoreService, Command)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    #[rpc(tx = spsc::Sender<super::Result<Hash>>)]
    ListBlobs(blobs::ListRequest),
    #[rpc(tx = oneshot::Sender<Scope>, rx = spsc::Receiver<BatchResponse>)]
    Batch(blobs::BatchRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteBlobs(blobs::DeleteRequest),
    #[rpc(rx = spsc::Receiver<BaoContentItem>, tx = oneshot::Sender<super::Result<()>>)]
    ImportBao(blobs::ImportBaoRequest),
    #[rpc(tx = spsc::Sender<EncodedItem>)]
    ExportBao(blobs::ExportBaoRequest),
    #[rpc(tx = spsc::Sender<Bitfield>)]
    Observe(blobs::ObserveRequest),
    #[rpc(tx = oneshot::Sender<BlobStatus>)]
    GetBlobStatus(blobs::BlobStatusRequest),
    #[rpc(tx = spsc::Sender<ImportProgress>)]
    ImportBytes(blobs::ImportBytesRequest),
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
    CreateTag(tags::CreateTagRequest),
    #[rpc(tx = oneshot::Sender<Vec<HashAndFormat>>)]
    ListTempTags(tags::TempTags),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SyncDb(SyncDb),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(ShutdownRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    ClearProtected(ClearProtected),
}
