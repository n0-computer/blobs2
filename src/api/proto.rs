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
use std::{fmt::Debug, io, pin::Pin};

use arrayvec::ArrayString;
use bao_tree::io::BaoContentItem;
use bytes::Bytes;
use irpc::channel::{oneshot, spsc};
use irpc_derive::rpc_requests;
use n0_future::Stream;
use serde::{Deserialize, Serialize};

use super::{
    blobs::{self, BatchResponse, Bitfield, BlobStatus, EncodedItem, ExportProgress, ImportProgress, Scope}, tags::{self, TagInfo}, ClearProtected, ShutdownRequest, SyncDbRequest
};
use crate::{store::util::Tag, util::temp_tag::TempTag, Hash, HashAndFormat};

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
    ListTags(tags::ListTagsRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SetTag(tags::SetTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteTags(tags::DeleteRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    RenameTag(tags::RenameRequest),
    #[rpc(tx = oneshot::Sender<super::Result<Tag>>)]
    CreateTag(tags::CreateTagRequest),
    #[rpc(tx = oneshot::Sender<Vec<HashAndFormat>>)]
    ListTempTags(tags::ListTempTagsRequest),
    #[rpc(tx = oneshot::Sender<TempTag>)]
    CreateTempTag(tags::CreateTempTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SyncDb(SyncDbRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(ShutdownRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    ClearProtected(ClearProtected),
}
