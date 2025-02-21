//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Command`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
use std::{io, path::PathBuf, pin::Pin};

use bao_tree::{blake3::Hash, io::BaoContentItem, ChunkRanges};
use bytes::Bytes;
use n0_future::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

pub use crate::bitfield::BitfieldEvent;
use crate::{util::Tag, BlobFormat, HashAndFormat};
pub use bao_tree::io::mixed::EncodedItem;

/// Import bao encoded data for the given hash with the iroh block size.
///
/// The result is just a single item, indicating if a write error occurred.
/// To observe the incoming data more granularly, use the `Observe` command
/// concurrently.
#[derive(Debug)]
pub struct ImportBao {
    pub hash: Hash,
    pub size: u64,
    pub data: mpsc::Receiver<BaoContentItem>,
    pub out: oneshot::Sender<anyhow::Result<()>>,
}

/// Observe the bitfield of the given hash.
#[derive(Debug)]
pub struct Observe {
    pub hash: Hash,
    pub out: mpsc::Sender<BitfieldEvent>,
}

/// Import the given bytes.
#[derive(Debug)]
pub struct ImportBytes {
    pub data: Bytes,
    pub out: mpsc::Sender<ImportProgress>,
}

/// Export the given sizes in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
#[derive(Debug)]
pub struct ExportBao {
    pub hash: Hash,
    pub ranges: ChunkRanges,
    pub out: mpsc::Sender<EncodedItem>,
}

/// Export a file to a target path.
///
/// For an incomplete file, the size might be truncated and gaps will be filled
/// with zeros. If possible, a store implementation should try to write as a
/// sparse file.
#[derive(Debug)]
pub struct ExportPath {
    pub hash: Hash,
    pub target: PathBuf,
    pub out: mpsc::Sender<ExportProgress>,
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

pub struct ImportByteStream {
    pub data: BoxedByteStream,
    pub out: tokio::sync::mpsc::Sender<ImportProgress>,
}

impl std::fmt::Debug for ImportByteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportByteStream").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ImportPath {
    pub path: PathBuf,
    pub mode: ImportMode,
    pub format: BlobFormat,
    pub out: mpsc::Sender<ImportProgress>,
}

#[derive(derive_more::Debug)]
pub struct Tags {
    #[debug(skip)]
    pub tx: oneshot::Sender<anyhow::Result<Vec<(Tag, HashAndFormat)>>>,
}

#[derive(derive_more::Debug)]
pub struct SetTag {
    pub tag: Tag,
    pub value: Option<HashAndFormat>,
    pub tx: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug)]
pub struct CreateTag {
    pub hash: HashAndFormat,
    pub tx: oneshot::Sender<anyhow::Result<Tag>>,
}

#[derive(Debug)]
pub struct SyncDb {
    pub tx: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug, derive_more::From)]
pub enum Command {
    ImportBao(ImportBao),
    ExportBao(ExportBao),
    Observe(Observe),
    ImportBytes(ImportBytes),
    ImportByteStream(ImportByteStream),
    ImportPath(ImportPath),
    ExportPath(ExportPath),
    Tags(Tags),
    SetTag(SetTag),
    CreateTag(CreateTag),
    SyncDb(SyncDb),
}

#[derive(Debug)]
pub enum ImportProgress {
    CopyProgress { offset: u64 },
    Size { size: u64 },
    CopyDone,
    OutboardProgress { offset: u64 },
    Done { hash: Hash },
    Error { cause: anyhow::Error },
}

#[derive(Debug)]
pub enum ExportProgress {
    Size { size: u64 },
    CopyProgress { offset: u64 },
    Done,
    Error { cause: anyhow::Error },
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
