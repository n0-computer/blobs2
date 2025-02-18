//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Command`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
use std::{io, path::PathBuf, pin::Pin};

use bao_tree::{blake3::Hash, io::BaoContentItem, ChunkRanges};
use bytes::Bytes;
use n0_future::Stream;

pub use crate::bitfield::BitfieldEvent;
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
    pub data: tokio::sync::mpsc::Receiver<BaoContentItem>,
    pub out: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
}

/// Observe the bitfield of the given hash.
#[derive(Debug)]
pub struct Observe {
    pub hash: Hash,
    pub out: tokio::sync::mpsc::Sender<BitfieldEvent>,
}

/// Import the given bytes.
#[derive(Debug)]
pub struct ImportBytes {
    pub data: Bytes,
    pub out: tokio::sync::mpsc::Sender<ImportProgress>,
}

/// Export the given sizes in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
#[derive(Debug)]
pub struct ExportBao {
    pub hash: Hash,
    pub ranges: ChunkRanges,
    pub out: tokio::sync::mpsc::Sender<EncodedItem>,
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
    pub out: tokio::sync::mpsc::Sender<ExportProgress>,
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
    pub out: tokio::sync::mpsc::Sender<ImportProgress>,
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
}

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
