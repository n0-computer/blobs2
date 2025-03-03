//! An implementation of a bao file, meaning some data blob with associated
//! outboard.
//!
//! Compared to just a pair of (data, outboard), this implementation also works
//! when both the data and the outboard is incomplete, and not even the size
//! is fully known.
//!
//! There is a full in memory implementation, and an implementation that uses
//! the file system for the data, outboard, and sizes file. There is also a
//! combined implementation that starts in memory and switches to file when
//! the memory limit is reached.
use std::{
    fs::{File, OpenOptions},
    io,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, Weak},
};

use bao_tree::{
    io::{
        fsm::BaoContentItem,
        outboard::PreOrderOutboard,
        sync::{ReadAt, WriteAt},
    },
    BaoTree, ChunkRanges,
};
use bytes::{Bytes, BytesMut};
use derive_more::Debug;
use tokio::sync::mpsc;

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    meta::{self, Update},
    options::PathOptions,
};
use crate::{
    bitfield::Bitfield,
    fs::{meta::raw_outboard_size, TaskContext},
    mem::{PartialMemStorage, SizeInfo},
    util::{
        observer::{Observable, Observer},
        MemOrFile, SparseMemFile,
    },
    Hash, IROH_BLOCK_SIZE,
};

/// Storage for complete blobs. There is no longer any uncertainty about the
/// size, so we don't need a sizes file.
///
/// Writing is not possible but also not needed, since the file is complete.
/// This covers all combinations of data and outboard being in memory or on
/// disk.
///
/// For the memory variant, it does reading in a zero copy way, since storage
/// is already a `Bytes`.
#[derive(Default, derive_more::Debug)]
pub struct CompleteStorage {
    /// data part, which can be in memory or on disk.
    #[debug("{:?}", data.as_ref().map_mem(|x| x.len()))]
    pub data: MemOrFile<Bytes, (File, u64)>,
    /// outboard part, which can be in memory or on disk.
    #[debug("{:?}", outboard.as_ref().map_mem(|x| x.len()))]
    pub outboard: MemOrFile<Bytes, (File, u64)>,
}

impl CompleteStorage {
    pub fn add_observer(&mut self, mut observer: Observer<Bitfield>) {
        let bitfield = Bitfield::complete(self.data.size());
        observer.send(bitfield).ok();
    }

    /// The size of the data file.
    pub fn data_size(&self) -> u64 {
        match &self.data {
            MemOrFile::Mem(mem) => mem.len() as u64,
            MemOrFile::File((_file, size)) => *size,
        }
    }
}

/// Create a file for reading and writing, but *without* truncating the existing
/// file.
fn create_read_write(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Read from the given file at the given offset, until end of file or max bytes.
fn read_to_end(file: impl ReadAt, offset: u64, max: usize) -> io::Result<Bytes> {
    let mut res = BytesMut::new();
    let mut buf = [0u8; 4096];
    let mut remaining = max;
    let mut offset = offset;
    while remaining > 0 {
        let end = buf.len().min(remaining);
        let read = file.read_at(offset, &mut buf[..end])?;
        if read == 0 {
            // eof
            break;
        }
        res.extend_from_slice(&buf[..read]);
        offset += read as u64;
        remaining -= read;
    }
    Ok(res.freeze())
}

fn max_offset(batch: &[BaoContentItem]) -> u64 {
    batch
        .iter()
        .filter_map(|item| match item {
            BaoContentItem::Leaf(leaf) => {
                let len = leaf.data.len().try_into().unwrap();
                let end = leaf
                    .offset
                    .checked_add(len)
                    .expect("u64 overflow for leaf end");
                Some(end)
            }
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// A file storage for an incomplete bao file.
#[derive(Debug)]
pub struct PartialFileStorage {
    data: std::fs::File,
    outboard: std::fs::File,
    sizes: std::fs::File,
    bitfield: Observable<Bitfield>,
}

impl PartialFileStorage {
    fn into_complete(
        self,
        _hash: &crate::Hash,
        ctx: &TaskContext,
    ) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let size = self.bitfield.state().size;
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if ctx.options.is_inlined_data(size) {
            let data = read_to_end(&self.data, 0, size as usize)?;
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            (
                MemOrFile::File((self.data, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if ctx.options.is_inlined_outboard(outboard_size) {
            if outboard_size == 0 {
                (MemOrFile::Mem(Bytes::new()), OutboardLocation::NotNeeded)
            } else {
                let outboard = read_to_end(&self.outboard, 0, outboard_size as usize)?;
                (
                    MemOrFile::Mem(outboard.clone()),
                    OutboardLocation::Inline(outboard),
                )
            }
        } else {
            (
                MemOrFile::File((self.outboard, outboard_size)),
                OutboardLocation::Owned,
            )
        };
        // todo: notify the store that the state has changed to complete
        Ok((
            CompleteStorage { data, outboard },
            EntryState::Complete {
                data_location,
                outboard_location,
            },
        ))
    }

    fn add_observer(&mut self, observer: Observer<Bitfield>) {
        self.bitfield.add_observer(observer);
    }

    /// Split into data, outboard and sizes files.
    pub fn into_parts(self) -> (File, File, File) {
        (self.data, self.outboard, self.sizes)
    }

    fn current_size(&self) -> io::Result<u64> {
        let len = self.sizes.metadata()?.len();
        if len < 8 {
            Ok(0)
        } else {
            // todo: use the last full u64 in case the sizes file is not a multiple of 8
            // bytes. Not sure how that would happen, but we should handle it.
            let mut buf = [0u8; 8];
            self.sizes.read_exact_at(len - 8, &mut buf)?;
            Ok(u64::from_le_bytes(buf))
        }
    }

    fn write_batch(
        &mut self,
        size: NonZeroU64,
        batch: &[BaoContentItem],
        ranges: &ChunkRanges,
    ) -> io::Result<()> {
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset * 64;
                        self.outboard
                            .write_all_at(o0, parent.pair.0.as_bytes().as_slice())?;
                        self.outboard
                            .write_all_at(o0 + 32, parent.pair.1.as_bytes().as_slice())?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    let o0 = leaf.offset;
                    // divide by chunk size, multiply by 8
                    let index = (leaf.offset >> (tree.block_size().chunk_log() + 10)) << 3;
                    tracing::trace!(
                        "write_batch f={:?} o={} l={}",
                        self.data,
                        o0,
                        leaf.data.len()
                    );
                    self.data.write_all_at(o0, leaf.data.as_ref())?;
                    let size = tree.size();
                    self.sizes.write_all_at(index, &size.to_le_bytes())?;
                }
            }
        }
        let current = &self.bitfield.state().ranges;
        let added = ranges - current;
        let update = Bitfield::new(added, size.get());
        self.bitfield.update(update);
        Ok(())
    }
}

/// The storage for a bao file. This can be either in memory or on disk.
#[derive(Debug, derive_more::From)]
pub(crate) enum BaoFileStorage {
    /// The entry is incomplete and in memory.
    ///
    /// Since it is incomplete, it must be writeable.
    ///
    /// This is used mostly for tiny entries, <= 16 KiB. But in principle it
    /// can be used for larger sizes.
    ///
    /// Incomplete mem entries are *not* persisted at all. So if the store
    /// crashes they will be gone.
    PartialMem(PartialMemStorage),
    /// The entry is incomplete and on disk.
    Partial(PartialFileStorage),
    /// The entry is complete. Outboard and data can come from different sources
    /// (memory or file).
    ///
    /// Writing to this is a no-op, since it is already complete.
    Complete(CompleteStorage),
}

impl Default for BaoFileStorage {
    fn default() -> Self {
        BaoFileStorage::Complete(Default::default())
    }
}

impl PartialMemStorage {
    /// Converts this storage into a complete storage, using the given hash for
    /// path names and the given options for decisions about inlining.
    fn into_complete(
        self,
        hash: &Hash,
        ctx: &TaskContext,
    ) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let size = self.size();
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if ctx.options.is_inlined_data(size) {
            let data: Bytes = self.data.to_vec().into();
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            let data_path = ctx.options.path.owned_data_path(hash);
            let mut data_file = create_read_write(&data_path)?;
            self.data.persist(&mut data_file)?;
            (
                MemOrFile::File((data_file, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if ctx.options.is_inlined_outboard(outboard_size) {
            let outboard: Bytes = self.outboard.to_vec().into();
            (
                MemOrFile::Mem(outboard.clone()),
                OutboardLocation::Inline(outboard),
            )
        } else {
            let outboard_path = ctx.options.path.owned_outboard_path(hash);
            let mut outboard_file = create_read_write(&outboard_path)?;
            self.outboard.persist(&mut outboard_file)?;
            let outboard_location = if outboard_size == 0 {
                OutboardLocation::NotNeeded
            } else {
                OutboardLocation::Owned
            };
            (
                MemOrFile::File((outboard_file, outboard_size)),
                outboard_location,
            )
        };
        Ok((
            CompleteStorage { data, outboard },
            EntryState::Complete {
                data_location,
                outboard_location,
            },
        ))
    }
}

fn send_update(
    ctx: &TaskContext,
    permit: mpsc::Permit<meta::Command>,
    hash: &Hash,
    update: EntryState<Bytes>,
) {
    permit.send(
        Update {
            epoch: ctx.epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            hash: *hash,
            state: update,
            tx: None,
        }
        .into(),
    );
}

impl BaoFileStorage {
    fn write_batch(
        self,
        size: NonZeroU64,
        batch: &[BaoContentItem],
        ranges: &ChunkRanges,
        ctx: &TaskContext,
        hash: &Hash,
        permit: mpsc::Permit<meta::Command>,
    ) -> io::Result<Self> {
        Ok(match self {
            BaoFileStorage::PartialMem(mut ms) => {
                // check if we need to switch to file mode, otherwise write to memory
                if max_offset(batch) <= ctx.options.inline.max_data_inlined {
                    ms.write_batch(size, batch, ranges)?;
                    if ms.bitfield.state().is_complete() {
                        let (state, update) = ms.into_complete(hash, ctx)?;
                        send_update(ctx, permit, hash, update);
                        state.into()
                    } else {
                        ms.into()
                    }
                } else {
                    // *first* switch to file mode, *then* write the batch.
                    //
                    // otherwise we might allocate a lot of memory if we get
                    // a write at the end of a very large file.
                    //
                    // opt: we should check if we become complete to avoid going from mem to partial to complete
                    let mut fs = ms.persist(&ctx.options.path, hash)?;
                    fs.write_batch(size, batch, ranges)?;
                    if fs.bitfield.state().is_complete() {
                        let (state, update) = fs.into_complete(hash, ctx)?;
                        send_update(ctx, permit, hash, update);
                        state.into()
                    } else {
                        let size = if fs.bitfield.state().is_validated() {
                            Some(fs.bitfield.state().size)
                        } else {
                            None
                        };
                        send_update(ctx, permit, hash, EntryState::Partial { size });
                        fs.into()
                    }
                }
            }
            BaoFileStorage::Partial(mut fs) => {
                let validated_before = fs.bitfield.state().is_validated();
                fs.write_batch(size, batch, ranges)?;
                if fs.bitfield.state().is_complete() {
                    let (cs, update) = fs.into_complete(hash, ctx)?;
                    send_update(ctx, permit, hash, update);
                    cs.into()
                } else {
                    if !validated_before && fs.bitfield.state().is_validated() {
                        // we are still partial, but now we know the size
                        send_update(
                            ctx,
                            permit,
                            hash,
                            EntryState::Partial {
                                size: Some(fs.bitfield.state().size),
                            },
                        );
                    }
                    fs.into()
                }
            }
            BaoFileStorage::Complete(_) => {
                // we are complete, so just ignore the write
                // unless there is a bug, this would just write the exact same data
                self
            }
        })
    }

    fn observer_dropped(&mut self) {
        match self {
            BaoFileStorage::Complete(_) => {}
            BaoFileStorage::Partial(i) => i.bitfield.observer_dropped(),
            BaoFileStorage::PartialMem(i) => i.bitfield.observer_dropped(),
        }
    }

    fn add_observer(&mut self, observer: Observer<Bitfield>) {
        match self {
            BaoFileStorage::Complete(c) => c.add_observer(observer),
            BaoFileStorage::Partial(i) => i.add_observer(observer),
            BaoFileStorage::PartialMem(i) => i.add_observer(observer),
        }
    }

    /// Create a new mutable mem storage.
    pub fn incomplete_mem() -> Self {
        Self::PartialMem(Default::default())
    }

    /// Call sync_all on all the files.
    fn sync_all(&self) -> io::Result<()> {
        match self {
            Self::Complete(_) => Ok(()),
            Self::PartialMem(_) => Ok(()),
            Self::Partial(file) => {
                file.data.sync_all()?;
                file.outboard.sync_all()?;
                file.sizes.sync_all()?;
                Ok(())
            }
        }
    }

    /// True if the storage is in memory.
    pub fn is_mem(&self) -> bool {
        match self {
            Self::PartialMem(_) => true,
            Self::Partial(_) => false,
            Self::Complete(c) => c.data.is_mem() && c.outboard.is_mem(),
        }
    }
}

/// A weak reference to a bao file handle.
#[derive(Debug, Clone)]
pub struct BaoFileHandleWeak(Weak<BaoFileHandleInner>);

impl BaoFileHandleWeak {
    /// Upgrade to a strong reference if possible.
    pub fn upgrade(&self) -> Option<BaoFileHandle> {
        self.0.upgrade().map(BaoFileHandle)
    }

    /// True if the handle is still live (has strong references)
    pub fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }
}

/// The inner part of a bao file handle.
#[derive(Debug)]
pub struct BaoFileHandleInner {
    pub(crate) storage: RwLock<Option<BaoFileStorage>>,
    hash: Hash,
}

/// A cheaply cloneable handle to a bao file, including the hash and the configuration.
#[derive(Debug, Clone, derive_more::Deref)]
pub struct BaoFileHandle(Arc<BaoFileHandleInner>);

/// A reader for a bao file, reading just the data.
#[derive(Debug)]
pub struct DataReader(BaoFileHandle);

impl ReadAt for DataReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.0.storage.read().unwrap();
        match guard.deref() {
            Some(BaoFileStorage::PartialMem(x)) => x.data.read_at(offset, buf),
            Some(BaoFileStorage::Partial(x)) => x.data.read_at(offset, buf),
            Some(BaoFileStorage::Complete(x)) => x.data.read_at(offset, buf),
            None => Err(io::Error::new(io::ErrorKind::Other, "handle poisoned")),
        }
    }
}

/// A reader for the outboard part of a bao file.
#[derive(Debug)]
pub struct OutboardReader(BaoFileHandle);

impl ReadAt for OutboardReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.0.storage.read().unwrap();
        match guard.deref() {
            Some(BaoFileStorage::Complete(x)) => x.outboard.read_at(offset, buf),
            Some(BaoFileStorage::PartialMem(x)) => x.outboard.read_at(offset, buf),
            Some(BaoFileStorage::Partial(x)) => x.outboard.read_at(offset, buf),
            None => Err(io::Error::new(io::ErrorKind::Other, "handle poisoned")),
        }
    }
}

impl BaoFileHandle {
    pub fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    /// Create a new bao file handle.
    ///
    /// This will create a new file handle with an empty memory storage.
    pub fn new_incomplete_mem(hash: Hash) -> Self {
        let storage = BaoFileStorage::incomplete_mem();
        Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
        }))
    }

    /// Create a new bao file handle with a partial file.
    pub fn new_incomplete_file(options: &PathOptions, hash: Hash) -> io::Result<Self> {
        let storage = PartialFileStorage {
            data: create_read_write(&options.owned_data_path(&hash))?,
            outboard: create_read_write(options.owned_outboard_path(&hash))?,
            sizes: create_read_write(options.owned_sizes_path(&hash))?,
            bitfield: Default::default(), // todo
        }
        .into();
        Ok(Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
        })))
    }

    /// Create a new complete bao file handle.
    pub fn new_complete(
        hash: Hash,
        data: MemOrFile<Bytes, (File, u64)>,
        outboard: MemOrFile<Bytes, (File, u64)>,
    ) -> Self {
        let storage = CompleteStorage { data, outboard }.into();
        Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
        }))
    }

    /// Complete the handle
    pub fn complete(
        &self,
        data: MemOrFile<Bytes, (File, u64)>,
        outboard: MemOrFile<Bytes, (File, u64)>,
    ) {
        let mut guard = self.storage.write().unwrap();
        let res = match guard.deref_mut() {
            Some(BaoFileStorage::Complete(_)) => None,
            Some(BaoFileStorage::PartialMem(entry)) => Some(&mut entry.bitfield),
            Some(BaoFileStorage::Partial(entry)) => Some(&mut entry.bitfield),
            None => None,
        };
        if let Some(bitfield) = res {
            bitfield.update(Bitfield {
                ranges: ChunkRanges::all(),
                size: data.size(),
            });
            *guard.deref_mut() = Some(BaoFileStorage::Complete(CompleteStorage { data, outboard }));
        }
    }

    pub fn add_observer(&self, observer: Observer<Bitfield>) {
        let mut guard = self.storage.write().unwrap();
        guard.deref_mut().as_mut().unwrap().add_observer(observer);
    }

    pub fn observer_dropped(&self) {
        let mut guard = self.storage.write().unwrap();
        guard.deref_mut().as_mut().unwrap().observer_dropped();
    }

    /// True if the file is complete.
    pub fn is_complete(&self) -> bool {
        matches!(
            self.storage.read().unwrap().deref(),
            Some(BaoFileStorage::Complete(_))
        )
    }

    /// An AsyncSliceReader for the data file.
    ///
    /// Caution: this is a reader for the unvalidated data file. Reading this
    /// can produce data that does not match the hash.
    pub fn data_reader(&self) -> DataReader {
        DataReader(self.clone())
    }

    /// An AsyncSliceReader for the outboard file.
    ///
    /// The outboard file is used to validate the data file. It is not guaranteed
    /// to be complete.
    pub fn outboard_reader(&self) -> OutboardReader {
        OutboardReader(self.clone())
    }

    /// The most precise known total size of the data file.
    pub fn current_size(&self) -> io::Result<u64> {
        match self.storage.read().unwrap().deref() {
            Some(BaoFileStorage::Complete(mem)) => Ok(mem.data_size()),
            Some(BaoFileStorage::PartialMem(mem)) => Ok(mem.size()),
            Some(BaoFileStorage::Partial(file)) => file.current_size(),
            None => Err(io::Error::new(io::ErrorKind::Other, "handle poisoned")),
        }
    }

    /// The outboard for the file.
    pub fn outboard(&self) -> io::Result<PreOrderOutboard<OutboardReader>> {
        let root = self.hash.into();
        let tree = BaoTree::new(self.current_size()?, IROH_BLOCK_SIZE);
        let outboard = self.outboard_reader();
        Ok(PreOrderOutboard {
            root,
            tree,
            data: outboard,
        })
    }

    /// The hash of the file.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Downgrade to a weak reference.
    pub fn downgrade(&self) -> BaoFileHandleWeak {
        BaoFileHandleWeak(Arc::downgrade(&self.0))
    }

    /// Write a batch and notify the db
    pub async fn write_batch(
        &self,
        size: NonZeroU64,
        batch: &[BaoContentItem],
        ranges: &ChunkRanges,
        ctx: &TaskContext,
    ) -> anyhow::Result<()> {
        let permit = ctx.db.sender.reserve().await?;
        let mut guard = self.storage.write().unwrap();
        if let Some(state) = guard.take() {
            match state.write_batch(size, batch, ranges, ctx, &self.hash, permit) {
                Ok(new_state) => {
                    *guard = Some(new_state);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "handle poisoned").into())
        }
    }
}

impl SizeInfo {
    /// Persist into a file where each chunk has its own slot.
    pub fn persist(&self, mut target: impl WriteAt) -> io::Result<()> {
        let size_offset = (self.offset >> IROH_BLOCK_SIZE.chunk_log()) << 3;
        target.write_all_at(size_offset, self.size.to_le_bytes().as_slice())?;
        Ok(())
    }

    /// Convert to a vec in slot format.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = Vec::new();
        self.persist(&mut res).expect("io error writing to vec");
        res
    }
}

impl PartialMemStorage {
    /// Persist the batch to disk, creating a FileBatch.
    fn persist(self, options: &PathOptions, hash: &Hash) -> io::Result<PartialFileStorage> {
        let mut data = create_read_write(&options.owned_data_path(hash))?;
        let mut outboard = create_read_write(options.owned_outboard_path(hash))?;
        let mut sizes = create_read_write(options.owned_sizes_path(hash))?;
        self.data.persist(&mut data)?;
        self.outboard.persist(&mut outboard)?;
        self.size.persist(&mut sizes)?;
        data.sync_all()?;
        outboard.sync_all()?;
        sizes.sync_all()?;
        Ok(PartialFileStorage {
            data,
            outboard,
            sizes,
            bitfield: self.bitfield,
        })
    }

    /// Get the parts data, outboard and sizes
    pub fn into_parts(self) -> (SparseMemFile, SparseMemFile, SizeInfo) {
        (self.data, self.outboard, self.size)
    }
}

// /// This is finally the thing for which we can implement BaoPairMut.
// ///
// /// It is a BaoFileHandle wrapped in an Option, so that we can take it out
// /// in the future.
// #[derive(Debug)]
// pub struct BaoFileWriter(Option<BaoFileHandle>);

// impl BaoBatchWriter for BaoFileWriter {
//     async fn write_batch(&mut self, size: u64, batch: Vec<BaoContentItem>) -> std::io::Result<()> {
//         let Some(handle) = self.0.take() else {
//             return Err(io::Error::new(io::ErrorKind::Other, "deferred batch busy"));
//         };
//         let (handle, change) = tokio::task::spawn_blocking(move || {
//             let change = handle.write_batch(size, &batch);
//             (handle, change)
//         })
//         .await
//         .expect("spawn_blocking failed");
//         match change? {
//             HandleChange::None => {}
//             HandleChange::MemToFile => {
//                 if let Some(cb) = handle.config.on_file_create.as_ref() {
//                     cb(&handle.hash)?;
//                 }
//             }
//         }
//         self.0 = Some(handle);
//         Ok(())
//     }

//     async fn sync(&mut self) -> io::Result<()> {
//         let Some(handle) = self.0.take() else {
//             return Err(io::Error::new(io::ErrorKind::Other, "deferred batch busy"));
//         };
//         let (handle, res) = tokio::task::spawn_blocking(move || {
//             let res = handle.storage.write().unwrap().sync_all();
//             (handle, res)
//         })
//         .await
//         .expect("spawn_blocking failed");
//         self.0 = Some(handle);
//         res
//     }
// }

// #[cfg(test)]
// pub mod test_support {
//     use std::{future::Future, io::Cursor, ops::Range};

//     use bao_tree::{
//         io::{
//             fsm::{ResponseDecoder, ResponseDecoderNext},
//             outboard::PostOrderMemOutboard,
//             round_up_to_chunks,
//             sync::encode_ranges_validated,
//         },
//         BlockSize, ChunkRanges,
//     };
//     use futures_lite::{Stream, StreamExt};
//     use iroh_io::AsyncStreamReader;
//     use rand::RngCore;
//     use range_collections::RangeSet2;

//     use super::*;
//     use crate::util::limited_range;

//     pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

//     /// Decode a response into a batch file writer.
//     pub async fn decode_response_into_batch<R, W>(
//         root: Hash,
//         block_size: BlockSize,
//         ranges: ChunkRanges,
//         mut encoded: R,
//         mut target: W,
//     ) -> io::Result<()>
//     where
//         R: AsyncStreamReader,
//         W: BaoBatchWriter,
//     {
//         let size = encoded.read::<8>().await?;
//         let size = u64::from_le_bytes(size);
//         let mut reading =
//             ResponseDecoder::new(root.into(), ranges, BaoTree::new(size, block_size), encoded);
//         let mut stack = Vec::new();
//         loop {
//             let item = match reading.next().await {
//                 ResponseDecoderNext::Done(_reader) => break,
//                 ResponseDecoderNext::More((next, item)) => {
//                     reading = next;
//                     item?
//                 }
//             };
//             match item {
//                 BaoContentItem::Parent(_) => {
//                     stack.push(item);
//                 }
//                 BaoContentItem::Leaf(_) => {
//                     // write a batch every time we see a leaf
//                     // the last item will be a leaf.
//                     stack.push(item);
//                     target.write_batch(size, std::mem::take(&mut stack)).await?;
//                 }
//             }
//         }
//         assert!(stack.is_empty(), "last item should be a leaf");
//         Ok(())
//     }

//     pub fn random_test_data(size: usize) -> Vec<u8> {
//         let mut rand = rand::thread_rng();
//         let mut res = vec![0u8; size];
//         rand.fill_bytes(&mut res);
//         res
//     }

//     /// Take some data and encode it
//     pub fn simulate_remote(data: &[u8]) -> (Hash, Cursor<Bytes>) {
//         let outboard = bao_tree::io::outboard::PostOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
//         let size = data.len() as u64;
//         let mut encoded = size.to_le_bytes().to_vec();
//         bao_tree::io::sync::encode_ranges_validated(
//             data,
//             &outboard,
//             &ChunkRanges::all(),
//             &mut encoded,
//         )
//         .unwrap();
//         let hash = outboard.root;
//         (hash.into(), Cursor::new(encoded.into()))
//     }

//     pub fn to_ranges(ranges: &[Range<u64>]) -> RangeSet2<u64> {
//         let mut range_set = RangeSet2::empty();
//         for range in ranges.as_ref().iter().cloned() {
//             range_set |= RangeSet2::from(range);
//         }
//         range_set
//     }

//     /// Simulate the send side, when asked to send bao encoded data for the given ranges.
//     pub fn make_wire_data(
//         data: &[u8],
//         ranges: impl AsRef<[Range<u64>]>,
//     ) -> (Hash, ChunkRanges, Vec<u8>) {
//         // compute a range set from the given ranges
//         let range_set = to_ranges(ranges.as_ref());
//         // round up to chunks
//         let chunk_ranges = round_up_to_chunks(&range_set);
//         // compute the outboard
//         let outboard = PostOrderMemOutboard::create(data, IROH_BLOCK_SIZE).flip();
//         let size = data.len() as u64;
//         let mut encoded = size.to_le_bytes().to_vec();
//         encode_ranges_validated(data, &outboard, &chunk_ranges, &mut encoded).unwrap();
//         (outboard.root.into(), chunk_ranges, encoded)
//     }

//     pub async fn validate(handle: &BaoFileHandle, original: &[u8], ranges: &[Range<u64>]) {
//         let mut r = handle.data_reader();
//         for range in ranges {
//             let start = range.start;
//             let len = (range.end - range.start).try_into().unwrap();
//             let data = &original[limited_range(start, len, original.len())];
//             let read = r.read_at(start, len).await.unwrap();
//             assert_eq!(data.len(), read.as_ref().len());
//             assert_eq!(data, read.as_ref());
//         }
//     }

//     /// Helper to simulate a slow request.
//     pub fn trickle(
//         data: &[u8],
//         mtu: usize,
//         delay: std::time::Duration,
//     ) -> impl Stream<Item = Bytes> {
//         let parts = data
//             .chunks(mtu)
//             .map(Bytes::copy_from_slice)
//             .collect::<Vec<_>>();
//         futures_lite::stream::iter(parts).then(move |part| async move {
//             tokio::time::sleep(delay).await;
//             part
//         })
//     }

//     pub async fn local<F>(f: F) -> F::Output
//     where
//         F: Future,
//     {
//         tokio::task::LocalSet::new().run_until(f).await
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::io::Write;

//     use bao_tree::{blake3, ChunkNum, ChunkRanges};
//     use futures_lite::StreamExt;
//     use iroh_io::TokioStreamReader;
//     use tests::test_support::{
//         decode_response_into_batch, local, make_wire_data, random_test_data, trickle, validate,
//     };
//     use tokio::task::JoinSet;

//     use super::*;
//     use crate::util::local_pool::LocalPool;

//     #[tokio::test]
//     async fn partial_downloads() {
//         local(async move {
//             let n = 1024 * 64u64;
//             let test_data = random_test_data(n as usize);
//             let temp_dir = tempfile::tempdir().unwrap();
//             let hash = blake3::hash(&test_data);
//             let handle = BaoFileHandle::incomplete_mem(
//                 Arc::new(BaoFileConfig::new(
//                     Arc::new(temp_dir.as_ref().to_owned()),
//                     1024 * 16,
//                     None,
//                 )),
//                 hash.into(),
//             );
//             let mut tasks = JoinSet::new();
//             for i in 1..3 {
//                 let file = handle.writer();
//                 let range = (i * (n / 4))..((i + 1) * (n / 4));
//                 println!("range: {:?}", range);
//                 let (hash, chunk_ranges, wire_data) = make_wire_data(&test_data, &[range]);
//                 let trickle = trickle(&wire_data, 1200, std::time::Duration::from_millis(10))
//                     .map(io::Result::Ok)
//                     .boxed();
//                 let trickle = TokioStreamReader::new(tokio_util::io::StreamReader::new(trickle));
//                 let _task = tasks.spawn_local(async move {
//                     decode_response_into_batch(hash, IROH_BLOCK_SIZE, chunk_ranges, trickle, file)
//                         .await
//                 });
//             }
//             while let Some(res) = tasks.join_next().await {
//                 res.unwrap().unwrap();
//             }
//             println!(
//                 "len {:?} {:?}",
//                 handle,
//                 handle.data_reader().size().await.unwrap()
//             );
//             #[allow(clippy::single_range_in_vec_init)]
//             let ranges = [1024 * 16..1024 * 48];
//             validate(&handle, &test_data, &ranges).await;

//             // let ranges =
//             // let full_chunks = bao_tree::io::full_chunk_groups();
//             let mut encoded = Vec::new();
//             let ob = handle.outboard().unwrap();
//             encoded
//                 .write_all(ob.tree.size().to_le_bytes().as_slice())
//                 .unwrap();
//             bao_tree::io::fsm::encode_ranges_validated(
//                 handle.data_reader(),
//                 ob,
//                 &ChunkRanges::from(ChunkNum(16)..ChunkNum(48)),
//                 encoded,
//             )
//             .await
//             .unwrap();
//         })
//         .await;
//     }

//     #[tokio::test]
//     async fn concurrent_downloads() {
//         let n = 1024 * 32u64;
//         let test_data = random_test_data(n as usize);
//         let temp_dir = tempfile::tempdir().unwrap();
//         let hash = blake3::hash(&test_data);
//         let handle = BaoFileHandle::incomplete_mem(
//             Arc::new(BaoFileConfig::new(
//                 Arc::new(temp_dir.as_ref().to_owned()),
//                 1024 * 16,
//                 None,
//             )),
//             hash.into(),
//         );
//         let local = LocalPool::default();
//         let mut tasks = Vec::new();
//         for i in 0..4 {
//             let file = handle.writer();
//             let range = (i * (n / 4))..((i + 1) * (n / 4));
//             println!("range: {:?}", range);
//             let (hash, chunk_ranges, wire_data) = make_wire_data(&test_data, &[range]);
//             let trickle = trickle(&wire_data, 1200, std::time::Duration::from_millis(10))
//                 .map(io::Result::Ok)
//                 .boxed();
//             let trickle = TokioStreamReader::new(tokio_util::io::StreamReader::new(trickle));
//             let task = local.spawn(move || async move {
//                 decode_response_into_batch(hash, IROH_BLOCK_SIZE, chunk_ranges, trickle, file).await
//             });
//             tasks.push(task);
//         }
//         for task in tasks {
//             task.await.unwrap().unwrap();
//         }
//         println!(
//             "len {:?} {:?}",
//             handle,
//             handle.data_reader().size().await.unwrap()
//         );
//         #[allow(clippy::single_range_in_vec_init)]
//         let ranges = [0..n];
//         validate(&handle, &test_data, &ranges).await;

//         let mut encoded = Vec::new();
//         let ob = handle.outboard().unwrap();
//         encoded
//             .write_all(ob.tree.size().to_le_bytes().as_slice())
//             .unwrap();
//         bao_tree::io::fsm::encode_ranges_validated(
//             handle.data_reader(),
//             ob,
//             &ChunkRanges::all(),
//             encoded,
//         )
//         .await
//         .unwrap();
//     }

//     #[tokio::test]
//     async fn stay_in_mem() {
//         let test_data = random_test_data(1024 * 17);
//         #[allow(clippy::single_range_in_vec_init)]
//         let ranges = [0..test_data.len().try_into().unwrap()];
//         let (hash, chunk_ranges, wire_data) = make_wire_data(&test_data, &ranges);
//         println!("file len is {:?}", chunk_ranges);
//         let temp_dir = tempfile::tempdir().unwrap();
//         let handle = BaoFileHandle::incomplete_mem(
//             Arc::new(BaoFileConfig::new(
//                 Arc::new(temp_dir.as_ref().to_owned()),
//                 1024 * 16,
//                 None,
//             )),
//             hash,
//         );
//         decode_response_into_batch(
//             hash,
//             IROH_BLOCK_SIZE,
//             chunk_ranges,
//             wire_data.as_slice(),
//             handle.writer(),
//         )
//         .await
//         .unwrap();
//         validate(&handle, &test_data, &ranges).await;

//         let mut encoded = Vec::new();
//         let ob = handle.outboard().unwrap();
//         encoded
//             .write_all(ob.tree.size().to_le_bytes().as_slice())
//             .unwrap();
//         bao_tree::io::fsm::encode_ranges_validated(
//             handle.data_reader(),
//             ob,
//             &ChunkRanges::all(),
//             encoded,
//         )
//         .await
//         .unwrap();
//         println!("{:?}", handle);
//     }
// }
