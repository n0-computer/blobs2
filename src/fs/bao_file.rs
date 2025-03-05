use core::fmt;
use std::{
    fs::{File, OpenOptions},
    io,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, RwLock, Weak},
};

use bao_tree::{
    blake3,
    io::{
        fsm::BaoContentItem,
        mixed::ReadBytesAt,
        outboard::PreOrderOutboard,
        sync::{ReadAt, WriteAt},
    },
    BaoTree, ChunkRanges,
};
use bytes::{Bytes, BytesMut};
use derive_more::Debug;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    meta::{self, Update},
    options::{Options, PathOptions},
};
use crate::{
    bitfield::Bitfield,
    fs::{meta::raw_outboard_size, TaskContext},
    hash::DD,
    mem::{PartialMemStorage, SizeInfo},
    util::{
        observer::{Observable, Observer},
        read_checksummed_and_truncate, write_checksummed, FixedSize, MemOrFile, SparseMemFile,
        ValueOrPoisioned,
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
#[derive(Default)]
pub struct CompleteStorage {
    /// data part, which can be in memory or on disk.
    pub data: MemOrFile<Bytes, FixedSize<File>>,
    /// outboard part, which can be in memory or on disk.
    pub outboard: MemOrFile<Bytes, File>,
}

impl fmt::Debug for CompleteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteStorage")
            .field("data", &DD::from(self.data.fmt_short()))
            .field("outboard", &DD::from(self.outboard.fmt_short()))
            .finish()
    }
}

impl CompleteStorage {
    pub fn add_observer(&mut self, mut observer: Observer<Bitfield>) {
        let bitfield = Bitfield::complete(self.data.size());
        observer.send(bitfield).ok();
    }

    /// The size of the data file.
    pub fn size(&self) -> u64 {
        match &self.data {
            MemOrFile::Mem(mem) => mem.len() as u64,
            MemOrFile::File(file) => file.size,
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
    fn sync_all(&self, bitfield_path: &Path) -> io::Result<()> {
        //self.data.sync_all().ok();
        //self.outboard.sync_all().ok();
        //self.sizes.sync_all().ok();
        write_checksummed(bitfield_path, self.bitfield.state()).ok();
        Ok(())
    }

    fn load(hash: &Hash, options: &PathOptions) -> io::Result<Self> {
        let bitfield_path = options.bitfield_path(&hash);
        let data = create_read_write(&options.owned_data_path(&hash))?;
        let outboard = create_read_write(options.owned_outboard_path(&hash))?;
        let sizes = create_read_write(options.owned_sizes_path(&hash))?;
        let bitfield = match read_checksummed_and_truncate(&bitfield_path) {
            Ok(bitfield) => bitfield,
            Err(cause) => {
                warn!(
                    "failed to read bitfield for {} at {}: {:?}",
                    hash.to_hex(),
                    bitfield_path.display(),
                    cause
                );
                trace!("reconstructing bitfield from outboard");
                let size = read_size(&sizes).ok().unwrap_or_default();
                let outboard = PreOrderOutboard {
                    data: &outboard,
                    tree: BaoTree::new(size, IROH_BLOCK_SIZE),
                    root: blake3::Hash::from(*hash),
                };
                let mut ranges = ChunkRanges::all();
                for range in bao_tree::io::sync::valid_ranges(outboard, &data, &ChunkRanges::all())
                {
                    if let Ok(range) = range {
                        ranges |= ChunkRanges::from(range);
                    }
                }
                trace!("reconstructed range is {:?}", ranges);
                Bitfield::new(ranges, size)
            }
        };
        let bitfield = Observable::new(bitfield);
        Ok(Self {
            data,
            outboard,
            sizes,
            bitfield,
        })
    }

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
                MemOrFile::File(FixedSize::new(self.data, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if ctx.options.is_inlined_outboard(outboard_size) {
            if outboard_size == 0 {
                (MemOrFile::Mem(Bytes::new()), OutboardLocation::NotNeeded)
            } else {
                let outboard = read_to_end(&self.outboard, 0, outboard_size as usize)?;
                trace!("read outboard from file: {:?}", outboard.len());
                (
                    MemOrFile::Mem(outboard.clone()),
                    OutboardLocation::Inline(outboard),
                )
            }
        } else {
            (MemOrFile::File(self.outboard), OutboardLocation::Owned)
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

    fn current_size(&self) -> io::Result<u64> {
        read_size(&self.sizes)
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

fn read_size(size_file: &File) -> io::Result<u64> {
    let len = size_file.metadata()?.len();
    if len < 8 {
        Ok(0)
    } else {
        let len = len & !7;
        let mut buf = [0u8; 8];
        size_file.read_exact_at(len - 8, &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

/// The storage for a bao file. This can be either in memory or on disk.
#[derive(derive_more::From)]
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

impl fmt::Debug for BaoFileStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaoFileStorage::PartialMem(x) => x.fmt(f),
            BaoFileStorage::Partial(x) => x.fmt(f),
            BaoFileStorage::Complete(x) => x.fmt(f),
        }
    }
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
        let size = self.current_size();
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if ctx.options.is_inlined_data(size) {
            let data: Bytes = self.data.to_vec().into();
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            let data_path = ctx.options.path.owned_data_path(hash);
            let mut data_file = create_read_write(&data_path)?;
            self.data.persist(&mut data_file)?;
            (
                MemOrFile::File(FixedSize::new(data_file, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if ctx.options.is_inlined_outboard(outboard_size) {
            if outboard_size > 0 {
                let outboard: Bytes = self.outboard.to_vec().into();
                (
                    MemOrFile::Mem(outboard.clone()),
                    OutboardLocation::Inline(outboard),
                )
            } else {
                (MemOrFile::Mem(Bytes::new()), OutboardLocation::NotNeeded)
            }
        } else {
            let outboard_path = ctx.options.path.owned_outboard_path(hash);
            let mut outboard_file = create_read_write(&outboard_path)?;
            self.outboard.persist(&mut outboard_file)?;
            let outboard_location = if outboard_size == 0 {
                OutboardLocation::NotNeeded
            } else {
                OutboardLocation::Owned
            };
            (MemOrFile::File(outboard_file), outboard_location)
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
    pub fn partial_mem() -> Self {
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
pub struct BaoFileHandleInner {
    pub(crate) storage: RwLock<Option<BaoFileStorage>>,
    hash: Hash,
    options: Option<Arc<Options>>,
}

impl fmt::Debug for BaoFileHandleInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.storage.read().unwrap();
        let storage = ValueOrPoisioned(guard.deref().as_ref());
        f.debug_struct("BaoFileHandleInner")
            .field("hash", &DD::from(self.hash))
            .field("storage", &storage)
            .finish_non_exhaustive()
    }
}

/// A cheaply cloneable handle to a bao file, including the hash and the configuration.
#[derive(Debug, Clone, derive_more::Deref)]
pub struct BaoFileHandle(Arc<BaoFileHandleInner>);

impl Drop for BaoFileHandleInner {
    fn drop(&mut self) {
        if let Ok(Some(storage)) = self.storage.get_mut() {
            if let BaoFileStorage::Partial(fs) = &storage {
                let options = self.options.as_ref().unwrap();
                let path = options.path.bitfield_path(&self.hash);
                info!(
                    "writing bitfield for hash {} to {}",
                    self.hash.to_hex(),
                    path.display()
                );
                fs.sync_all(&path).ok();
            }
        }
    }
}

/// A reader for a bao file, reading just the data.
#[derive(Debug)]
pub struct DataReader(BaoFileHandle);

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        let guard = self.0.storage.read().unwrap();
        match guard.deref() {
            Some(BaoFileStorage::PartialMem(x)) => x.data.read_bytes_at(offset, size),
            Some(BaoFileStorage::Partial(x)) => x.data.read_bytes_at(offset, size),
            Some(BaoFileStorage::Complete(x)) => x.data.read_bytes_at(offset, size),
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
    pub fn new_partial_mem(hash: Hash, options: Arc<Options>) -> Self {
        let storage = BaoFileStorage::partial_mem();
        Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
            options: Some(options),
        }))
    }

    /// Create a new bao file handle with a partial file.
    pub fn new_partial_file(hash: Hash, options: Arc<Options>) -> io::Result<Self> {
        let storage = PartialFileStorage::load(&hash, &options.path)?.into();
        Ok(Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
            options: Some(options),
        })))
    }

    /// Create a new complete bao file handle.
    pub fn new_complete(
        hash: Hash,
        data: MemOrFile<Bytes, FixedSize<File>>,
        outboard: MemOrFile<Bytes, File>,
    ) -> Self {
        let storage = CompleteStorage { data, outboard }.into();
        Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(Some(storage)),
            hash,
            options: None,
        }))
    }

    /// Complete the handle
    pub fn complete(
        &self,
        data: MemOrFile<Bytes, FixedSize<File>>,
        outboard: MemOrFile<Bytes, File>,
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
            Some(BaoFileStorage::Complete(mem)) => Ok(mem.size()),
            Some(BaoFileStorage::PartialMem(mem)) => Ok(mem.current_size()),
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
        trace!(
            "write_batch size={} ranges={:?} batch={}",
            size,
            ranges,
            batch.len()
        );
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
