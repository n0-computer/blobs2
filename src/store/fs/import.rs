//! The entire logic for importing data from three different sources: bytes, byte stream, and file path.
//!
//! For bytes, the size is known in advance. But they might still have to be persisted to disk if they are too large.
//! E.g. you add a 1GB bytes to the store, you still want this to end up on disk.
//!
//! For byte streams, the size is not known in advance.
//!
//! For file paths, the size is known in advance. This is also the only case where we might reference the data instead
//! of copying it.
//!
//! The various ..._task fns return an `Option<ImportEntry>`. If import fails for whatever reason, the error goes
//! to the requester, and the task returns None.
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, Write},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use bao_tree::{
    io::{
        outboard::{PreOrderMemOutboard, PreOrderOutboard},
        sync::WriteAt,
    },
    BaoTree,
};
use bytes::Bytes;
use n0_future::{stream, Stream, StreamExt};
use quic_rpc::channel::{none::NoReceiver, spsc};
use smallvec::SmallVec;
use tracing::{instrument, trace};

use super::{meta::raw_outboard_size, options::Options, TaskContext};
use crate::{
    store::{
        proto::{
            HashSpecific, ImportByteStream, ImportByteStreamMsg, ImportBytes, ImportBytesMsg,
            ImportMode, ImportPath, ImportPathMsg, ImportProgress,
        },
        util::{MemOrFile, ProgressReader, QuicRpcSenderProgressExt},
        IROH_BLOCK_SIZE,
    },
    BlobFormat, Hash,
};

/// An import source.
///
/// It must provide a way to read the data synchronously, as well as the size
/// and the file location.
///
/// This serves as an intermediate result between copying and outboard computation.
#[derive(derive_more::Debug)]
pub enum ImportSource {
    TempFile(PathBuf, File, u64),
    External(PathBuf, File, u64),
    Memory(#[debug(skip)] Bytes),
}

impl ImportSource {
    pub fn fmt_short(&self) -> String {
        match self {
            Self::TempFile(path, _, _) => format!("TempFile({})", path.display()),
            Self::External(path, _, _) => format!("External({})", path.display()),
            Self::Memory(data) => format!("Memory({})", data.len()),
        }
    }

    fn is_mem(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// A reader for the import source.
    fn read(&self) -> MemOrFile<std::io::Cursor<&[u8]>, &File> {
        match self {
            Self::TempFile(_, file, _) => MemOrFile::File(file),
            Self::External(_, file, _) => MemOrFile::File(file),
            Self::Memory(data) => MemOrFile::Mem(std::io::Cursor::new(data.as_ref())),
        }
    }

    /// The size of the import source.
    fn size(&self) -> u64 {
        match self {
            Self::TempFile(_, _, size) => *size,
            Self::External(_, _, size) => *size,
            Self::Memory(data) => data.len() as u64,
        }
    }
}

/// An import entry.
///
/// This is the final result of an import operation. It gets passed to the store
/// for integration.
///
/// The store can assume that the outboard, if on disk, is in a location where
/// it can be moved to the final location (basically it needs to be on the same device).
pub struct ImportEntry {
    pub hash: Hash,
    pub format: BlobFormat,
    pub source: ImportSource,
    pub outboard: MemOrFile<Bytes, PathBuf>,
}

pub struct ImportEntryMsg {
    pub inner: ImportEntry,
    pub tx: spsc::Sender<ImportProgress>,
}

impl Deref for ImportEntryMsg {
    type Target = ImportEntry;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl HashSpecific for ImportEntryMsg {
    fn hash(&self) -> crate::Hash {
        self.hash
    }
}

impl fmt::Debug for ImportEntryMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportEntry")
            .field("hash", &self.hash)
            .field("source", &self.source.fmt_short())
            .field("outboard", &self.outboard.fmt_short())
            .finish()
    }
}

impl ImportEntryMsg {
    /// True if both data and outboard are in memory.
    pub fn is_mem(&self) -> bool {
        self.source.is_mem() && self.outboard.is_mem()
    }
}

/// Start a task to import from a [`Bytes`] in memory.
#[instrument(skip_all, fields(data = cmd.inner.data.len()))]
pub async fn import_bytes(cmd: ImportBytesMsg, ctx: Arc<TaskContext>) {
    let size = cmd.inner.data.len() as u64;
    if ctx.options.is_inlined_all(size) {
        import_bytes_tiny_outer(cmd, ctx).await;
    } else {
        let cmd = ImportByteStreamMsg {
            inner: ImportByteStream {
                format: cmd.inner.format,
                data: vec![cmd.inner.data],
            },
            tx: cmd.tx,
            rx: NoReceiver,
        };
        import_byte_stream_outer(cmd, ctx).await;
    }
}

async fn import_bytes_tiny_outer(mut cmd: ImportBytesMsg, ctx: Arc<TaskContext>) {
    match import_bytes_tiny_impl(cmd.inner, &mut cmd.tx).await {
        Ok(entry) => {
            let entry = ImportEntryMsg {
                inner: entry,
                tx: cmd.tx,
            };
            ctx.internal_cmd_tx.send(entry.into()).await.ok();
        }
        Err(cause) => {
            cmd.tx.send(cause.into()).await.ok();
        }
    }
}

async fn import_bytes_tiny_impl(
    cmd: ImportBytes,
    tx: &mut spsc::Sender<ImportProgress>,
) -> io::Result<ImportEntry> {
    let size = cmd.data.len() as u64;
    // send the required progress events
    // ImportProgress::Done will be sent when finishing the import!
    tx.send(ImportProgress::Size { size })
        .await
        .map_err(|_e| io::Error::other("error"))?;
    tx.send(ImportProgress::CopyDone)
        .await
        .map_err(|_e| io::Error::other("error"))?;
    Ok(if raw_outboard_size(size) == 0 {
        // the thing is so small that it does not even need an outboard

        ImportEntry {
            hash: Hash::new(&cmd.data),
            format: cmd.format,
            source: ImportSource::Memory(cmd.data),
            outboard: MemOrFile::empty(),
        }
    } else {
        // we still know that computing the outboard will be super fast
        let outboard = PreOrderMemOutboard::create(&cmd.data, IROH_BLOCK_SIZE);
        ImportEntry {
            hash: outboard.root.into(),
            format: cmd.format,
            source: ImportSource::Memory(cmd.data),
            outboard: MemOrFile::Mem(Bytes::from(outboard.data)),
        }
    })
}

#[instrument(skip_all)]
pub async fn import_byte_stream(cmd: ImportByteStreamMsg, ctx: Arc<TaskContext>) {
    import_byte_stream_outer(cmd, ctx).await;
}

pub async fn import_byte_stream_outer(mut cmd: ImportByteStreamMsg, ctx: Arc<TaskContext>) {
    match import_byte_stream_impl(cmd.inner, &mut cmd.tx, ctx.options.clone()).await {
        Ok(entry) => {
            let entry = ImportEntryMsg {
                inner: entry,
                tx: cmd.tx,
            };
            ctx.internal_cmd_tx.send(entry.into()).await.ok();
        }
        Err(cause) => {
            cmd.tx.send(cause.into()).await.ok();
        }
    }
}

async fn import_byte_stream_impl(
    cmd: ImportByteStream,
    tx: &mut spsc::Sender<ImportProgress>,
    options: Arc<Options>,
) -> io::Result<ImportEntry> {
    let ImportByteStream { format, data } = cmd;
    let data = futures_lite::stream::iter(data).map(io::Result::Ok);
    let import_source = get_import_source(data, tx, &options).await?;
    tx.send(ImportProgress::Size {
        size: import_source.size(),
    })
    .await
    .map_err(|_e| io::Error::other("error"))?;
    tx.send(ImportProgress::CopyDone)
        .await
        .map_err(|_e| io::Error::other("error"))?;
    compute_outboard(import_source, format, options, tx).await
}

async fn get_import_source(
    stream: impl Stream<Item = io::Result<Bytes>> + Unpin,
    tx: &mut spsc::Sender<ImportProgress>,
    options: &Options,
) -> io::Result<ImportSource> {
    let mut stream = stream.fuse();
    let mut peek = SmallVec::<[_; 2]>::new();
    let Some(first) = stream.next().await.transpose()? else {
        return Ok(ImportSource::Memory(Bytes::new()));
    };
    match stream.next().await.transpose()? {
        Some(second) => {
            peek.push(Ok(first));
            peek.push(Ok(second));
        }
        None => {
            let size = first.len() as u64;
            if options.is_inlined_data(size) {
                return Ok(ImportSource::Memory(first));
            }
            peek.push(Ok(first));
        }
    };
    // todo: if both first and second are giant, we might want to write them to disk immediately
    let mut stream = stream::iter(peek).chain(stream);
    let mut size = 0;
    let mut data = Vec::new();
    let mut disk = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        size += chunk.len() as u64;
        if size > options.inline.max_data_inlined {
            let temp_path = options.path.temp_file_name();
            trace!("writing to temp file: {:?}", temp_path);
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            file.write_all(&data)?;
            file.write_all(&chunk)?;
            data.clear();
            disk = Some((file, temp_path));
            break;
        } else {
            data.extend_from_slice(&chunk);
        }
        // todo: don't send progress for every chunk if the chunks are small?
        tx.send_progress(ImportProgress::CopyProgress { offset: size })
            .await
            .map_err(|_e| io::Error::other("error"))?;
    }
    Ok(if let Some((mut file, temp_path)) = disk {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)?;
            size += chunk.len() as u64;
            tx.send(ImportProgress::CopyProgress { offset: size })
                .await
                .map_err(|_e| io::Error::other("error"))?;
        }
        ImportSource::TempFile(temp_path, file, size)
    } else {
        ImportSource::Memory(data.into())
    })
}

async fn compute_outboard(
    source: ImportSource,
    format: BlobFormat,
    options: Arc<Options>,
    _tx: &mut spsc::Sender<ImportProgress>,
) -> io::Result<ImportEntry> {
    let size = source.size();
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let root = bao_tree::blake3::Hash::from_bytes([0; 32]);
    let outboard_size = raw_outboard_size(size);
    let send_progress = |offset| {
        // tx.send_progress(ImportProgress::OutboardProgress { offset })
        //     .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "receiver dropped"))
        Ok(())
    };
    let mut data = source.read();
    data.rewind()?;
    let (hash, outboard) = if outboard_size > options.inline.max_outboard_inlined {
        // outboard will eventually be stored as a file, so compute it directly to a file
        // we don't know the hash yet, so we need to create a temp file
        let outboard_path = options.path.temp_file_name();
        trace!("Creating outboard file in {}", outboard_path.display());
        // we don't need to read from this file!
        let mut outboard_file = File::create(&outboard_path)?;
        let mut outboard = PreOrderOutboard {
            tree,
            root,
            data: &mut outboard_file,
        };
        init_outboard(data, &mut outboard, send_progress)?;
        (outboard.root, MemOrFile::File(outboard_path))
    } else {
        // outboard will be stored in memory, so compute it to a memory buffer
        trace!("Creating outboard in memory");
        let mut outboard_file: Vec<u8> = Vec::new();
        let mut outboard = PreOrderOutboard {
            tree,
            root,
            data: &mut outboard_file,
        };
        init_outboard(data, &mut outboard, send_progress)?;
        (outboard.root, MemOrFile::Mem(Bytes::from(outboard_file)))
    };
    Ok(ImportEntry {
        hash: hash.into(),
        format,
        source,
        outboard,
    })
}

pub(crate) fn init_outboard<R: Read + Seek, W: WriteAt>(
    data: R,
    outboard: &mut PreOrderOutboard<W>,
    progress: impl Fn(u64) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use bao_tree::io::sync::CreateOutboard;

    // wrap the reader in a progress reader, so we can report progress.
    let reader = ProgressReader::new(data, progress);
    // wrap the reader in a buffered reader, so we read in large chunks
    // this reduces the number of io ops and also the number of progress reports
    let buf_size = usize::try_from(outboard.tree.size())
        .unwrap_or(usize::MAX)
        .min(1024 * 1024);
    let reader = BufReader::with_capacity(buf_size, reader);

    outboard.init_from(reader)?;
    Ok(())
}

#[instrument(skip_all, fields(path = %cmd.inner.path.display()))]
pub async fn import_path(mut cmd: ImportPathMsg, context: Arc<TaskContext>) {
    match import_path_impl(cmd.inner, &mut cmd.tx, context.options.clone()).await {
        Ok(inner) => {
            let res = ImportEntryMsg { inner, tx: cmd.tx };
            context.internal_cmd_tx.send(res.into()).await.ok();
        }
        Err(cause) => {
            cmd.tx.send(cause.into()).await.ok();
        }
    }
}

async fn import_path_impl(
    cmd: ImportPath,
    tx: &mut spsc::Sender<ImportProgress>,
    options: Arc<Options>,
) -> io::Result<ImportEntry> {
    let ImportPath { path, mode, format } = cmd;
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path must be absolute",
        ));
    }
    if !path.is_file() && !path.is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a file or symlink",
        ));
    }

    let size = path.metadata()?.len();
    tx.send_progress(ImportProgress::Size { size })
        .await
        .map_err(|_e| io::Error::other("error"))?;
    let import_source = if size <= options.inline.max_data_inlined {
        let data = std::fs::read(path)?;
        tx.send_progress(ImportProgress::CopyDone)
            .await
            .map_err(|_e| io::Error::other("error"))?;
        ImportSource::Memory(data.into())
    } else if mode == ImportMode::TryReference {
        // reference where it is. We are going to need the file handle to
        // compute the outboard, so open it here. If this fails, the import
        // can't proceed.
        let file = OpenOptions::new().read(true).open(&path)?;
        ImportSource::External(path, file, size)
    } else {
        let temp_path = options.path.temp_file_name();
        // todo: if reflink works, we don't need progress.
        // But if it does not, it might take a while and we won't get progress.
        if reflink_copy::reflink_or_copy(&path, &temp_path)?.is_none() {
            trace!("reflinked {} to {}", path.display(), temp_path.display());
        } else {
            trace!("copied {} to {}", path.display(), temp_path.display());
        }
        // copy from path to temp_path
        let file = OpenOptions::new().read(true).open(&temp_path)?;
        tx.send_progress(ImportProgress::CopyDone)
            .await
            .map_err(|_| io::Error::other("error"))?;
        ImportSource::TempFile(temp_path, file, size)
    };
    compute_outboard(import_source, format, options, tx).await
}

#[cfg(test)]
mod tests {

    use bao_tree::io::outboard::PreOrderMemOutboard;
    use n0_future::stream;
    use quic_rpc::RpcMessage;
    use testresult::TestResult;

    use super::*;
    use crate::store::{
        fs::options::{InlineOptions, PathOptions},
        proto::{BoxedByteStream, ImportMode},
        BlobFormat,
    };

    async fn drain<T: RpcMessage>(mut recv: spsc::Receiver<T>) -> TestResult<Vec<T>> {
        let mut res = Vec::new();
        while let Some(item) = recv.recv().await? {
            res.push(item);
        }
        Ok(res)
    }

    fn assert_expected_progress(progress: &[ImportProgress]) {
        assert!(progress
            .iter()
            .any(|x| matches!(&x, ImportProgress::Size { .. })));
        assert!(progress
            .iter()
            .any(|x| matches!(&x, ImportProgress::CopyDone)));
    }

    fn chunk_bytes(data: Bytes, chunk_size: usize) -> impl Iterator<Item = Bytes> {
        assert!(chunk_size > 0, "Chunk size must be positive");
        (0..data.len())
            .step_by(chunk_size)
            .map(move |i| data.slice(i..std::cmp::min(i + chunk_size, data.len())))
    }

    async fn test_import_byte_stream_task(data: Bytes, options: Arc<Options>) -> TestResult<()> {
        let stream: BoxedByteStream =
            Box::pin(stream::iter(chunk_bytes(data.clone(), 999).map(Ok)));
        let expected_outboard = PreOrderMemOutboard::create(data.as_ref(), IROH_BLOCK_SIZE);
        // make the channel absurdly large, so we don't have to drain it
        let (mut tx, rx) = spsc::channel(1024 * 1024);
        let data = stream.collect::<Vec<_>>().await;
        let data = data.into_iter().collect::<io::Result<Vec<_>>>()?;
        let cmd = ImportByteStream {
            data,
            format: BlobFormat::Raw,
        };
        let res = import_byte_stream_impl(cmd, &mut tx, options).await;
        let Ok(res) = res else {
            panic!("import failed");
        };
        let ImportEntry { outboard, .. } = res;
        drop(tx);
        let actual_outboard = match &outboard {
            MemOrFile::Mem(data) => data.clone(),
            MemOrFile::File(path) => std::fs::read(path)?.into(),
        };
        assert_eq!(expected_outboard.data.as_slice(), actual_outboard.as_ref());
        let progress = drain(rx).await?;
        assert_expected_progress(&progress);
        Ok(())
    }

    async fn test_import_file_task(data: Bytes, options: Arc<Options>) -> TestResult<()> {
        let path = options.path.temp_file_name();
        std::fs::write(&path, &data)?;
        let expected_outboard = PreOrderMemOutboard::create(data.as_ref(), IROH_BLOCK_SIZE);
        // make the channel absurdly large, so we don't have to drain it
        let (mut tx, rx) = spsc::channel(1024 * 1024);
        let cmd = ImportPath {
            path,
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
        };
        let res = import_path_impl(cmd, &mut tx, options).await;
        let Ok(res) = res else {
            panic!("import failed");
        };
        let ImportEntry { outboard, .. } = res;
        drop(tx);
        let actual_outboard = match &outboard {
            MemOrFile::Mem(data) => data.clone(),
            MemOrFile::File(path) => std::fs::read(path)?.into(),
        };
        assert_eq!(expected_outboard.data.as_slice(), actual_outboard.as_ref());
        let progress = drain(rx).await?;
        assert_expected_progress(&progress);
        Ok(())
    }

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("data"))?;
        std::fs::create_dir_all(dir.path().join("temp"))?;
        let options = Arc::new(Options {
            inline: InlineOptions {
                max_data_inlined: 1024 * 16,
                max_outboard_inlined: 1024 * 16,
            },
            batch: Default::default(),
            path: PathOptions::new(dir.path()),
        });
        // test different sizes, below, at, and above the inline threshold
        let sizes = [
            0,               // empty, no outboard
            1024,            // data in mem, no outboard
            1024 * 16 - 1,   // data in mem, no outboard
            1024 * 16,       // data in mem, no outboard
            1024 * 16 + 1,   // data in file, outboard in mem
            1024 * 1024,     // data in file, outboard in mem
            1024 * 1024 * 8, // data in file, outboard in file
        ];
        for size in sizes {
            let data = Bytes::from(vec![0; size]);
            test_import_byte_stream_task(data.clone(), options.clone()).await?;
            test_import_file_task(data, options.clone()).await?;
        }
        Ok(())
    }
}
