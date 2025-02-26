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
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, Write},
    path::PathBuf,
    sync::Arc,
};

use bao_tree::{
    blake3::Hash,
    io::{outboard::PreOrderOutboard, sync::WriteAt},
    BaoTree,
};
use bytes::Bytes;
use n0_future::StreamExt;
use tokio::{sync::mpsc, task::yield_now};
use tracing::trace;

use super::{meta::raw_outboard_size, options::Options};
use crate::{
    proto::{ImportByteStream, ImportBytes, ImportPath, ImportProgress},
    util::{MemOrFile, ProgressReader, SenderProgressExt},
    IROH_BLOCK_SIZE,
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
#[derive(Debug)]
pub struct ImportEntry {
    pub hash: Hash,
    pub source: ImportSource,
    pub outboard: MemOrFile<Bytes, PathBuf>,
    pub out: mpsc::OwnedPermit<ImportProgress>,
}

/// Start a task to import from a [`Bytes`] in memory.
pub async fn import_bytes_task(cmd: ImportBytes, options: Arc<Options>) -> Option<ImportEntry> {
    let cmd = ImportByteStream {
        data: Box::pin(n0_future::stream::once(Ok(cmd.data))),
        out: cmd.out,
    };
    import_byte_stream_task(cmd, options).await
}

pub async fn import_byte_stream_task(
    cmd: ImportByteStream,
    options: Arc<Options>,
) -> Option<ImportEntry> {
    let send = cmd.out.clone().reserve_owned().await.ok()?;
    match import_byte_stream_impl(cmd, options).await {
        Ok(entry) => Some(entry),
        Err(cause) => {
            send.send(ImportProgress::Error { cause });
            None
        }
    }
}

async fn import_byte_stream_impl(
    cmd: ImportByteStream,
    options: Arc<Options>,
) -> anyhow::Result<ImportEntry> {
    let ImportByteStream { data, out } = cmd;
    let mut stream = data.fuse();
    let mut size = 0;
    let mut data = Vec::new();
    let mut disk = None;
    // todo: for the case where the stream has just 1 element below the inline threshold,
    // we should just return the bytes directly as a memory source
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
        out.send_progress(ImportProgress::CopyProgress { offset: size })?;
    }
    let import_source = if let Some((mut file, temp_path)) = disk {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)?;
            size += chunk.len() as u64;
            out.send(ImportProgress::CopyProgress { offset: size })
                .await?;
        }
        ImportSource::TempFile(temp_path, file, size)
    } else {
        ImportSource::Memory(data.into())
    };
    out.send(ImportProgress::Size { size }).await?;
    out.send(ImportProgress::CopyDone).await?;
    compute_outboard(import_source, options, out).await
}

async fn compute_outboard(
    source: ImportSource,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let size = source.size();
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let root = bao_tree::blake3::Hash::from_bytes([0; 32]);
    let outboard_size = raw_outboard_size(size);
    let out2 = out.clone();
    let send_progress = move |offset| {
        out2.send_progress(ImportProgress::OutboardProgress { offset })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "receiver dropped"))
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
        source,
        outboard,
        out: out.reserve_owned().await?,
    })
}

pub(crate) fn init_outboard<R: Read + Seek, W: WriteAt>(
    data: R,
    outboard: &mut PreOrderOutboard<W>,
    progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
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

pub async fn import_path_task(cmd: ImportPath, options: Arc<Options>) -> Option<ImportEntry> {
    let send = cmd.out.clone().reserve_owned().await.ok()?;
    match import_path_impl(cmd, options).await {
        Ok(res) => Some(res),
        Err(cause) => {
            send.send(ImportProgress::Error { cause });
            None
        }
    }
}

async fn import_path_impl(cmd: ImportPath, options: Arc<Options>) -> anyhow::Result<ImportEntry> {
    let ImportPath { path, out, .. } = cmd;
    if !path.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "path must be absolute").into());
    }
    if !path.is_file() && !path.is_symlink() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "path is not a file or symlink").into(),
        );
    }

    let size = path.metadata()?.len();
    out.send_progress(ImportProgress::Size { size })?;
    let import_source = if size <= options.inline.max_data_inlined {
        let data = std::fs::read(path)?;
        out.send_progress(ImportProgress::CopyDone)?;
        ImportSource::Memory(data.into())
    } else {
        let temp_path = options.path.temp_file_name();
        // copy from path to temp_path in increments of 64k and send progress
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;
        let mut source = File::open(&path)?;
        let mut buffer = [0u8; 64 * 1024];
        let mut offset = 0;
        while let Ok(n) = source.read(&mut buffer) {
            if n == 0 {
                break;
            }
            file.write_all(&buffer[..n])?;
            offset += n as u64;
            out.send_progress(ImportProgress::CopyProgress { offset })?;
            yield_now().await;
        }
        out.send_progress(ImportProgress::CopyDone)?;
        ImportSource::TempFile(temp_path, file, size)
    };
    compute_outboard(import_source, options, out).await
}

#[cfg(test)]
mod tests {

    use bao_tree::io::outboard::PreOrderMemOutboard;
    use n0_future::stream;
    use testresult::TestResult;

    use super::*;
    use crate::{
        fs::options::{InlineOptions, PathOptions},
        proto::{BoxedByteStream, ImportMode},
        BlobFormat,
    };

    async fn drain<T>(mut recv: mpsc::Receiver<T>) -> TestResult<Vec<T>> {
        let mut res = Vec::new();
        while let Some(item) = recv.recv().await {
            res.push(item);
        }
        Ok(res)
    }

    fn assert_expected_progress(progress: &[ImportProgress]) {
        assert!(progress
            .iter()
            .find(|x| matches!(x, ImportProgress::Size { .. }))
            .is_some());
        assert!(progress
            .iter()
            .find(|x| matches!(x, ImportProgress::CopyDone { .. }))
            .is_some());
    }

    async fn test_import_bytes_task(data: Bytes, options: Arc<Options>) -> TestResult<()> {
        let expected_outboard = PreOrderMemOutboard::create(data.as_ref(), IROH_BLOCK_SIZE);
        // make the channel absurdly large, so we don't have to drain it
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let cmd = ImportBytes { data, out: tx };
        let res = import_bytes_task(cmd, options).await;
        let Some(res) = res else {
            panic!("import failed");
        };
        let ImportEntry { outboard, out, .. } = res;
        drop(out);
        let actual_outboard = match &outboard {
            MemOrFile::Mem(data) => data.clone(),
            MemOrFile::File(path) => std::fs::read(path)?.into(),
        };
        assert_eq!(expected_outboard.data.as_slice(), actual_outboard.as_ref());
        let progress = drain(rx).await?;
        assert_expected_progress(&progress);
        Ok(())
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
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let cmd = ImportByteStream {
            data: stream,
            out: tx,
        };
        let res = import_byte_stream_task(cmd, options).await;
        let Some(res) = res else {
            panic!("import failed");
        };
        let ImportEntry { outboard, out, .. } = res;
        drop(out);
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
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let cmd = ImportPath {
            path,
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
            out: tx,
        };
        let res = import_path_task(cmd, options).await;
        let Some(res) = res else {
            panic!("import failed");
        };
        let ImportEntry { outboard, out, .. } = res;
        drop(out);
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
            test_import_bytes_task(data.clone(), options.clone()).await?;
            test_import_byte_stream_task(data.clone(), options.clone()).await?;
            test_import_file_task(data, options.clone()).await?;
        }
        Ok(())
    }
}
