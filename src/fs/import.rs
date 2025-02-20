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
use bao_tree::{
    blake3::Hash,
    io::{outboard::PreOrderOutboard, sync::WriteAt},
    BaoTree,
};
use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Write},
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use n0_future::StreamExt;
use tokio::{sync::mpsc, task::yield_now};

use super::{meta::raw_outboard_size, options::Options};
use crate::{
    proto::{BoxedByteStream, ImportPath, ImportProgress},
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
enum ImportSource {
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
pub struct ImportEntry {
    hash: Hash,
    source: ImportSource,
    outboard: MemOrFile<Bytes, (PathBuf, u64)>,
}

/// Start a task to import from a [`Bytes`] in memory.
pub async fn import_bytes_task(
    data: Bytes,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> Option<ImportEntry> {
    let send = out.clone().reserve_owned().await.ok()?;
    match import_bytes_impl(data, options, out).await {
        Ok(entry) => Some(entry),
        Err(cause) => {
            send.send(ImportProgress::Error { cause });
            None
        }
    }
}

async fn import_bytes_impl(
    data: Bytes,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    import_byte_stream_impl(Box::pin(n0_future::stream::once(Ok(data))), options, out).await
}

pub async fn import_byte_stream_task(
    stream: BoxedByteStream,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> Option<ImportEntry> {
    let send = out.clone().reserve_owned().await.ok()?;
    match import_byte_stream_impl(stream, options, out).await {
        Ok(entry) => Some(entry),
        Err(cause) => {
            send.send(ImportProgress::Error { cause });
            None
        }
    }
}

async fn import_byte_stream_impl(
    mut stream: BoxedByteStream,
    options: Arc<Options>,
    out: mpsc::Sender<ImportProgress>,
) -> anyhow::Result<ImportEntry> {
    let mut size = 0;
    let mut data = Vec::new();
    let mut disk = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let new_size = size + chunk.len() as u64;
        if new_size > options.inline.max_data_inlined {
            let temp_path = options.path.temp_file_name();
            let mut file = fs::File::create(&temp_path)?;
            file.write_all(&data)?;
            file.write_all(&chunk)?;
            data.clear();
            disk = Some((file, temp_path));
            break;
        } else {
            data.extend_from_slice(&chunk);
            size = new_size;
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
    let data = source.read();
    let (hash, outboard) = if outboard_size > options.inline.max_outboard_inlined {
        // outboard will eventually be stored as a file, so compute it directly to a file
        // we don't know the hash yet, so we need to create a temp file
        let outboard_path = options.path.temp_file_name();
        let mut outboard_file = File::create(&outboard_path)?;
        let mut outboard = PreOrderOutboard {
            tree,
            root,
            data: &mut outboard_file,
        };
        init_outboard(data, &mut outboard, send_progress)?;
        (outboard.root, MemOrFile::File((outboard_path, size)))
    } else {
        // outboard will be stored in memory, so compute it to a memory buffer
        let mut outboard_file: Vec<u8> = Vec::new();
        let mut outboard = PreOrderOutboard {
            tree,
            root,
            data: &mut outboard_file,
        };
        init_outboard(data, &mut outboard, send_progress)?;
        (outboard.root, MemOrFile::Mem(Bytes::from(outboard_file)))
    };
    out.send(ImportProgress::Done { hash }).await?;
    Ok(ImportEntry {
        hash: hash.into(),
        source,
        outboard,
    })
}

pub(crate) fn init_outboard<R: Read, W: WriteAt>(
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
        let mut file = File::create(&temp_path)?;
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
