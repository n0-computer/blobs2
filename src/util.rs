use std::{
    any::TypeId,
    borrow::Borrow,
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{self, Read, Write},
    path::Path,
    time::SystemTime,
};

use arrayvec::ArrayString;
use bao_tree::{blake3, io::sync::ReadAt};
use bytes::Bytes;
use derive_more::{From, Into};

mod mem_or_file;
mod sparse_mem_file;
pub use mem_or_file::{FixedSize, MemOrFile};
use range_collections::{range_set::RangeSetEntry, RangeSetRef};
use serde::{de::DeserializeOwned, Serialize};
pub use sparse_mem_file::SparseMemFile;
use tokio::sync::mpsc;
use tracing::info;
pub mod observer;

/// A tag
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, From, Into)]
pub struct Tag(pub Bytes);

impl Borrow<[u8]> for Tag {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<String> for Tag {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&str> for Tag {
    fn from(value: &str) -> Self {
        Self(Bytes::from(value.to_owned()))
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.as_ref();
        match std::str::from_utf8(bytes) {
            Ok(s) => write!(f, "\"{}\"", s),
            Err(_) => write!(f, "{}", hex::encode(bytes)),
        }
    }
}

impl Tag {
    /// Create a new tag that does not exist yet.
    pub fn auto(time: SystemTime, exists: impl Fn(&[u8]) -> bool) -> Self {
        let now = chrono::DateTime::<chrono::Utc>::from(time);
        let mut i = 0;
        loop {
            let mut text = format!("auto-{}", now.format("%Y-%m-%dT%H:%M:%S%.3fZ"));
            if i != 0 {
                text.push_str(&format!("-{}", i));
            }
            if !exists(text.as_bytes()) {
                return Self::from(text);
            }
            i += 1;
        }
    }
}

struct DD<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DD<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tag").field(&DD(self)).finish()
    }
}

pub(crate) fn limited_range(offset: u64, len: usize, buf_len: usize) -> std::ops::Range<usize> {
    if offset < buf_len as u64 {
        let start = offset as usize;
        let end = start.saturating_add(len).min(buf_len);
        start..end
    } else {
        0..0
    }
}

/// zero copy get a limited slice from a `Bytes` as a `Bytes`.
#[allow(dead_code)]
pub(crate) fn get_limited_slice(bytes: &Bytes, offset: u64, len: usize) -> Bytes {
    bytes.slice(limited_range(offset, len, bytes.len()))
}

mod redb_support {
    use bytes::Bytes;
    use redb::{Key as RedbKey, Value as RedbValue};

    use super::Tag;

    impl RedbValue for Tag {
        type SelfType<'a> = Self;

        type AsBytes<'a> = bytes::Bytes;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a,
        {
            Self(Bytes::copy_from_slice(data))
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
        where
            Self: 'a,
            Self: 'b,
        {
            value.0.clone()
        }

        fn type_name() -> redb::TypeName {
            redb::TypeName::new("Tag")
        }
    }

    impl RedbKey for Tag {
        fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
            data1.cmp(data2)
        }
    }
}

pub trait SenderProgressExt<T> {
    fn send_progress<V: Into<T>>(
        &self,
        value: V,
    ) -> std::result::Result<(), mpsc::error::TrySendError<T>>;
}

impl<T> SenderProgressExt<T> for tokio::sync::mpsc::Sender<T> {
    fn send_progress<V: Into<T>>(
        &self,
        value: V,
    ) -> std::result::Result<(), mpsc::error::TrySendError<T>> {
        let value = value.into();
        match self.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(e @ mpsc::error::TrySendError::Closed(_)) => Err(e),
        }
    }
}

/// A reader that calls a callback with the number of bytes read after each read.
pub(crate) struct ProgressReader<R, F: Fn(u64) -> std::io::Result<()>> {
    inner: R,
    offset: u64,
    cb: F,
}

impl<R: std::io::Read, F: Fn(u64) -> std::io::Result<()>> ProgressReader<R, F> {
    pub fn new(inner: R, cb: F) -> Self {
        Self {
            inner,
            offset: 0,
            cb,
        }
    }
}

impl<R: std::io::Read, F: Fn(u64) -> std::io::Result<()>> std::io::Read for ProgressReader<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.offset += read as u64;
        (self.cb)(self.offset)?;
        Ok(read)
    }
}

pub trait RangeSetExt<T> {
    fn upper_bound(&self) -> Option<T>;
}

impl<T: RangeSetEntry + Clone> RangeSetExt<T> for RangeSetRef<T> {
    /// The upper (exclusive) bound of the bitfield
    fn upper_bound(&self) -> Option<T> {
        let boundaries = self.boundaries();
        if boundaries.is_empty() {
            Some(RangeSetEntry::min_value())
        } else if boundaries.len() % 2 == 0 {
            Some(boundaries[boundaries.len() - 1].clone())
        } else {
            None
        }
    }
}

pub fn write_checksummed<P: AsRef<Path>, T: Serialize>(path: P, data: &T) -> io::Result<()> {
    // Build Vec with space for hash
    let mut buffer = Vec::with_capacity(32 + 128);
    buffer.extend_from_slice(&[0u8; 32]);

    // Serialize directly into buffer
    postcard::to_io(data, &mut buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Compute hash over data (skip first 32 bytes)
    let data_slice = &buffer[32..];
    let hash = blake3::hash(data_slice);
    buffer[..32].copy_from_slice(hash.as_bytes());

    // Write all at once
    let mut file = File::create(&path)?;
    file.write_all(&buffer)?;

    Ok(())
}

pub fn read_checksummed_and_truncate<P: AsRef<Path>, T: DeserializeOwned>(
    path: P,
) -> io::Result<T> {
    let path = path.as_ref();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    file.set_len(0)?;
    file.sync_all()?;
    info!("{} {}", path.display(), hex::encode(&buffer));

    if buffer.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "File marked dirty",
        ));
    }

    if buffer.len() < 32 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "File too short"));
    }

    let stored_hash = &buffer[..32];
    let data = &buffer[32..];

    let computed_hash = blake3::hash(data);
    if computed_hash.as_bytes() != stored_hash {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Hash mismatch"));
    }

    let deserialized =
        postcard::from_bytes(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(deserialized)
}

pub fn read_checksummed<T: DeserializeOwned>(path: impl AsRef<Path>) -> io::Result<T> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    info!("{} {}", path.display(), hex::encode(&buffer));

    if buffer.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "File marked dirty",
        ));
    }

    if buffer.len() < 32 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "File too short"));
    }

    let stored_hash = &buffer[..32];
    let data = &buffer[32..];

    let computed_hash = blake3::hash(data);
    if computed_hash.as_bytes() != stored_hash {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Hash mismatch"));
    }

    let deserialized =
        postcard::from_bytes(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(deserialized)
}

/// Helper trait for bytes for debugging
pub trait SliceInfoExt: AsRef<[u8]> {
    // get the addr of the actual data, to check if data was copied
    fn addr(&self) -> usize;

    // a short symbol string for the address
    fn addr_short(&self) -> ArrayString<10> {
        let addr = self.addr().to_le_bytes();
        let hash = crate::Hash::new(&addr);
        hash.fmt_short()
    }

    fn hash_short(&self) -> ArrayString<10> {
        crate::Hash::new(self.as_ref()).fmt_short()
    }
}

impl<T: AsRef<[u8]>> SliceInfoExt for T {
    fn addr(&self) -> usize {
        self.as_ref() as *const [u8] as *const u8 as usize
    }

    fn hash_short(&self) -> ArrayString<10> {
        crate::Hash::new(self.as_ref()).fmt_short()
    }
}

pub fn symbol_string(data: &[u8]) -> ArrayString<12> {
    const SYMBOLS: &[char] = &[
        '★', '●', '▲', '■', '◆', '◉', '△', '□', '◇', '○', '✦', '✧', '⊕', '⊗', '♠', '♣', '♥', '♦',
        '✓', '✗', '☀', '☁', '☂', '☃', '☄', '☆', '☇', '☈', '☉', '☊', '☋', '☌', '☍', '☎', '☏', '♩',
        '♪', '♫', '♬', '⚀', '⚁', '⚂', '⚃', '⚄', '⚅', '⚡', '⚪', '⚫', '⚽', '⚾', '⛄', '⛅', '⛈',
        '⛓', '⛩', '✈', '⛵', '⛽', '⛺', '⛰', '♨', '⛸', '⛹', '⛻',
    ];
    const BASE: usize = SYMBOLS.len(); // 64

    // Hash the input with BLAKE3
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes(); // 32-byte hash

    // Create an ArrayString with capacity 12 (bytes)
    let mut result = ArrayString::<12>::new();

    // Fill with 3 symbols
    for i in 0..3 {
        let byte = bytes[i] as usize;
        let index = byte % BASE;
        result.push(SYMBOLS[index]); // Each char can be up to 4 bytes
    }

    result
}
