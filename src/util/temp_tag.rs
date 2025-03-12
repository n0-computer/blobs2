use std::sync::{Arc, Weak};

use tracing::warn;

use crate::{BlobFormat, Hash, HashAndFormat};

/// A hash and format pair that is protected from garbage collection.
///
/// If format is raw, this will protect just the blob
/// If format is collection, this will protect the collection and all blobs in it
#[derive(Debug)]
pub struct TempTag {
    /// The hash and format we are pinning
    inner: HashAndFormat,
    /// optional callback to call on drop
    on_drop: Option<Weak<dyn TagDrop>>,
}

/// A trait for things that can track liveness of blobs and collections.
///
/// This trait works together with [TempTag] to keep track of the liveness of a
/// blob or collection.
///
/// It is important to include the format in the liveness tracking, since
/// protecting a collection means protecting the blob and all its children,
/// whereas protecting a raw blob only protects the blob itself.
pub trait TagCounter: TagDrop + Sized {
    /// Called on creation of a temp tag
    fn on_create(&self, inner: &HashAndFormat);

    /// Get this as a weak reference for use in temp tags
    fn as_weak(self: &Arc<Self>) -> Weak<dyn TagDrop> {
        let on_drop: Arc<dyn TagDrop> = self.clone();
        Arc::downgrade(&on_drop)
    }

    /// Create a new temp tag for the given hash and format
    fn temp_tag(self: &Arc<Self>, inner: HashAndFormat) -> TempTag {
        self.on_create(&inner);
        TempTag::new(inner, Some(self.as_weak()))
    }
}

/// Trait used from temp tags to notify an abstract store that a temp tag is
/// being dropped.
pub trait TagDrop: std::fmt::Debug + Send + Sync + 'static {
    /// Called on drop
    fn on_drop(&self, inner: &HashAndFormat);
}

impl Into<HashAndFormat> for &TempTag {
    fn into(self) -> HashAndFormat {
        self.inner
    }
}

impl Into<HashAndFormat> for TempTag {
    fn into(self) -> HashAndFormat {
        self.inner
    }
}

impl TempTag {
    /// Create a new temp tag for the given hash and format
    ///
    /// This should only be used by store implementations.
    ///
    /// The caller is responsible for increasing the refcount on creation and to
    /// make sure that temp tags that are created between a mark phase and a sweep
    /// phase are protected.
    pub fn new(inner: HashAndFormat, on_drop: Option<Weak<dyn TagDrop>>) -> Self {
        Self { inner, on_drop }
    }

    /// The hash of the pinned item
    pub fn inner(&self) -> &HashAndFormat {
        &self.inner
    }

    /// The hash of the pinned item
    pub fn hash(&self) -> &Hash {
        &self.inner.hash
    }

    /// The format of the pinned item
    pub fn format(&self) -> BlobFormat {
        self.inner.format
    }

    /// The hash and format of the pinned item
    pub fn hash_and_format(&self) -> HashAndFormat {
        self.inner
    }

    /// Keep the item alive until the end of the process
    pub fn leak(mut self) {
        // set the liveness tracker to None, so that the refcount is not decreased
        // during drop. This means that the refcount will never reach 0 and the
        // item will not be gced until the end of the process.
        self.on_drop = None;
    }
}

impl Drop for TempTag {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            if let Some(on_drop) = on_drop.upgrade() {
                on_drop.on_drop(&self.inner);
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
struct TempCounters {
    /// number of raw temp tags for a hash
    raw: u64,
    /// number of hash seq temp tags for a hash
    hash_seq: u64,
}

impl TempCounters {
    fn counter(&mut self, format: BlobFormat) -> &mut u64 {
        match format {
            BlobFormat::Raw => &mut self.raw,
            BlobFormat::HashSeq => &mut self.hash_seq,
        }
    }

    fn inc(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.checked_add(1).unwrap();
    }

    fn dec(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.saturating_sub(1);
    }

    fn is_empty(&self) -> bool {
        self.raw == 0 && self.hash_seq == 0
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TempCounterMap(std::collections::BTreeMap<Hash, TempCounters>);

impl TempCounterMap {
    fn inc(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        self.0.entry(*hash).or_default().inc(*format)
    }

    fn dec(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        let Some(counters) = self.0.get_mut(hash) else {
            warn!("Decrementing non-existent temp tag");
            return;
        };
        counters.dec(*format);
        if counters.is_empty() {
            self.0.remove(hash);
        }
    }

    fn contains(&self, hash: &Hash) -> bool {
        self.0.contains_key(hash)
    }

    fn keys(&self) -> impl Iterator<Item = HashAndFormat> {
        let mut res = Vec::new();
        for (k, v) in self.0.iter() {
            if v.raw > 0 {
                res.push(HashAndFormat::raw(*k));
            }
            if v.hash_seq > 0 {
                res.push(HashAndFormat::hash_seq(*k));
            }
        }
        res.into_iter()
    }
}
