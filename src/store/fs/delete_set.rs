use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use tracing::warn;

use super::options::PathOptions;
use crate::Hash;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BaoFilePart {
    Outboard,
    Data,
    Sizes,
    Bitfield,
}

pub fn pair(options: Arc<PathOptions>) -> (ProtectHandle, DeleteSetHandle) {
    let ds = Arc::new(Mutex::new(DeleteSet::default()));
    (ProtectHandle(ds.clone()), DeleteSetHandle::new(ds, options))
}

/// Helper to keep track of files to delete after a transaction is committed.
#[derive(Debug, Default)]
struct DeleteSet(BTreeSet<(Hash, BaoFilePart)>);

impl DeleteSet {
    /// Mark a file as to be deleted after the transaction is committed.
    pub fn delete(&mut self, hash: Hash, parts: impl IntoIterator<Item = BaoFilePart>) {
        for part in parts {
            self.0.insert((hash, part));
        }
    }

    /// Mark a file as to be kept after the transaction is committed.
    ///
    /// This will cancel any previous delete for the same file in the same transaction.
    pub fn protect(&mut self, hash: Hash, parts: impl IntoIterator<Item = BaoFilePart>) {
        for part in parts {
            self.0.remove(&(hash, part));
        }
    }

    /// Apply the delete set and clear it.
    ///
    /// This will delete all files marked for deletion and then clear the set.
    /// Errors will just be logged.
    pub fn commit(&mut self, options: &PathOptions) {
        for (hash, to_delete) in &self.0 {
            tracing::debug!("deleting {:?} for {hash}", to_delete);
            let path = match to_delete {
                BaoFilePart::Data => options.data_path(hash),
                BaoFilePart::Outboard => options.outboard_path(hash),
                BaoFilePart::Sizes => options.sizes_path(hash),
                BaoFilePart::Bitfield => options.bitfield_path(hash),
            };
            if let Err(cause) = std::fs::remove_file(&path) {
                // Ignore NotFound errors, if the file is already gone that's fine.
                if cause.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        "failed to delete {:?} {}: {}",
                        to_delete,
                        path.display(),
                        cause
                    );
                }
            }
        }
        self.0.clear();
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProtectHandle(Arc<Mutex<DeleteSet>>);

/// Protect handle, to be used concurrently with transactions to mark files for keeping.
impl ProtectHandle {
    /// Inside or outside a transaction, mark files as to be kept
    ///
    /// If we are not inside a transaction, this will do nothing.
    pub(super) fn protect(&self, hash: Hash, parts: impl IntoIterator<Item = BaoFilePart>) {
        let mut guard = self.0.lock().unwrap();
        guard.protect(hash, parts);
    }
}

#[derive(Debug)]
pub(super) struct DeleteSetHandle {
    ds: Arc<Mutex<DeleteSet>>,
    options: Arc<PathOptions>,
}

impl DeleteSetHandle {
    fn new(ds: Arc<Mutex<DeleteSet>>, options: Arc<PathOptions>) -> Self {
        Self { ds, options }
    }

    pub fn begin_write(&mut self) -> FileTransaction<'_> {
        FileTransaction::new(self)
    }
}

/// Delete handle, to be used linearly with transactions to mark files for deletion.
#[derive(Debug)]
pub(super) struct FileTransaction<'a>(&'a DeleteSetHandle);

impl<'a> FileTransaction<'a> {
    pub fn new(inner: &'a DeleteSetHandle) -> Self {
        let guard = inner.ds.lock().unwrap();
        debug_assert!(guard.is_empty());
        drop(guard);
        Self(inner)
    }

    /// Inside a transaction, mark files as to be deleted
    pub fn delete(&self, hash: Hash, parts: impl IntoIterator<Item = BaoFilePart>) {
        let mut guard = self.0.ds.lock().unwrap();
        guard.delete(hash, parts);
    }

    /// After a successful commit, apply the delete set and clear it.
    pub fn commit(self) {
        let mut guard = self.0.ds.lock().unwrap();
        guard.commit(&self.0.options);
    }
}

impl Drop for FileTransaction<'_> {
    fn drop(&mut self) {
        self.0.ds.lock().unwrap().clear();
    }
}
