//! Readonly in-memory store.
//!
//! This can only serve data that is provided at creation time. It is much simpler
//! than the mutable in-memory store and the file system store, and can serve as a
//! good starting point for custom implementations.
//!
//! It can also be useful as a lightweight store for tests.
use std::{
    collections::HashMap,
    io::{self, Write},
    ops::Deref,
    path::PathBuf,
};

use bao_tree::{
    BaoTree, ChunkRanges,
    io::{
        mixed::{EncodedItem, traverse_ranges_validated},
        outboard::PreOrderMemOutboard,
        sync::ReadAt,
    },
};
use bytes::Bytes;
use irpc::channel::spsc;
use n0_future::future::yield_now;
use ref_cast::RefCast;
use tokio::task::{JoinError, JoinSet};

use super::util::{BaoTreeSender, observer::Observer2};
use crate::{
    Hash,
    api::{
        self, ApiClient,
        blobs::{AddProgressItem, Bitfield, ExportProgressItem},
        proto::{
            self, Command, ExportBaoMsg, ExportBaoRequest, ExportPathMsg, ExportPathRequest,
            ImportBaoMsg, ImportByteStreamMsg, ImportBytesMsg, ImportPathMsg, ObserveMsg,
            ObserveRequest,
        },
    },
    store::{IROH_BLOCK_SIZE, mem::CompleteStorage},
    util::channel::mpsc,
};

#[derive(Debug, Clone)]
pub struct ReadonlyMemStore {
    client: ApiClient,
}

impl Deref for ReadonlyMemStore {
    type Target = crate::api::Store;

    fn deref(&self) -> &Self::Target {
        crate::api::Store::ref_from_sender(&self.client)
    }
}

struct Actor {
    commands: mpsc::Receiver<proto::Command>,
    unit_tasks: JoinSet<()>,
    data: HashMap<Hash, CompleteStorage>,
}

impl Actor {
    fn new(commands: mpsc::Receiver<proto::Command>, data: HashMap<Hash, CompleteStorage>) -> Self {
        Self {
            data,
            commands,
            unit_tasks: JoinSet::new(),
        }
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::ImportBao(ImportBaoMsg { tx, .. }) => {
                tx.send(Err(api::Error::other("import not supported")))
                    .await
                    .ok();
            }
            Command::ImportBytes(ImportBytesMsg { mut tx, .. }) => {
                tx.send(AddProgressItem::Error(io::Error::other(
                    "import not supported",
                )))
                .await
                .ok();
            }
            Command::ImportByteStream(ImportByteStreamMsg { mut tx, .. }) => {
                tx.send(AddProgressItem::Error(io::Error::other(
                    "import not supported",
                )))
                .await
                .ok();
            }
            Command::ImportPath(ImportPathMsg { mut tx, .. }) => {
                tx.send(AddProgressItem::Error(io::Error::other(
                    "import not supported",
                )))
                .await
                .ok();
            }
            Command::Observe(ObserveMsg {
                inner: ObserveRequest { hash },
                tx,
                ..
            }) => {
                let observer = if let Some(entry) = self.data.get_mut(&hash) {
                    entry.subscribe()
                } else {
                    Observer2::once(Bitfield::empty())
                };
                self.unit_tasks.spawn(async move {
                    observer.forward(tx).await.ok();
                });
            }
            Command::ExportBao(ExportBaoMsg {
                inner: ExportBaoRequest { hash, ranges },
                tx,
                ..
            }) => {
                let entry = self
                    .data
                    .get(&hash)
                    .map(|e| (e.data.clone(), e.outboard.clone()));
                self.unit_tasks
                    .spawn(export_bao_task(hash, entry, ranges, tx));
            }
            Command::ExportPath(ExportPathMsg {
                inner: ExportPathRequest { hash, target, .. },
                tx,
                ..
            }) => {
                let entry = self
                    .data
                    .get(&hash)
                    .map(|e| (e.data.clone(), e.outboard.clone()));
                self.unit_tasks.spawn(export_path_task(entry, target, tx));
            }
            _ => {}
        }
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            tracing::error!("task failed: {e}");
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(cmd) = command else {
                        break;
                    };
                    self.handle_command(cmd).await;
                },
                Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
                    self.log_unit_task(res);
                }
            }
        }
    }
}

async fn export_bao_task(
    hash: Hash,
    entry: Option<(Bytes, Bytes)>,
    ranges: ChunkRanges,
    mut sender: spsc::Sender<EncodedItem>,
) {
    let (data, outboard) = match entry {
        Some(entry) => entry,
        None => {
            sender
                .send(EncodedItem::Error(bao_tree::io::EncodeError::Io(
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "export task ended unexpectedly",
                    ),
                )))
                .await
                .ok();
            return;
        }
    };
    let size = data.as_ref().len() as u64;
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let outboard = PreOrderMemOutboard {
        root: hash.into(),
        tree,
        data: outboard,
    };
    let sender = BaoTreeSender::ref_cast_mut(&mut sender);
    traverse_ranges_validated(data.as_ref(), outboard, &ranges, sender)
        .await
        .ok();
}

impl ReadonlyMemStore {
    pub fn new(items: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        let mut entries = HashMap::new();
        for item in items {
            let data = Bytes::copy_from_slice(item.as_ref());
            let (hash, entry) = CompleteStorage::create(data);
            entries.insert(hash, entry);
        }
        let (sender, receiver) = mpsc::channel(1);
        let actor = Actor::new(receiver, entries);
        tokio::spawn(actor.run());
        let local = irpc::LocalSender::from(sender);
        Self {
            client: local.into(),
        }
    }
}

async fn export_path_task(
    entry: Option<(Bytes, Bytes)>,
    target: PathBuf,
    mut tx: spsc::Sender<ExportProgressItem>,
) {
    let Some(entry) = entry else {
        tx.send(api::Error::io(io::ErrorKind::NotFound, "hash not found").into())
            .await
            .ok();
        return;
    };
    match export_path_impl(entry, target, &mut tx).await {
        Ok(()) => tx.send(ExportProgressItem::Done).await.ok(),
        Err(cause) => tx.send(api::Error::from(cause).into()).await.ok(),
    };
}

async fn export_path_impl(
    (data, _): (Bytes, Bytes),
    target: PathBuf,
    tx: &mut spsc::Sender<ExportProgressItem>,
) -> io::Result<()> {
    // todo: for partial entries make sure to only write the part that is actually present
    let mut file = std::fs::File::create(&target)?;
    let size = data.len() as u64;
    tx.send(ExportProgressItem::Size(size)).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        data.as_ref().read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        tx.try_send(ExportProgressItem::CopyProgress(offset))
            .await
            .map_err(|_e| io::Error::other("error"))?;
        yield_now().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bao_tree::blake3;

    #[tokio::test]
    async fn smoke() {
        let data = b"hello world";
        let _hash = blake3::hash(data);
    }
}
