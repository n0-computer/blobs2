use std::{
    collections::HashMap,
    io::{self, Write},
    path::PathBuf,
};

use bao_tree::{
    blake3::Hash,
    io::{mixed::traverse_ranges_validated, outboard::PreOrderMemOutboard, sync::ReadAt},
    BaoTree, ChunkRanges,
};
use bytes::Bytes;
use n0_future::future::yield_now;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::{JoinError, JoinSet},
};

use crate::{
    bitfield::BitfieldState, mem::CompleteEntry, proto::*, util::SenderProgressExt, Store,
    IROH_BLOCK_SIZE,
};

struct Actor {
    commands: mpsc::Receiver<Command>,
    unit_tasks: JoinSet<()>,
    data: HashMap<Hash, CompleteEntry>,
    observers: Vec<mpsc::Sender<BitfieldEvent>>,
}

impl Actor {
    fn new(
        commands: tokio::sync::mpsc::Receiver<Command>,
        data: HashMap<Hash, CompleteEntry>,
    ) -> Self {
        Self {
            data,
            commands,
            unit_tasks: JoinSet::new(),
            observers: Vec::new(),
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::ImportBao(ImportBao { out, .. }) => {
                out.send(Err(anyhow::anyhow!("import not supported"))).ok();
            }
            Command::ImportBytes(ImportBytes { out, .. }) => {
                out.try_send(ImportProgress::Error {
                    cause: anyhow::anyhow!("import not supported"),
                })
                .ok();
            }
            Command::ImportByteStream(ImportByteStream { out, .. }) => {
                out.try_send(ImportProgress::Error {
                    cause: anyhow::anyhow!("import not supported"),
                })
                .ok();
            }
            Command::ImportPath(ImportPath { out, .. }) => {
                out.try_send(ImportProgress::Error {
                    cause: anyhow::anyhow!("import not supported"),
                })
                .ok();
            }
            Command::Observe(Observe { hash, out }) => {
                if let Some(entry) = self.data.get_mut(&hash) {
                    let ranges = entry.bitfield().clone();
                    let size = entry.size().into();
                    // if we can't send, we drop out, so the caller will get an error
                    if out.try_send(BitfieldState { ranges, size }.into()).is_err() {
                        return;
                    }
                    entry.observers().push(out);
                } else {
                    // if we can't send, we drop out, so the caller will get an error
                    if out.try_send(BitfieldState::unknown().into()).is_err() {
                        return;
                    }
                    self.observers.push(out);
                }
            }
            Command::ExportBao(ExportBao { hash, ranges, out }) => {
                let entry = self.data.get(&hash).cloned();
                self.unit_tasks
                    .spawn(export_bao_task(hash, entry, ranges, out));
            }
            Command::ExportPath(ExportPath {
                hash, target, out, ..
            }) => {
                let entry = self.data.get(&hash).cloned();
                self.unit_tasks.spawn(export_path_task(entry, target, out));
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
                    self.handle_command(cmd);
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
    entry: Option<CompleteEntry>,
    ranges: ChunkRanges,
    sender: tokio::sync::mpsc::Sender<EncodedItem>,
) {
    let entry = match entry {
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
    let data = entry.data();
    let outboard = entry.outboard();
    let size = data.as_ref().len() as u64;
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let outboard = PreOrderMemOutboard {
        root: hash,
        tree,
        data: outboard,
    };
    traverse_ranges_validated(data.as_ref(), outboard, &ranges, &sender).await;
}

impl Store {
    pub fn readonly_mem(items: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Store {
        let mut entries = HashMap::new();
        for item in items {
            let data = Bytes::copy_from_slice(item.as_ref());
            let (hash, entry) = CompleteEntry::create(data);
            entries.insert(hash, entry);
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let actor = Actor::new(receiver, entries);
        tokio::spawn(actor.run());
        Store::from_sender(sender)
    }
}

async fn export_path_task(
    entry: Option<CompleteEntry>,
    target: PathBuf,
    out: mpsc::Sender<ExportProgress>,
) {
    let Some(entry) = entry else {
        out.send(ExportProgress::Error {
            cause: anyhow::anyhow!("hash not found"),
        })
        .await
        .ok();
        return;
    };
    match export_path_impl(entry, target, &out).await {
        Ok(()) => out.send(ExportProgress::Done).await.ok(),
        Err(e) => out.send(ExportProgress::Error { cause: e }).await.ok(),
    };
}

async fn export_path_impl(
    entry: CompleteEntry,
    target: PathBuf,
    out: &mpsc::Sender<ExportProgress>,
) -> anyhow::Result<()> {
    // todo: for partial entries make sure to only write the part that is actually present
    let mut file = std::fs::File::create(&target)?;
    let size = entry.size().value();
    out.send(ExportProgress::Size { size }).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        entry.data().as_ref().read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        out.send_progress(ExportProgress::CopyProgress {
            offset: offset as u64,
        })?;
        yield_now().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bao_tree::blake3;

    use super::*;

    #[tokio::test]
    async fn smoke() {
        let data = b"hello world";
        let hash = blake3::hash(data);
    }
}
