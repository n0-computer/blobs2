use std::collections::HashSet;

use bao_tree::ChunkRanges;
use genawaiter::sync::{Co, Gen};
use n0_future::{Stream, StreamExt};
use tracing::{debug, error, warn};

use crate::{api::Store, Hash, HashAndFormat};

/// An event related to GC
#[derive(Debug)]
pub enum GcMarkEvent {
    /// A custom event (info)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<crate::api::Error>),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// An event related to GC
#[derive(Debug)]
pub enum GcSweepEvent {
    /// A custom event (debug)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<crate::api::Error>),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// Compute the set of live hashes
pub(super) async fn gc_mark_task<'a>(
    store: &Store,
    live: &'a mut HashSet<Hash>,
    co: &Co<GcMarkEvent>,
) -> crate::api::Result<()> {
    macro_rules! trace {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomDebug(format!($($arg)*))).await;
        };
    }
    macro_rules! warn {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomWarning(format!($($arg)*), None)).await;
        };
    }
    let mut roots = HashSet::new();
    trace!("traversing tags");
    let mut tags = store.tags().list().await?;
    while let Some(tag) = tags.next().await {
        let info = tag?;
        trace!("adding root {:?} {:?}", info.name, info.hash_and_format());
        roots.insert(info.hash_and_format());
    }
    trace!("traversing temp roots");
    let mut tts = store.tags().temp_tags().await?;
    while let Some(tt) = tts.next().await {
        trace!("adding temp root {:?}", tt);
        roots.insert(tt);
    }
    for HashAndFormat { hash, format } in roots {
        // we need to do this for all formats except raw
        if live.insert(hash) && !format.is_raw() {
            let mut stream = store.export_bao(hash, ChunkRanges::all()).hashes();
            while let Some(res) = stream.next().await {
                if let Ok(hash) = res {
                    live.insert(hash);
                }
            }
        }
    }
    trace!("gc mark done. found {} live blobs", live.len());
    Ok(())
}

async fn gc_sweep_task(
    store: &Store,
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> crate::api::Result<()> {
    let mut blobs = store.blobs().list().stream().await?;
    let mut count = 0;
    let mut batch = Vec::new();
    while let Some(hash) = blobs.next().await {
        let hash = hash?;
        if !live.contains(&hash) {
            batch.push(hash);
            count += 1;
        }
        if batch.len() >= 100 {
            store.blobs().delete(batch.clone()).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.blobs().delete(batch).await?;
    }
    co.yield_(GcSweepEvent::CustomDebug(format!(
        "deleted {} blobs",
        count
    )))
    .await;
    Ok(())
}

fn gc_mark<'a>(
    store: &'a Store,
    live: &'a mut HashSet<Hash>,
) -> impl Stream<Item = GcMarkEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_mark_task(store, live, &co).await {
            co.yield_(GcMarkEvent::Error(e)).await;
        }
    })
}

fn gc_sweep<'a>(
    store: &'a Store,
    live: &'a HashSet<Hash>,
) -> impl Stream<Item = GcSweepEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_sweep_task(store, live, &co).await {
            co.yield_(GcSweepEvent::Error(e)).await;
        }
    })
}

pub struct GcConfig {
    pub interval: std::time::Duration,
}

pub async fn gc_run_once(store: &Store, live: &mut HashSet<Hash>) -> crate::api::Result<()> {
    {
        let mut stream = gc_mark(&store, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcMarkEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcMarkEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcMarkEvent::Error(err) => {
                    error!("error during gc mark: {:?}", err);
                    return Err(err);
                }
            }
        }
    }
    {
        let mut stream = gc_sweep(&store, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcSweepEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcSweepEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcSweepEvent::Error(err) => {
                    error!("error during gc sweep: {:?}", err);
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

pub async fn run_gc(store: Store, config: GcConfig) {
    let mut live = HashSet::new();
    loop {
        tokio::time::sleep(config.interval).await;
        live.clear();
        if let Err(_) = gc_run_once(&store, &mut live).await {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use testresult::TestResult;
    use crate::{api::{blobs::ImportBytes, Scope, Store}, hashseq::HashSeq, BlobFormat};

    async fn gc_smoke(path: &Path, store: &Store) -> TestResult<()> {
        let blobs = store.blobs();
        let a = blobs.import_slice("a").hash().await?;
        let b = blobs.import_slice("b").hash().await?;
        let c = blobs.import_slice("c").hash().await?;
        let d = blobs.import_slice("d").hash().await?;
        let e = blobs.import_slice("e").hash().await?;
        let f = blobs.import_slice("f").hash().await?;
        let g = blobs.import_slice("g").hash().await?;
        store.tags().set("c", c.hash_and_format()).await?;
        let hs = [d.hash(), e.hash()].into_iter().collect::<HashSeq>();
        let hs = blobs.import_bytes_with_opts(ImportBytes {
            data: hs.into(),
            format: BlobFormat::HashSeq,
            scope: Scope::GLOBAL,
        }).hash().await?;
        drop(b);
        drop(d);
        drop(e);
        drop(f);
        drop(g);
        let mut live = HashSet::new();
        gc_run_once(store, &mut live).await?;
        drop(a);
        drop(c);
        drop(hs);
        Ok(())
    }

    #[tokio::test]
    async fn gc_smoke_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_smoke(testdir.path(), &store).await?;
        Ok(())
    }

}