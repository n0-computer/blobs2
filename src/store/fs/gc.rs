use core::hash;
use std::collections::HashSet;

use bao_tree::{io::mixed::EncodedItem, ChunkRanges};
use chrono::format::Item;
use genawaiter::sync::{Co, Gen};
use n0_future::{Stream, StreamExt};

use crate::{
    api::{blobs::ExportBaoResult, Store},
    get::db::HashSeqChunk,
    Hash, HashAndFormat,
};

/// An event related to GC
#[derive(Debug)]
pub enum GcMarkEvent {
    /// A custom event (info)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<anyhow::Error>),
    /// An unrecoverable error during GC
    Error(anyhow::Error),
}

/// An event related to GC
#[derive(Debug)]
pub enum GcSweepEvent {
    /// A custom event (debug)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<anyhow::Error>),
    /// An unrecoverable error during GC
    Error(anyhow::Error),
}

/// Compute the set of live hashes
pub(super) async fn gc_mark_task<'a>(
    store: Store,
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
    store: Store,
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> anyhow::Result<()> {
    let mut blobs = store.blobs().list().await?;
    let mut count = 0;
    let mut batch = Vec::new();
    while let Some(hash) = blobs.recv().await? {
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
