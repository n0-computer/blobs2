use std::{env, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::SecretKey;
use iroh_base::ticket::NodeTicket;
use iroh_blobs::{
    provider::Event,
    store::fs::FsStore,
    test::{add_hash_sequences, create_random_blobs},
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio::{signal::ctrl_c, sync::mpsc};
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Random seed for reproducible results
    #[arg(long)]
    pub seed: Option<u64>,

    /// Path for store, none for in-memory store
    #[arg(long)]
    pub path: Option<PathBuf>,

    /// Number of blobs to generate
    #[arg(long, default_value_t = 100)]
    pub num_blobs: usize,

    /// Size of each blob in bytes
    #[arg(long, default_value_t = 100000)]
    pub blob_size: usize,

    /// Number of hash sequences
    #[arg(long, default_value_t = 1)]
    pub hash_seqs: usize,

    /// Size of each hash sequence
    #[arg(long, default_value_t = 100)]
    pub hash_seq_size: usize,

    /// Size of each hash sequence
    #[arg(long, default_value_t = false)]
    pub allow_push: bool,
}

pub fn get_or_generate_secret_key() -> Result<SecretKey> {
    if let Ok(secret) = env::var("IROH_SECRET") {
        // Parse the secret key from string
        SecretKey::from_str(&secret).context("Invalid secret key format")
    } else {
        // Generate a new random key
        let secret_key = SecretKey::generate(&mut rand::thread_rng());
        println!("Generated new random secret key");
        println!("To reuse this key, set the IROH_SECRET={secret_key}");
        Ok(secret_key)
    }
}

pub fn dump_provider_events(
    allow_push: bool,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Sender<iroh_blobs::provider::Event>,
) {
    let (tx, mut rx) = mpsc::channel(100);
    let dump_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                Event::ClientConnected {
                    node_id,
                    connection_id,
                    permitted,
                } => {
                    permitted.send(true).await.ok();
                    println!("Client connected: {node_id} {connection_id}");
                }
                Event::GetRequestReceived {
                    connection_id,
                    request_id,
                    hash,
                    ranges,
                } => {
                    println!(
                        "Get request received: {connection_id} {request_id} {hash} {ranges:?}"
                    );
                }
                Event::TransferCompleted {
                    connection_id,
                    request_id,
                    stats,
                } => {
                    println!("Transfer completed: {connection_id} {request_id} {stats:?}");
                }
                Event::TransferAborted {
                    connection_id,
                    request_id,
                    stats,
                } => {
                    println!("Transfer aborted: {connection_id} {request_id} {stats:?}");
                }
                Event::TransferProgress {
                    connection_id,
                    request_id,
                    index,
                    end_offset,
                } => {
                    info!("Transfer progress: {connection_id} {request_id} {index} {end_offset}");
                }
                Event::PushRequestReceived {
                    connection_id,
                    request_id,
                    hash,
                    ranges,
                    permitted,
                } => {
                    if allow_push {
                        permitted.send(true).await.ok();
                        println!(
                            "Push request received: {connection_id} {request_id} {hash} {ranges:?}"
                        );
                    } else {
                        permitted.send(false).await.ok();
                        println!(
                            "Push request denied: {connection_id} {request_id} {hash} {ranges:?}"
                        );
                    }
                }
                _ => {
                    info!("Received event: {:?}", event);
                }
            }
        }
    });
    (dump_task, tx)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    println!("{:?}", args);
    let path = args.path.unwrap_or_else(|| PathBuf::from("tmp"));
    let store = FsStore::load(&path).await?;
    println!("Using store at: {}", path.display());
    let mut rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };
    let blobs = create_random_blobs(
        &store,
        args.num_blobs,
        |_, rand| rand.gen_range(1..=args.blob_size),
        &mut rng,
    )
    .await?;
    let hs = add_hash_sequences(
        &store,
        &blobs,
        args.hash_seqs,
        |_, rand| rand.gen_range(1..=args.hash_seq_size),
        &mut rng,
    )
    .await?;
    println!(
        "Created {} blobs and {} hash sequences",
        blobs.len(),
        hs.len()
    );
    for (i, info) in blobs.iter().enumerate() {
        println!("blob {i} {}", info.hash);
    }
    for (i, info) in hs.iter().enumerate() {
        println!("hash_seq {i} {}", info.hash);
    }
    let secret_key = get_or_generate_secret_key()?;
    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .bind()
        .await?;
    let (dump_task, events_tx) = dump_provider_events(args.allow_push);
    let blobs = iroh_blobs::net_protocol::Blobs::new(store, endpoint.clone(), Some(events_tx));
    let router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .spawn()
        .await?;
    let addr = router.endpoint().node_addr().await?;
    let ticket = NodeTicket::from(addr.clone());
    println!("Node address: {:?}", addr);
    println!("ticket:\n{}", ticket);
    ctrl_c().await?;
    router.shutdown().await?;
    dump_task.abort();
    Ok(())
}
