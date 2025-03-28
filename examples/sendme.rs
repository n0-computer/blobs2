use bao_tree::{ChunkNum, ChunkRanges};
use blobs2::{
    get::db::{execute_request, Dialer, GetConnection},
    protocol::{GetRequest, RangeSpecSeq},
    store::fs::FsStore,
    ticket::BlobTicket,
    HashAndFormat,
};
use clap::Parser;
use iroh::endpoint::Connection;

#[derive(Parser, Debug)]
pub struct ReceiveArgs {
    /// The ticket to use to connect to the sender.
    pub ticket: BlobTicket,
}

#[allow(dead_code)]
async fn get_one_by_one(connection: Connection, content: HashAndFormat) -> anyhow::Result<()> {
    println!("Getting hash_seq: {}", content.hash);
    let hash_seq = blobs2::get::request::get_blob(connection.clone(), content.hash)
        .bytes()
        .await?;
    let hash_seq = blobs2::hashseq::HashSeq::try_from(hash_seq)?;
    for hash in hash_seq {
        println!("Getting blob: {}", hash);
        let _blob = blobs2::get::request::get_blob(connection.clone(), content.hash)
            .bytes()
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    // let args = ReceiveArgs::parse();
    // let ticket = args.ticket;
    let ticket: BlobTicket = "blobadp7jtzebap5f7qkyqs3bqolu2jidzquqsbjnyzvwm2psfn5rztbaajdnb2hi4dthixs6zlvo4ys2mjoojswyylzfzuxe33ifzxgk5dxn5zgwlrpaqaauaqaakm3sayaj57y2b62muae674na6m3sayaycuab4uzxebqc6bra2oajzeub2khp26aibktirqlu44ejzmf2a4lww7kdyffrfrj".parse().unwrap();
    let dirname = format!(".sendme2-recv-{}", ticket.hash().to_hex());
    let store = FsStore::load(dirname).await?;
    let blobs = store.blobs();
    // let blobs = Blobs::new(store.clone());
    let endpoint = iroh::Endpoint::builder().bind().await?;
    // let router = Router::builder(endpoint).accept(blobs2::ALPN, blobs.clone()).build();
    let addr = ticket.node_addr().clone();
    let content = ticket.hash_and_format();
    store.dump().await?;
    // let hs = blobs.export_bytes(ticket.hash()).await?;
    // println!("hs: {}", hs.len() / 32);
    // let hs = HashSeq::try_from(hs)?;
    // for hash in hs {
    //     let data = blobs.export_bytes(hash).await?;
    //     let bitfield = blobs.get_missing(hash).await?;
    //     println!("Got hash: {} {} {:?}", hash, data.len(), bitfield);
    // }
    // let ranges = blobs.get_missing(content).await?;
    // let ranges: RangeSpecSeq = RangeSpecSeq::verified_child_sizes();
    let mut dialer = Dialer::new(endpoint.clone(), ticket.node_addr().clone());
    let conn = dialer.connection().await?;
    println!("Connected to {:?}", addr);
    // let ranges = RangeSpecSeq::from_ranges_infinite([
    //     ChunkRanges::all(),
    //     ChunkRanges::from(ChunkNum(u64::MAX)..),
    // ]);
    // println!("Ranges: {:?}", ranges);
    // execute_request(&store, conn, GetRequest::new(ticket.hash(), ranges)).await?;
    // store.dump().await?;
    // return Ok(());
    let stats = get_one_by_one(conn, content);
    // let stats = blobs2::get::db::get_all(dialer, content, &store);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::select! {
        _ = ctrl_c => {
            println!("Ctrl-C received, shutting down");
        }
        stats = stats => {
            if let Err(cause) = stats {
                eprintln!("Error: {} {:#?}", cause, cause.backtrace());
            }
            println!("Done");
        }
    }
    // let ranges = blobs2::get::db::get_missing(content, &store).await?;
    // println!("Missing ranges {:?}", ranges);
    endpoint.close().await;
    store.dump().await?;
    store.shutdown().await?;
    Ok(())
}
