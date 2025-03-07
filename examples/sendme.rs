use blobs2::{store::fs::FsStore, ticket::BlobTicket, HashAndFormat};
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
    let args = ReceiveArgs::parse();
    let ticket = args.ticket;
    // let ticket: BlobTicket = "blobaclnb5ydo34g46l57wpcgjnkquwd5bzuhrvxshzhbybh4x5pr5usaajdnb2hi4dthixs6zlvo4ys2mjoojswyylzfzuxe33ifzxgk5dxn5zgwlrpaiaakd3fsxvimayaycuab4xkqybqdqeibyo2s474znqwd3oetwf2duzsgkzlh6tlb57yerg4drxaojzc".parse().unwrap();
    let store = FsStore::load("blobs").await?;
    let endpoint = iroh::Endpoint::builder().bind().await?;
    let addr = ticket.node_addr().clone();
    let content = ticket.hash_and_format();
    let connection = endpoint.connect(addr.clone(), blobs2::ALPN).await?;
    println!("Connected to {:?}", addr);
    // get_one_by_one(connection, content).await?;
    let stats = blobs2::get::db::get_all(connection, content, &store).await?;
    println!("Stats: {:?}", stats);
    endpoint.close().await;
    store.shutdown().await?;
    Ok(())
}
