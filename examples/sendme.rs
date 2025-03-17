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
    // let ticket: BlobTicket = "blobadez2n2x3z6hvrp53h5kty2l53xpcdqyv2jaasjqt2aapiuavvcdiajdnb2hi4dthixs6zlvo4ys2mjoojswyylzfzuxe33ifzxgk5dxn5zgwlrpaiaakd3fswf7wayaycuab4ul7mbqddbhyacyfy226kffhjqhq3vt2lj7ab2cam74wthkkrxtrbyl4s6y".parse().unwrap();
    let dirname = format!(".sendme2-recv-{}", ticket.hash().to_hex());
    let store = FsStore::load(dirname).await?;
    // let blobs = Blobs::new(store.clone());
    let endpoint = iroh::Endpoint::builder().bind().await?;
    // let router = Router::builder(endpoint).accept(blobs2::ALPN, blobs.clone()).build();
    let addr = ticket.node_addr().clone();
    let content = ticket.hash_and_format();
    let connection = endpoint.connect(addr.clone(), blobs2::ALPN).await?;
    // let test = blobs2::get::request::get_blob(
    //     connection.clone(),
    //     "ac87e3ab04a6b02f2347b94a11c4d04a4ffe21a22672033057e14396b5703b7f"
    //         .parse()
    //         .unwrap(),
    // )
    // .bytes()
    // .await?;
    // println!("Test: {:?} {}", test, test.len());
    let ranges = blobs2::get::db::get_missing(content, &store).await?;
    println!("Missing ranges {:?}", ranges);
    panic!();
    println!("Connected to {:?}", addr);
    // get_one_by_one(connection, content).await?;
    let stats = blobs2::get::db::get_all(connection, content, &store);
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
    endpoint.close().await;
    store.shutdown().await?;
    Ok(())
}
