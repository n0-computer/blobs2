use core::hash;

use iroh::{protocol::Router, Endpoint};
use n0_future::StreamExt;
use testresult::TestResult;
use tracing::info;

use crate::{
    get,
    hashseq::HashSeq,
    store::fs::tests::{test_data, INTERESTING_SIZES},
    Hash,
};

#[tokio::test]
async fn node_serve_hash_seq() -> TestResult<()> {
    tracing_subscriber::fmt::try_init()?;
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::DbStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    let mut hashes = Vec::new();
    // add all the sizes
    for size in sizes {
        let hash = store.import_bytes(test_data(size)).await?;
        hashes.push(Hash::from(hash));
    }
    let hash_seq = hashes.into_iter().collect::<HashSeq>();
    let root = Hash::from(store.import_bytes(hash_seq).await?);
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone());
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    info!("node addr: {addr1:?}");
    let endpoint2 = Endpoint::builder().discovery_n0().bind().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    let (hs, sizes) = get::request::get_hash_seq_and_sizes(&conn, &root, 1024).await?;
    println!("hash seq: {:?}", hs);
    println!("sizes: {:?}", sizes);
    r1.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn node_sizes() -> TestResult<()> {
    tracing_subscriber::fmt::try_init()?;
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::DbStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    // add all the sizes
    for size in sizes {
        store.import_bytes(test_data(size)).await?;
    }
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone());
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    info!("node addr: {addr1:?}");
    let endpoint2 = Endpoint::builder().discovery_n0().bind().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    for size in sizes {
        let expected = test_data(size);
        let hash = Hash::new(&expected);
        let mut stream = get::request::get_blob(conn.clone(), hash);
        while let Some(item) = stream.next().await {
            println!("{:?}", item);
        }
        let actual = get::request::get_blob(conn.clone(), hash).bytes().await?;
        assert_eq!(actual.len(), expected.len(), "size: {}", size);
    }
    r1.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn node_smoke() -> TestResult<()> {
    tracing_subscriber::fmt::try_init()?;
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::DbStore::load(&db_path).await?;
    let hash = store.import_bytes(b"hello world".to_vec()).await?;
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone());
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    info!("node addr: {addr1:?}");
    let endpoint2 = Endpoint::builder().discovery_n0().bind().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    let (size, stats) = get::request::get_unverified_size(&conn, &hash.into()).await?;
    info!("size: {} stats: {:?}", size, stats);
    let data = get::request::get_blob(conn, hash.into()).bytes().await?;
    assert_eq!(data.as_ref(), b"hello world");
    r1.shutdown().await?;
    Ok(())
}
