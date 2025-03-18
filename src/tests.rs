use iroh::{protocol::Router, Endpoint};
use n0_future::StreamExt;
use testresult::TestResult;
use tracing::info;

use crate::{
    get,
    hashseq::HashSeq,
    net_protocol::Blobs,
    store::fs::{
        tests::{test_data, INTERESTING_SIZES},
        FsStore,
    },
    Hash, HashAndFormat,
};

#[tokio::test]
async fn two_nodes_blobs() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db1_path = testdir.path().join("db1");
    let db2_path = testdir.path().join("db2");
    let store1 = FsStore::load(&db1_path).await?;
    let store2 = FsStore::load(&db2_path).await?;
    let sizes = INTERESTING_SIZES;
    for size in sizes {
        store1.import_bytes(test_data(size)).await.hash().await?;
    }
    let ep1 = Endpoint::builder().discovery_n0().bind().await?;
    let ep2 = Endpoint::builder().bind().await?;
    let blobs1 = Blobs::new(&store1, ep1.clone());
    let blobs2 = Blobs::new(&store2, ep2.clone());
    let r1 = Router::builder(ep1)
        .accept(crate::ALPN, blobs1)
        .spawn()
        .await?;
    let r2 = Router::builder(ep2)
        .accept(crate::ALPN, blobs2)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    for size in sizes {
        let hash = Hash::new(test_data(size));
        // let data = get::request::get_blob(conn.clone(), hash).bytes().await?;
        get::db::get_all(conn.clone(), hash, &store2).await?;
    }
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_hash_seq() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db1_path = testdir.path().join("db1");
    let db2_path = testdir.path().join("db2");
    let store1 = FsStore::load(&db1_path).await?;
    let store2 = FsStore::load(&db2_path).await?;
    let sizes = INTERESTING_SIZES;
    let mut hashes = Vec::new();
    for size in sizes {
        let hash = store1.import_bytes(test_data(size)).await.hash().await?;
        hashes.push(hash);
    }
    let hash_seq = hashes.into_iter().collect::<HashSeq>();
    let root = store1.import_bytes(hash_seq).await.hash().await?;
    let ep1 = Endpoint::builder().discovery_n0().bind().await?;
    let ep2 = Endpoint::builder().bind().await?;
    let blobs1 = Blobs::new(&store1, ep1.clone());
    let blobs2 = Blobs::new(&store2, ep2.clone());
    let r1 = Router::builder(ep1)
        .accept(crate::ALPN, blobs1)
        .spawn()
        .await?;
    let r2 = Router::builder(ep2)
        .accept(crate::ALPN, blobs2)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    get::db::get_all(conn, HashAndFormat::hash_seq(root), &store2).await?;
    Ok(())
}

/// A node serves a hash sequence with all the interesting sizes.
///
/// The client requests the hash sequence and the children, but does not store the data.
#[tokio::test]
async fn node_serve_hash_seq() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    let mut hashes = Vec::new();
    // add all the sizes
    for size in sizes {
        let hash = store.import_bytes(test_data(size)).await.hash().await?;
        hashes.push(hash);
    }
    let hash_seq = hashes.into_iter().collect::<HashSeq>();
    let root = store.import_bytes(hash_seq).await.hash().await?;
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

/// A node serves individual blobs with all the interesting sizes.
///
/// The client requests them all one by one, but does not store it.
#[tokio::test]
async fn node_serve_blobs() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    // add all the sizes
    for size in sizes {
        store.import_bytes(test_data(size)).await.hash().await?;
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
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let hash = store
        .import_bytes(b"hello world".to_vec())
        .await
        .hash()
        .await?;
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
    let (size, stats) = get::request::get_unverified_size(&conn, &hash).await?;
    info!("size: {} stats: {:?}", size, stats);
    let data = get::request::get_blob(conn, hash).bytes().await?;
    assert_eq!(data.as_ref(), b"hello world");
    r1.shutdown().await?;
    Ok(())
}
