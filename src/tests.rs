use std::path::PathBuf;

use bao_tree::ChunkRanges;
use bytes::Bytes;
use iroh::{Endpoint, protocol::Router};
use irpc::channel::spsc;
use n0_future::{StreamExt, pin};
use tempfile::TempDir;
use testresult::TestResult;
use tokio::select;
use tracing::info;

use crate::{
    BlobFormat, Hash, HashAndFormat,
    api::Store,
    get,
    hashseq::HashSeq,
    net_protocol::Blobs,
    protocol::{GetRequest, RangeSpec, RangeSpecSeq},
    store::fs::{
        FsStore,
        tests::{INTERESTING_SIZES, create_n0_bao, test_data},
    },
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
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store1.add_bytes(test_data(size)).await?);
    }
    let ep1 = Endpoint::builder().discovery_n0().bind().await?;
    let ep2 = Endpoint::builder().bind().await?;
    let blobs1 = Blobs::new(&store1, ep1.clone(), None);
    let blobs2 = Blobs::new(&store2, ep2.clone(), None);
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
        store2.download().fetch(conn.clone(), hash, None).await?;
    }
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

pub async fn add_test_hash_seq(
    blobs: &Store,
    sizes: impl IntoIterator<Item = usize>,
) -> TestResult<HashAndFormat> {
    let batch = blobs.batch().await?;
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(batch.add_bytes(test_data(size)).await?);
    }
    let hash_seq = tts.iter().map(|tt| *tt.hash()).collect::<HashSeq>();
    let root = batch
        .add_bytes_with_opts((hash_seq, BlobFormat::HashSeq))
        .with_named_tag("hs")
        .await?;
    Ok(root)
}

pub async fn add_test_hash_seq_incomplete(
    blobs: &Store,
    sizes: impl IntoIterator<Item = usize>,
    present: impl Fn(usize) -> ChunkRanges,
) -> TestResult<HashAndFormat> {
    let batch = blobs.batch().await?;
    let mut tts = Vec::new();
    for (i, size) in sizes.into_iter().enumerate() {
        let data = test_data(size);
        // figure out the ranges to import, and manually create a temp tag.
        let ranges = present(i + 1);
        let (hash, bao) = create_n0_bao(&data, &ranges)?;
        // why isn't import_bao_bytes returning a temp tag anyway?
        tts.push(batch.temp_tag(hash).await?);
        if !ranges.is_empty() {
            blobs.import_bao_bytes(hash, ranges, bao).await?;
        }
    }
    let hash_seq = tts.iter().map(|tt| *tt.hash()).collect::<HashSeq>();
    let hash_seq_bytes = Bytes::from(hash_seq);
    let ranges = present(0);
    let (root, bao) = create_n0_bao(&hash_seq_bytes, &ranges)?;
    let content = HashAndFormat::hash_seq(root);
    blobs.tags().create(content).await?;
    blobs.import_bao_bytes(root, ranges, bao).await?;
    Ok(content)
}

async fn check_presence(store: &Store, sizes: &[usize]) -> TestResult<()> {
    for size in sizes {
        let expected = test_data(*size);
        let hash = Hash::new(&expected);
        let actual = store
            .export_bao(hash, ChunkRanges::all())
            .data_to_bytes()
            .await?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

async fn two_node_test_setup() -> TestResult<(
    TempDir,
    (Router, FsStore, PathBuf),
    (Router, FsStore, PathBuf),
)> {
    let testdir = tempfile::tempdir().unwrap();
    let db1_path = testdir.path().join("db1");
    let db2_path = testdir.path().join("db2");
    let store1 = FsStore::load(&db1_path).await.unwrap();
    let store2 = FsStore::load(&db2_path).await.unwrap();
    let ep1 = Endpoint::builder().discovery_n0().bind().await.unwrap();
    let ep2 = Endpoint::builder().bind().await.unwrap();
    let blobs1 = Blobs::new(&store1, ep1.clone(), None);
    let blobs2 = Blobs::new(&store2, ep2.clone(), None);
    let r1 = Router::builder(ep1)
        .accept(crate::ALPN, blobs1)
        .spawn()
        .await?;
    let r2 = Router::builder(ep2)
        .accept(crate::ALPN, blobs2)
        .spawn()
        .await?;
    Ok((testdir, (r1, store1, db1_path), (r2, store2, db2_path)))
}

#[tokio::test]
async fn two_nodes_hash_seq() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup().await?;
    let addr1 = r1.endpoint().node_addr().await?;
    let sizes = INTERESTING_SIZES;
    let root = add_test_hash_seq(&store1, sizes).await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    store2.download().fetch(conn, root, None).await?;
    check_presence(&store2, &sizes).await?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_hash_seq_progress() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup().await?;
    let addr1 = r1.endpoint().node_addr().await?;
    let sizes = INTERESTING_SIZES;
    let root = add_test_hash_seq(&store1, sizes).await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    let (tx, rx) = spsc::channel::<u64>(16);
    let res = store2.download().fetch(conn, root, Some(tx));
    pin!(rx);
    pin!(res);
    loop {
        select! {
            total = rx.recv() => {
                println!("progress: {:?}", total);
            }
            res = &mut res => {
                res?;
                break;
            }
        }
    }
    check_presence(&store2, &sizes).await?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_size_request() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup().await?;
    let addr1 = r1.endpoint().node_addr().await?;
    let sizes = INTERESTING_SIZES;
    let root = add_test_hash_seq(&store1, sizes).await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    let sizes = store2.download().local(root).await?.complete_sizes();
    assert_eq!(sizes.ranges, RangeSpecSeq::verified_child_sizes());
    // get the first 3 items (hash_seq, and 2 children)
    store2
        .download()
        .execute(
            conn.clone(),
            GetRequest::new(
                root.hash,
                RangeSpecSeq::new([
                    RangeSpec::all(),
                    RangeSpec::all(),
                    RangeSpec::all(),
                    RangeSpec::EMPTY,
                ]),
            ),
            None,
        )
        .await?;
    // check that sizes for the data we have already are omitted
    let sizes = store2.download().local(root).await?.complete_sizes();
    assert_eq!(
        sizes.ranges,
        RangeSpecSeq::new([
            RangeSpec::EMPTY,
            RangeSpec::EMPTY,
            RangeSpec::EMPTY,
            RangeSpec::verified_size(),
            RangeSpec::verified_size(),
            RangeSpec::verified_size(),
            RangeSpec::verified_size(),
            RangeSpec::verified_size(),
            RangeSpec::verified_size(),
            RangeSpec::EMPTY
        ])
    );
    store2
        .download()
        .execute(conn.clone(), sizes, None)
        .await?;

    let local = store2.download().local(root).await?;
    // let missing_chunks = missing.chunks();
    // println!("{:?} {:?}", missing, missing_chunks);
    store2
        .download()
        .execute(conn, local.missing(), None)
        .await?;
    check_presence(&store2, &INTERESTING_SIZES).await?;
    // for size in INTERESTING_SIZES {
    //     let hash = Hash::new(test_data(size));
    //     let bitfield = store2.observe(hash).await?;
    //     println!("size: {} bitfield: {:?}", size, bitfield);
    // }
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
    let mut tts = Vec::new();
    // add all the sizes
    for size in sizes {
        let tt = store.add_bytes(test_data(size)).await?;
        tts.push(tt);
    }
    let hash_seq = tts.iter().map(|x| *x.hash()).collect::<HashSeq>();
    let root_tt = store.add_bytes(hash_seq).await?;
    let root = *root_tt.hash();
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone(), None);
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn()
        .await?;
    let addr1 = r1.endpoint().node_addr().await?;
    info!("node addr: {addr1:?}");
    let endpoint2 = Endpoint::builder().discovery_n0().bind().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    let (hs, sizes) = get::request::get_hash_seq_and_sizes(&conn, &root, 1024, None).await?;
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
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store.add_bytes(test_data(size)).await?);
    }
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone(), None);
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
        let actual = get::request::get_blob(conn.clone(), hash).await?;
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
    let tt = store.add_bytes(b"hello world".to_vec()).temp_tag().await?;
    let hash = *tt.hash();
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = crate::net_protocol::Blobs::new(store, endpoint.clone(), None);
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
    let data = get::request::get_blob(conn, hash).await?;
    assert_eq!(data.as_ref(), b"hello world");
    r1.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_export_chunk() -> TestResult {
    tracing_subscriber::fmt::try_init().ok();
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let blobs = store.blobs();
    for size in [1024 * 18 + 1] {
        let data = vec![0u8; size];
        let tt = store.add_slice(&data).temp_tag().await?;
        let hash = *tt.hash();
        let c = blobs.export_chunk(hash, 0).await;
        println!("{:?}", c);
        let c = blobs.export_chunk(hash, 1000000).await;
        println!("{:?}", c);
    }
    Ok(())
}
