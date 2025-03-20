use std::{
    hash,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
};

use blobs2::{
    store::{api::ImportProgress, fs::FsStore, Blobs, Store},
    Hash,
};
use n0_future::StreamExt;
use testresult::TestResult;
use tracing::info;

/// Interesting sizes for testing.
pub const INTERESTING_SIZES: [usize; 8] = [
    0,               // annoying corner case - always present, handled by the api
    1,               // less than 1 chunk, data inline, outboard not needed
    1024,            // exactly 1 chunk, data inline, outboard not needed
    1024 * 16 - 1,   // less than 1 chunk group, data inline, outboard not needed
    1024 * 16,       // exactly 1 chunk group, data inline, outboard not needed
    1024 * 16 + 1,   // data file, outboard inline (just 1 hash pair)
    1024 * 1024,     // data file, outboard inline (many hash pairs)
    1024 * 1024 * 8, // data file, outboard file
];

async fn blobs_smoke(path: &Path, blobs: &Blobs) -> TestResult<()> {
    // test importing and exporting bytes
    {
        let expected = b"hello".to_vec();
        let expected_hash = Hash::new(&expected);
        let hash = blobs.import_bytes(expected.clone()).hash().await?;
        assert_eq!(hash, expected_hash);
        let actual = blobs.export_bytes(hash).await?;
        assert_eq!(actual, expected);
    }

    // test importing and exporting a file
    {
        let expected = b"somestuffinafile".to_vec();
        let temp1 = path.join("test1");
        std::fs::write(&temp1, &expected)?;
        let hash = blobs.import_path(temp1).hash().await?;
        let expected_hash = Hash::new(&expected);
        assert_eq!(hash, expected_hash);

        let temp2 = path.join("test2");
        blobs.export_path(hash, &temp2).finish().await?;
        let actual = std::fs::read(&temp2)?;
        assert_eq!(actual, expected);
    }

    // test importing a large file with progress
    {
        let expected = vec![0u8; 1024 * 1024 * 1024];
        let temp1 = path.join("test3");
        std::fs::write(&temp1, &expected)?;
        let mut stream = blobs.import_path(temp1).stream().await?;
        let mut res = None;
        while let Some(item) = stream.next().await {
            info!("Progress: {:?}", item);
            if let ImportProgress::Done { hash } = item {
                res = Some(hash);
                break;
            }
        }
        let expected_hash = Hash::new(&expected);
        assert_eq!(res, Some(expected_hash));
    }
    Ok(())
}

#[tokio::test]
async fn blobs_smoke_fs() -> TestResult {
    tracing_subscriber::fmt::try_init().ok();
    let td = tempfile::tempdir()?;
    let store = FsStore::load(td.path().join("blobs.db")).await?;
    blobs_smoke(td.path(), store.blobs()).await?;
    Ok(())
}

#[tokio::test]
async fn blobs_smoke_rpc() -> TestResult {
    tracing_subscriber::fmt::try_init().ok();
    let unspecified = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let (server, cert) = quic_rpc::util::make_server_endpoint(unspecified)?;
    let client = quic_rpc::util::make_client_endpoint(unspecified, &[cert.as_ref()])?;
    let td = tempfile::tempdir()?;
    let store = FsStore::load(td.path().join("blobs.db")).await?;
    tokio::spawn(store.clone().listen(server.clone()));
    let api = Store::connect(client, server.local_addr()?);
    blobs_smoke(td.path(), api.blobs()).await?;
    Ok(())
}
