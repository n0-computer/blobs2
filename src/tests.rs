use iroh::{protocol::Router, Endpoint};
use testresult::TestResult;
use tracing::info;

use crate::{get, protocol::GetRequest};

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
    r1.shutdown().await?;
    Ok(())
}
