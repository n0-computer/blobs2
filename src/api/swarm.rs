use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Deref,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, bail};
use genawaiter::sync::Gen;
use iroh::{Endpoint, NodeId, endpoint::Connection};
use irpc::channel::{oneshot, spsc};
use irpc_derive::rpc_requests;
use n0_future::{BufferedStreamExt, Stream, StreamExt, future};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize, de::Error};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
};
use tracing::instrument::Instrument;

use super::{Store, remote::GetConnection};
use crate::{
    Hash, HashAndFormat,
    downloader::{self, DownloadError, Downloader},
    get::{GetError, Stats},
    protocol::{GetManyRequest, GetRequest},
    store,
};

#[derive(Debug, Clone)]
pub struct Swarm {
    client: irpc::Client<SwarmMsg, SwarmProtocol, SwarmService>,
}

#[derive(Debug, Clone)]
pub struct SwarmService;

impl irpc::Service for SwarmService {}

#[rpc_requests(SwarmService, message = SwarmMsg, alias = "Msg")]
#[derive(Debug, Serialize, Deserialize)]
enum SwarmProtocol {
    #[rpc(tx = spsc::Sender<DownloadProgessItem>)]
    Download(DownloadRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    AddProvider(AddProviderRequest),
}

struct SwarmActor {
    store: Store,
    pool: ConnectionPool,
    tasks: JoinSet<()>,
    running: HashSet<tokio::task::Id>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgessItem {
    #[serde(skip)]
    Error(anyhow::Error),
    DownloadError,
    PartComplete {
        hash: Hash,
    },
}

impl SwarmActor {
    fn new(store: Store, endpoint: Endpoint) -> Self {
        Self {
            store,
            pool: ConnectionPool::new(endpoint, crate::ALPN.to_vec()),
            tasks: JoinSet::new(),
            running: HashSet::new(),
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<SwarmMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                SwarmMsg::Download(request) => {
                    self.spawn(handle_download(
                        self.store.clone(),
                        self.pool.clone(),
                        request,
                    ));
                }
                SwarmMsg::AddProvider(request) => {
                    todo!()
                }
            }
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        let span = tracing::Span::current();
        let id = self.tasks.spawn(fut.instrument(span)).id();
        self.running.insert(id);
    }
}

async fn handle_add_provider(mut downloader: Downloader, msg: AddProviderMsg) {
    let AddProviderMsg {
        inner: AddProviderRequest { hash, providers },
        tx,
        ..
    } = msg;
    downloader.nodes_have(hash, providers).await;
    tx.send(()).await.ok();
}

async fn handle_download(store: Store, pool: ConnectionPool, msg: DownloadMsg) {
    let DownloadMsg { inner, mut tx, .. } = msg;
    if let Err(cause) = handle_download_impl(store, pool, inner, &mut tx).await {
        tx.send(DownloadProgessItem::Error(cause)).await.ok();
    }
}

async fn handle_download_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut spsc::Sender<DownloadProgessItem>,
) -> anyhow::Result<()> {
    match request.strategy {
        SplitStrategy::Split => handle_download_split_impl(store, pool, request, tx).await?,
        SplitStrategy::None => match request.request {
            FiniteRequest::Get(get) => {
                execute_get(&pool, get, &request.providers, &store).await?;
            }
            FiniteRequest::GetMany(_) => {
                todo!()
            }
        },
    }
    Ok(())
}

async fn handle_download_split_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut spsc::Sender<DownloadProgessItem>,
) -> anyhow::Result<()> {
    let providers = request.providers;
    let requests = split_request(request.request, &providers, &pool, &store)
        .await?;
    // todo: this is it's own mini actor, we should probably refactor this out
    let mut n = requests.len();
    let (requests_tx, requests_rx) = mpsc::channel::<GetRequest>(n);
    let permits = requests_tx.reserve_many(n).await.unwrap();
    for (request, permit) in requests.into_iter().zip(permits) {
        permit.send(request);
    }
    let mut futs = into_stream(requests_rx)
        .map(|request| {
            let pool = pool.clone();
            let providers = providers.clone();
            let store = store.clone();
            async move {
                let hash = request.hash;
                let res = execute_get(&pool, request, &providers, &store).await;
                (hash, res)
            }
        })
        .buffered_unordered(32);
    loop {
        tokio::select! {
            res = futs.next() => {
                println!("Got result: {:?}", res);
                n -= 1;
                match res {
                    Some((hash, Ok(stats))) => {
                        tx.send(DownloadProgessItem::PartComplete { hash }).await?;
                    }
                    Some((hash, Err(e))) => {
                        tx.send(DownloadProgessItem::DownloadError).await?;
                    }
                    None => break,
                }
                if n == 0 {
                    // All requests have been processed.
                    break;
                }
            }
            _ = tx.closed() => {
                // The sender has been closed, we should stop processing.
                break;
            }
        }
    }
    Ok(())
}

fn into_stream<T>(mut recv: mpsc::Receiver<T>) -> impl Stream<Item = T> {
    Gen::new(|co| async move {
        while let Some(item) = recv.recv().await {
            co.yield_(item).await;
        }
    })
}

#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum FiniteRequest {
    Get(GetRequest),
    GetMany(GetManyRequest),
}

pub trait SupportedRequest {
    fn into_request(self) -> FiniteRequest;
}

impl<I: Into<Hash>, T: IntoIterator<Item = I>> SupportedRequest for T {
    fn into_request(self) -> FiniteRequest {
        let hashes = self.into_iter().map(Into::into).collect::<GetManyRequest>();
        FiniteRequest::GetMany(hashes)
    }
}

impl SupportedRequest for GetRequest {
    fn into_request(self) -> FiniteRequest {
        FiniteRequest::Get(self)
    }
}

impl SupportedRequest for GetManyRequest {
    fn into_request(self) -> FiniteRequest {
        FiniteRequest::GetMany(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddProviderRequest {
    pub hash: Hash,
    pub providers: Vec<NodeId>,
}

#[derive(Debug)]
pub struct DownloadRequest {
    pub request: FiniteRequest,
    pub providers: Arc<dyn ContentDiscovery>,
    pub strategy: SplitStrategy,
}

impl DownloadRequest {
    pub fn new(
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
        strategy: SplitStrategy,
    ) -> Self {
        Self {
            request: request.into_request(),
            providers: Arc::new(providers),
            strategy,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SplitStrategy {
    None,
    Split,
}

impl Serialize for DownloadRequest {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "cannot serialize DownloadRequest",
        ))
    }
}

// Implement Deserialize to always fail
impl<'de> Deserialize<'de> for DownloadRequest {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize DownloadRequest"))
    }
}

pub type DownloadOptions = DownloadRequest;

pub struct DownloadProgress {
    fut: future::Boxed<Result<spsc::Receiver<DownloadProgessItem>, irpc::Error>>,
}

impl DownloadProgress {
    pub fn new(
        fut: future::Boxed<Result<spsc::Receiver<DownloadProgessItem>, irpc::Error>>,
    ) -> Self {
        Self { fut }
    }

    pub async fn stream(self) -> Result<impl Stream<Item = DownloadProgessItem>, irpc::Error> {
        let rx = self.fut.await?;
        Ok(rx.into_stream().map(|item| match item {
            Ok(item) => item,
            Err(e) => DownloadProgessItem::Error(e.into()),
        }))
    }

    async fn complete(self) -> anyhow::Result<()> {
        let rx = self.fut.await?;
        let mut stream = Box::pin(rx.into_stream());
        while let Some(item) = stream.next().await {
            match item? {
                DownloadProgessItem::Error(e) => Err(e)?,
                DownloadProgessItem::DownloadError => anyhow::bail!("Download error"),
                _ => {}
            }
        }
        Ok(())
    }
}

impl IntoFuture for DownloadProgress {
    type Output = anyhow::Result<()>;
    type IntoFuture = future::Boxed<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.complete())
    }
}

impl Swarm {
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        let (tx, rx) = mpsc::channel::<SwarmMsg>(32);
        let actor = SwarmActor::new(store.clone(), endpoint.clone());
        tokio::spawn(actor.run(rx));
        Self { client: tx.into() }
    }

    pub fn download(
        &self,
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
    ) -> DownloadProgress {
        let request = request.into_request();
        let providers = Arc::new(providers);
        self.download_with_opts(DownloadOptions {
            request,
            providers,
            strategy: SplitStrategy::None,
        })
    }

    pub fn download_with_opts(&self, options: DownloadOptions) -> DownloadProgress {
        let fut = self.client.server_streaming(options, 32);
        DownloadProgress::new(Box::pin(fut))
    }

    pub async fn add_provider(&self, hash: Hash, provider: NodeId) -> Result<(), irpc::Error> {
        let request = AddProviderRequest {
            hash,
            providers: vec![provider],
        };
        self.client.rpc(request).await
    }
}

/// Split a request into multiple requests that can be run in parallel.
async fn split_request(
    request: FiniteRequest,
    providers: &Arc<dyn ContentDiscovery>,
    pool: &ConnectionPool,
    store: &Store,
) -> anyhow::Result<Vec<GetRequest>> {
    Ok(match request {
        FiniteRequest::Get(req) => {
            let Some(first) = req.ranges.iter().next() else {
                return Ok(vec![]);
            };
            let first = GetRequest::blob(req.hash);
            execute_get(pool, first, providers, store).await?;
            let res = if req.ranges.is_infinite() {
                // todo: just leave the last one open
                vec![req]
            } else {
                req.ranges
                    .iter_non_empty_infinite()
                    .filter_map(|(i, ranges)| {
                        if i != 0 {
                            Some(
                                GetRequest::builder()
                                    .offset(i, ranges.clone())
                                    .build(req.hash),
                            )
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            println!("Split request: {:?}", res);
            res
        }
        FiniteRequest::GetMany(req) => req
            .hashes
            .into_iter()
            .enumerate()
            .map(|(i, hash)| GetRequest::blob_ranges(hash, req.ranges[i as u64].clone()))
            .collect(),
    })
}

#[derive(Debug, Clone)]
struct ConnectionPool {
    alpn: Vec<u8>,
    endpoint: Endpoint,
    connections: Arc<Mutex<HashMap<NodeId, Arc<Mutex<SlotState>>>>>,
    retry_delay: Duration,
}

#[derive(Debug, Default)]
enum SlotState {
    #[default]
    Initial,
    Connected(Connection),
    AttemptFailed(SystemTime),
    Evil(String),
}

impl ConnectionPool {
    fn new(endpoint: Endpoint, alpn: Vec<u8>) -> Self {
        Self {
            endpoint,
            alpn,
            connections: Default::default(),
            retry_delay: Duration::from_secs(5),
        }
    }

    fn dial(&self, id: NodeId) -> DialNode {
        DialNode {
            pool: self.clone(),
            id,
        }
    }

    async fn mark_evil(&self, id: NodeId, reason: String) {
        let slot = self.connections.lock().await.entry(id).or_default().clone();
        *slot.lock().await = SlotState::Evil(reason)
    }

    async fn mark_closed(&self, id: NodeId) {
        let slot = self.connections.lock().await.entry(id).or_default().clone();
        *slot.lock().await = SlotState::Initial
    }
}

/// Execute a get request sequentially.
///
/// It will try each provider in order
/// until it finds one that can fulfill the request. When trying a new provider,
/// it takes the progress from the previous providers into account, so e.g.
/// if the first provider had the first 10% of the data, it will only ask the next
/// provider for the remaining 90%.
/// 
/// This is fully sequential, so there will only be one request in flight at a time.
///
/// If the request is not complete after trying all providers, it will return an error.
/// If the provider stream never ends, it will try indefinitely.
async fn execute_get(
    pool: &ConnectionPool,
    request: GetRequest,
    providers: &Arc<dyn ContentDiscovery>,
    store: &Store,
) -> anyhow::Result<Stats> {
    let mut last_error = None;
    let remote = store.remote();
    let mut providers = providers.find_providers(request.content());
    while let Some(provider) = providers.next().await {
        let mut conn = pool.dial(provider);
        let local = remote.local_for_request(request.clone()).await?;
        if local.is_complete() {
            return Ok(Stats::default());
        }
        let conn = conn.connection().await?;
        match remote.execute(conn, local.missing(), None).await {
            Ok(stats) => {
                return Ok(stats);
            }
            Err(cause) => {
                last_error = Some(cause);
                continue;
            }
        }
    }
    if let Some(error) = last_error {
        return Err(error.into());
    }
    bail!("No providers found for hash: {}", request.hash);
}

struct DialNode {
    pool: ConnectionPool,
    id: NodeId,
}

impl GetConnection for DialNode {
    async fn connection(&mut self) -> anyhow::Result<Connection> {
        let slot = self
            .pool
            .connections
            .lock()
            .await
            .entry(self.id)
            .or_default()
            .clone();
        let mut guard = slot.lock().await;
        match guard.deref() {
            SlotState::Connected(conn) => {
                return Ok(conn.clone());
            }
            SlotState::AttemptFailed(time) => {
                let elapsed = time.elapsed().unwrap_or_default();
                if elapsed <= self.pool.retry_delay {
                    bail!(
                        "Connection attempt failed {} seconds ago",
                        elapsed.as_secs_f64()
                    );
                }
            }
            SlotState::Evil(reason) => {
                bail!("Node is banned due to evil behavior: {reason}");
            }
            SlotState::Initial => {}
        }
        let res = self.pool.endpoint.connect(self.id, &self.pool.alpn).await;
        match &res {
            Ok(conn) => {
                *guard = SlotState::Connected(conn.clone());
            }
            Err(e) => {
                *guard = SlotState::AttemptFailed(SystemTime::now());
                bail!("Failed to connect to node: {}", e);
            }
        }
        res
    }
}

/// Trait for pluggable content discovery strategies.
pub trait ContentDiscovery: Debug + Send + Sync + 'static {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<NodeId>;
}

impl<C, I> ContentDiscovery for C
    where
        C: Debug + Clone + IntoIterator<Item = I> + Send + Sync + 'static,
        C::IntoIter: Send + Sync + 'static,
        I: Into<NodeId> + Send + Sync + 'static,
{
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<NodeId> {
        let providers = self.clone();
        n0_future::stream::iter(providers.into_iter().map(Into::into)).boxed()
    }
}

#[derive(derive_more::Debug)]
pub struct Shuffled {
    nodes: Vec<NodeId>,
}

impl Shuffled {
    fn new(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }
}

impl ContentDiscovery for Shuffled {
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<NodeId> {
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rand::thread_rng());
        n0_future::stream::iter(nodes).boxed()
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::ChunkRanges;
    use testresult::TestResult;

    use crate::{
        api::{
            blobs::AddBytesOptions,
            swarm::{DownloadOptions, Shuffled, SplitStrategy, Swarm},
        },
        hashseq::HashSeq,
        protocol::{GetManyRequest, GetRequest},
        tests::node_test_setup,
    };

    #[tokio::test]
    async fn swarm_get_many_smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _) = node_test_setup(testdir.path().join("a")).await?;
        let (r2, store2, _) = node_test_setup(testdir.path().join("b")).await?;
        let (r3, store3, _) = node_test_setup(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let node1_addr = r1.endpoint().node_addr().await?;
        let node1_id = node1_addr.node_id;
        let node2_addr = r2.endpoint().node_addr().await?;
        let node2_id = node2_addr.node_id;
        // let conn = r2.endpoint().connect(node1_addr, crate::ALPN).await?;
        // store2.remote().fetch(conn, *tt.hash(), None).await?;
        let swarm = Swarm::new(&store3, r3.endpoint());
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetManyRequest::builder()
            .hash(*tt1.hash(), ChunkRanges::all())
            .hash(*tt2.hash(), ChunkRanges::all())
            .build();
        let progress = swarm.download(request, Shuffled::new(vec![node1_id, node2_id]));
        progress.complete().await?;
        Ok(())
    }

    #[tokio::test]
    async fn swarm_get_smoke() -> TestResult<()> {
        // tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _) = node_test_setup(testdir.path().join("a")).await?;
        let (r2, store2, _) = node_test_setup(testdir.path().join("b")).await?;
        let (r3, store3, _) = node_test_setup(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let hs = [*tt1.hash(), *tt2.hash()].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let root2 = store2
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().node_addr().await?;
        let node1_id = node1_addr.node_id;
        let node2_addr = r2.endpoint().node_addr().await?;
        let node2_id = node2_addr.node_id;
        // let conn = r2.endpoint().connect(node1_addr, crate::ALPN).await?;
        // store2.remote().fetch(conn, *tt.hash(), None).await?;
        let swarm = Swarm::new(&store3, r3.endpoint());
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetRequest::builder()
            .root(ChunkRanges::all())
            .next(ChunkRanges::all())
            .next(ChunkRanges::all())
            .build(*root.hash());
        if true {
            let progress = swarm.download_with_opts(DownloadOptions::new(request, [node1_id, node2_id], SplitStrategy::None));
            progress.complete().await?;
        }
        if false {
            let conn = r3.endpoint().connect(node1_addr, crate::ALPN).await?;
            let remote = store3.remote();
            let rh = remote
                .execute(
                    conn.clone(),
                    GetRequest::builder()
                        .root(ChunkRanges::all())
                        .build(*root.hash()),
                    None,
                )
                .await?;
            let h1 = remote.execute(
                conn.clone(),
                GetRequest::builder()
                    .child(0, ChunkRanges::all())
                    .build(*root.hash()),
                None,
            );
            let h2 = remote.execute(
                conn.clone(),
                GetRequest::builder()
                    .child(1, ChunkRanges::all())
                    .build(*root.hash()),
                None,
            );
            h1.await?;
            h2.await?;
        }
        Ok(())
    }
}
