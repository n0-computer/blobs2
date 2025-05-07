use std::{collections::HashSet, io};

use genawaiter::sync::Gen;
use iroh::NodeId;
use irpc::channel::{oneshot, spsc};
use irpc_derive::rpc_requests;
use n0_future::{BufferedStreamExt, Stream, StreamExt, future};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::instrument::Instrument;

use crate::{
    downloader::{self, DownloadError, Downloader}, get::Stats, protocol::{GetManyRequest, GetRequest}, Hash
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
    downloader: downloader::Downloader,
    tasks: JoinSet<()>,
    running: HashSet<tokio::task::Id>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgessItem {
    Error(#[serde(with = "crate::util::serde::io_error_serde")] io::Error),
    DownloadError(DownloadError),
    PartComplete { hash: Hash, stats: Stats },
}

impl SwarmActor {
    fn new(downloader: downloader::Downloader) -> Self {
        Self {
            downloader,
            tasks: JoinSet::new(),
            running: HashSet::new(),
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<SwarmMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                SwarmMsg::Download(request) => {
                    self.spawn(handle_download(self.downloader.clone(), request));
                }
                SwarmMsg::AddProvider(request) => {
                    self.spawn(handle_add_provider(self.downloader.clone(), request));
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

async fn handle_add_provider(
    mut downloader: Downloader,
    msg: AddProviderMsg,
) {
    let AddProviderMsg {
        inner: AddProviderRequest { hash, providers },
        tx,
        ..
    } = msg;
    downloader.nodes_have(hash, providers).await;
    tx.send(()).await.ok();
}

async fn handle_download(downloader: Downloader, msg: DownloadMsg) {
    let DownloadMsg {
        inner,
        mut tx,
        ..
    } = msg;
    if let Err(cause) = handle_download_impl(downloader, inner, &mut tx).await {
        tx.send(DownloadProgessItem::Error(cause)).await.ok();
    }
}

fn into_stream<T>(mut recv: mpsc::Receiver<T>) -> impl Stream<Item = T> {
    Gen::new(|co| {
        async move {
            while let Some(item) = recv.recv().await {
                co.yield_(item).await;
            }
        }
    })
}

async fn handle_download_impl(
    downloader: Downloader,
    request: DownloadRequest,
    tx: &mut spsc::Sender<DownloadProgessItem>,
) -> io::Result<()> {
    let requests = split_request(request.request).await;
    let providers = request.providers;
    // todo: this is it's own mini actor, we should probably refactor this out
    let mut n = requests.len();
    let (requests_tx, requests_rx) = mpsc::channel::<GetRequest>(n);
    let permits = requests_tx.reserve_many(n).await.unwrap();
    for (request, permit) in requests.into_iter().zip(permits) {
        permit.send(request);
    }
    let mut futs = into_stream(requests_rx)
        .map(|request| {
            let downloader = downloader.clone();
            let providers = providers.clone();
            async move {
                let hash = request.hash;
                let download_request =
                    downloader::DownloadRequest::new(request, providers);
                let handle = downloader.queue(download_request).await;
                let res = handle.await;
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
                        tx.send(DownloadProgessItem::PartComplete { hash, stats }).await?;
                    }
                    Some((hash, Err(e))) => {
                        match e {
                            DownloadError::NoProviders => {
                                
                            }
                            _ => {}
                        }
                        tx.send(DownloadProgessItem::DownloadError(e)).await?;
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

#[derive(Debug, Serialize, Deserialize)]
pub enum FiniteRequest {
    Get(GetRequest),
    GetMany(GetManyRequest),
}

pub trait SupportedRequest {
    fn into_request(self) -> FiniteRequest;
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

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub request: FiniteRequest,
    pub providers: Vec<NodeId>,
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
                DownloadProgessItem::DownloadError(e) => Err(e)?,
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
    pub fn new(downloader: Downloader) -> Self {
        let (tx, rx) = mpsc::channel::<SwarmMsg>(32);
        let actor = SwarmActor::new(downloader);
        tokio::spawn(actor.run(rx));
        Self { client: tx.into() }
    }

    pub fn download(&self, request: impl SupportedRequest, providers: impl AsRef<[NodeId]>) -> DownloadProgress {
        let request = request.into_request();
        let providers = providers.as_ref().to_vec();
        self.download_with_opts(DownloadOptions { request, providers })
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
async fn split_request(request: FiniteRequest) -> Vec<GetRequest> {
    match request {
        FiniteRequest::Get(req) => {
            let res = if req.ranges.is_infinite() {
                // todo: just leave the last one open
                vec![req]
            } else {
                req.ranges.iter_non_empty_infinite().map(|(i, ranges)| {
                    GetRequest::builder().offset(i, ranges.clone()).build(req.hash)
                }).collect()
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
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::ChunkRanges;
    use testresult::TestResult;

    use crate::{
        api::swarm::Swarm, downloader::Downloader, protocol::GetManyRequest, tests::node_test_setup
    };

    #[tokio::test]
    async fn swarm_smoke() -> TestResult<()> {
        tracing_subscriber::fmt::try_init().ok();
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
        let dl3 = Downloader::new(store3.clone(), r3.endpoint().clone());
        let swarm = Swarm::new(dl3);
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetManyRequest::builder()
            .hash(*tt1.hash(), ChunkRanges::all())
            .hash(*tt2.hash(), ChunkRanges::all())
            .build();
        let progress = swarm.download(request, [node1_id, node2_id]);
        progress.complete().await?;
        Ok(())
    }
}
