// //! Mutable in-memory blob store.
// //!
// //! Being a memory store, this store has to import all data into memory before it can
// //! serve it. So the amount of data you can serve is limited by your available memory.
// //! Other than that this is a fully featured store that provides all features such as
// //! tags and garbage collection.
// //!
// //! For many use cases this can be quite useful, since it does not require write access
// //! to the file system.
// use std::{
//     collections::{BTreeMap, HashMap},
//     io::{self, Write},
//     num::NonZeroU64,
//     ops::{Deref, DerefMut},
//     sync::{Arc, RwLock},
//     time::SystemTime,
// };

// use bao_tree::{
//     BaoTree, ChunkNum, ChunkRanges, ChunkRangesRef, TreeNode, blake3,
//     io::{
//         BaoContentItem,
//         mixed::{EncodedItem, ReadBytesAt, traverse_ranges_validated},
//         outboard::PreOrderMemOutboard,
//         sync::{Outboard, ReadAt, WriteAt},
//     },
// };
// use bytes::Bytes;
// use irpc::channel::spsc;
// use n0_future::{StreamExt, future::yield_now};
// use tokio::{
//     io::AsyncReadExt,
//     sync::mpsc,
//     task::{JoinError, JoinSet},
// };
// use tracing::{error, info, instrument};

// use super::{
//     BlobFormat,
//     util::{BaoTreeSender, PartialMemStorage, observer::BitfieldObserver},
// };
// use crate::{
//     Hash,
//     api::{
//         self, ApiClient,
//         blobs::{AddProgressItem, Bitfield, ExportProgressItem},
//         proto::{
//             BoxedByteStream, Command, CreateTagMsg, CreateTagRequest, DeleteTagsMsg,
//             DeleteTagsRequest, ExportBaoMsg, ExportBaoRequest, ExportPathMsg, ExportPathRequest,
//             ImportBaoMsg, ImportBaoRequest, ImportByteStreamMsg, ImportBytesMsg,
//             ImportBytesRequest, ImportPathMsg, ImportPathRequest, ListTagsMsg, ListTagsRequest,
//             ObserveMsg, ObserveRequest, RenameTagMsg, RenameTagRequest, SetTagMsg, SetTagRequest,
//             ShutdownMsg, SyncDbMsg,
//         },
//         tags::TagInfo,
//     },
//     store::{HashAndFormat, IROH_BLOCK_SIZE, util::Tag},
//     util::temp_tag::TempTag,
// };

// #[derive(Debug, Clone)]
// #[repr(transparent)]
// pub struct MemStore {
//     client: ApiClient,
// }

// impl AsRef<crate::api::Store> for MemStore {
//     fn as_ref(&self) -> &crate::api::Store {
//         crate::api::Store::ref_from_sender(&self.client)
//     }
// }

// impl Deref for MemStore {
//     type Target = crate::api::Store;

//     fn deref(&self) -> &Self::Target {
//         crate::api::Store::ref_from_sender(&self.client)
//     }
// }

// impl Default for MemStore {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl MemStore {
//     pub fn from_sender(client: ApiClient) -> Self {
//         Self { client }
//     }

//     pub fn new() -> Self {
//         let (sender, receiver) = mpsc::channel(32);
//         tokio::spawn(
//             Actor {
//                 commands: receiver,
//                 unit_tasks: JoinSet::new(),
//                 import_tasks: JoinSet::new(),
//                 state: State {
//                     data: HashMap::new(),
//                     tags: BTreeMap::new(),
//                 },
//             }
//             .run(),
//         );
//         Self::from_sender(sender.into())
//     }
// }

// struct Actor {
//     commands: mpsc::Receiver<Command>,
//     unit_tasks: JoinSet<()>,
//     import_tasks: JoinSet<anyhow::Result<ImportEntry>>,
//     state: State,
// }

// impl Actor {
//     async fn handle_command(&mut self, cmd: Command) -> Option<ShutdownMsg> {
//         match cmd {
//             Command::ImportBao(ImportBaoMsg {
//                 inner: ImportBaoRequest { hash, size },
//                 rx: data,
//                 tx,
//                 ..
//             }) => {
//                 let entry = self.state.data.entry(hash).or_default();
//                 self.unit_tasks
//                     .spawn(import_bao_task(entry.clone(), size, data, tx));
//             }
//             Command::Observe(ObserveMsg {
//                 inner: ObserveRequest { hash },
//                 tx,
//                 ..
//             }) => {
//                 let entry = self.state.data.entry(hash).or_default();
//                 let mut entry = entry.write().unwrap();
//                 let observer = entry.subscribe();
//                 self.unit_tasks.spawn(async move {
//                     observer.forward(tx).await.ok();
//                 });
//             }
//             Command::ImportBytes(ImportBytesMsg {
//                 inner: ImportBytesRequest { data, .. },
//                 tx,
//                 ..
//             }) => {
//                 self.import_tasks.spawn(import_bytes(data, tx));
//             }
//             Command::ImportByteStream(ImportByteStreamMsg { inner, tx, .. }) => {
//                 let stream = Box::pin(n0_future::stream::iter(inner.data).map(io::Result::Ok));
//                 self.import_tasks.spawn(import_byte_stream(stream, tx));
//             }
//             Command::ImportPath(cmd) => {
//                 self.import_tasks.spawn(import_path(cmd));
//             }
//             Command::ExportBao(ExportBaoMsg {
//                 inner: ExportBaoRequest { hash, ranges },
//                 tx,
//                 ..
//             }) => {
//                 let entry = self.state.data.entry(hash).or_default();
//                 self.unit_tasks
//                     .spawn(export_bao_task(hash, entry.clone(), ranges, tx));
//             }
//             Command::ExportPath(cmd) => {
//                 let entry = self.state.data.get(&cmd.inner.hash).cloned();
//                 self.unit_tasks.spawn(export_path_task(entry, cmd));
//             }
//             Command::DeleteTags(cmd) => {
//                 let DeleteTagsMsg {
//                     inner: DeleteTagsRequest { from, to },
//                     tx,
//                     ..
//                 } = cmd;
//                 info!("deleting tags from {:?} to {:?}", from, to);
//                 // state.tags.remove(&from.unwrap());
//                 // todo: more efficient impl
//                 self.state.tags.retain(|tag, _| {
//                     if let Some(from) = &from {
//                         if tag < from {
//                             return true;
//                         }
//                     }
//                     if let Some(to) = &to {
//                         if tag >= to {
//                             return true;
//                         }
//                     }
//                     info!("    removing {:?}", tag);
//                     false
//                 });
//                 tx.send(Ok(())).await.ok();
//             }
//             Command::RenameTag(cmd) => {
//                 let RenameTagMsg {
//                     inner: RenameTagRequest { from, to },
//                     tx,
//                     ..
//                 } = cmd;
//                 let tags = &mut self.state.tags;
//                 let value = match tags.remove(&from) {
//                     Some(value) => value,
//                     None => {
//                         tx.send(Err(api::Error::io(
//                             io::ErrorKind::NotFound,
//                             format!("tag not found: {:?}", from),
//                         )))
//                         .await
//                         .ok();
//                         return None;
//                     }
//                 };
//                 tags.insert(to, value);
//             }
//             Command::ListTags(cmd) => {
//                 let ListTagsMsg {
//                     inner:
//                         ListTagsRequest {
//                             from,
//                             to,
//                             raw,
//                             hash_seq,
//                         },
//                     tx,
//                     ..
//                 } = cmd;
//                 let tags = self
//                     .state
//                     .tags
//                     .iter()
//                     .filter(move |(tag, value)| {
//                         if let Some(from) = &from {
//                             if tag < &from {
//                                 return false;
//                             }
//                         }
//                         if let Some(to) = &to {
//                             if tag >= &to {
//                                 return false;
//                             }
//                         }
//                         raw && value.format.is_raw() || hash_seq && value.format.is_hash_seq()
//                     })
//                     .map(|(tag, value)| TagInfo {
//                         name: tag.clone(),
//                         hash: value.hash,
//                         format: value.format,
//                     })
//                     .map(Ok);
//                 tx.send(tags.collect()).await.ok();
//             }
//             Command::SetTag(SetTagMsg {
//                 inner: SetTagRequest { name: tag, value },
//                 tx,
//                 ..
//             }) => {
//                 self.state.tags.insert(tag, value);
//                 tx.send(Ok(())).await.ok();
//             }
//             Command::CreateTag(CreateTagMsg {
//                 inner: CreateTagRequest { value },
//                 tx,
//                 ..
//             }) => {
//                 let tag = Tag::auto(SystemTime::now(), |tag| self.state.tags.contains_key(tag));
//                 self.state.tags.insert(tag.clone(), value);
//                 tx.send(Ok(tag)).await.ok();
//             }
//             Command::CreateTempTag(_cmd) => {
//                 todo!()
//             }
//             Command::ListTempTags(_cmd) => {
//                 todo!()
//             }
//             Command::ListBlobs(_cmd) => {
//                 todo!()
//             }
//             Command::BlobStatus(_cmd) => {
//                 todo!()
//             }
//             Command::DeleteBlobs(_cmd) => {
//                 todo!()
//             }
//             Command::Batch(_cmd) => {
//                 todo!()
//             }
//             Command::ClearProtected(_cmd) => {
//                 todo!()
//             }
//             Command::SyncDb(SyncDbMsg { tx, .. }) => {
//                 tx.send(Ok(())).await.ok();
//             }
//             Command::Shutdown(cmd) => {
//                 return Some(cmd);
//             }
//         }
//         None
//     }

//     async fn finish_import(&mut self, res: Result<anyhow::Result<ImportEntry>, JoinError>) {
//         let mut import_data = match res {
//             Ok(Ok(entry)) => entry,
//             Ok(Err(e)) => {
//                 tracing::error!("import failed: {e}");
//                 return;
//             }
//             Err(e) => {
//                 if e.is_cancelled() {
//                     tracing::warn!("import task failed: {e}");
//                 } else {
//                     tracing::error!("import task panicked: {e}");
//                 }
//                 return;
//             }
//         };
//         let hash = import_data.outboard.root().into();
//         let size = import_data.data.len() as u64;
//         let entry = self.state.data.entry(hash).or_default();
//         {
//             let mut entry = entry.write().unwrap();
//             let Entry::Partial(incomplete) = entry.deref_mut() else {
//                 drop(entry);
//                 return;
//             };
//             incomplete.update(Bitfield::complete(size));
//             *entry =
//                 CompleteStorage::new(import_data.data, import_data.outboard.data.into()).into();
//         }
//         let tt = TempTag::new(
//             HashAndFormat {
//                 hash,
//                 format: BlobFormat::Raw,
//             },
//             None,
//         );
//         import_data.tx.send(AddProgressItem::Done(tt)).await.ok();
//     }

//     fn log_unit_task(&self, res: Result<(), JoinError>) {
//         if let Err(e) = res {
//             error!("task failed: {e}");
//         }
//     }

//     pub async fn run(mut self) {
//         let shutdown = loop {
//             tokio::select! {
//                 cmd = self.commands.recv() => {
//                     let Some(cmd) = cmd else {
//                         // last sender has been dropped.
//                         // exit immediately.
//                         break None;
//                     };
//                     if let Some(cmd) = self.handle_command(cmd).await {
//                         break Some(cmd);
//                     }
//                 }
//                 Some(res) = self.import_tasks.join_next(), if !self.import_tasks.is_empty() => {
//                     self.finish_import(res).await;
//                 }
//                 Some(res) = self.unit_tasks.join_next(), if !self.unit_tasks.is_empty() => {
//                     self.log_unit_task(res);
//                 }
//             }
//         };
//         if let Some(shutdown) = shutdown {
//             shutdown.tx.send(()).await.ok();
//         }
//     }
// }

// async fn import_bao_task(
//     entry: Arc<RwLock<Entry>>,
//     size: NonZeroU64,
//     mut stream: spsc::Receiver<BaoContentItem>,
//     tx: irpc::channel::oneshot::Sender<api::Result<()>>,
// ) {
//     let size = size.get();
//     if let Some(entry) = entry.write().unwrap().incomplete_mut() {
//         entry.size.write(0, size);
//     }
//     let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
//     while let Some(item) = stream.recv().await.unwrap() {
//         let mut guard = entry.write().unwrap();
//         let Some(entry) = guard.incomplete_mut() else {
//             // entry was completed somewhere else, no need to write
//             break;
//         };
//         match item {
//             BaoContentItem::Parent(parent) => {
//                 if let Some(offset) = tree.pre_order_offset(parent.node) {
//                     let mut pair = [0u8; 64];
//                     pair[..32].copy_from_slice(parent.pair.0.as_bytes());
//                     pair[32..].copy_from_slice(parent.pair.1.as_bytes());
//                     entry
//                         .outboard
//                         .write_at(offset * 64, &pair)
//                         .expect("writing to mem can never fail");
//                 }
//             }
//             BaoContentItem::Leaf(leaf) => {
//                 let start = leaf.offset;
//                 let end = start + (leaf.data.len() as u64);
//                 entry
//                     .data
//                     .write_at(start, &leaf.data)
//                     .expect("writing to mem can never fail");
//                 let added = ChunkRanges::from(ChunkNum::chunks(start)..ChunkNum::full_chunks(end));
//                 let added = entry.bitfield.with_state(|x| &added - &x.ranges);
//                 if added.is_empty() {
//                     continue;
//                 }
//                 let update = entry.bitfield.update(Bitfield::new(added, size));
//                 if update.new().complete {
//                     let data = std::mem::take(&mut entry.data);
//                     let outboard = std::mem::take(&mut entry.outboard);
//                     let data: Bytes = <Vec<u8>>::try_from(data).unwrap().into();
//                     let outboard: Bytes = <Vec<u8>>::try_from(outboard).unwrap().into();
//                     *guard = CompleteStorage::new(data, outboard).into();
//                 }
//             }
//         }
//     }
//     tx.send(Ok(())).await.ok();
// }

// async fn export_bao_task(
//     hash: Hash,
//     entry: Arc<RwLock<Entry>>,
//     ranges: ChunkRanges,
//     mut sender: spsc::Sender<EncodedItem>,
// ) {
//     let size = entry.read().unwrap().size();
//     let data = ExportData {
//         data: entry.clone(),
//     };
//     let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
//     let outboard = ExportOutboard {
//         hash: hash.into(),
//         tree,
//         data: entry.clone(),
//     };
//     let tx = BaoTreeSender::new(&mut sender);
//     traverse_ranges_validated(data, outboard, &ranges, tx)
//         .await
//         .ok();
// }

// async fn import_bytes(
//     data: Bytes,
//     mut tx: spsc::Sender<AddProgressItem>,
// ) -> anyhow::Result<ImportEntry> {
//     tx.send(AddProgressItem::Size(data.len() as u64)).await?;
//     tx.send(AddProgressItem::CopyDone).await?;
//     let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
//     Ok(ImportEntry { data, outboard, tx })
// }

// async fn import_byte_stream(
//     mut data: BoxedByteStream,
//     mut tx: spsc::Sender<AddProgressItem>,
// ) -> anyhow::Result<ImportEntry> {
//     let mut res = Vec::new();
//     while let Some(item) = data.next().await {
//         let item = item?;
//         res.extend_from_slice(&item);
//         tx.try_send(AddProgressItem::CopyProgress(res.len() as u64))
//             .await?;
//     }
//     import_bytes(res.into(), tx).await
// }

// #[instrument(skip_all, fields(path = %cmd.inner.path.display()))]
// async fn import_path(cmd: ImportPathMsg) -> anyhow::Result<ImportEntry> {
//     let ImportPathMsg {
//         inner: ImportPathRequest { path, .. },
//         mut tx,
//         ..
//     } = cmd;
//     let mut res = Vec::new();
//     let mut file = tokio::fs::File::open(path).await?;
//     let mut buf = [0u8; 1024 * 64];
//     loop {
//         let size = file.read(&mut buf).await?;
//         if size == 0 {
//             break;
//         }
//         res.extend_from_slice(&buf[..size]);
//         tx.send(AddProgressItem::CopyProgress(res.len() as u64))
//             .await?;
//     }
//     import_bytes(res.into(), tx).await
// }

// async fn export_path_task(entry: Option<Arc<RwLock<Entry>>>, cmd: ExportPathMsg) {
//     let ExportPathMsg { inner, mut tx, .. } = cmd;
//     let Some(entry) = entry else {
//         tx.send(ExportProgressItem::Error(api::Error::io(
//             io::ErrorKind::NotFound,
//             "hash not found",
//         )))
//         .await
//         .ok();
//         return;
//     };
//     match export_path_impl(entry, inner, &mut tx).await {
//         Ok(()) => tx.send(ExportProgressItem::Done).await.ok(),
//         Err(e) => tx.send(ExportProgressItem::Error(e.into())).await.ok(),
//     };
// }

// async fn export_path_impl(
//     entry: Arc<RwLock<Entry>>,
//     cmd: ExportPathRequest,
//     tx: &mut spsc::Sender<ExportProgressItem>,
// ) -> io::Result<()> {
//     let ExportPathRequest { target, .. } = cmd;
//     // todo: for partial entries make sure to only write the part that is actually present
//     let mut file = std::fs::File::create(target)?;
//     let size = entry.read().unwrap().size();
//     tx.send(ExportProgressItem::Size(size)).await?;
//     let mut buf = [0u8; 1024 * 64];
//     for offset in (0..size).step_by(1024 * 64) {
//         let len = std::cmp::min(size - offset, 1024 * 64) as usize;
//         let buf = &mut buf[..len];
//         entry.read().unwrap().data().read_exact_at(offset, buf)?;
//         file.write_all(buf)?;
//         tx.try_send(ExportProgressItem::CopyProgress(offset))
//             .await
//             .map_err(|_e| io::Error::other(""))?;
//         yield_now().await;
//     }
//     Ok(())
// }

// struct ImportEntry {
//     data: Bytes,
//     outboard: PreOrderMemOutboard,
//     tx: spsc::Sender<AddProgressItem>,
// }

// struct ExportOutboard {
//     hash: blake3::Hash,
//     tree: BaoTree,
//     data: Arc<RwLock<Entry>>,
// }

// struct ExportData {
//     data: Arc<RwLock<Entry>>,
// }

// impl ReadBytesAt for ExportData {
//     fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
//         let entry = self.data.read().unwrap();
//         entry.data().read_bytes_at(offset, size)
//     }
// }

// impl Outboard for ExportOutboard {
//     fn root(&self) -> blake3::Hash {
//         self.hash
//     }

//     fn tree(&self) -> BaoTree {
//         self.tree
//     }

//     fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
//         let Some(offset) = self.tree.pre_order_offset(node) else {
//             return Ok(None);
//         };
//         let mut buf = [0u8; 64];
//         let size = self
//             .data
//             .read()
//             .unwrap()
//             .outboard()
//             .read_at(offset * 64, &mut buf)?;
//         if size != 64 {
//             return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
//         }
//         let left: [u8; 32] = buf[..32].try_into().unwrap();
//         let right: [u8; 32] = buf[32..].try_into().unwrap();
//         Ok(Some((left.into(), right.into())))
//     }
// }

// struct State {
//     data: HashMap<Hash, Arc<RwLock<Entry>>>,
//     tags: BTreeMap<Tag, HashAndFormat>,
// }

// #[derive(Debug, derive_more::From)]
// enum Entry {
//     Partial(PartialMemStorage),
//     Complete(CompleteStorage),
// }

// impl Default for Entry {
//     fn default() -> Self {
//         Self::Partial(Default::default())
//     }
// }

// impl Entry {

//     fn data(&self) -> &[u8] {
//         match self {
//             Self::Partial(entry) => entry.data.as_ref(),
//             Self::Complete(entry) => &entry.data,
//         }
//     }

//     fn outboard(&self) -> &[u8] {
//         match self {
//             Self::Partial(entry) => entry.outboard.as_ref(),
//             Self::Complete(entry) => &entry.outboard,
//         }
//     }

//     fn size(&self) -> u64 {
//         match self {
//             Self::Partial(entry) => entry.current_size(),
//             Self::Complete(entry) => entry.size(),
//         }
//     }

//     fn incomplete_mut(&mut self) -> Option<&mut PartialMemStorage> {
//         match self {
//             Self::Partial(entry) => Some(entry),
//             Self::Complete(_) => None,
//         }
//     }
// }

// #[derive(Debug)]
// pub(crate) struct CompleteStorage {
//     pub(crate) data: Bytes,
//     pub(crate) outboard: Bytes,
// }

// impl CompleteStorage {
//     pub fn create(data: Bytes) -> (Hash, Self) {
//         let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
//         let hash = outboard.root().into();
//         let outboard = outboard.data.into();
//         let entry = Self::new(data, outboard);
//         (hash, entry)
//     }

//     pub fn subscribe(&mut self) -> BitfieldObserver {
//         BitfieldObserver::once(Bitfield::complete(self.size()))
//     }

//     pub fn new(data: Bytes, outboard: Bytes) -> Self {
//         Self { data, outboard }
//     }

//     pub fn size(&self) -> u64 {
//         self.data.len() as u64
//     }
// }

// #[allow(dead_code)]
// fn print_outboard(hashes: &[u8]) {
//     assert!(hashes.len() % 64 == 0);
//     for chunk in hashes.chunks(64) {
//         let left: [u8; 32] = chunk[..32].try_into().unwrap();
//         let right: [u8; 32] = chunk[32..].try_into().unwrap();
//         let left = blake3::Hash::from(left);
//         let right = blake3::Hash::from(right);
//         println!("l: {:?}, r: {:?}", left, right);
//     }
// }

// #[cfg(test)]
// mod tests {
//     use n0_future::StreamExt;
//     use testresult::TestResult;

//     use super::*;

//     #[tokio::test]
//     async fn smoke() -> TestResult<()> {
//         let store = MemStore::new();
//         let tt = store.add_bytes(vec![0u8; 1024 * 64]).temp_tag().await?;
//         let hash = *tt.hash();
//         println!("hash: {:?}", hash);
//         let mut stream = store.export_bao(hash, ChunkRanges::all()).stream();
//         while let Some(item) = stream.next().await {
//             println!("item: {:?}", item);
//         }
//         let stream = store.export_bao(hash, ChunkRanges::all());
//         let exported = stream.bao_to_vec().await?;

//         let store2 = MemStore::new();
//         let mut or = store2.observe(hash).stream().await?;
//         tokio::spawn(async move {
//             while let Some(event) = or.next().await {
//                 println!("event: {:?}", event);
//             }
//         });
//         store2
//             .import_bao_bytes(hash, ChunkRanges::all(), exported.clone())
//             .await?;

//         let exported2 = store2
//             .export_bao(hash, ChunkRanges::all())
//             .bao_to_vec()
//             .await?;
//         assert_eq!(exported, exported2);

//         Ok(())
//     }
// }
