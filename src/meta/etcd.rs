use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use compact_str::CompactString;
use either::Either;
use etcd_client::{
    Client,
    Compare,
    CompareOp,
    Event,
    EventType,
    GetOptions,
    KvClient,
    PutOptions,
    Txn,
    TxnOp,
    TxnOpResponse,
    WatchClient,
    WatchOptions,
    WatchStream,
    Watcher,
};
use ignore_result::Ignore;
use uuid::Uuid;

use super::serde;
use super::types::{
    self,
    BookieRegistrationClient,
    BookieServiceInfo,
    BookieUpdate,
    LedgerIdStoreClient,
    LedgerMetadataStoreClient,
    MetaStore,
    MetaVersion,
    Versioned,
};
use crate::client::errors::{BkError, ErrorKind};
use crate::client::{LedgerId, LedgerMetadata};

pub struct BookieUpdateStream {
    bookie_path: String,
    stream: WatchStream,
    events: VecDeque<Event>,
}

#[async_trait]
impl types::BookieUpdateStream for BookieUpdateStream {
    async fn next(&mut self) -> Result<BookieUpdate, BkError> {
        loop {
            while let Some(event) = self.events.pop_front() {
                if let Some(kv) = event.kv() {
                    let result = if event.event_type() == EventType::Put {
                        let bookie_service_info =
                            serde::deserialize_bookie_service_info(&self.bookie_path, kv.key(), kv.value())?;
                        BookieUpdate::Add(bookie_service_info)
                    } else {
                        let bookie_id = serde::deserialize_bookie_id(&self.bookie_path, kv.key())?;
                        BookieUpdate::Remove(bookie_id)
                    };
                    return Ok(result);
                }
            }
            if let Some(response) = self.stream.message().await? {
                let events = response.events();
                events.iter().for_each(|e| self.events.push_back(e.clone()));
            }
        }
    }
}

impl BookieUpdateStream {
    fn new(bookie_path: String, stream: WatchStream) -> BookieUpdateStream {
        BookieUpdateStream { bookie_path, stream, events: VecDeque::new() }
    }
}

pub struct LedgerMetadataStream {
    ledger_id: LedgerId,
    watcher: Watcher,
    stream: WatchStream,
    events: VecDeque<Event>,
    cancelled: bool,
}

#[async_trait]
impl types::LedgerMetadataStream for LedgerMetadataStream {
    async fn cancel(&mut self) {
        if self.cancelled {
            return;
        }
        self.events.clear();
        self.cancelled = true;
        self.watcher.cancel().await.ignore();
    }

    async fn next(&mut self) -> Result<Versioned<LedgerMetadata>, BkError> {
        loop {
            while let Some(event) = self.events.pop_front() {
                if let Some(kv) = event.kv() {
                    if event.event_type() == EventType::Delete {
                        return Err(BkError::new(ErrorKind::LedgerNotExisted));
                    }
                    let version = kv.mod_revision();
                    let metadata = serde::deserialize_ledger_metadata(self.ledger_id, kv.value())?;
                    return Ok(Versioned::new(MetaVersion(version), metadata));
                }
            }
            if let Some(response) = self.stream.message().await? {
                let events = response.events();
                events.iter().for_each(|e| self.events.push_back(e.clone()));
            };
        }
    }
}

pub struct EtcdConfiguration {
    scope: CompactString,
    #[allow(dead_code)]
    user: Option<(String, String)>,
    #[allow(dead_code)]
    keep_alive: Option<(Duration, Duration)>,
}

impl EtcdConfiguration {
    pub fn new(scope: CompactString) -> EtcdConfiguration {
        EtcdConfiguration { scope, user: None, keep_alive: None }
    }

    #[allow(dead_code)]
    pub fn with_user(self, name: String, password: String) -> EtcdConfiguration {
        EtcdConfiguration { user: Some((name, password)), ..self }
    }

    #[allow(dead_code)]
    pub fn with_keep_alive(self, interval: Duration, timeout: Duration) -> EtcdConfiguration {
        EtcdConfiguration { keep_alive: Some((interval, timeout)), ..self }
    }
}

pub struct EtcdMetaStore {
    scope: CompactString,
    client: KvClient,
    watcher: WatchClient,
    bucket_counter: AtomicU64,
}

impl Clone for EtcdMetaStore {
    fn clone(&self) -> EtcdMetaStore {
        EtcdMetaStore {
            scope: self.scope.clone(),
            client: self.client.clone(),
            watcher: self.watcher.clone(),
            bucket_counter: AtomicU64::new(0),
        }
    }
}

unsafe impl Send for EtcdMetaStore {}
unsafe impl Sync for EtcdMetaStore {}

const NUM_BUCKETS: u64 = 0x80;
const BUCKET_SHIFT: i32 = 56;
const MAX_ID_PER_BUCKET: u64 = 0x00ffffffffffffff;

impl EtcdMetaStore {
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        configuration: EtcdConfiguration,
    ) -> Result<EtcdMetaStore, BkError> {
        let client = Client::connect(endpoints, None).await?;
        Ok(EtcdMetaStore {
            scope: configuration.scope,
            client: client.kv_client(),
            watcher: client.watch_client(),
            bucket_counter: AtomicU64::new(0),
        })
    }

    fn next_bucket(&self) -> u64 {
        let u = self.bucket_counter.fetch_add(1, Ordering::Relaxed);
        u % NUM_BUCKETS
    }

    fn bucket_path(&self, bucket: u64) -> String {
        return format!("{}/buckets/{:03}", self.scope, bucket);
    }

    fn ledger_path(&self, ledger_id: LedgerId) -> String {
        let least64: i64 = ledger_id.into();
        let combined_id = least64 as u64 as u128;
        let uuid = Uuid::from_u128(combined_id);
        return format!("{}/ledgers/{}", self.scope, uuid);
    }

    fn writable_bookie_directory_path(&self) -> String {
        return format!("{}/bookies/writable/", self.scope);
    }

    fn readable_bookie_directory_path(&self) -> String {
        return format!("{}/bookies/readable/", self.scope);
    }

    async fn watch_bookies_update(
        &self,
        bookie_path: String,
        start_revision: i64,
    ) -> Result<BookieUpdateStream, BkError> {
        let mut bookie_path_end: Vec<u8> = bookie_path.clone().into();
        bookie_path_end.push(0xff);
        let options = WatchOptions::new().with_range(bookie_path_end).with_start_revision(start_revision);
        let (_watcher, stream) = self.watcher.clone().watch(bookie_path.clone(), Some(options)).await?;
        Ok(BookieUpdateStream::new(bookie_path, stream))
    }

    async fn watch_bookies(
        &self,
        bookie_path: String,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let mut bookie_path_end: Vec<u8> = bookie_path.clone().into();
        bookie_path_end.push(0xff);
        let options = GetOptions::new().with_range(bookie_path_end).with_serializable();
        let mut client = self.client.clone();
        let response = client.get(bookie_path.clone(), Some(options)).await?;
        let Some(header) = response.header() else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"no header");
            return Err(err);
        };
        let start_revision = header.revision();
        let mut err: Option<BkError> = None;
        let bookies: Vec<BookieServiceInfo> = response
            .kvs()
            .iter()
            .filter_map(|kv| match serde::deserialize_bookie_service_info(&bookie_path, kv.key(), kv.value()) {
                Err(e) => {
                    err = Some(e);
                    None
                },
                Ok(bookie) => Some(bookie),
            })
            .collect();
        if let Some(err) = err {
            return Err(err);
        }
        let update_stream = self.watch_bookies_update(bookie_path, start_revision + 1).await?;
        Ok((bookies, Box::new(update_stream)))
    }
}

#[async_trait]
impl BookieRegistrationClient for EtcdMetaStore {
    async fn watch_readable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let bookie_path = self.writable_bookie_directory_path();
        self.watch_bookies(bookie_path).await
    }

    async fn watch_writable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let bookie_path = self.readable_bookie_directory_path();
        self.watch_bookies(bookie_path).await
    }
}

#[async_trait]
impl LedgerIdStoreClient for EtcdMetaStore {
    async fn generate_ledger_id(&self) -> Result<LedgerId, BkError> {
        let bucket_id = self.next_bucket();
        let bucket_path = self.bucket_path(bucket_id);
        let options = PutOptions::new().with_prev_key();
        let mut client = self.client.clone();
        let response = client.put(bucket_path, Vec::new(), Some(options)).await?;
        let previous_version = response.prev_key().map_or(0, |kv| kv.version()) as u64;
        let version = previous_version + 1;
        assert!(version <= MAX_ID_PER_BUCKET);
        let id = (bucket_id << BUCKET_SHIFT) | version;
        return Ok(LedgerId(id as i64));
    }
}

impl From<etcd_client::Error> for BkError {
    fn from(e: etcd_client::Error) -> BkError {
        BkError::new(ErrorKind::MetaClientError).cause_by(e)
    }
}

impl From<prost::DecodeError> for BkError {
    fn from(e: prost::DecodeError) -> BkError {
        BkError::new(ErrorKind::MetaInvalidData).cause_by(e)
    }
}

#[async_trait]
impl LedgerMetadataStoreClient for EtcdMetaStore {
    async fn create_ledger_metadata(&self, metadata: &LedgerMetadata) -> Result<MetaVersion, BkError> {
        let ledger_path = self.ledger_path(metadata.ledger_id);
        let serialized_metadata = serde::serialize_ledger_metadata(metadata)?;
        let compare = Compare::create_revision(ledger_path.clone(), CompareOp::Greater, 0);
        let create = TxnOp::put(ledger_path, serialized_metadata, None);
        let txn = Txn::new().when(vec![compare]).or_else(vec![create]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await?;
        if response.succeeded() {
            return Err(BkError::new(ErrorKind::LedgerExisted));
        }
        let Some(TxnOpResponse::Put(put_response)) = response.op_responses().into_iter().next() else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"put succeed with no put response");
            return Err(err);
        };
        let Some(revision) = put_response.header().map(|h| h.revision()) else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"no header in put response");
            return Err(err);
        };
        return Ok(MetaVersion::from(revision));
    }

    async fn remove_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        expected_version: Option<MetaVersion>,
    ) -> Result<(), BkError> {
        let ledger_path = self.ledger_path(ledger_id);
        let mut client = self.client.clone();
        let Some(version) = expected_version else {
            let response = client.delete(ledger_path, None).await?;
            if response.deleted() <= 0 {
                return Err(BkError::new(ErrorKind::LedgerNotExisted));
            }
            return Ok(());
        };
        let compare = Compare::mod_revision(ledger_path.clone(), CompareOp::Equal, version.into());
        let delete = TxnOp::delete(ledger_path.clone(), None);
        let get_op = TxnOp::get(ledger_path, Some(GetOptions::new().with_count_only()));
        let txn = Txn::new().when(vec![compare]).and_then(vec![delete]).or_else(vec![get_op]);
        let response = client.txn(txn).await?;
        if response.succeeded() {
            return Ok(());
        }
        let Some(TxnOpResponse::Get(get_response)) = response.op_responses().into_iter().next() else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"get succeed with no get response");
            return Err(err);
        };
        if get_response.count() != 0 {
            return Err(BkError::new(ErrorKind::MetaVersionMismatch));
        }
        return Err(BkError::new(ErrorKind::LedgerNotExisted));
    }

    async fn read_ledger_metadata(&self, ledger_id: LedgerId) -> Result<Versioned<LedgerMetadata>, BkError> {
        let ledger_path = self.ledger_path(ledger_id);
        let mut client = self.client.clone();
        let response = client.get(ledger_path, None).await?;
        let Some(kv) = response.kvs().first() else {
            return Err(BkError::new(ErrorKind::LedgerNotExisted));
        };
        let metadata = serde::deserialize_ledger_metadata(ledger_id, kv.value())?;
        let version = MetaVersion::from(kv.mod_revision());
        return Ok(Versioned::new(version, metadata));
    }

    async fn watch_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        start_version: MetaVersion,
    ) -> Result<Box<dyn types::LedgerMetadataStream>, BkError> {
        let ledger_path = self.ledger_path(ledger_id);
        let options = WatchOptions::new().with_start_revision(start_version.into());
        let mut client = self.watcher.clone();
        let (watcher, stream) = client.watch(ledger_path, Some(options)).await?;
        let stream = LedgerMetadataStream { ledger_id, watcher, stream, events: VecDeque::new(), cancelled: false };
        return Ok(Box::new(stream));
    }

    async fn write_ledger_metadata(
        &self,
        metadata: &LedgerMetadata,
        expected_version: MetaVersion,
    ) -> Result<Either<Versioned<LedgerMetadata>, MetaVersion>, BkError> {
        let serialized_metadata = serde::serialize_ledger_metadata(metadata)?;
        let ledger_path = self.ledger_path(metadata.ledger_id);
        let compare = Compare::mod_revision(ledger_path.clone(), CompareOp::Equal, expected_version.into());
        let put_op = TxnOp::put(ledger_path.clone(), serialized_metadata, None);
        let get_op = TxnOp::get(ledger_path, Some(GetOptions::new().with_count_only()));
        let txn = Txn::new().when(vec![compare]).and_then(vec![put_op]).or_else(vec![get_op]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await?;
        if response.succeeded() {
            let Some(TxnOpResponse::Put(put_response)) = response.op_responses().into_iter().next() else {
                let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"put succeed with no put response");
                return Err(err);
            };
            let Some(revision) = put_response.header().map(|h| h.revision()) else {
                let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"no header in put response");
                return Err(err);
            };
            return Ok(either::Right(MetaVersion::from(revision)));
        }
        let Some(TxnOpResponse::Get(get_response)) = response.op_responses().into_iter().next() else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"get succeed with no get response");
            return Err(err);
        };
        let Some(kv) = get_response.kvs().first() else {
            return Err(BkError::new(ErrorKind::LedgerNotExisted));
        };
        let Some(conflicting_revision) = get_response.header().map(|h| h.revision()) else {
            let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"no header in get response");
            return Err(err);
        };
        let conflicting_metadata = serde::deserialize_ledger_metadata(metadata.ledger_id, kv.value())?;
        return Ok(either::Left(Versioned::new(conflicting_revision, conflicting_metadata)));
    }
}

impl MetaStore for EtcdMetaStore {}
