use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::mem::MaybeUninit;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use compact_str::CompactString;
use either::Either;
use ignore_result::Ignore;
use log::debug;
use uuid::Uuid;
use zookeeper_client as zk;

use super::serde;
use super::types::{
    self,
    BookieRegistrationClient,
    BookieServiceInfo,
    BookieUpdate,
    BookieUpdateStream,
    LedgerIdStoreClient,
    LedgerMetadataStoreClient,
    LedgerMetadataStream,
    MetaStore,
    MetaVersion,
    Versioned,
};
use crate::client::errors::{BkError, BkResult, ErrorKind};
use crate::client::service_uri::ServiceUri;
use crate::client::{BookieId, LedgerId, LedgerMetadata};

#[derive(Clone)]
pub struct ZkConfiguration {
    ledger_id_format: Option<LedgerIdFormat>,
    root: CompactString,
    connect: CompactString,
    timeout: Duration,
}

impl ZkConfiguration {
    pub fn from_service_uri(uri: ServiceUri) -> Result<ZkConfiguration, BkError> {
        assert_eq!(uri.scheme, "zk");
        let ServiceUri { spec, address, path, .. } = uri;
        let ledger_id_format = match spec.as_str() {
            "" | "null" => None,
            "flat" => Some(LedgerIdFormat::Flat),
            "hierarchical" => Some(LedgerIdFormat::Hierarchical),
            "longhierarchical" => Some(LedgerIdFormat::LongHierarchical),
            _ => {
                return Err(BkError::with_message(
                    ErrorKind::InvalidServiceUri,
                    format!("unknown ledger id format {}", spec),
                ))
            },
        };
        let config =
            ZkConfiguration { ledger_id_format, root: path, connect: address, timeout: Duration::from_secs(10) };
        Ok(config)
    }
}

enum Error {
    SessionExpired,
}

impl From<Error> for BkError {
    fn from(err: Error) -> BkError {
        use Error::*;
        match err {
            SessionExpired => BkError::with_description(ErrorKind::MetaClientError, &"zk session expired"),
        }
    }
}

struct ZkClient {
    client: zk::Client,
}

impl ZkClient {
    fn terminated(&self) -> bool {
        let state = self.client.state();
        state.is_terminated()
    }

    async fn connected(&self) -> Result<(), Error> {
        let mut watcher = self.client.state_watcher();
        let mut state = watcher.state();
        loop {
            match state {
                zk::SessionState::SyncConnected => return Ok(()),
                zk::SessionState::Disconnected => {},
                _ => return Err(Error::SessionExpired),
            };
            state = watcher.changed().await;
        }
    }
}

impl std::ops::Deref for ZkClient {
    type Target = zk::Client;

    fn deref(&self) -> &zk::Client {
        &self.client
    }
}

impl From<zk::Stat> for MetaVersion {
    fn from(stat: zk::Stat) -> MetaVersion {
        MetaVersion(stat.mzxid)
    }
}

#[async_trait]
impl BookieRegistrationClient for ZkMetaStore {
    async fn watch_readable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let mut client = self.get_connected_client().await?;
        client.watch_bookies("/available/readonly", "").await
    }

    async fn watch_writable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let mut client = self.get_connected_client().await?;
        client.watch_bookies("/available", "readonly").await
    }
}

#[async_trait]
impl LedgerIdStoreClient for ZkMetaStore {
    async fn generate_ledger_id(&self) -> Result<LedgerId, BkError> {
        let mut client = self.get_connected_client().await?;
        client.generate_ledger_id().await
    }
}

impl From<zk::Error> for BkError {
    fn from(err: zk::Error) -> BkError {
        let kind = match err {
            zk::Error::BadVersion => ErrorKind::MetaVersionMismatch,
            _ => ErrorKind::MetaClientError,
        };
        BkError::new(kind).cause_by(err)
    }
}

struct ZkSessionClient {
    client: Arc<ZkClient>,
    counter: usize,
    session: String,
}

struct ZkClientManager {
    config: ZkConfiguration,
    client: tokio::sync::Mutex<ZkSessionClient>,
}

impl ZkClientManager {
    fn new(config: ZkConfiguration, client: Arc<ZkClient>) -> ZkClientManager {
        let client = ZkSessionClient { client, counter: 0, session: Uuid::new_v4().to_string() };
        ZkClientManager { config, client: tokio::sync::Mutex::new(client) }
    }

    async fn replace_client(&self, client: &Arc<ZkClient>) -> Result<Arc<ZkClient>, BkError> {
        let mut guard = self.client.lock().await;
        if !Arc::ptr_eq(client, &guard.client) {
            return Ok(guard.client.clone());
        }
        let zookeeper =
            zk::Client::builder().with_session_timeout(self.config.timeout).connect(&self.config.connect).await?;
        let client = Arc::new(ZkClient { client: zookeeper });
        guard.session.truncate(36);
        guard.counter += 1;
        let counter = guard.counter;
        write!(&mut guard.session, "-{}", counter).ignore();
        let options = zk::CreateMode::Ephemeral.with_acls(zk::Acls::creator_all());
        // A quorum write is necessary to ensure cross-client linearizable.
        client.create(&guard.session, Default::default(), &options).await?;
        guard.client = client.clone();
        Ok(client)
    }
}

struct ZkLedgerMetadataStream {
    watcher: Option<zk::OneshotWatcher>,
    version: i64,
    updated: i64,
    ledger_id: LedgerId,
    ledger_path: String,
    client: Arc<ZkClient>,
    manager: Arc<ZkClientManager>,
}

impl ZkLedgerMetadataStream {
    async fn new_client(&mut self) -> Result<(), BkError> {
        let client = self.manager.replace_client(&self.client).await?;
        self.client = client;
        Ok(())
    }

    async fn connected(&mut self) -> Result<(), BkError> {
        match self.client.connected().await {
            Ok(_) => Ok(()),
            Err(Error::SessionExpired) => self.new_client().await,
        }
    }

    async fn watch_exists(&mut self) -> Result<(Option<zk::Stat>, zk::OneshotWatcher), zk::Error> {
        self.client.check_and_watch_stat(&self.ledger_path).await
    }
}

#[async_trait]
impl LedgerMetadataStream for ZkLedgerMetadataStream {
    async fn cancel(&mut self) {}

    async fn next(&mut self) -> Result<Versioned<LedgerMetadata>, BkError> {
        loop {
            if self.updated > self.version {
                match self.client.get_data(&self.ledger_path).await {
                    Ok((data, stat)) if stat.mzxid > self.version => {
                        self.version = stat.mzxid;
                        let metadata = serde::deserialize_ledger_metadata(self.ledger_id, &data)?;
                        return Ok(Versioned::new(stat, metadata));
                    },
                    Err(zk::Error::NoNode) => {
                        self.version = self.updated;
                        return Err(BkError::new(ErrorKind::LedgerNotExisted));
                    },
                    _ => {},
                };
            }
            if let Some(watcher) = self.watcher.take() {
                watcher.changed().await;
            }
            match self.watch_exists().await {
                Ok(r) => {
                    let (stat, watcher) = r;
                    self.watcher = Some(watcher);
                    if let Some(stat) = stat {
                        self.updated = self.updated.max(stat.mzxid);
                        continue;
                    }
                    return Err(BkError::new(ErrorKind::LedgerNotExisted));
                },
                Err(zk::Error::ConnectionLoss) => {
                    self.connected().await.ignore();
                },
                Err(zk::Error::SessionExpired) => {
                    self.new_client().await?;
                },
                Err(e) => {
                    return Err(BkError::new(ErrorKind::MetaClientError).cause_by(e));
                },
            };
        }
    }
}

struct ZkBookieUpdateStream {
    path: &'static str,
    exclude: &'static str,
    scratch: String,
    client: Arc<ZkClient>,
    manager: Arc<ZkClientManager>,
    watcher: zk::PersistentWatcher,
    expired: bool,
}

impl ZkBookieUpdateStream {
    async fn new_client(&mut self) -> Result<(), BkError> {
        let client = self.manager.replace_client(&self.client).await?;
        self.client = client;
        Ok(())
    }

    async fn connected(&mut self) -> Result<(), BkError> {
        match self.client.connected().await {
            Ok(_) => Ok(()),
            Err(_) => self.new_client().await,
        }
    }

    async fn get_bookies(
        client: &ZkClient,
        path: &str,
        children: &[String],
        exclude: &str,
        scratch: &mut String,
    ) -> Result<Vec<BookieServiceInfo>, BkError> {
        let mut bookies = Vec::with_capacity(children.len());
        for bookie_path in children.iter() {
            if !exclude.is_empty() && bookie_path.starts_with(exclude) {
                continue;
            }
            scratch.clear();
            write!(scratch, "{}/{}", path, &bookie_path).ignore();
            let data = match client.get_data(scratch).await {
                Err(zk::Error::NoNode) => continue,
                Err(err) => return Err(From::from(err)),
                Ok((data, _)) => data,
            };
            let bookie_id = BookieId::new(bookie_path);
            let bookie_result = if data.is_empty() {
                BookieServiceInfo::from_legacy(bookie_id)
            } else {
                BookieServiceInfo::from_protobuf(bookie_id, &data)
            };
            match bookie_result {
                Err(_) => continue,
                Ok(bookie) => bookies.push(bookie),
            }
        }
        Ok(bookies)
    }

    async fn rebuild(
        client: &ZkClient,
        path: &str,
        exclude: &str,
        scratch: &mut String,
    ) -> Result<(Vec<BookieServiceInfo>, zk::PersistentWatcher), BkError> {
        let watcher = client.watch(path, zk::AddWatchMode::PersistentRecursive).await?;
        let children = match client.list_children(path).await {
            Err(zk::Error::NoNode) => Default::default(),
            Err(err) => return Err(From::from(err)),
            Ok(children) => children,
        };
        let bookies = Self::get_bookies(client, path, &children, exclude, scratch).await?;
        Ok((bookies, watcher))
    }

    async fn on_session_event(&mut self, state: zk::SessionState) -> Result<(), BkError> {
        if state == zk::SessionState::Disconnected {
            self.connected().await?;
        } else if state.is_terminated() {
            self.new_client().await?;
            self.expired = true;
        }
        Ok(())
    }
}

#[async_trait]
impl BookieUpdateStream for ZkBookieUpdateStream {
    async fn next(&mut self) -> BkResult<BookieUpdate> {
        loop {
            if self.expired {
                let (bookies, watcher) =
                    Self::rebuild(&self.client, self.path, self.exclude, &mut self.scratch).await?;
                self.watcher = watcher;
                self.expired = false;
                return Ok(BookieUpdate::Reconstruction(bookies));
            }
            let event = self.watcher.changed().await;
            if event.event_type == zk::EventType::Session {
                self.on_session_event(event.session_state).await?;
                continue;
            } else if event.event_type != zk::EventType::NodeChildrenChanged {
                continue;
            }
            let bookie_path = match event.path.strip_prefix(self.path).and_then(|path| path.strip_prefix('/')) {
                None => continue,
                Some(bookie_path) => bookie_path,
            };
            if bookie_path == self.exclude {
                continue;
            }
            let bookie_id = BookieId::new(bookie_path);
            if event.event_type == zk::EventType::NodeDeleted {
                return Ok(BookieUpdate::Remove(bookie_id));
            }
            let (data, _) = match self.client.get_data(&event.path).await {
                Err(_) => continue,
                Ok(result) => result,
            };
            let bookie_result = if data.is_empty() {
                BookieServiceInfo::from_legacy(bookie_id.clone())
            } else {
                BookieServiceInfo::from_protobuf(bookie_id.clone(), &data)
            };
            let bookie = match bookie_result {
                Err(err) => {
                    debug!("fail to parse bookie info for bookie id {}: {}", bookie_id, err);
                    continue;
                },
                Ok(bookie) => bookie,
            };
            return Ok(BookieUpdate::Add(bookie));
        }
    }
}

struct ZkMetaClientGuard<'a> {
    client: MaybeUninit<ZkMetaClient>,
    store: &'a ZkMetaStore,
}

impl std::ops::Deref for ZkMetaClientGuard<'_> {
    type Target = ZkMetaClient;

    fn deref(&self) -> &ZkMetaClient {
        unsafe { &*self.client.as_ptr() }
    }
}

impl std::ops::DerefMut for ZkMetaClientGuard<'_> {
    fn deref_mut(&mut self) -> &mut ZkMetaClient {
        unsafe { &mut *self.client.as_mut_ptr() }
    }
}

impl Drop for ZkMetaClientGuard<'_> {
    fn drop(&mut self) {
        let client = unsafe { self.client.as_ptr().read() };
        self.store.release_client(client);
    }
}

#[derive(Copy, Clone, PartialEq, Eq, strum::Display)]
enum LedgerIdFormat {
    #[strum(serialize = "flat")]
    Flat,
    #[strum(serialize = "hierarchical")]
    Hierarchical,
    #[strum(serialize = "longhierarchical")]
    LongHierarchical,
}

impl LedgerIdFormat {
    fn try_from_ledger_layout(data: Vec<u8>) -> Result<LedgerIdFormat, BkError> {
        let Ok(s) = String::from_utf8(data) else {
            return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"ledger layout is not utf8"));
        };
        let mut lines = s.split('\n');
        let Ok(format_version) = lines.next().unwrap().parse::<i32>() else {
            return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"invalid ledger layout format version"));
        };
        if format_version != 1 && format_version != 2 {
            let msg = format!("unsupported ledger layout format version {}", format_version);
            return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
        }
        let Some(manager_line) = lines.next() else {
            return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"no ledger manager in ledger layout"));
        };
        let mut manager_splits = manager_line.split(':');
        let manager_class = manager_splits.next().unwrap();
        let Some(manager_version) = manager_splits.next() else {
            return Err(BkError::with_description(
                ErrorKind::MetaInvalidData,
                &"no ledger manager version in ledger layout",
            ));
        };
        if manager_version.parse::<i32>().is_err() {
            return Err(BkError::with_description(
                ErrorKind::MetaInvalidData,
                &"invalid ledger manager version in ledger layout",
            ));
        };
        if format_version == 1 {
            if manager_class == "flat" {
                return Ok(LedgerIdFormat::Flat);
            } else if manager_class == "hierarchical" {
                return Ok(LedgerIdFormat::Hierarchical);
            }
            let msg =
                format!("unknown ledger manager type {} in ledger layout version {}", manager_class, format_version);
            return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
        }
        if manager_class == "org.apache.bookkeeper.meta.FlatLedgerManagerFactory" {
            return Ok(LedgerIdFormat::Flat);
        } else if manager_class == "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory" {
            return Ok(LedgerIdFormat::Hierarchical);
        } else if manager_class == "org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory" {
            return Ok(LedgerIdFormat::LongHierarchical);
        }
        let msg = format!("unknown ledger manager class {} in ledger layout version {}", manager_class, format_version);
        Err(BkError::with_message(ErrorKind::MetaInvalidData, msg))
    }
}

#[derive(Clone)]
struct ZkMetaClient {
    client: Arc<ZkClient>,
    scratch: String,
    ledger_root: CompactString,
    ledger_id_format: LedgerIdFormat,
    ledger_hob: i32,
    ledger_path: String,
    manager: Arc<ZkClientManager>,
}

impl ZkMetaClient {
    fn new(
        client: Arc<ZkClient>,
        manager: Arc<ZkClientManager>,
        ledger_id_format: LedgerIdFormat,
        ledger_hob: i32,
    ) -> ZkMetaClient {
        ZkMetaClient {
            client,
            scratch: String::with_capacity(50),
            ledger_root: Default::default(),
            ledger_id_format,
            ledger_hob,
            ledger_path: String::with_capacity(50),
            manager,
        }
    }

    fn terminated(&self) -> bool {
        self.client.terminated()
    }

    async fn check_expiration(&mut self) -> Result<(), BkError> {
        if self.terminated() {
            self.client = self.manager.replace_client(&self.client).await?;
        }
        Ok(())
    }

    fn format_flat_ledger_path(&mut self, ledger_id: LedgerId) {
        write!(&mut self.ledger_path, "{}/L{:010}", &self.ledger_root, ledger_id).unwrap();
    }

    fn format_hierarchical_ledger_path(&mut self, ledger_id: LedgerId) {
        if ledger_id.0 < i32::MAX as i64 {
            self.format_short_hierarchical_ledger_path(ledger_id);
        } else {
            self.format_long_hierarchical_ledger_path(ledger_id);
        }
    }

    fn format_short_hierarchical_ledger_path(&mut self, ledger_id: LedgerId) {
        self.scratch.clear();
        write!(&mut self.scratch, "{:010}", ledger_id).unwrap();
        write!(
            &mut self.ledger_path,
            "{}/{}/{}/{}",
            &self.ledger_root,
            &self.scratch[..2],
            &self.scratch[2..6],
            &self.scratch[6..10]
        )
        .unwrap();
    }

    fn format_long_hierarchical_ledger_path(&mut self, ledger_id: LedgerId) {
        write!(&mut self.scratch, "{:019}", ledger_id).unwrap();
        write!(
            &mut self.ledger_path,
            "{}/{}/{}/{}/{}/L{}",
            &self.ledger_root,
            &self.scratch[..3],
            &self.scratch[3..7],
            &self.scratch[7..11],
            &self.scratch[11..15],
            &self.scratch[15..19]
        )
        .unwrap();
    }

    fn format_ledger_path(&mut self, ledger_id: LedgerId) {
        self.ledger_path.clear();
        match self.ledger_id_format {
            LedgerIdFormat::Flat => self.format_flat_ledger_path(ledger_id),
            LedgerIdFormat::Hierarchical => self.format_hierarchical_ledger_path(ledger_id),
            LedgerIdFormat::LongHierarchical => self.format_long_hierarchical_ledger_path(ledger_id),
        };
    }

    async fn generate_flat_ledger_id(&mut self) -> Result<LedgerId, BkError> {
        let options = zk::CreateMode::EphemeralSequential.with_acls(zk::Acls::anyone_all());
        let (_, sequence) = self.client.create("/ID-", Default::default(), &options).await?;
        if sequence.0 < 0 {
            return Err(BkError::new(ErrorKind::LedgerIdOverflow));
        }
        Ok(LedgerId(sequence.0 as i64))
    }

    async fn generate_short_ledger_id(&self) -> Result<i64, BkError> {
        let (_, sequence) =
            self.create_path_optimistic("/idgen/ID-", Default::default(), zk::CreateMode::EphemeralSequential).await?;
        Ok(sequence.0 as i64)
    }

    async fn create_ledger_hob_directory(&mut self, i: i32) -> Result<(), zk::Error> {
        write!(&mut self.scratch, "/idgen-long/{:010}", i).ignore();
        self.create_directory_optimistic(&self.scratch).await
    }

    async fn generate_ledger_lob_id(&mut self, hob: i32) -> Result<i32, zk::Error> {
        write!(&mut self.scratch, "/idgen-long/{:010}/ID-", hob).ignore();
        let (_, sequence) =
            self.create_path_optimistic(&self.scratch, Default::default(), zk::CreateMode::EphemeralSequential).await?;
        Ok(sequence.0)
    }

    fn extract_max_hob(children: &[String]) -> Result<i32, BkError> {
        let mut max_hob = i32::MIN;
        for child in children.iter() {
            let hob_path = match child.strip_prefix("HOB-") {
                None => {
                    let msg = format!("invalid ledger generation path {}", child);
                    return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
                },
                Some(path) => path,
            };
            let hob = match hob_path.parse::<i32>() {
                Err(_) => {
                    let msg = format!("invalid ledger generation path {}", child);
                    return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
                },
                Ok(n) => n,
            };
            max_hob = max_hob.max(hob);
        }
        if max_hob <= 0 {
            let msg = format!("no valid ledger generation path, last one is {}", children.last().unwrap());
            return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
        }
        Ok(max_hob)
    }

    async fn get_max_hob(client: &ZkClient) -> Result<i32, BkError> {
        let children = match client.list_children("/idgen-long").await {
            Err(zk::Error::NoNode) => return Ok(-1),
            Err(err) => return Err(From::from(err)),
            Ok(children) => children,
        };
        Self::extract_max_hob(&children)
    }

    async fn generate_ledger_hob(&mut self) -> Result<(), BkError> {
        let mut children = self.client.list_children("/idgen-long").await?;
        if children.is_empty() {
            self.create_ledger_hob_directory(1).await?;
            children = self.client.list_children("/idgen-long").await?;
        }
        if children.is_empty() {
            return Err(BkError::new(ErrorKind::MetaConcurrentOperation));
        }
        self.ledger_hob = Self::extract_max_hob(&children)?;
        Ok(())
    }

    async fn generate_long_ledger_id(&mut self) -> Result<LedgerId, BkError> {
        loop {
            if self.ledger_hob < 0 {
                self.generate_ledger_hob().await?;
            }
            let hob = self.ledger_hob;
            let lob = self.generate_ledger_lob_id(hob).await?;
            if (0..i32::MAX).contains(&lob) {
                let id = (hob as i64) << 32 | (lob as i64);
                return Ok(LedgerId(id));
            }
            self.ledger_hob = -1;
        }
    }

    async fn generate_ledger_id(&mut self) -> Result<LedgerId, BkError> {
        if self.ledger_id_format == LedgerIdFormat::Flat {
            return self.generate_flat_ledger_id().await;
        }
        if self.ledger_hob <= 0 {
            let sequence = self.generate_short_ledger_id().await?;
            if sequence >= 0 && sequence < i32::MAX as i64 {
                return Ok(LedgerId(sequence));
            }
            self.create_directory_optimistic("/idgen-long").await?;
            self.create_ledger_hob_directory(1).await?;
            self.ledger_hob = 1;
        }
        self.generate_long_ledger_id().await
    }

    async fn create_ledger_metadata(&mut self, metadata: &LedgerMetadata) -> Result<MetaVersion, BkError> {
        let data = serde::serialize_ledger_metadata(metadata)?;
        self.format_ledger_path(metadata.ledger_id);
        let (stat, _) = self.create_path_optimistic(&self.ledger_path, &data, zk::CreateMode::Persistent).await?;
        Ok(MetaVersion::from(stat))
    }

    #[async_recursion::async_recursion]
    async fn create_directory_optimistic(&self, path: &str) -> Result<(), zk::Error> {
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        loop {
            let result = self.client.create(path, Default::default(), &options).await;
            match result {
                Err(zk::Error::NoNode) => {
                    let last_slash_index = path.rfind('/').unwrap();
                    let parent = &path[..last_slash_index];
                    self.create_directory_optimistic(parent).await?;
                },
                Err(zk::Error::NodeExists) => {
                    return Ok(());
                },
                Err(err) => return Err(err),
                Ok(_) => return Ok(()),
            }
        }
    }

    async fn create_path_optimistic(
        &self,
        path: &str,
        data: &[u8],
        mode: zk::CreateMode,
    ) -> Result<(zk::Stat, zk::CreateSequence), zk::Error> {
        let options = mode.with_acls(zk::Acls::anyone_all());
        loop {
            let result = self.client.create(path, data, &options).await;
            match result {
                Err(zk::Error::NoNode) => {
                    let last_slash_index = path.rfind('/').unwrap();
                    let parent = &path[..last_slash_index];
                    self.create_directory_optimistic(parent).await?;
                },
                Err(err) => return Err(err),
                Ok(result) => {
                    return Ok(result);
                },
            };
        }
    }

    async fn read_metadata(
        &self,
        ledger_id: LedgerId,
        ledger_path: &str,
    ) -> Result<Versioned<LedgerMetadata>, BkError> {
        let (data, stat) = match self.client.get_data(ledger_path).await {
            Err(zk::Error::NoNode) => return Err(BkError::new(ErrorKind::LedgerNotExisted)),
            Err(err) => return Err(From::from(err)),
            Ok(result) => result,
        };
        let metadata = serde::deserialize_ledger_metadata(ledger_id, &data)?;
        Ok(Versioned::new(stat, metadata))
    }

    async fn read_ledger_metadata(&mut self, ledger_id: LedgerId) -> Result<Versioned<LedgerMetadata>, BkError> {
        self.format_ledger_path(ledger_id);
        self.read_metadata(ledger_id, &self.ledger_path).await
    }

    async fn remove_ledger_metadata(
        &mut self,
        ledger_id: LedgerId,
        expected_version: Option<MetaVersion>,
    ) -> Result<(), BkError> {
        self.format_ledger_path(ledger_id);
        let xid = match expected_version {
            None => return self.delete_ledger_path(&self.ledger_path, None).await,
            Some(version) => version.0,
        };
        let stat = match self.client.check_stat(&self.ledger_path).await? {
            None => return Err(BkError::new(ErrorKind::LedgerNotExisted)),
            Some(stat) => stat,
        };
        if stat.mzxid != xid {
            return Err(BkError::new(ErrorKind::MetaVersionMismatch));
        }
        self.delete_ledger_path(&self.ledger_path, Some(stat.version)).await
    }

    async fn watch_ledger_metadata(
        &mut self,
        ledger_id: LedgerId,
        start_version: MetaVersion,
    ) -> BkResult<Box<dyn LedgerMetadataStream>> {
        self.format_ledger_path(ledger_id);
        let (stat, watcher) = match self.client.check_and_watch_stat(&self.ledger_path).await? {
            (None, _) => return Err(BkError::new(ErrorKind::LedgerNotExisted)),
            (Some(stat), watcher) => (stat, watcher),
        };
        let metadata_stream = ZkLedgerMetadataStream {
            watcher: Some(watcher),
            version: start_version.into(),
            updated: stat.mzxid,
            ledger_id,
            ledger_path: self.ledger_path.clone(),
            client: self.client.clone(),
            manager: self.manager.clone(),
        };
        Ok(Box::new(metadata_stream))
    }

    async fn write_ledger_metadata(
        &mut self,
        metadata: &LedgerMetadata,
        expected_version: MetaVersion,
    ) -> Result<Either<Versioned<LedgerMetadata>, MetaVersion>, BkError> {
        let ledger_id = metadata.ledger_id;
        let data = serde::serialize_ledger_metadata(metadata)?;
        self.format_ledger_path(ledger_id);
        let stat = match self.client.check_stat(&self.ledger_path).await? {
            None => return Err(BkError::new(ErrorKind::LedgerNotExisted)),
            Some(stat) => stat,
        };
        match i64::from(expected_version).cmp(&stat.mzxid) {
            Ordering::Less => {
                let metadata = self.read_metadata(ledger_id, &self.ledger_path).await?;
                return Ok(either::Left(metadata));
            },
            Ordering::Greater => {
                return Err(BkError::with_description(ErrorKind::MetaClientError, &"zk client is not in sync"));
            },
            _ => {},
        }

        let result = self.client.set_data(&self.ledger_path, &data, Some(stat.version)).await;
        match result {
            Err(zk::Error::NoNode) => {
                let err = BkError::new(ErrorKind::LedgerNotExisted);
                return Err(err);
            },
            Err(zk::Error::BadVersion) => {},
            Err(e) => {
                let err = BkError::new(ErrorKind::MetaClientError).cause_by(e);
                return Err(err);
            },
            Ok(stat) => {
                let version = MetaVersion::from(stat);
                return Ok(either::Right(version));
            },
        };
        let metadata = self.read_metadata(ledger_id, &self.ledger_path).await?;
        Ok(either::Left(metadata))
    }

    async fn delete_ledger_path(&self, path: &str, version: Option<i32>) -> BkResult<()> {
        if let Err(err) = self.client.delete(path, version).await {
            if err == zk::Error::NoNode {
                return Err(BkError::new(ErrorKind::LedgerNotExisted));
            }
            return Err(From::from(err));
        };
        Ok(())
    }

    async fn watch_bookies(
        &mut self,
        path: &'static str,
        exclude: &'static str,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn types::BookieUpdateStream>), BkError> {
        let (bookies, watcher) = ZkBookieUpdateStream::rebuild(&self.client, path, exclude, &mut self.scratch).await?;
        let stream = ZkBookieUpdateStream {
            expired: false,
            client: self.client.clone(),
            path,
            exclude,
            scratch: String::with_capacity(50),
            watcher,
            manager: self.manager.clone(),
        };
        Ok((bookies, Box::new(stream)))
    }
}

#[derive(Clone)]
pub struct ZkMetaStore {
    clients: Arc<Mutex<VecDeque<ZkMetaClient>>>,
}

impl ZkMetaStore {
    pub async fn new(mut config: ZkConfiguration) -> Result<ZkMetaStore, BkError> {
        let configed_ledger_id_format = config.ledger_id_format.take();
        let zookeeper = zk::Client::builder()
            .with_session_timeout(config.timeout)
            .connect(&config.connect)
            .await?
            .chroot(&config.root)
            .unwrap();
        let client = Arc::new(ZkClient { client: zookeeper });
        let manager = Arc::new(ZkClientManager::new(config, client.clone()));
        let layout_data = match client.get_data("/LAYOUT").await {
            Err(zk::Error::NoNode) => return Err(BkError::new(ErrorKind::MetaClusterUninitialized)),
            Err(err) => return Err(From::from(err)),
            Ok((data, _)) => data,
        };
        let ledger_id_format = LedgerIdFormat::try_from_ledger_layout(layout_data)?;
        if let Some(expected_ledger_id_format) = configed_ledger_id_format {
            if ledger_id_format != expected_ledger_id_format {
                let msg = format!(
                    "expect ledger id format {}, got {} from ZooKeeper",
                    expected_ledger_id_format, ledger_id_format
                );
                return Err(BkError::with_message(ErrorKind::MetaInvalidData, msg));
            }
        }
        let ledger_hob =
            if ledger_id_format == LedgerIdFormat::Flat { -1 } else { ZkMetaClient::get_max_hob(&client).await? };
        let meta_client = ZkMetaClient::new(client, manager, ledger_id_format, ledger_hob);
        let meta_store =
            ZkMetaStore { clients: Arc::new(Mutex::new(VecDeque::from([meta_client.clone(), meta_client]))) };
        Ok(meta_store)
    }

    fn get_client(&self) -> ZkMetaClientGuard<'_> {
        let mut clients = self.clients.lock().unwrap();
        let client = if clients.len() == 1 { clients[0].clone() } else { clients.pop_front().unwrap() };
        drop(clients);
        ZkMetaClientGuard { client: MaybeUninit::new(client), store: self }
    }

    async fn get_connected_client(&self) -> Result<ZkMetaClientGuard<'_>, BkError> {
        let mut client = self.get_client();
        client.check_expiration().await?;
        Ok(client)
    }

    fn release_client(&self, client: ZkMetaClient) {
        if client.terminated() {
            return;
        }
        let mut clients = self.clients.lock().unwrap();
        clients.push_back(client);
    }
}

#[async_trait]
impl LedgerMetadataStoreClient for ZkMetaStore {
    async fn create_ledger_metadata(&self, metadata: &LedgerMetadata) -> Result<MetaVersion, BkError> {
        let mut client = self.get_connected_client().await?;
        return client.create_ledger_metadata(metadata).await;
    }

    async fn remove_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        expected_version: Option<MetaVersion>,
    ) -> Result<(), BkError> {
        let mut client = self.get_connected_client().await?;
        return client.remove_ledger_metadata(ledger_id, expected_version).await;
    }

    async fn read_ledger_metadata(&self, ledger_id: LedgerId) -> Result<Versioned<LedgerMetadata>, BkError> {
        let mut client = self.get_connected_client().await?;
        return client.read_ledger_metadata(ledger_id).await;
    }

    async fn watch_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        start_version: MetaVersion,
    ) -> BkResult<Box<dyn LedgerMetadataStream>> {
        let mut client = self.get_connected_client().await?;
        return client.watch_ledger_metadata(ledger_id, start_version).await;
    }

    async fn write_ledger_metadata(
        &self,
        metadata: &LedgerMetadata,
        expected_version: MetaVersion,
    ) -> Result<Either<Versioned<LedgerMetadata>, MetaVersion>, BkError> {
        let mut client = self.get_connected_client().await?;
        return client.write_ledger_metadata(metadata, expected_version).await;
    }
}

impl MetaStore for ZkMetaStore {}
