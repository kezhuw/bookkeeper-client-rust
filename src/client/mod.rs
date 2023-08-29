mod bookie;
mod cell;
mod entry_distribution;
pub(crate) mod errors;
pub(crate) mod local_rc;
pub(crate) mod metadata;
mod placement;
mod reader;
pub mod service_uri;
mod writer;

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;

use ignore_result::Ignore;
use tokio::select;
use tokio::sync::{mpsc, watch};

use self::bookie::PoolledClient;
use self::cell::{Cell, RefCell};
use self::entry_distribution::EntryDistribution;
pub use self::errors::{BkError, ErrorKind, Result};
pub use self::metadata::{
    BookieId,
    DigestType,
    EntryId,
    LedgerEnsemble,
    LedgerId,
    LedgerLength,
    LedgerMetadata,
    LedgerState,
};
use self::placement::{EnsembleOptions, PlacementPolicy, RandomPlacementPolicy};
pub use self::reader::{LacOptions, LedgerReader, PollOptions, ReadOptions};
use self::service_uri::ServiceUri;
pub use self::writer::{CloseOptions, LedgerAppender};
use self::writer::{LedgerWriter, WriteRequest, WriterOptions};
use super::digest::{self, Algorithm as DigestAlgorithm};
use super::meta::util::BookieRegistry;
use super::meta::{
    EtcdConfiguration,
    EtcdMetaStore,
    LedgerMetadataStream,
    MetaStore,
    MetaVersion,
    Versioned,
    ZkConfiguration,
    ZkMetaStore,
};

/// Options to create ledger.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CreateOptions {
    ledger_id: Option<LedgerId>,
    ensemble_size: u32,
    write_quorum_size: u32,
    ack_quorum_size: u32,
    password: Vec<u8>,
    custom_metadata: HashMap<String, Vec<u8>>,
    digest_type: DigestType,

    // XXX write option ?
    deferred_sync: bool,
}

impl CreateOptions {
    fn validate(&self) -> Result<()> {
        if self.ensemble_size >= self.write_quorum_size && self.write_quorum_size >= self.ack_quorum_size {
            return Ok(());
        }
        let msg = format!(
            "unfulfilled ensemble requirement: ensemble_size({}) >= write_quorum_size({}) >= ack_quorum_size({})",
            self.ensemble_size, self.write_quorum_size, self.ack_quorum_size
        );
        Err(BkError::with_message(ErrorKind::InvalidMetadata, msg))
    }

    /// Constructs options for ledger creation.
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> CreateOptions {
        CreateOptions {
            ledger_id: None,
            ensemble_size: ensemble_size as u32,
            write_quorum_size: write_quorum_size as u32,
            ack_quorum_size: ack_quorum_size as u32,
            password: Default::default(),
            custom_metadata: Default::default(),
            digest_type: DigestType::DUMMY,
            deferred_sync: false,
        }
    }

    pub fn digest(self, digest_type: DigestType, password: Option<Vec<u8>>) -> Self {
        CreateOptions { digest_type, password: password.unwrap_or_default(), ..self }
    }

    pub fn ledger_id(self, ledger_id: LedgerId) -> Self {
        CreateOptions { ledger_id: Some(ledger_id), ..self }
    }

    pub fn custom_metadata(self, metadata: HashMap<String, Vec<u8>>) -> Self {
        CreateOptions { custom_metadata: metadata, ..self }
    }

    pub fn deferred_sync(self) -> Self {
        CreateOptions { deferred_sync: true, ..self }
    }
}

/// Options to open ledger.
#[derive(Clone)]
#[non_exhaustive]
pub struct OpenOptions<'a> {
    recovery: bool,
    password: &'a [u8],
    digest_type: DigestType,
    administrative: bool,
}

impl<'a> OpenOptions<'a> {
    /// Constructs options for opening ledger.
    pub fn new(digest_type: DigestType, password: Option<&'a [u8]>) -> OpenOptions<'a> {
        OpenOptions { recovery: false, digest_type, password: password.unwrap_or_default(), administrative: false }
    }

    /// Recovers(aka. fence and close) possible writing ledger in opening.
    pub fn recovery(self) -> Self {
        OpenOptions { recovery: true, ..self }
    }

    /// Grants adminstrative to bypass digest and password check.
    pub fn administrative(self) -> Self {
        OpenOptions { administrative: true, ..self }
    }
}

/// Options to delete ledger.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DeleteOptions {}

/// Configuration for BookKeeper client.
#[derive(Clone)]
#[non_exhaustive]
pub struct Configuration {
    service_uri: String,
    bookies: Option<String>,
}

impl Configuration {
    /// Constructs configuration with given service uri.
    pub fn new(service_uri: String) -> Configuration {
        Configuration { service_uri, bookies: None }
    }

    /// Specifies static bookie cluster.
    pub fn bookies(self, bookies: String) -> Self {
        Configuration { bookies: Some(bookies), ..self }
    }
}

/// BookKeeper client.
#[derive(Clone)]
pub struct Bookkeeper {
    meta_store: Arc<dyn MetaStore>,
    bookie_client: Arc<PoolledClient>,
    placement_policy: Arc<RandomPlacementPolicy>,
}

async fn relay_metadata_stream(
    mut version: MetaVersion,
    mut stream: Box<dyn LedgerMetadataStream>,
    sender: watch::Sender<Versioned<LedgerMetadata>>,
) {
    loop {
        select! {
            _ = sender.closed() => break,
            r = stream.next() => {
                match r {
                    Err(_) => continue,
                    Ok(metadata) if metadata.version > version => {
                        version = metadata.version;
                        sender.send(metadata).ignore();
                    },
                    _ => {},
                };
            },
        }
    }
    stream.cancel().await;
}

async fn merge_metadata_stream_and_updates(
    mut version: MetaVersion,
    mut stream: Box<dyn LedgerMetadataStream>,
    mut updates: mpsc::Receiver<Versioned<LedgerMetadata>>,
    sender: watch::Sender<Versioned<LedgerMetadata>>,
) {
    loop {
        select! {
            _ = sender.closed() => break,
            r = stream.next() => {
                let metadata = match r {
                    Err(_) => continue,
                    Ok(metadata) if metadata.version > version => { metadata },
                    Ok(_) => continue,
                };
                version = metadata.version;
                sender.send(metadata).ignore();
            },
            r = updates.recv() => {
                let metadata = if let Some(metadata) = r {
                    if metadata.version > version { metadata } else { continue }
                } else {
                    tokio::spawn(async move {
                        relay_metadata_stream(version, stream, sender).await;
                    });
                    return;
                };
                version = metadata.version;
                sender.send(metadata).ignore();
            },
        }
    }
    stream.cancel().await;
}

fn watch_metadata_stream(
    metadata: Versioned<LedgerMetadata>,
    stream: Box<dyn LedgerMetadataStream>,
) -> watch::Receiver<Versioned<LedgerMetadata>> {
    let version = metadata.version;
    let (sender, receiver) = watch::channel(metadata);
    tokio::spawn(async move {
        relay_metadata_stream(version, stream, sender).await;
    });
    receiver
}

fn watch_metadata_stream_and_updates(
    metadata: Versioned<LedgerMetadata>,
    stream: Box<dyn LedgerMetadataStream>,
    updates: mpsc::Receiver<Versioned<LedgerMetadata>>,
) -> watch::Receiver<Versioned<LedgerMetadata>> {
    let version = metadata.version;
    let (sender, receiver) = watch::channel(metadata);
    tokio::spawn(async move {
        merge_metadata_stream_and_updates(version, stream, updates, sender).await;
    });
    receiver
}

impl Bookkeeper {
    /// Constructs BookKeeper client with given configuration.
    pub async fn new(config: Configuration) -> Result<Bookkeeper> {
        let service_uri = config.service_uri.parse::<ServiceUri>()?;
        let bookie_registry = match &config.bookies {
            None => None,
            Some(bookie_addresses) => Some(BookieRegistry::with_bookies(bookie_addresses)?),
        };
        let (meta_store, bookie_registry): (Arc<dyn MetaStore>, _) = if service_uri.scheme == "etcd" {
            let endpoints = [service_uri.address.as_str()];
            let etcd_configuration = EtcdConfiguration::new(service_uri.path);
            let mut meta_store = EtcdMetaStore::connect(&endpoints, etcd_configuration).await?;
            let bookie_registry = match bookie_registry {
                None => BookieRegistry::new(&mut meta_store).await?,
                Some(bookie_registry) => bookie_registry,
            };
            (Arc::new(meta_store), bookie_registry)
        } else if service_uri.scheme == "zk" {
            let zk_configuration = ZkConfiguration::from_service_uri(service_uri)?;
            let mut meta_store = ZkMetaStore::new(zk_configuration).await?;
            let bookie_registry = match bookie_registry {
                None => BookieRegistry::new(&mut meta_store).await?,
                Some(bookie_registry) => bookie_registry,
            };
            (Arc::new(meta_store), bookie_registry)
        } else {
            let msg = format!("unknown service scheme {}", service_uri.scheme);
            return Err(BkError::with_message(ErrorKind::InvalidServiceUri, msg));
        };
        let placement_policy = RandomPlacementPolicy::new(bookie_registry.clone());
        let poolled_client = Arc::new(PoolledClient::new(bookie_registry));
        let bookkeeper =
            Bookkeeper { meta_store, bookie_client: poolled_client, placement_policy: Arc::new(placement_policy) };
        Ok(bookkeeper)
    }

    /// Opens ledger for reading.
    pub async fn open_ledger(&self, ledger_id: LedgerId, options: &OpenOptions<'_>) -> Result<LedgerReader> {
        let Versioned { version, value: metadata } = self.meta_store.read_ledger_metadata(ledger_id).await?;
        let ensemble = metadata.last_ensemble();
        let entry_distribution = EntryDistribution::from_metadata(&metadata);
        if !options.administrative
            && (options.digest_type != metadata.digest_type || options.password != metadata.password)
        {
            return Err(BkError::new(ErrorKind::UnauthorizedAccess));
        }
        let closed = metadata.closed();
        let needs_recovery = options.recovery && !closed;
        let digest_algorithm = DigestAlgorithm::new(metadata.digest_type, &metadata.password);
        let master_key = digest::generate_master_key(&metadata.password);
        let metadata_stream = self.meta_store.watch_ledger_metadata(ledger_id, version).await?;
        let (metadata_watcher, metadata_sender) = if needs_recovery {
            let (metadata_sender, metadata_receiver) = mpsc::channel(128);
            let metadata = Versioned::new(version, metadata.clone());
            let metadata_watcher = watch_metadata_stream_and_updates(metadata, metadata_stream, metadata_receiver);
            (metadata_watcher, Some(metadata_sender))
        } else {
            let metadata = Versioned::new(version, metadata.clone());
            let watcher = watch_metadata_stream(metadata, metadata_stream);
            (watcher, None)
        };
        let last_add_confirmed = if closed { metadata.last_entry_id } else { ensemble.first_entry_id - 1 };
        let mut ledger = LedgerReader {
            ledger_id,
            metadata: RefCell::new(metadata),
            metadata_version: Cell::new(version),
            client: self.bookie_client.clone(),
            last_add_confirmed: Cell::new(last_add_confirmed),
            entry_distribution,
            master_key,
            digest_algorithm,
            updating_metadata: Cell::new(Some(metadata_watcher)),
            marker: PhantomData,
        };
        if let Some(metadata_sender) = metadata_sender {
            ledger.recover(version, metadata_sender, &self.meta_store, self.placement_policy.clone()).await?;
        }
        Ok(ledger)
    }

    /// Creates ledger for appending.
    pub async fn create_ledger(&self, options: CreateOptions) -> Result<LedgerAppender> {
        options.validate()?;
        let ledger_id = if let Some(ledger_id) = options.ledger_id {
            ledger_id
        } else {
            self.meta_store.generate_ledger_id().await?
        };
        let ensemble = self.placement_policy.select_ensemble(&EnsembleOptions {
            ensemble_size: options.ensemble_size,
            write_quorum: options.write_quorum_size,
            ack_quorum: options.ack_quorum_size,
            custom_metadata: &options.custom_metadata,
            preferred_bookies: &[],
            excluded_bookies: HashSet::new(),
        })?;
        let metadata = LedgerMetadata {
            ledger_id,
            length: LedgerLength::ZERO,
            last_entry_id: EntryId::INVALID,
            state: LedgerState::Open,
            password: options.password,
            ensemble_size: options.ensemble_size,
            write_quorum_size: options.write_quorum_size,
            ack_quorum_size: options.ack_quorum_size,
            ensembles: vec![LedgerEnsemble { first_entry_id: EntryId::MIN, bookies: ensemble }],
            digest_type: options.digest_type,
            custom_metadata: options.custom_metadata,
            format_version: 3,
            creation_time: Some(SystemTime::now()),
            creator_token: (rand::random::<usize>() & i64::MAX as usize) as i64,
        };
        let version = self.meta_store.create_ledger_metadata(&metadata).await?;
        let master_key = digest::generate_master_key(&metadata.password);
        let digest_algorithm = DigestAlgorithm::new(metadata.digest_type, &metadata.password);
        let writer_options = WriterOptions {
            deferred_sync: options.deferred_sync,
            master_key,
            digest_algorithm: digest_algorithm.clone(),
        };
        let (request_sender, metadata_watcher) =
            self.start_ledger_writer(writer_options, version, metadata.clone()).await?;
        let entry_distribution = EntryDistribution::from_metadata(&metadata);
        Ok(LedgerAppender {
            reader: LedgerReader {
                ledger_id,
                metadata: RefCell::new(metadata),
                metadata_version: Cell::new(version),
                client: self.bookie_client.clone(),
                last_add_confirmed: Cell::new(EntryId::INVALID),
                entry_distribution,
                master_key,
                digest_algorithm,
                updating_metadata: Cell::new(Some(metadata_watcher)),
                marker: PhantomData,
            },
            last_add_entry_id: Cell::new(EntryId::INVALID),
            request_sender,
        })
    }

    async fn start_ledger_writer(
        &self,
        options: WriterOptions,
        version: MetaVersion,
        metadata: LedgerMetadata,
    ) -> Result<(mpsc::Sender<WriteRequest>, watch::Receiver<Versioned<LedgerMetadata>>)> {
        let (request_sender, request_receiver) = mpsc::channel(512);
        let metadata_stream = self.meta_store.watch_ledger_metadata(metadata.ledger_id, version).await?;
        let (metadata_sender, metadata_receiver) = mpsc::channel(128);
        let metadata_watcher = watch_metadata_stream_and_updates(
            Versioned::new(version, metadata.clone()),
            metadata_stream,
            metadata_receiver,
        );
        let writer = LedgerWriter {
            ledger_id: metadata.ledger_id,
            client: self.bookie_client.clone(),
            deferred_sync: options.deferred_sync,
            entry_distribution: EntryDistribution::from_metadata(&metadata),
            master_key: options.master_key,
            digest_algorithm: options.digest_algorithm,
            meta_store: self.meta_store.clone(),
            placement_policy: self.placement_policy.clone(),
        };
        tokio::spawn(async move {
            writer
                .write_state_loop(
                    Versioned::new(version, metadata),
                    EntryId::INVALID,
                    0i64.into(),
                    request_receiver,
                    metadata_sender,
                )
                .await;
        });
        Ok((request_sender, metadata_watcher))
    }

    /// Deletes ledger with given id.
    ///
    /// # Notable errors
    /// * [ErrorKind::LedgerNotExisted] if no such ledger.
    pub async fn delete_ledger(&self, ledger_id: LedgerId, _options: DeleteOptions) -> Result<()> {
        self.meta_store.remove_ledger_metadata(ledger_id, None).await?;
        Ok(())
    }
}
