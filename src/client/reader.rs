use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{ready, FusedFuture, FutureExt};
use static_assertions::{assert_impl_all, assert_not_impl_any};
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};

use super::bookie::{self, PolledEntry, PoolledClient};
use super::cell::{Cell, RefCell};
use super::digest::Algorithm as DigestAlgorithm;
use super::entry_distribution::{EntryDistribution, HasEntryDistribution};
use super::errors::{BkError, ErrorKind};
use super::metadata::{BookieId, EntryId, LedgerId, LedgerLength, LedgerMetadata, LedgerState};
use super::placement::RandomPlacementPolicy;
use super::writer::{LedgerWriter, WriteRequest};
use crate::future::SelectAll;
use crate::marker::Sendable;
use crate::meta::{MetaStore, MetaVersion, Versioned};

type Result<T> = std::result::Result<T, BkError>;

/// Options to read entries.
#[derive(Default)]
#[non_exhaustive]
pub struct ReadOptions {
    parallel: bool,
}

impl ReadOptions {
    /// Reads entries from bookies parallelly.
    pub fn parallel(self) -> Self {
        ReadOptions { parallel: true, ..self }
    }
}

/// Options to poll written or about-to-write entry.
#[derive(Debug)]
#[non_exhaustive]
pub struct PollOptions {
    parallel: bool,
    timeout: Duration,
}

impl PollOptions {
    /// Constructs options for polling entry with given timeout.
    pub fn new(timeout: Duration) -> PollOptions {
        PollOptions { parallel: false, timeout }
    }

    /// Polls entry from write bookies parallelly.
    pub fn parallel(self) -> Self {
        PollOptions { parallel: true, ..self }
    }
}

/// Options to read last_add_confirmed.
#[derive(Default, Debug)]
#[non_exhaustive]
pub struct LacOptions {
    quorum: bool,
}

impl LacOptions {
    /// Waits reads from quorum of ensemble to consider success.
    pub fn quorum(self) -> Self {
        LacOptions { quorum: true, ..self }
    }
}

/// Ledger reader.
pub struct LedgerReader {
    pub(crate) ledger_id: LedgerId,
    pub(crate) metadata: RefCell<LedgerMetadata>,
    pub(crate) metadata_version: Cell<MetaVersion>,
    pub(crate) last_add_confirmed: Cell<EntryId>,
    pub(crate) client: Arc<PoolledClient>,
    pub(crate) entry_distribution: EntryDistribution,
    pub(crate) master_key: [u8; 20],
    pub(crate) digest_algorithm: DigestAlgorithm,
    pub(crate) updating_metadata: Cell<Option<watch::Receiver<Versioned<LedgerMetadata>>>>,
    pub(crate) marker: PhantomData<Sendable>,
}

unsafe impl Send for LedgerReader {}

assert_impl_all!(LedgerReader: Send);
assert_not_impl_any!(LedgerReader: Sync);
assert_not_impl_any!(Arc<LedgerReader>: Send);

impl std::fmt::Debug for LedgerReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LedgerReader{{ledger_id: {}}}", self.ledger_id)
    }
}

impl Clone for LedgerReader {
    fn clone(&self) -> LedgerReader {
        // Cell need Copy to be Clone while watch::Receiver is not Copy.
        let updating_metadata = unsafe { &*self.updating_metadata.as_ptr() };
        LedgerReader {
            ledger_id: self.ledger_id,
            metadata: self.metadata.clone(),
            metadata_version: self.metadata_version.clone(),
            last_add_confirmed: self.last_add_confirmed.clone(),
            client: self.client.clone(),
            entry_distribution: self.entry_distribution,
            master_key: self.master_key,
            digest_algorithm: self.digest_algorithm.clone(),
            updating_metadata: Cell::new(updating_metadata.clone()),
            marker: PhantomData,
        }
    }
}

impl HasEntryDistribution for LedgerReader {
    fn entry_distribution(&self) -> &EntryDistribution {
        &self.entry_distribution
    }
}

impl LedgerReader {
    /// Returns ledger id.
    pub fn id(&self) -> LedgerId {
        self.ledger_id
    }

    unsafe fn set_newer_metadata(&self, newer: Versioned<LedgerMetadata>) {
        self.metadata_version.set(newer.version);
        let metadata = &mut *self.metadata.as_ptr();
        if metadata.state != LedgerState::Closed && newer.value.state == LedgerState::Closed {
            self.last_add_confirmed.set(newer.value.last_entry_id);
        }
        *metadata = newer.value;
    }

    pub(crate) fn update_metadata(&mut self, metadata: Versioned<LedgerMetadata>) {
        if metadata.version > self.metadata_version.get() {
            unsafe { self.set_newer_metadata(metadata) };
        }
    }

    // SAFETY: This function depends on a not-verified assumption that select! with ready
    // clause will not yield its execution, otherwise metadata could borrowed by new
    // operation.
    async fn sync_metadata(&self) {
        if self.metadata.try_borrow_mut().is_err() {
            // Don't change metadata when there are ongoing reads.
            return;
        };
        let mut receiver = if let Some(receiver) = self.updating_metadata.take() {
            receiver
        } else {
            return;
        };
        select! {
            biased;
            r = receiver.changed() => {
                if r.is_err() {
                    return;
                }
                let updated = receiver.borrow();
                if updated.version > self.metadata_version.get() {
                    let newer = updated.clone();
                    drop(updated);
                    unsafe { self.set_newer_metadata(newer) };
                }
            },
            _ = ready(()) => {},
        }
        self.updating_metadata.set(Some(receiver));
    }

    /// Gets local cached last_add_confirmed.
    pub fn last_add_confirmed(&self) -> EntryId {
        self.last_add_confirmed.get()
    }

    fn read_options(&self, fence: bool) -> bookie::ReadOptions<'_> {
        bookie::ReadOptions {
            fence_ledger: fence,
            high_priority: fence,
            digest_algorithm: &self.digest_algorithm,
            master_key: if fence { Some(&self.master_key) } else { None },
        }
    }

    async fn poll_sequentially(
        &self,
        entry_id: EntryId,
        bookies: &[BookieId],
        timeout: Duration,
    ) -> Result<PolledEntry> {
        let options = bookie::PollOptions { timeout, digest_algorithm: &self.digest_algorithm };
        let write_set = self.new_write_set(entry_id);
        let mut err = None;
        for i in write_set.iter() {
            let bookie_id = &bookies[i];
            let result = self.client.poll_entry(bookie_id, self.id(), entry_id, &options).await;
            match result {
                Ok(polled_entry) => return Ok(polled_entry),
                Err(e) => err = err.or(Some(e)),
            }
        }
        Err(err.unwrap())
    }

    async fn poll_parallelly(&self, entry_id: EntryId, bookies: &[BookieId], timeout: Duration) -> Result<PolledEntry> {
        let options = bookie::PollOptions { timeout, digest_algorithm: &self.digest_algorithm };
        let mut futures = Vec::with_capacity(bookies.len());
        for bookie_id in bookies {
            let future = self.client.poll_entry(bookie_id, self.id(), entry_id, &options);
            futures.push(future.fuse());
        }
        let mut select_all = SelectAll::new(&mut futures);
        let mut err = None;
        for _ in 0..bookies.len() {
            let (_, r) = select_all.next().await;
            match r {
                Ok(polled_entry) => return Ok(polled_entry),
                Err(e) => err = err.or(Some(e)),
            }
        }
        Err(err.unwrap())
    }

    async fn read_sequentially<'a>(
        &'a self,
        entry_id: EntryId,
        fence: bool,
        ensemble: &'a [BookieId],
    ) -> Result<bookie::FetchedEntry> {
        let ensemble = unsafe { std::slice::from_raw_parts(ensemble.as_ptr(), ensemble.len()) };
        let write_set = self.new_write_set(entry_id);
        let read_options = self.read_options(fence);
        let mut err = None;
        for i in write_set.iter() {
            let bookie_id = &ensemble[i];
            let result = self.client.read_entry(bookie_id, self.ledger_id, entry_id, &read_options).await;
            match result {
                Ok(fetched_entry) => return Ok(fetched_entry),
                Err(e) => err = err.or(Some(e)),
            }
        }
        Err(err.unwrap())
    }

    async fn read_parallelly(
        &self,
        entry_id: EntryId,
        fence: bool,
        ensemble: &[BookieId],
    ) -> Result<bookie::FetchedEntry> {
        let write_set = self.new_write_set(entry_id);
        let read_options = self.read_options(fence);
        let mut futures = Vec::with_capacity(ensemble.len());
        for i in write_set.iter() {
            let bookie_id = &ensemble[i];
            let future = self.client.read_entry(bookie_id, self.ledger_id, entry_id, &read_options);
            futures.push(future.fuse());
        }
        let mut select_all = SelectAll::new(&mut futures);
        let mut err = None;
        for _ in write_set.iter() {
            let (_, r) = select_all.next().await;
            match r {
                Ok(fetched_entry) => return Ok(fetched_entry),
                Err(e) => err = err.or(Some(e)),
            }
        }
        Err(err.unwrap())
    }

    async fn read_entries<'a, F, R>(
        &'a self,
        first_entry: EntryId,
        last_entry: EntryId,
        metadata: &'a LedgerMetadata,
        read_fn: F,
    ) -> Result<Vec<Vec<u8>>>
    where
        R: Future<Output = Result<bookie::FetchedEntry>>,
        F: Fn(EntryId, &'a [BookieId]) -> R, {
        let n_entries = (last_entry - first_entry) as usize + 1;
        let mut reading_futures = Vec::with_capacity(n_entries);
        let mut reading_entry = first_entry;
        let mut ensemble_iter = metadata.ensemble_iter(first_entry);
        let (_, mut bookies, mut next_ensemble_entry_id) = unsafe { ensemble_iter.next().unwrap_unchecked() };
        while reading_entry <= last_entry {
            if reading_entry == next_ensemble_entry_id {
                (_, bookies, next_ensemble_entry_id) = unsafe { ensemble_iter.next().unwrap_unchecked() };
            }
            reading_futures.push(read_fn(reading_entry, bookies).fuse());
            reading_entry += 1;
        }
        let mut select_all = SelectAll::new(&mut reading_futures);
        let mut results = Vec::with_capacity(n_entries);
        results.resize(n_entries, Vec::new());
        let mut i = 0;
        while i < n_entries {
            let (j, r) = select_all.next().await;
            match r {
                Err(e) => return Err(e),
                Ok(r) => results[j] = r.payload,
            }
            i += 1;
        }
        Ok(results)
    }

    async fn read_internally(
        &self,
        first_entry: EntryId,
        last_entry: EntryId,
        options: Option<&ReadOptions>,
    ) -> Result<Vec<Vec<u8>>> {
        let parallel = options.map(|o| o.parallel).unwrap_or(false);
        let metadata = self.metadata.borrow();
        if metadata.closed() && last_entry > self.last_add_confirmed.get() {
            return Err(BkError::new(ErrorKind::ReadExceedLastAddConfirmed));
        }
        let entries = if parallel {
            self.read_entries(first_entry, last_entry, &metadata, |entry_id, bookies| {
                self.read_parallelly(entry_id, false, bookies)
            })
            .await?
        } else {
            self.read_entries(first_entry, last_entry, &metadata, |entry_id, bookies| {
                self.read_sequentially(entry_id, false, bookies)
            })
            .await?
        };
        drop(metadata);
        Ok(entries)
    }

    /// Reads entries from `first_entry` to `last_entry`.
    pub async fn read(
        &self,
        first_entry: EntryId,
        last_entry: EntryId,
        options: Option<&ReadOptions>,
    ) -> Result<Vec<Vec<u8>>> {
        assert!(first_entry <= last_entry);
        assert!(first_entry >= EntryId::MIN);
        self.sync_metadata().await;
        let last_add_confirmed = self.last_add_confirmed();
        if last_entry > last_add_confirmed {
            return Err(BkError::new(ErrorKind::ReadExceedLastAddConfirmed));
        }
        self.read_internally(first_entry, last_entry, options).await
    }

    /// Polls entry with given id.
    ///
    /// # Cautions
    /// * Ledger closing will not interrupt this operation.
    pub async fn poll(&self, entry_id: EntryId, options: &PollOptions) -> Result<Vec<u8>> {
        assert!(entry_id >= EntryId::MIN);
        let parallel = options.parallel;
        let mut timeout = options.timeout;
        let deadline = Instant::now() + timeout;
        let epsilon = Duration::from_millis(1);
        loop {
            self.sync_metadata().await;
            let mut last_add_confirmed = self.last_add_confirmed();
            let metadata = self.metadata.borrow();
            let (_, bookies, _) = metadata.ensemble_at(entry_id);
            if entry_id <= last_add_confirmed {
                let entry = if parallel {
                    self.read_parallelly(entry_id, false, bookies).await?
                } else {
                    self.read_sequentially(entry_id, false, bookies).await?
                };
                return Ok(entry.payload);
            }
            if timeout < epsilon {
                return Err(BkError::new(ErrorKind::Timeout));
            }
            let polled_entry = if parallel {
                self.poll_parallelly(entry_id, bookies, timeout).await?
            } else {
                self.poll_sequentially(entry_id, bookies, timeout).await?
            };
            if polled_entry.last_add_confirmed > last_add_confirmed {
                last_add_confirmed = polled_entry.last_add_confirmed;
                self.last_add_confirmed.set(last_add_confirmed);
            }
            if let Some(payload) = polled_entry.payload {
                return Ok(payload);
            } else if entry_id > last_add_confirmed {
                return Err(BkError::new(ErrorKind::ReadExceedLastAddConfirmed));
            }
            timeout = deadline.saturating_duration_since(Instant::now());
        }
    }

    async fn cover_quorum<R, T, Fu, Fn>(&self, futures: &mut [Fu], initial: R, mut f: Fn) -> Result<R>
    where
        Fu: FusedFuture<Output = Result<T>>,
        Fn: FnMut(R, T) -> R, {
        assert_eq!(futures.len(), self.entry_distribution.ensemble_size);
        let mut acc = initial;
        let mut err = None;
        let mut quorum = self.entry_distribution.new_quorum_coverage_set();
        let mut select_all = SelectAll::new(futures);
        loop {
            let (i, r) = select_all.next().await;
            match r {
                Err(e) => {
                    if e.kind() == ErrorKind::LedgerNotExisted || e.kind() == ErrorKind::EntryNotExisted {
                        quorum.complete_bookie(i);
                    } else {
                        quorum.fail_bookie(i);
                        err = err.or(Some(e));
                    }
                },
                Ok(value) => {
                    acc = f(acc, value);
                    quorum.complete_bookie(i);
                },
            };
            if let Some(covered) = quorum.covered() {
                if covered {
                    return Ok(acc);
                }
                return Err(err.unwrap());
            }
        }
    }

    async fn read_last_confirmed_meta(&self, fence: bool) -> Result<(EntryId, LedgerLength)> {
        self.sync_metadata().await;
        let metadata = self.metadata.borrow();
        if metadata.closed() {
            return Ok((metadata.last_entry_id, metadata.length));
        }
        let ensemble = metadata.last_ensemble();
        let options = bookie::ReadOptions {
            fence_ledger: fence,
            high_priority: false,
            master_key: if fence { Some(&self.master_key) } else { None },
            digest_algorithm: &self.digest_algorithm,
        };
        let mut readings = Vec::with_capacity(ensemble.bookies.len());
        for bookie_id in ensemble.bookies.iter() {
            let read = self.client.read_last_entry(bookie_id, self.id(), &options);
            readings.push(read.fuse());
        }
        let last_add_confirmed = self
            .cover_quorum(
                &mut readings,
                ensemble.first_entry_id - 1,
                |last_add_confirmed, (_, bookie::FetchedEntry { max_lac, .. })| last_add_confirmed.max(max_lac),
            )
            .await?;
        if last_add_confirmed == EntryId::INVALID {
            return Ok((EntryId::INVALID, 0i64.into()));
        }
        let (_, bookies, _) = metadata.ensemble_at(last_add_confirmed);
        let fetched_entry = self.read_parallelly(last_add_confirmed, false, bookies).await?;
        Ok((last_add_confirmed, fetched_entry.ledger_length))
    }

    /// Reads last_add_confirmed from latest ensemble.
    pub async fn read_last_add_confirmed(&self, options: &LacOptions) -> Result<EntryId> {
        self.sync_metadata().await;
        if self.closed() {
            return Ok(self.last_add_confirmed());
        }
        let metadata = self.metadata.borrow();
        let ensemble = metadata.last_ensemble();
        let mut readings = Vec::with_capacity(ensemble.bookies.len());
        for bookie_id in ensemble.bookies.iter() {
            let read = self.client.read_lac(bookie_id, self.id(), &self.digest_algorithm);
            readings.push(read.fuse());
        }
        if !options.quorum {
            let mut select_all = SelectAll::new(&mut readings);
            let mut err = None;
            loop {
                select! {
                    (_, r) = select_all.next() => {
                        match r {
                            Err(e) => err = err.or(Some(e)),
                            Ok(entry_id) if entry_id > self.last_add_confirmed.get() => {
                                self.last_add_confirmed.set(entry_id);
                                return Ok(entry_id);
                            },
                            _ => {},
                        };
                    },
                }
                if select_all.is_terminated() {
                    if let Some(err) = err {
                        return Err(err);
                    }
                    return Ok(self.last_add_confirmed());
                }
            }
        }
        let last_add_confirmed =
            self.cover_quorum(&mut readings, self.last_add_confirmed(), |acc, new| acc.max(new)).await?;
        self.last_add_confirmed.set(last_add_confirmed);
        Ok(last_add_confirmed)
    }

    /// Reads entries without checking `last_add_confirmed` locally if ledger not considered
    /// closed.
    ///
    /// # Notable errors
    /// * [ErrorKind::ReadExceedLastAddConfirmed] if ledger closed and given entry id exceed last
    /// add confirmed.
    /// * [ErrorKind::EntryNotExisted] if given entry does not exists.
    pub async fn read_unconfirmed(
        &self,
        first_entry: EntryId,
        last_entry: EntryId,
        options: Option<&ReadOptions>,
    ) -> Result<Vec<Vec<u8>>> {
        assert!(first_entry <= last_entry);
        assert!(first_entry >= EntryId::MIN);
        self.sync_metadata().await;
        self.read_internally(first_entry, last_entry, options).await
    }

    async fn recover_open_metadata(
        &self,
        mut version: MetaVersion,
        mut metadata: LedgerMetadata,
        meta_store: &Arc<dyn MetaStore>,
    ) -> Result<(MetaVersion, LedgerMetadata)> {
        loop {
            if metadata.state == LedgerState::Closed {
                return Ok((version, metadata));
            } else if metadata.state == LedgerState::InRecovery {
                // Someone is recovering, let it go.
                return Err(BkError::with_description(ErrorKind::LedgerConcurrentClose, &"ledger already in recovery"));
            }
            metadata.state = LedgerState::InRecovery;
            let r = meta_store.write_ledger_metadata(&metadata, version).await?;
            match r {
                either::Right(version) => return Ok((version, metadata)),
                either::Left(Versioned { version: conflicting_version, value: conflicting_metadata }) => {
                    version = conflicting_version;
                    metadata = conflicting_metadata;
                },
            }
        }
    }

    fn start_recover_writer(
        &self,
        metadata: Versioned<LedgerMetadata>,
        metadata_sender: mpsc::Sender<Versioned<LedgerMetadata>>,
        meta_store: &Arc<dyn MetaStore>,
        placement_policy: Arc<RandomPlacementPolicy>,
        last_confirmed_entry_id: EntryId,
        last_confirmed_ledger_length: LedgerLength,
    ) -> mpsc::Sender<WriteRequest> {
        let (request_sender, request_receiver) = mpsc::channel(50);
        let writer = LedgerWriter {
            ledger_id: metadata.value.ledger_id,
            client: self.client.clone(),
            deferred_sync: false,
            entry_distribution: EntryDistribution::from_metadata(&metadata.value),
            master_key: self.master_key,
            digest_algorithm: self.digest_algorithm.clone(),
            meta_store: meta_store.clone(),
            placement_policy,
        };
        tokio::spawn(async move {
            writer
                .write_state_loop(
                    metadata,
                    last_confirmed_entry_id,
                    last_confirmed_ledger_length,
                    request_receiver,
                    metadata_sender,
                )
                .await;
        });
        request_sender
    }

    pub(crate) async fn recover(
        &mut self,
        version: MetaVersion,
        metadata_sender: mpsc::Sender<Versioned<LedgerMetadata>>,
        meta_store: &Arc<dyn MetaStore>,
        placement_policy: Arc<RandomPlacementPolicy>,
    ) -> Result<()> {
        let metadata = self.metadata.get_mut().clone();
        let (version, metadata) = self.recover_open_metadata(version, metadata, meta_store).await?;
        if metadata.closed() {
            *self.metadata.get_mut() = metadata;
            return Ok(());
        }
        let (mut last_add_confirmed, ledger_length) = self.read_last_confirmed_meta(true).await?;
        let request_sender = self.start_recover_writer(
            Versioned::new(version, metadata.clone()),
            metadata_sender,
            meta_store,
            placement_policy,
            last_add_confirmed,
            ledger_length,
        );
        let ensemble = metadata.last_ensemble();
        loop {
            let entry_id = last_add_confirmed + 1;
            let payload = match self.read_parallelly(entry_id, true, &ensemble.bookies).await {
                Err(e) => {
                    let kind = e.kind();
                    if kind == ErrorKind::EntryNotExisted || kind == ErrorKind::LedgerNotExisted {
                        break;
                    }
                    return Err(e);
                },
                Ok(fetched_entry) => fetched_entry.payload,
            };
            let (sender, receiver) = oneshot::channel();
            if request_sender.send(WriteRequest::Append { entry_id, payload, responser: sender }).await.is_err() {
                let err = BkError::with_description(ErrorKind::UnexpectedError, &"writer closed during recovery");
                return Err(err);
            }
            receiver.await.map_err(|_| {
                BkError::with_description(ErrorKind::UnexpectedError, &"writer failure during recovery")
            })??;
            last_add_confirmed = entry_id;
        }
        let (close_sender, close_receiver) = oneshot::channel();
        request_sender.send(WriteRequest::Close { responser: close_sender }).await.unwrap();
        let metadata = close_receiver.await.unwrap()?;
        self.update_metadata(metadata);
        Ok(())
    }

    /// Returns whether ledger has been closed or not.
    pub fn closed(&self) -> bool {
        return self.metadata.borrow().closed();
    }
}
