use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::{Fuse, FusedFuture, FutureExt};
use ignore_result::Ignore;
use static_assertions::{assert_impl_all, assert_not_impl_any};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Sleep};

use super::bookie::{self, PoolledClient};
use super::cell::Cell;
use super::digest::Algorithm as DigestAlgorithm;
use super::entry_distribution::{AckSet, EntryDistribution, HasEntryDistribution, WriteSet};
use super::errors::{BkError, ErrorKind};
use super::local_rc::LocalRc;
use super::metadata::{
    BookieId,
    EntryId,
    HasLedgerMetadata,
    LedgerEnsemble,
    LedgerId,
    LedgerLength,
    LedgerMetadata,
    LedgerState,
};
use super::placement::{EnsembleOptions, PlacementPolicy, RandomPlacementPolicy};
use super::reader::LedgerReader;
use crate::future::{SelectAll, SelectIterable};
use crate::meta::{MetaStore, MetaVersion, Versioned};

type Result<T> = std::result::Result<T, BkError>;

#[derive(Clone, Copy, Debug)]
pub struct AddOptions {
    recovery_add: bool,
    high_priority: bool,
    last_add_confirmed: EntryId,
    ledger_length: LedgerLength,
}

/// Options to close ledgerr.
#[derive(Default)]
pub struct CloseOptions {}

pub(crate) struct WriterOptions {
    pub deferred_sync: bool,
    pub master_key: [u8; 20],
    pub digest_algorithm: DigestAlgorithm,
}

struct AddEntryFuture<F: Future> {
    entry_id: EntryId,
    payload: Vec<u8>,
    recovery: bool,
    last_add_confirmed: EntryId,
    ledger_length: LedgerLength,
    write_set: WriteSet,
    ack_set: AckSet,
    write_futures: Vec<Fuse<F>>,
    responser: Option<oneshot::Sender<Result<AddedEntry>>>,
}

impl<F: Future> AddEntryFuture<F> {
    fn to_bookie_index(&self, write_index: usize) -> usize {
        self.write_set.bookie_index(write_index)
    }

    fn start_write<'a, 'b, AddEntryFn>(&'a mut self, bookies: &[BookieId], add_entry: &AddEntryFn)
    where
        AddEntryFn: Fn(BookieId, EntryId, &'b [u8], AddOptions) -> F, {
        let options = AddOptions {
            last_add_confirmed: self.last_add_confirmed,
            ledger_length: self.ledger_length,
            recovery_add: self.recovery,
            high_priority: false,
        };
        let payload = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(self.payload.as_slice()) };
        for bookie_index in self.write_set.iter() {
            let bookie_id = bookies[bookie_index].clone();
            self.write_futures.push(add_entry(bookie_id, self.entry_id, payload, options).fuse());
        }
    }

    fn update_write<'a, 'b, AddEntryFn>(&'a mut self, changed: &[bool], bookies: &[BookieId], add_entry: &AddEntryFn)
    where
        AddEntryFn: Fn(BookieId, EntryId, &'b [u8], AddOptions) -> F, {
        let options = AddOptions {
            last_add_confirmed: self.last_add_confirmed,
            ledger_length: self.ledger_length,
            recovery_add: self.recovery,
            high_priority: false,
        };
        let payload = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(self.payload.as_slice()) };
        for (write_index, bookie_index) in self.write_set.iter().enumerate() {
            if !changed[bookie_index] {
                continue;
            }
            let bookie_id = bookies[bookie_index].clone();
            self.write_futures[write_index] = add_entry(bookie_id, self.entry_id, payload, options).fuse();
            self.ack_set.unset_write(write_index);
        }
    }

    fn terminate_write(&mut self, changed: &[bool]) {
        for (write_index, bookie_index) in self.write_set.iter().enumerate() {
            if changed[bookie_index] {
                self.write_futures[write_index] = Fuse::terminated();
            }
        }
    }
}

impl<F: Future> Unpin for AddEntryFuture<F> {}
unsafe impl<F: Future> Send for AddEntryFuture<F> {}

impl<F: Future> Future for AddEntryFuture<F> {
    type Output = (usize, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut iter = SelectIterable::new(&mut self.write_futures);
        let pinned = unsafe { Pin::new_unchecked(&mut iter) };
        pinned.poll(cx)
    }
}

impl<F: Future> FusedFuture for AddEntryFuture<F> {
    fn is_terminated(&self) -> bool {
        return self.write_futures.iter().all(|f| f.is_terminated());
    }
}

pub(crate) struct LedgerWriter {
    pub ledger_id: LedgerId,
    pub client: Arc<PoolledClient>,
    pub entry_distribution: EntryDistribution,
    pub placement_policy: Arc<RandomPlacementPolicy>,
    pub meta_store: Arc<dyn MetaStore>,
    pub master_key: [u8; 20],
    pub digest_algorithm: DigestAlgorithm,

    pub deferred_sync: bool,
}

struct WriterState<
    'a,
    AddFuture,
    EnsembleFuture,
    ForceFuture,
    CloseFuture,
    LacFuture,
    EntryWriter,
    LedgerForcer,
    LedgerCloser,
    EnsembleChanger,
    LacFlusher,
> where
    AddFuture: Future,
    EnsembleFuture: Future,
    ForceFuture: Future,
    CloseFuture: Future,
    LacFuture: Future,
    EntryWriter: Fn(BookieId, EntryId, &'static [u8], AddOptions) -> AddFuture,
    LedgerForcer: Fn(Vec<BookieId>, EntryId) -> ForceFuture,
    LedgerCloser: Fn(Versioned<LedgerMetadata>, EntryId, LedgerLength, bool, Vec<LedgerEnsemble>) -> CloseFuture,
    EnsembleChanger: Fn(Versioned<LedgerMetadata>, EntryId, Vec<bool>) -> EnsembleFuture,
    LacFlusher: Fn(Vec<BookieId>, EntryId) -> LacFuture, {
    state: LedgerState,
    recovery: bool,
    recovery_ensembles: Vec<LedgerEnsemble>,

    fatal: Option<BkError>,
    closing: bool,
    close_future: Fuse<CloseFuture>,
    close_waiters: Vec<oneshot::Sender<Result<Versioned<LedgerMetadata>>>>,
    ledger_closer: LedgerCloser,

    writer: &'a LedgerWriter,
    metadata: LocalRc<Versioned<LedgerMetadata>>,
    write_ensemble: LedgerEnsemble,
    changed_bookies: Box<[bool]>,

    write_quorum_size: usize,

    last_adding_entry_id: EntryId,
    last_add_entry_id: EntryId,
    last_add_confirmed: EntryId,
    last_add_ledger_length: LedgerLength,
    ledger_length: LedgerLength,

    adding_entries: VecDeque<AddEntryFuture<AddFuture>>,
    confirmed_entries: VecDeque<AddEntryFuture<AddFuture>>,
    released_futures: Vec<Vec<Fuse<AddFuture>>>,

    force_future: Fuse<ForceFuture>,
    force_entry_id: EntryId,
    pending_forces: VecDeque<PendingForce>,
    ledger_forcer: LedgerForcer,

    failed_bookies: Vec<bool>,
    has_failed_bookies: bool,
    ensemble_changing: Fuse<EnsembleFuture>,
    ensemble_changer: EnsembleChanger,

    lac_future: Fuse<LacFuture>,
    lac_timeout: Fuse<Sleep>,
    lac_flushed: EntryId,
    lac_flusher: LacFlusher,
    lac_duration: Duration,

    entry_writer: EntryWriter,
}

impl<
        AddFuture,
        EnsembleFuture,
        ForceFuture,
        CloseFuture,
        LacFuture,
        EntryWriter,
        LedgerForcer,
        LedgerCloser,
        EnsembleChanger,
        LacFlusher,
    >
    WriterState<
        '_,
        AddFuture,
        EnsembleFuture,
        ForceFuture,
        CloseFuture,
        LacFuture,
        EntryWriter,
        LedgerForcer,
        LedgerCloser,
        EnsembleChanger,
        LacFlusher,
    >
where
    AddFuture: Future<Output = Result<()>>,
    EnsembleFuture: Future<Output = Result<Versioned<LedgerMetadata>>>,
    ForceFuture: Future<Output = Result<EntryId>>,
    CloseFuture: Future<Output = Result<Versioned<LedgerMetadata>>>,
    LacFuture: Future<Output = EntryId>,
    EntryWriter: Fn(BookieId, EntryId, &'static [u8], AddOptions) -> AddFuture,
    LedgerForcer: Fn(Vec<BookieId>, EntryId) -> ForceFuture,
    LedgerCloser: Fn(Versioned<LedgerMetadata>, EntryId, LedgerLength, bool, Vec<LedgerEnsemble>) -> CloseFuture,
    EnsembleChanger: Fn(Versioned<LedgerMetadata>, EntryId, Vec<bool>) -> EnsembleFuture,
    LacFlusher: Fn(Vec<BookieId>, EntryId) -> LacFuture,
{
    async fn run_once(&mut self) -> Option<Versioned<LedgerMetadata>> {
        self.check_ensemble();
        futures::select! {
            (entry_index, (write_index, r)) = SelectIterable::next(&mut self.adding_entries).fuse() => {
                self.complete_add(entry_index, write_index, r);
            },
            (entry_index, (write_index, r)) = SelectIterable::next(&mut self.confirmed_entries).fuse() => {
                self.complete_confirmed(entry_index, write_index, r);
            },
            r = unsafe {Pin::new_unchecked(&mut self.ensemble_changing)} => {
                let new_metadata = match r {
                    Err(err) => {
                        self.on_fatal_error(err);
                        return None;
                    },
                    Ok(new_metadata) => new_metadata,
                };
                self.complete_ensemble_change(new_metadata.clone());
                return Some(new_metadata);
            },
            r = unsafe { Pin::new_unchecked(&mut self.force_future) } => {
                match r {
                    Err(err) => self.fail_ledger_force(err),
                    Ok(last_add_confirmed) => self.complete_ledger_force(last_add_confirmed),
                }
            },
            r = unsafe { Pin::new_unchecked(&mut self.close_future) } => {
                match r {
                    Err(err) => self.fail_ledger_close(err),
                    Ok(metadata) => {
                        self.complete_ledger_close(metadata.clone());
                        return Some(metadata);
                    },
                }
            },
            lac = unsafe { Pin::new_unchecked(&mut self.lac_future) } => {
                self.lac_flushed = lac;
            },
            _ = unsafe { Pin::new_unchecked(&mut self.lac_timeout) } => {
                self.on_lac_timeout();
            },
        }
        None
    }

    fn check_ensemble(&mut self) {
        if !self.has_failed_bookies || self.fatal.is_some() || self.last_add_entry_id != self.last_add_confirmed {
            return;
        }
        if self.recovery {
            let bookies = match self.writer.select_ensemble(&self.metadata.value, &self.failed_bookies) {
                Err(_) => {
                    return;
                },
                Ok(bookies) => bookies,
            };
            let first_entry_id = self.last_add_confirmed + 1;
            let ensemble = LedgerEnsemble { first_entry_id, bookies };
            let replace = self.recovery_ensembles.last().map(|e| e.first_entry_id == first_entry_id).unwrap_or(false);
            if replace {
                self.recovery_ensembles.pop();
            }
            self.update_write_ensemble(&ensemble.bookies);
            self.recovery_ensembles.push(ensemble);
            return;
        }
        if self.ensemble_changing.is_terminated() {
            self.ensemble_changing = (self.ensemble_changer)(
                self.metadata.as_ref().clone(),
                self.last_add_confirmed,
                self.failed_bookies.clone(),
            )
            .fuse();
        }
    }

    fn closed(&self) -> bool {
        self.state == LedgerState::Closed
    }

    fn close_ledger(&mut self, responser: oneshot::Sender<Result<Versioned<LedgerMetadata>>>) {
        if self.closed() {
            let metadata = self.metadata.as_ref().clone();
            responser.send(Ok(metadata)).ignore();
            return;
        } else if self.closing {
            self.close_waiters.push(responser);
            return;
        }
        self.closing = true;
        self.close_waiters.push(responser);
        self.confirmed_entries.clear();
        self.ensemble_changing = Fuse::terminated();
        self.has_failed_bookies = false;
        self.check_pending_close();
    }

    fn check_pending_close(&mut self) {
        if self.close_waiters.is_empty() || !self.adding_entries.is_empty() || !self.close_future.is_terminated() {
            return;
        }
        if self.last_add_confirmed < self.last_add_entry_id {
            self.start_ledger_force();
            return;
        }
        let ensembles = self.recovery_ensembles.clone();
        self.close_future = (self.ledger_closer)(
            self.metadata.as_ref().clone(),
            self.last_add_confirmed,
            self.last_add_ledger_length,
            self.recovery,
            ensembles,
        )
        .fuse();
    }

    fn drain_ledger_close(&mut self, err: BkError) {
        let err = Err(err);
        self.closing = false;
        self.close_waiters.drain(..).for_each(|responser| {
            responser.send(err.clone()).ignore();
        });
    }

    fn fail_ledger_close(&mut self, err: BkError) {
        self.drain_ledger_close(err);
    }

    fn complete_ledger_close(&mut self, metadata: Versioned<LedgerMetadata>) {
        self.state = LedgerState::Closed;
        self.closing = false;
        self.metadata = LocalRc::new(metadata.clone());
        self.close_waiters.drain(..).for_each(|responser| {
            responser.send(Ok(metadata.clone())).ignore();
        });
    }

    fn check_append<R>(&self, responser: oneshot::Sender<Result<R>>) -> Option<oneshot::Sender<Result<R>>> {
        if self.closed() {
            responser.send(Err(BkError::new(ErrorKind::LedgerClosed))).ignore();
            return None;
        } else if let Some(err) = &self.fatal {
            responser.send(Err(err.clone())).ignore();
            return None;
        } else if self.closing {
            responser.send(Err(BkError::new(ErrorKind::LedgerClosing))).ignore();
            return None;
        }
        Some(responser)
    }

    fn append_entry(
        &mut self,
        expected_entry_id: EntryId,
        payload: Vec<u8>,
        responser: oneshot::Sender<Result<AddedEntry>>,
    ) {
        let Some(responser) = self.check_append(responser) else {
            return;
        };
        let entry_id = self.last_adding_entry_id + 1;
        if expected_entry_id.is_valid() && expected_entry_id != entry_id {
            let msg = format!("expect next entry id {}, got {}", entry_id, expected_entry_id);
            let err = BkError::with_message(ErrorKind::UnexpectedError, msg);
            responser.send(Err(err)).ignore();
            return;
        };
        self.ledger_length += payload.len() as i64;
        self.last_adding_entry_id = entry_id;
        let mut add_entry_future = AddEntryFuture {
            entry_id,
            payload,
            recovery: self.recovery,
            last_add_confirmed: self.last_add_confirmed,
            ledger_length: self.ledger_length,
            write_set: self.new_write_set(entry_id),
            ack_set: self.new_ack_set(),
            write_futures: self.released_futures.pop().unwrap_or_else(|| Vec::with_capacity(self.write_quorum_size)),
            responser: Some(responser),
        };
        add_entry_future.start_write(&self.write_ensemble.bookies, &self.entry_writer);
        self.adding_entries.push_back(add_entry_future);
    }

    fn is_terminal_error(err: ErrorKind) -> bool {
        matches!(err, ErrorKind::LedgerFenced | ErrorKind::UnauthorizedAccess)
    }

    fn on_fatal_error(&mut self, err: BkError) {
        if self.fatal.is_some() {
            return;
        }
        self.abort_write(&err);
        self.fatal = Some(err);
    }

    fn abort_write(&mut self, err: &BkError) {
        while let Some(entry_future) = self.adding_entries.pop_front() {
            let responser = entry_future.responser.unwrap();
            responser.send(Err(err.clone())).ignore();
        }
        self.confirmed_entries.clear();
        self.released_futures.clear();
        self.ensemble_changing = Fuse::terminated();
    }

    fn complete_add(&mut self, entry_index: usize, write_index: usize, result: Result<()>) {
        if let Err(err) = result {
            if Self::is_terminal_error(err.kind()) {
                self.on_fatal_error(err);
                return;
            }
            let entry_future = &mut self.adding_entries[entry_index];
            let bookie_index = entry_future.to_bookie_index(write_index);
            self.failed_bookies[bookie_index] = true;
            self.has_failed_bookies = true;
            return;
        }
        let entry_future = &mut self.adding_entries[entry_index];
        let completed = entry_future.ack_set.complete_write(write_index);
        if entry_index != 0 || !completed || !self.ensemble_changing.is_terminated() {
            return;
        }
        self.confirm_acked_entries();
    }

    fn complete_confirmed(&mut self, entry_index: usize, write_index: usize, result: Result<()>) {
        let entry_future = &self.confirmed_entries[entry_index];
        if result.is_err() {
            let bookie_index = entry_future.to_bookie_index(write_index);
            self.failed_bookies[bookie_index] = true;
            self.has_failed_bookies = true;
        }
        if entry_future.is_terminated() {
            let AddEntryFuture { mut write_futures, .. } =
                self.confirmed_entries.swap_remove_back(entry_index).unwrap();
            write_futures.clear();
            self.released_futures.push(write_futures);
        }
    }

    fn confirm_acked_entries(&mut self) {
        while let Some(entry_future) = self.adding_entries.front() {
            if !entry_future.ack_set.completed() {
                break;
            }
            let mut entry_future = self.adding_entries.pop_front().unwrap();
            let responser = entry_future.responser.take().unwrap();
            let added_entry = AddedEntry {
                entry_id: entry_future.entry_id,
                last_add_confirmed: if self.writer.deferred_sync {
                    self.last_add_confirmed
                } else {
                    entry_future.entry_id
                },
            };
            responser.send(Ok(added_entry)).ignore();
            self.last_add_entry_id = entry_future.entry_id;
            self.last_add_ledger_length += entry_future.payload.len();
            if !entry_future.is_terminated() {
                self.confirmed_entries.push_back(entry_future);
            } else {
                entry_future.write_futures.clear();
                self.released_futures.push(entry_future.write_futures);
            }
        }
        if !self.writer.deferred_sync {
            self.update_last_add_confirmed(self.last_add_entry_id);
        }
        self.check_pending_close();
    }

    fn update_last_add_confirmed(&mut self, last_add_confirmed: EntryId) {
        self.last_add_confirmed = last_add_confirmed;
        if last_add_confirmed > self.lac_flushed && !self.lac_duration.is_zero() && self.lac_timeout.is_terminated() {
            self.lac_timeout = time::sleep(self.lac_duration).fuse();
        }
    }

    fn on_lac_timeout(&mut self) {
        if self.last_add_confirmed == self.lac_flushed {
            return;
        }
        if self.lac_future.is_terminated() && self.ensemble_changing.is_terminated() {
            self.lac_future = (self.lac_flusher)(self.write_ensemble.bookies.clone(), self.last_add_confirmed).fuse();
        }
        self.lac_timeout = time::sleep(self.lac_duration).fuse();
    }

    fn force_ledger(&mut self, entry_id: EntryId, responser: oneshot::Sender<Result<EntryId>>) {
        if self.last_add_confirmed == self.last_add_entry_id || self.last_add_confirmed >= entry_id {
            responser.send(Ok(self.last_add_confirmed)).ignore();
            return;
        }
        self.pending_forces.push_back(PendingForce { last_add_entry_id: self.last_add_entry_id, responser });
        self.start_ledger_force();
    }

    fn force_last_added_entry(&mut self) {
        self.force_future = (self.ledger_forcer)(self.write_ensemble.bookies.clone(), self.last_add_entry_id).fuse();
        self.force_entry_id = self.last_add_entry_id;
    }

    fn start_ledger_force(&mut self) {
        if self.force_future.is_terminated() {
            self.force_last_added_entry();
        }
    }

    fn fail_ledger_force(&mut self, err: BkError) {
        self.pending_forces.drain(..).for_each(|force| {
            force.responser.send(Err(err.clone())).ignore();
        });
        if self.adding_entries.is_empty() && self.force_entry_id == self.last_add_entry_id {
            let err = BkError::new(ErrorKind::LedgerForceFailed).cause_by(err);
            self.drain_ledger_close(err);
        }
    }

    fn complete_ledger_force(&mut self, last_add_confirmed: EntryId) {
        self.update_last_add_confirmed(last_add_confirmed);
        while let Some(pending_force) = self.pending_forces.front() {
            if pending_force.last_add_entry_id > self.last_add_confirmed {
                self.force_last_added_entry();
                return;
            }
            let pending_force = self.pending_forces.pop_front().unwrap();
            pending_force.responser.send(Ok(self.last_add_confirmed)).ignore();
        }
        self.check_pending_close();
    }

    fn update_write_ensemble(&mut self, new_bookies: &[BookieId]) {
        let bookies = &mut self.write_ensemble.bookies;
        let changed_bookies = self.changed_bookies.as_mut();
        self.has_failed_bookies = false;
        for (i, bookie_id) in new_bookies.iter().enumerate() {
            if bookies[i] == *bookie_id {
                self.has_failed_bookies |= self.failed_bookies[i];
            } else {
                bookies[i] = bookie_id.clone();
                changed_bookies[i] = true;
                self.failed_bookies[i] = false;
            }
        }
        self.on_write_ensemble_changed();
        self.changed_bookies.fill(false);
    }

    fn on_write_ensemble_changed(&mut self) {
        let bookies = &self.write_ensemble.bookies;
        let changed_bookies = self.changed_bookies.as_ref();
        let mut i = 0usize;
        while i < self.confirmed_entries.len() {
            let entry_future = &mut self.confirmed_entries[i];
            entry_future.terminate_write(changed_bookies);
            if !entry_future.is_terminated() {
                i += 1;
                continue;
            }
            let AddEntryFuture { mut write_futures, .. } = self.confirmed_entries.swap_remove_back(i).unwrap();
            write_futures.clear();
            self.released_futures.push(write_futures);
        }
        for entry_future in self.adding_entries.iter_mut() {
            entry_future.update_write(changed_bookies, bookies, &self.entry_writer);
        }
        self.writer.client.prepare_ensemble(bookies.as_slice());
        self.confirm_acked_entries();
    }

    fn complete_ensemble_change(&mut self, new_metadata: Versioned<LedgerMetadata>) {
        let state = new_metadata.value.state;
        if state == LedgerState::InRecovery {
            let err = BkError::new(ErrorKind::LedgerFenced);
            self.abort_write(&err);
            self.fatal = Some(err.clone());
        } else if state == LedgerState::Closed {
            self.state = LedgerState::Closed;
            self.abort_write(&BkError::new(ErrorKind::LedgerClosed));
        } else {
            let last_ensemble = new_metadata.last_ensemble();
            self.update_write_ensemble(&last_ensemble.bookies);
        }
        self.metadata = LocalRc::new(new_metadata);
    }
}

impl<
        AddFuture,
        EnsembleFuture,
        ForceFuture,
        CloseFuture,
        LacFuture,
        EntryWriter,
        LedgerForcer,
        LedgerCloser,
        EnsembleChanger,
        LacFlusher,
    > HasEntryDistribution
    for WriterState<
        '_,
        AddFuture,
        EnsembleFuture,
        ForceFuture,
        CloseFuture,
        LacFuture,
        EntryWriter,
        LedgerForcer,
        LedgerCloser,
        EnsembleChanger,
        LacFlusher,
    >
where
    AddFuture: Future,
    EnsembleFuture: Future,
    ForceFuture: Future,
    CloseFuture: Future,
    LacFuture: Future,
    EntryWriter: Fn(BookieId, EntryId, &'static [u8], AddOptions) -> AddFuture,
    LedgerForcer: Fn(Vec<BookieId>, EntryId) -> ForceFuture,
    LedgerCloser: Fn(Versioned<LedgerMetadata>, EntryId, LedgerLength, bool, Vec<LedgerEnsemble>) -> CloseFuture,
    EnsembleChanger: Fn(Versioned<LedgerMetadata>, EntryId, Vec<bool>) -> EnsembleFuture,
    LacFlusher: Fn(Vec<BookieId>, EntryId) -> LacFuture,
{
    fn entry_distribution(&self) -> &EntryDistribution {
        self.writer.entry_distribution()
    }
}

impl HasEntryDistribution for LedgerWriter {
    fn entry_distribution(&self) -> &EntryDistribution {
        &self.entry_distribution
    }
}

impl LedgerWriter {
    pub(crate) async fn write_state_loop(
        &self,
        metadata: Versioned<LedgerMetadata>,
        last_confirmed_entry_id: EntryId,
        last_confirmed_ledger_length: LedgerLength,
        mut request_receiver: mpsc::Receiver<WriteRequest>,
        metadata_sender: mpsc::Sender<Versioned<LedgerMetadata>>,
    ) {
        let ensemble_size = metadata.value.ensemble_size as usize;
        let write_quorum_size = metadata.value.write_quorum_size as usize;
        let last_ensemble = metadata.last_ensemble().clone();
        let ledger_state = metadata.value.state;
        let mut state = WriterState {
            writer: self,
            state: ledger_state,
            recovery: ledger_state == LedgerState::InRecovery,
            recovery_ensembles: Vec::new(),

            fatal: None,
            closing: false,
            close_future: Fuse::terminated(),
            close_waiters: Vec::with_capacity(5),
            ledger_closer: |metadata, last_add_confirmed, ledger_length, recovery, ensembles| {
                self.close_ledger(metadata, last_add_confirmed, ledger_length, recovery, ensembles)
            },

            metadata: LocalRc::new(metadata),
            write_ensemble: last_ensemble,
            changed_bookies: vec![false; ensemble_size].into_boxed_slice(),
            write_quorum_size,

            adding_entries: VecDeque::new(),
            confirmed_entries: VecDeque::new(),
            released_futures: Vec::new(),

            last_adding_entry_id: last_confirmed_entry_id,
            last_add_entry_id: last_confirmed_entry_id,
            last_add_confirmed: last_confirmed_entry_id,
            last_add_ledger_length: last_confirmed_ledger_length,
            ledger_length: last_confirmed_ledger_length,

            force_future: Fuse::terminated(),
            force_entry_id: EntryId::INVALID,
            pending_forces: VecDeque::new(),
            ledger_forcer: |ensemble, last_add_entry_id| self.force_ledger(ensemble, last_add_entry_id),

            failed_bookies: vec![false; ensemble_size],
            has_failed_bookies: false,
            ensemble_changing: Fuse::terminated(),

            lac_future: Fuse::terminated(),
            lac_timeout: Fuse::terminated(),
            lac_flushed: last_confirmed_entry_id,
            lac_flusher: |ensemble, last_add_confirmed| self.write_lac(ensemble, last_add_confirmed),
            lac_duration: Duration::from_secs(1),

            entry_writer: |bookie_id, entry_id, payload, options| self.add_entry(bookie_id, entry_id, payload, options),
            ensemble_changer: |metadata: Versioned<LedgerMetadata>, last_add_confirmed, failed_bookies| {
                self.change_ensemble(metadata.version, metadata.value, last_add_confirmed + 1, failed_bookies)
            },
        };

        let mut channel_closed = false;
        let mut metadata_sending = Fuse::terminated();
        while !(channel_closed && state.closed()) {
            select! {
                request = request_receiver.recv(), if !channel_closed => {
                    let Some(request) = request else {
                        channel_closed = true;
                        continue;
                    };
                    match request {
                        WriteRequest::Append{entry_id, payload, responser} => {
                            state.append_entry(entry_id, payload, responser);
                        },
                        WriteRequest::Force{entry_id, responser} => {
                            state.force_ledger(entry_id, responser);
                        },
                        WriteRequest::Close{responser} => {
                            state.close_ledger(responser);
                        },
                    }
                },
                new_metadata = state.run_once() => {
                    if let Some(metadata) = new_metadata {
                        metadata_sending = metadata_sender.send(metadata).fuse();
                    }
                },
                _ = unsafe {Pin::new_unchecked(&mut metadata_sending)} => {},
            }
        }
    }

    fn select_ensemble(&self, metadata: &LedgerMetadata, failed_bookies: &[bool]) -> Result<Vec<BookieId>> {
        let ensemble = metadata.last_ensemble();
        let ensemble_size = ensemble.bookies.len();
        let mut preferred_bookies = Vec::with_capacity(ensemble_size);
        let mut excluded_bookies = HashSet::with_capacity(ensemble_size);
        for (i, bookie_id) in ensemble.bookies.iter().enumerate() {
            if failed_bookies[i] {
                excluded_bookies.insert(bookie_id);
            } else {
                preferred_bookies.push(bookie_id);
            }
        }
        if excluded_bookies.is_empty() {
            return Ok(ensemble.bookies.to_vec());
        }
        let options = EnsembleOptions {
            ensemble_size: metadata.ensemble_size,
            write_quorum: metadata.write_quorum_size,
            ack_quorum: metadata.ack_quorum_size,
            custom_metadata: &metadata.custom_metadata,
            preferred_bookies: &preferred_bookies,
            excluded_bookies: excluded_bookies.clone(),
        };
        let mut selected_bookies = self.placement_policy.select_ensemble(&options)?;
        let mut ordered_bookies = Vec::with_capacity(ensemble_size);
        for bookie_id in preferred_bookies.into_iter() {
            if let Some(j) = selected_bookies.iter().position(|id| id == bookie_id) {
                ordered_bookies.push(selected_bookies.swap_remove(j));
            }
        }
        for (i, bookie_id) in ensemble.bookies.iter().enumerate() {
            if !failed_bookies[i] && i < ordered_bookies.len() && *bookie_id == ordered_bookies[i] {
                continue;
            }
            ordered_bookies.insert(i, selected_bookies.swap_remove(0));
        }
        Ok(ordered_bookies)
    }

    async fn change_ensemble(
        &self,
        mut version: MetaVersion,
        mut metadata: LedgerMetadata,
        first_entry_id: EntryId,
        failed_bookies: Vec<bool>,
    ) -> Result<Versioned<LedgerMetadata>> {
        let mut failed_ensemble_container = None;
        let original_bookies =
            unsafe { std::mem::transmute::<&[BookieId], &'_ [BookieId]>(metadata.last_ensemble().bookies.as_slice()) };
        let ensemble_size = metadata.ensemble_size as usize;
        let mut excluded_bookies = HashSet::with_capacity(ensemble_size);
        for (i, bookie_id) in original_bookies.iter().enumerate() {
            if failed_bookies[i] {
                excluded_bookies.insert(bookie_id);
            }
        }
        let mut preferred_bookies = Vec::with_capacity(ensemble_size);
        let mut ordered_bookies = Vec::with_capacity(ensemble_size);
        loop {
            if excluded_bookies.is_empty() {
                return Ok(Versioned::new(version, metadata));
            }
            let ensemble = metadata.last_ensemble();
            if first_entry_id < ensemble.first_entry_id {
                let err = BkError::with_description(ErrorKind::UnexpectedError, &"ledger ensemble changed");
                return Err(err);
            }
            let preferred_bookies =
                unsafe { std::mem::transmute::<&mut Vec<&BookieId>, &'_ mut Vec<&BookieId>>(&mut preferred_bookies) };
            preferred_bookies.extend(ensemble.bookies.iter().filter(|bookie_id| !excluded_bookies.contains(bookie_id)));
            let options = EnsembleOptions {
                ensemble_size: metadata.ensemble_size,
                write_quorum: metadata.write_quorum_size,
                ack_quorum: metadata.ack_quorum_size,
                custom_metadata: &metadata.custom_metadata,
                preferred_bookies: preferred_bookies.as_slice(),
                excluded_bookies: excluded_bookies.clone(),
            };
            let mut selected_bookies = self.placement_policy.select_ensemble(&options)?;
            for bookie_id in preferred_bookies.iter() {
                if let Some(i) = selected_bookies.iter().position(|id| id == *bookie_id) {
                    ordered_bookies.push(selected_bookies.swap_remove(i));
                }
            }
            preferred_bookies.clear();
            let mut i = 0;
            while i < ordered_bookies.len() {
                if ordered_bookies[i] != ensemble.bookies[i] {
                    ordered_bookies.insert(i, selected_bookies.pop().unwrap());
                }
                i += 1;
            }
            ordered_bookies.append(&mut selected_bookies);

            if first_entry_id == ensemble.first_entry_id {
                let replaced_ensemble = metadata.ensembles.pop();
                failed_ensemble_container = failed_ensemble_container.or(replaced_ensemble);
            }
            metadata.ensembles.push(LedgerEnsemble { first_entry_id, bookies: ordered_bookies });
            match self.meta_store.write_ledger_metadata(&metadata, version).await? {
                either::Right(new_version) => return Ok(Versioned::new(new_version, metadata)),
                either::Left(Versioned { version: conflicting_version, value: conflicting_metadata }) => {
                    ordered_bookies = metadata.ensembles.pop().unwrap().bookies;
                    ordered_bookies.clear();
                    version = conflicting_version;
                    metadata = conflicting_metadata;
                },
            }
            if metadata.state != LedgerState::Open {
                return Ok(Versioned::new(version, metadata));
            }
        }
    }

    async fn close_ledger(
        &self,
        metadata: Versioned<LedgerMetadata>,
        last_add_confirmed: EntryId,
        ledger_length: LedgerLength,
        recovery: bool,
        ensembles: Vec<LedgerEnsemble>,
    ) -> Result<Versioned<LedgerMetadata>> {
        let Versioned { mut version, value: mut metadata } = metadata;
        loop {
            if recovery {
                if metadata.state != LedgerState::InRecovery {
                    let err = BkError::with_description(ErrorKind::LedgerConcurrentClose, &"ledger not in recovery");
                    return Err(err);
                }
            } else if metadata.state == LedgerState::InRecovery {
                let err = BkError::with_description(ErrorKind::LedgerConcurrentClose, &"ledger in recovery");
                return Err(err);
            } else if metadata.state == LedgerState::Closed {
                if metadata.last_entry_id == last_add_confirmed && metadata.length == ledger_length {
                    return Ok(Versioned::new(version, metadata));
                }
                let msg = format!(
                    "attemp to close ledger with (last_entry_id, length) ({}, {}), but got ({}, {})",
                    last_add_confirmed, ledger_length, metadata.last_entry_id, metadata.length
                );
                let err = BkError::with_message(ErrorKind::LedgerConcurrentClose, msg);
                return Err(err);
            }
            if !ensembles.is_empty() {
                let last_ensemble_entry_id = metadata.last_ensemble().first_entry_id;
                match last_ensemble_entry_id.cmp(&ensembles[0].first_entry_id) {
                    Ordering::Greater => {
                        let err = BkError::with_description(
                            ErrorKind::UnexpectedError,
                            &"ledger ensembles changed in recovery mode",
                        );
                        return Err(err);
                    },
                    Ordering::Less => {
                        metadata.ensembles.extend_from_slice(&ensembles);
                    },
                    Ordering::Equal => {
                        metadata.ensembles.pop();
                    },
                }
            }
            metadata.state = LedgerState::Closed;
            metadata.last_entry_id = last_add_confirmed;
            metadata.length = ledger_length;
            let r = self.meta_store.write_ledger_metadata(&metadata, version).await?;
            match r {
                either::Right(version) => return Ok(Versioned::new(version, metadata)),
                either::Left(Versioned { version: conflicting_version, value: conflicting_metadata }) => {
                    version = conflicting_version;
                    metadata = conflicting_metadata;
                },
            }
        }
    }

    async fn write_lac(&self, bookies: Vec<BookieId>, lac: EntryId) -> EntryId {
        let options =
            bookie::WriteLacOptions { master_key: &self.master_key, digest_algorithm: &self.digest_algorithm };
        let n = bookies.len();
        let mut futures = Vec::with_capacity(n);
        let write_set = self.new_write_set(lac);
        for bookie_index in write_set.iter() {
            let bookie_id = &bookies[bookie_index];
            futures.push(self.client.write_lac(bookie_id, self.ledger_id, lac, &options).fuse());
        }
        let mut ack_set = self.new_ack_set();
        let mut failed = 0;
        let mut select_all = SelectAll::new(&mut futures);
        let mut err = None;
        while failed <= self.entry_distribution.failure_threshold() {
            let (write_index, write_result) = select_all.next().await;
            if let Err(e) = write_result {
                err = err.or(Some(e));
                failed += 1;
            } else if ack_set.complete_write(write_index) {
                return lac;
            }
        }
        lac
    }

    async fn force_ledger(&self, bookies: Vec<BookieId>, last_add_entry_id: EntryId) -> Result<EntryId> {
        let mut futures = Vec::with_capacity(bookies.len());
        for bookie_id in bookies.iter() {
            futures.push(self.client.force_ledger(bookie_id, self.ledger_id).fuse());
        }
        let mut select_all = SelectAll::new(&mut futures);
        for _ in 0..bookies.len() {
            select_all.next().await.1?;
        }
        Ok(last_add_entry_id)
    }

    async fn add_entry(
        &self,
        bookie_id: BookieId,
        entry_id: EntryId,
        payload: &'static [u8],
        options: AddOptions,
    ) -> Result<()> {
        let adding_entry = bookie::AddingEntry {
            ledger_id: self.ledger_id,
            entry_id,
            last_add_confirmed: options.last_add_confirmed,
            accumulated_ledger_length: options.ledger_length,
            payload,
        };
        let add_options = bookie::AddOptions {
            recovery_add: options.recovery_add,
            high_priority: options.high_priority,
            deferred_sync: self.deferred_sync,
            master_key: &self.master_key,
            digest_algorithm: &self.digest_algorithm,
        };
        self.client.add_entry(&bookie_id, &adding_entry, &add_options).await?;
        Ok(())
    }
}

/// Ledger appender.
#[derive(Clone)]
pub struct LedgerAppender {
    pub(crate) reader: LedgerReader,
    pub(crate) last_add_entry_id: Cell<EntryId>,
    pub(crate) request_sender: mpsc::Sender<WriteRequest>,
}

assert_impl_all!(LedgerAppender: Send);
assert_not_impl_any!(LedgerAppender: Sync);
assert_not_impl_any!(Arc<LedgerAppender>: Send);

#[derive(Debug)]
pub struct AddedEntry {
    entry_id: EntryId,
    last_add_confirmed: EntryId,
}

#[derive(Debug)]
pub enum WriteRequest {
    Append { entry_id: EntryId, payload: Vec<u8>, responser: oneshot::Sender<Result<AddedEntry>> },
    Force { entry_id: EntryId, responser: oneshot::Sender<Result<EntryId>> },
    Close { responser: oneshot::Sender<Result<Versioned<LedgerMetadata>>> },
}

#[derive(Debug)]
struct PendingForce {
    last_add_entry_id: EntryId,
    responser: oneshot::Sender<Result<EntryId>>,
}

impl LedgerAppender {
    pub fn id(&self) -> LedgerId {
        self.reader.ledger_id
    }

    /// Closes ledger.
    pub async fn close(&mut self, _options: CloseOptions) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let request = WriteRequest::Close { responser: sender };
        if self.request_sender.send(request).await.is_err() {
            return Err(BkError::new(ErrorKind::LedgerClosed));
        }
        let metadata = receiver.await.unwrap()?;
        self.reader.update_metadata(metadata);
        Ok(())
    }

    /// Constructs a ledger reader.
    pub fn reader(&self) -> Result<LedgerReader> {
        Ok(self.reader.clone())
    }

    /// Gets local cached last_add_confirmed.
    pub fn last_add_confirmed(&self) -> EntryId {
        self.reader.last_add_confirmed()
    }

    fn update_last_add_entry_id(&self, entry_id: EntryId) {
        if entry_id > self.last_add_entry_id.get() {
            self.last_add_entry_id.set(entry_id);
        }
    }

    fn update_last_add_confirmed(&self, last_add_confirmed: EntryId) {
        if last_add_confirmed > self.reader.last_add_confirmed.get() {
            self.reader.last_add_confirmed.set(last_add_confirmed);
        }
    }

    /// Syncs writen entries on last ensemble.
    pub async fn force(&self) -> Result<()> {
        let last_add_entry_id = self.last_add_entry_id.get();
        if last_add_entry_id <= self.reader.last_add_confirmed.get() {
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        let request = WriteRequest::Force { entry_id: last_add_entry_id, responser: sender };
        if self.request_sender.send(request).await.is_err() {
            return Err(BkError::new(ErrorKind::LedgerClosed));
        }
        let last_add_confirmed = receiver.await.unwrap()?;
        self.update_last_add_confirmed(last_add_confirmed);
        Ok(())
    }

    /// Appends data to ledger.
    pub async fn append(&self, data: &[u8]) -> Result<EntryId> {
        let (sender, receiver) = oneshot::channel();
        let request = WriteRequest::Append { entry_id: EntryId::INVALID, payload: data.to_vec(), responser: sender };
        if self.request_sender.send(request).await.is_err() {
            return Err(BkError::new(ErrorKind::LedgerClosed));
        }
        let AddedEntry { entry_id, last_add_confirmed } = receiver.await.unwrap()?;
        self.update_last_add_entry_id(entry_id);
        self.update_last_add_confirmed(last_add_confirmed);
        Ok(entry_id)
    }
}
