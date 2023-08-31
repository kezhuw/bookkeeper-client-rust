use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::{ArcSwap, Guard};
use atomic::Atomic;
use static_assertions::assert_not_impl_any;

use super::errors::{BkError, ErrorKind};
use crate::meta::{MetaVersion, Versioned};

type Result<T, E = BkError> = std::result::Result<T, E>;

/// Ledger id.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LedgerId(pub(crate) i64);

impl TryFrom<i64> for LedgerId {
    type Error = BkError;

    fn try_from(i: i64) -> Result<LedgerId> {
        if i < 0 {
            return Err(BkError::new(ErrorKind::InvalidLedgerId));
        }
        Ok(LedgerId(i))
    }
}

impl From<LedgerId> for i64 {
    fn from(ledger_id: LedgerId) -> Self {
        ledger_id.0
    }
}

impl PartialEq<i64> for LedgerId {
    fn eq(&self, other: &i64) -> bool {
        self.0.eq(other)
    }
}

impl Display for LedgerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

/// Ledger length.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LedgerLength(i64);

impl LedgerLength {
    pub const ZERO: LedgerLength = LedgerLength(0);
}

impl From<i64> for LedgerLength {
    fn from(i: i64) -> LedgerLength {
        LedgerLength(i)
    }
}

impl From<LedgerLength> for i64 {
    fn from(ledger_length: LedgerLength) -> i64 {
        ledger_length.0
    }
}

impl From<usize> for LedgerLength {
    fn from(u: usize) -> LedgerLength {
        LedgerLength(u as i64)
    }
}

impl Display for LedgerLength {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

impl std::ops::Sub<i64> for LedgerLength {
    type Output = Self;

    fn sub(self, rhs: i64) -> LedgerLength {
        LedgerLength(self.0 - rhs)
    }
}

impl std::ops::Add<i64> for LedgerLength {
    type Output = Self;

    fn add(self, rhs: i64) -> LedgerLength {
        LedgerLength(self.0 + rhs)
    }
}

impl std::ops::SubAssign<i64> for LedgerLength {
    fn sub_assign(&mut self, rhs: i64) {
        self.0 -= rhs;
    }
}

impl std::ops::AddAssign<i64> for LedgerLength {
    fn add_assign(&mut self, rhs: i64) {
        self.0 += rhs;
    }
}

impl std::ops::Sub<usize> for LedgerLength {
    type Output = Self;

    fn sub(self, rhs: usize) -> LedgerLength {
        LedgerLength(self.0 - rhs as i64)
    }
}

impl std::ops::Add<usize> for LedgerLength {
    type Output = Self;

    fn add(self, rhs: usize) -> LedgerLength {
        LedgerLength(self.0 + rhs as i64)
    }
}

impl std::ops::SubAssign<usize> for LedgerLength {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs as i64;
    }
}

impl std::ops::AddAssign<usize> for LedgerLength {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as i64;
    }
}

/// Ledger entry id.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
#[derive(bytemuck::NoUninit)]
pub struct EntryId(pub(crate) i64);

impl Display for EntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        Display::fmt(&self.0, f)
    }
}

impl std::cmp::PartialEq<i64> for EntryId {
    fn eq(&self, other: &i64) -> bool {
        self.0.eq(other)
    }
}

impl std::cmp::PartialOrd<i64> for EntryId {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl std::ops::Sub for EntryId {
    type Output = i64;

    fn sub(self, rhs: EntryId) -> i64 {
        self.0 - rhs.0
    }
}

impl std::ops::Sub<i64> for EntryId {
    type Output = Self;

    fn sub(self, rhs: i64) -> EntryId {
        EntryId(self.0 - rhs)
    }
}

impl std::ops::Add<i64> for EntryId {
    type Output = Self;

    fn add(self, rhs: i64) -> EntryId {
        EntryId(self.0 + rhs)
    }
}

impl std::ops::SubAssign<i64> for EntryId {
    fn sub_assign(&mut self, rhs: i64) {
        self.0 -= rhs;
    }
}

impl std::ops::AddAssign<i64> for EntryId {
    fn add_assign(&mut self, rhs: i64) {
        self.0 += rhs;
    }
}

impl EntryId {
    /// Well-known invalid entry id.
    pub const INVALID: EntryId = EntryId(-1);
    /// First valid entry id.
    pub const MIN: EntryId = EntryId(0);

    /// Returns whether entry id is valid.
    pub const fn is_valid(&self) -> bool {
        self.0 >= 0
    }

    /// Constructs entry id from i64.
    ///
    /// # Safety
    /// Returned entry id is invalid if given i64 is negative.
    pub const unsafe fn unchecked_from_i64(i: i64) -> EntryId {
        EntryId(i)
    }
}

impl TryFrom<i64> for EntryId {
    type Error = BkError;

    fn try_from(i: i64) -> Result<EntryId> {
        if i < 0 {
            return Err(BkError::new(ErrorKind::InvalidEntryId));
        }
        Ok(EntryId(i))
    }
}

impl From<EntryId> for i64 {
    fn from(entry_id: EntryId) -> i64 {
        entry_id.0
    }
}

pub(crate) struct AtomicEntryId {
    entry_id: atomic::Atomic<EntryId>,
}

impl AtomicEntryId {
    pub fn get(&self) -> EntryId {
        self.entry_id.load(atomic::Ordering::Relaxed)
    }

    pub fn update(&self, entry_id: EntryId) -> EntryId {
        let mut current = self.get();
        if entry_id <= current {
            return current;
        }
        while entry_id > current {
            match self.entry_id.compare_exchange(
                current,
                entry_id,
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
            ) {
                Ok(_) => return entry_id,
                Err(new_entry_id) => current = new_entry_id,
            }
        }
        current
    }
}

impl From<EntryId> for AtomicEntryId {
    fn from(entry_id: EntryId) -> Self {
        Self { entry_id: Atomic::new(entry_id) }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerEnsemble {
    pub(crate) first_entry_id: EntryId,
    pub(crate) bookies: Vec<BookieId>,
}

/// Ledger metadata.
#[derive(Clone, Debug)]
pub struct LedgerMetadata {
    pub ledger_id: LedgerId,
    pub length: LedgerLength,
    pub last_entry_id: EntryId,
    pub state: LedgerState,
    pub password: Vec<u8>,
    pub ensemble_size: u32,
    pub write_quorum_size: u32,
    pub ack_quorum_size: u32,
    pub ensembles: Vec<LedgerEnsemble>,
    pub digest_type: DigestType,
    pub creation_time: Option<SystemTime>,
    pub creator_token: i64,
    pub custom_metadata: HashMap<String, Vec<u8>>,
    pub format_version: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum LedgerState {
    Open,
    InRecovery,
    Closed,
}

/// DigestType specifies digest method in ledger entry writting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[non_exhaustive]
pub enum DigestType {
    CRC32,
    MAC,
    CRC32C,
    DUMMY,
}

pub struct EnsembleIterator<'a> {
    next: usize,
    ensembles: &'a [LedgerEnsemble],
}

impl<'a> Iterator for EnsembleIterator<'a> {
    type Item = (EntryId, &'a [BookieId], EntryId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.ensembles.len() {
            return None;
        }
        let ensemble = &self.ensembles[self.next];
        self.next += 1;
        let next_ensemble_entry_id =
            if self.next >= self.ensembles.len() { EntryId::INVALID } else { self.ensembles[self.next].first_entry_id };
        Some((ensemble.first_entry_id, &ensemble.bookies, next_ensemble_entry_id))
    }
}

impl LedgerMetadata {
    /// Returns ensemble for given entry id and next entry id that will have a different ensemble.
    pub fn ensemble_at(&self, entry_id: EntryId) -> (EntryId, &[BookieId], EntryId) {
        assert!(entry_id >= EntryId::MIN);
        assert!(!self.ensembles.is_empty());
        assert!(self.ensembles[0].first_entry_id == EntryId::MIN);
        let i = match self.ensembles.binary_search_by_key(&entry_id, |e| e.first_entry_id) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        if i + 1 == self.ensembles.len() {
            (self.ensembles[i].first_entry_id, &self.ensembles[i].bookies, EntryId::INVALID)
        } else {
            (self.ensembles[i].first_entry_id, &self.ensembles[i].bookies, self.ensembles[i + 1].first_entry_id)
        }
    }

    pub fn ensemble_iter(&self, entry_id: EntryId) -> EnsembleIterator<'_> {
        assert!(entry_id >= EntryId::MIN);
        assert!(!self.ensembles.is_empty());
        assert!(self.ensembles[0].first_entry_id == EntryId::MIN);
        let i = match self.ensembles.binary_search_by_key(&entry_id, |e| e.first_entry_id) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        EnsembleIterator { next: i, ensembles: &self.ensembles }
    }

    pub fn last_ensemble(&self) -> &LedgerEnsemble {
        &self.ensembles[self.ensembles.len() - 1]
    }

    pub fn last_add_confirmed(&self) -> EntryId {
        if self.closed() {
            self.last_entry_id
        } else if self.ensembles.is_empty() {
            EntryId::INVALID
        } else {
            self.last_ensemble().first_entry_id - 1
        }
    }

    pub fn closed(&self) -> bool {
        self.state == LedgerState::Closed
    }
}

pub(crate) trait HasLedgerMetadata {
    fn metadata(&self) -> &LedgerMetadata;

    fn ensemble_at(&self, entry_id: EntryId) -> (EntryId, &[BookieId], EntryId) {
        return self.metadata().ensemble_at(entry_id);
    }

    fn ensemble_iter(&self, entry_id: EntryId) -> EnsembleIterator<'_> {
        return self.metadata().ensemble_iter(entry_id);
    }

    fn last_ensemble(&self) -> &LedgerEnsemble {
        return self.metadata().last_ensemble();
    }

    fn closed(&self) -> bool {
        return self.metadata().closed();
    }
}

pub struct BorrowedLedgerMetadata {
    metadata: Guard<Arc<Versioned<LedgerMetadata>>>,
    _marker: PhantomData<std::rc::Rc<()>>,
}

assert_not_impl_any!(BorrowedLedgerMetadata: Send, Sync);

impl std::ops::Deref for BorrowedLedgerMetadata {
    type Target = Versioned<LedgerMetadata>;

    fn deref(&self) -> &Versioned<LedgerMetadata> {
        &self.metadata
    }
}

impl BorrowedLedgerMetadata {
    pub fn into_owned(self) -> Arc<Versioned<LedgerMetadata>> {
        Guard::into_inner(self.metadata)
    }
}

/// ## Synchronization semantics:
/// 1. Exposed lac synchronizes `lac` and `metadata`.
/// 2. It is ok for thread to read delayed data.
/// 3. In asynchronous rust, code before `.await` happens before code after `.await`.
/// 4. All to all, read your write and read your read.
#[derive(Clone)]
pub struct UpdatingLedgerMetadata {
    lac: Arc<AtomicEntryId>,
    metadata: Arc<ArcSwap<Versioned<LedgerMetadata>>>,
}

impl UpdatingLedgerMetadata {
    pub fn new(metadata: Versioned<LedgerMetadata>) -> Self {
        let lac = metadata.last_add_confirmed();
        Self { lac: Arc::new(lac.into()), metadata: Arc::new(ArcSwap::from_pointee(metadata)) }
    }

    pub fn closed_entry_id(&self) -> Option<EntryId> {
        let metadata = self.metadata.load();
        if metadata.closed() {
            Some(metadata.last_entry_id)
        } else {
            None
        }
    }

    pub fn check_read(&self, entry_id: EntryId) -> Result<Arc<Versioned<LedgerMetadata>>> {
        let (lac, metadata) = self.lac_for_read();
        if entry_id > lac {
            return Err(BkError::new(ErrorKind::ReadExceedLastAddConfirmed));
        }
        Ok(metadata.into_owned())
    }

    pub fn check_unconfirmed_read(&self, entry_id: EntryId) -> Result<Arc<Versioned<LedgerMetadata>>> {
        let metadata = self.borrow();
        if metadata.closed() && entry_id > metadata.last_entry_id {
            return Err(BkError::new(ErrorKind::ReadExceedLastAddConfirmed));
        }
        Ok(metadata.into_owned())
    }

    fn lac_for_read(&self) -> (EntryId, BorrowedLedgerMetadata) {
        let metadata = self.borrow();
        if metadata.closed() {
            (metadata.last_entry_id, metadata)
        } else {
            (metadata.last_add_confirmed().max(self.lac.get()), metadata)
        }
    }

    pub fn lac(&self) -> EntryId {
        let metadata = self.borrow();
        if metadata.closed() {
            return metadata.last_entry_id;
        }
        metadata.last_add_confirmed().max(self.lac.get())
    }

    pub fn last_confirmed_meta(&self) -> Result<(EntryId, LedgerLength), Arc<Versioned<LedgerMetadata>>> {
        let metadata = self.borrow();
        if metadata.closed() {
            Ok((metadata.last_entry_id, metadata.length))
        } else {
            Err(metadata.into_owned())
        }
    }

    pub fn update_lac(&self, entry_id: EntryId) -> EntryId {
        self.lac.update(entry_id)
    }

    pub fn read(&self) -> Arc<Versioned<LedgerMetadata>> {
        self.metadata.load_full()
    }

    pub fn borrow(&self) -> BorrowedLedgerMetadata {
        BorrowedLedgerMetadata { metadata: self.metadata.load(), _marker: PhantomData }
    }

    pub fn update(&mut self, metadata: Versioned<LedgerMetadata>) {
        self.lac.update(metadata.last_add_confirmed());
        let mut last_metadata = self.metadata.load();
        if metadata.version <= last_metadata.version {
            return;
        }
        let metadata = Arc::new(metadata);
        loop {
            let current_metadata = self.metadata.compare_and_swap(&last_metadata, metadata.clone());
            if Arc::ptr_eq(&*current_metadata, &*last_metadata) || metadata.version <= current_metadata.version {
                return;
            }
            last_metadata = current_metadata;
        }
    }
}

pub struct LedgerMetadataUpdater {
    version: MetaVersion,
    metadata: UpdatingLedgerMetadata,
}

impl LedgerMetadataUpdater {
    pub fn new(metadata: Versioned<LedgerMetadata>) -> Self {
        let version = metadata.version;
        Self { version, metadata: UpdatingLedgerMetadata::new(metadata) }
    }

    pub fn subscribe(&self) -> UpdatingLedgerMetadata {
        self.metadata.clone()
    }

    pub fn update(&mut self, metadata: Versioned<LedgerMetadata>) {
        if self.version >= metadata.version {
            return;
        }
        self.version = metadata.version;
        self.metadata.update(metadata);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BookieId(compact_str::CompactString);

impl BookieId {
    pub fn new(s: &str) -> BookieId {
        BookieId(s.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for BookieId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        Display::fmt(&self.0.as_str(), f)
    }
}

impl std::ops::Deref for BookieId {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for BookieId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Borrow<str> for BookieId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}
