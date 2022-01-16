use std::collections::{HashMap, HashSet};
use std::iter::Iterator;
use std::sync::Mutex;

use rand::seq::SliceRandom;

use super::errors::{BkError, ErrorKind};
use super::metadata::BookieId;
use crate::meta::util::{BookieRegistry, BookieRegistrySnapshot};

pub struct EnsembleOptions<'a> {
    pub ensemble_size: u32,
    pub write_quorum: u32,
    pub ack_quorum: u32,
    pub custom_metadata: &'a HashMap<String, Vec<u8>>,
    pub preferred_bookies: &'a [&'a BookieId],
    pub excluded_bookies: HashSet<&'a BookieId>,
}

pub trait PlacementPolicy {
    fn select_ensemble(&self, options: &EnsembleOptions<'_>) -> Result<Vec<BookieId>, BkError>;
}

pub struct RandomPlacementPolicy {
    registry: BookieRegistry,
    snapshot: Mutex<BookieRegistrySnapshot>,
}

impl RandomPlacementPolicy {
    pub fn new(registry: BookieRegistry) -> RandomPlacementPolicy {
        let snapshot = registry.snapshot();
        RandomPlacementPolicy { registry, snapshot: Mutex::new(snapshot) }
    }

    fn latest_registry_snapshot(&self) -> BookieRegistrySnapshot {
        let mut snapshot = self.snapshot.lock().unwrap();
        self.registry.update(&mut snapshot);
        snapshot.clone()
    }
}

impl PlacementPolicy for RandomPlacementPolicy {
    fn select_ensemble(&self, options: &EnsembleOptions<'_>) -> Result<Vec<BookieId>, BkError> {
        let snapshot = self.latest_registry_snapshot();
        let writable_bookies = snapshot.writable_bookies();
        let ensemble_size = options.ensemble_size as usize;
        let mut bookies = Vec::with_capacity(ensemble_size);
        options
            .preferred_bookies
            .iter()
            .filter_map(|id| writable_bookies.get(*id))
            .take(ensemble_size)
            .for_each(|bookie| bookies.push(bookie.bookie_id.clone()));
        if bookies.len() >= ensemble_size {
            return Ok(bookies);
        }
        let mut excluded_bookies = options.excluded_bookies.clone();
        excluded_bookies.extend(options.preferred_bookies.iter().copied());
        let mut candidate_bookies: Vec<&BookieId> = writable_bookies.keys().collect();
        candidate_bookies.shuffle(&mut rand::thread_rng());
        candidate_bookies
            .into_iter()
            .filter(|id| !excluded_bookies.contains(id) && writable_bookies.contains_key(*id))
            .take(ensemble_size - bookies.len())
            .for_each(|bookie_id| bookies.push(bookie_id.clone()));
        if bookies.len() < ensemble_size {
            return Err(BkError::new(ErrorKind::BookieNotEnough));
        }
        Ok(bookies)
    }
}
