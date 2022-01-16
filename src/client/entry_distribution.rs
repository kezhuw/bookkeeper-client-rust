use super::{EntryId, LedgerMetadata};

#[derive(Clone, Copy, Debug)]
pub struct EntryDistribution {
    pub ensemble_size: usize,
    pub write_quorum_size: usize,
    pub ack_quorum_size: usize,
}

pub(crate) trait HasEntryDistribution {
    fn entry_distribution(&self) -> &EntryDistribution;

    fn new_write_set(&self, entry_id: EntryId) -> WriteSet {
        return self.entry_distribution().new_write_set(entry_id);
    }

    fn new_ack_set(&self) -> AckSet {
        return self.entry_distribution().new_ack_set();
    }

    fn new_quorum_coverage_set(&self) -> QuorumCoverageSet {
        return self.entry_distribution().new_quorum_coverage_set();
    }
}

impl HasEntryDistribution for EntryDistribution {
    fn entry_distribution(&self) -> &EntryDistribution {
        self
    }
}

impl EntryDistribution {
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> EntryDistribution {
        EntryDistribution { ensemble_size, write_quorum_size, ack_quorum_size }
    }

    pub fn from_metadata(metadata: &LedgerMetadata) -> EntryDistribution {
        EntryDistribution::new(
            metadata.ensemble_size as usize,
            metadata.write_quorum_size as usize,
            metadata.ack_quorum_size as usize,
        )
    }

    pub fn new_write_set(&self, entry_id: EntryId) -> WriteSet {
        let mut should_write = vec![false; self.ensemble_size];
        let offset = entry_id.0;
        for write_index in 0..self.write_quorum_size {
            let bookie_index = ((offset + write_index as i64) % (self.ensemble_size as i64)) as usize;
            should_write[bookie_index] = true;
        }
        WriteSet { ensemble: should_write }
    }

    pub fn new_ack_set(&self) -> AckSet {
        AckSet {
            completed: 0,
            ack_quorum_size: self.ack_quorum_size,
            write_quorum: vec![false; self.write_quorum_size],
        }
    }

    pub fn new_quorum_coverage_set(&self) -> QuorumCoverageSet {
        QuorumCoverageSet {
            ensemble: vec![None; self.ensemble_size],
            write_quorum_size: self.write_quorum_size,
            ack_quorum_size: self.ack_quorum_size,
        }
    }

    pub fn failure_threshold(&self) -> usize {
        self.write_quorum_size - self.ack_quorum_size
    }
}

pub struct AckSet {
    completed: usize,
    ack_quorum_size: usize,
    write_quorum: Vec<bool>,
}

impl AckSet {
    pub fn completed(&self) -> bool {
        self.completed >= self.ack_quorum_size
    }

    pub fn complete_write(&mut self, write_index: usize) -> bool {
        self.write_quorum[write_index] = true;
        self.completed += 1;
        self.completed()
    }

    pub fn unset_write(&mut self, write_index: usize) {
        let write_completed = self.write_quorum[write_index];
        self.write_quorum[write_index] = false;
        if write_completed {
            self.completed -= 1;
        }
    }
}

pub struct QuorumCoverageSet {
    ensemble: Vec<Option<bool>>,
    write_quorum_size: usize,
    ack_quorum_size: usize,
}

impl QuorumCoverageSet {
    pub fn fail_bookie(&mut self, bookie_index: usize) {
        self.ensemble[bookie_index] = Some(false);
    }

    pub fn complete_bookie(&mut self, bookie_index: usize) {
        self.ensemble[bookie_index] = Some(true);
    }

    fn quorum_covered(&self, quorum_index: usize) -> Option<bool> {
        let mut failed_bookies = 0;
        let mut completed_bookies = 0;
        for write_index in 0..self.write_quorum_size {
            let bookie_index = (quorum_index + write_index) % self.ensemble.len();
            match self.ensemble[bookie_index] {
                None => {},
                Some(false) => failed_bookies += 1,
                Some(true) => completed_bookies += 1,
            };
        }
        if completed_bookies >= self.ack_quorum_size {
            return Some(true);
        } else if failed_bookies > self.write_quorum_size - self.ack_quorum_size {
            return Some(false);
        }
        None
    }

    pub fn covered(&self) -> Option<bool> {
        if self.write_quorum_size == self.ensemble.len() {
            return self.quorum_covered(0);
        }
        let quorums = self.ensemble.len();
        (0..quorums)
            .map(|i| self.quorum_covered(i))
            .reduce(|acc, item| {
                if acc == Some(false) || item == Some(false) {
                    return Some(false);
                }
                if acc == None || item == None {
                    return None;
                }
                Some(true)
            })
            .unwrap()
    }
}

#[derive(Clone)]
pub struct WriteSet {
    ensemble: Vec<bool>,
}

pub struct WriteSetIterator<'a> {
    i: usize,
    ensemble: &'a [bool],
}

impl WriteSet {
    pub fn iter(&self) -> WriteSetIterator<'_> {
        WriteSetIterator { i: 0, ensemble: &self.ensemble }
    }

    pub fn bookie_index(&self, write_index: usize) -> usize {
        return self.iter().nth(write_index).expect("invalid write index");
    }
}

impl Iterator for WriteSetIterator<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        while self.i < self.ensemble.len() {
            let bookie_index = self.i;
            let should_write = self.ensemble[bookie_index];
            self.i += 1;
            if should_write {
                return Some(bookie_index);
            }
        }
        None
    }
}
