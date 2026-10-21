use crate::rng::SimRng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskError {
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiskFaults {
    pub torn_write: f64,
    pub lost_fsync: f64,
    pub bit_flip: f64,
    pub capacity: Option<usize>,
}

impl DiskFaults {
    pub fn none() -> Self {
        Self { torn_write: 0.0, lost_fsync: 0.0, bit_flip: 0.0, capacity: None }
    }
}

impl Default for DiskFaults {
    fn default() -> Self {
        Self::none()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    faults: DiskFaults,
    data: Vec<u8>,
    durable: usize,
}

impl SimDisk {
    pub fn new(faults: DiskFaults) -> Self {
        Self { faults, data: Vec::new(), durable: 0 }
    }

    pub fn set_faults(&mut self, faults: DiskFaults) {
        self.faults = faults;
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<(), DiskError> {
        if self.faults.capacity.is_some_and(|cap| self.data.len() + bytes.len() > cap) {
            return Err(DiskError::Full);
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
        self.durable = self.durable.min(len);
    }

    pub fn fsync_upto(&mut self, upto: usize, rng: &mut SimRng) {
        if !rng.chance(self.faults.lost_fsync) {
            self.durable = self.durable.max(upto.min(self.data.len()));
        }
    }

    pub fn fsync(&mut self, rng: &mut SimRng) -> Result<(), DiskError> {
        if !rng.chance(self.faults.lost_fsync) {
            self.durable = self.data.len();
        }
        Ok(())
    }

    pub fn read(&self) -> &[u8] {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn durable_len(&self) -> usize {
        self.durable
    }

    pub fn crash(&mut self, rng: &mut SimRng) {
        let mut keep = self.durable;
        if self.data.len() > self.durable && rng.chance(self.faults.torn_write) {
            keep = rng.range(self.durable as u64, self.data.len() as u64 + 1) as usize;
        }
        self.data.truncate(keep);
        if !self.data.is_empty() && rng.chance(self.faults.bit_flip) {
            let bit = rng.range(0, self.data.len() as u64 * 8);
            self.data[(bit / 8) as usize] ^= 1 << (bit % 8);
        }
        self.durable = self.data.len();
    }
}
