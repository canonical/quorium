use std::io;

use quorium_store::disk::{Disk, Syncer};

use crate::disk::{DiskError, DiskFaults, SimDisk};
use crate::rng::SimRng;

#[derive(Debug, Clone)]
pub struct SimStorage {
    disk: SimDisk,
    rng: SimRng,
}

impl SimStorage {
    pub fn new(disk: SimDisk, rng: SimRng) -> Self {
        Self { disk, rng }
    }

    pub fn crash(mut self) -> Self {
        self.disk.crash(&mut self.rng);
        self
    }

    pub fn set_faults(&mut self, faults: DiskFaults) {
        self.disk.set_faults(faults);
    }
}

fn full() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, "simulated disk is full")
}

impl Disk for SimStorage {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.disk.read().to_vec())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.disk.append(bytes).map_err(|DiskError::Full| full())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.disk.fsync(&mut self.rng).map_err(|DiskError::Full| full())
    }

    fn syncer(&self) -> io::Result<Syncer> {
        Ok(Box::new(|| Ok(())))
    }

    fn mark_synced(&mut self, upto: u64) {
        self.disk.fsync_upto(upto as usize, &mut self.rng);
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.disk.truncate(len as usize);
        self.disk.fsync(&mut self.rng).map_err(|DiskError::Full| full())
    }

    fn replace(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.disk.truncate(0);
        self.disk.append(bytes).map_err(|DiskError::Full| full())?;
        self.disk.fsync(&mut self.rng).map_err(|DiskError::Full| full())
    }

    fn len(&self) -> u64 {
        self.disk.len() as u64
    }
}
