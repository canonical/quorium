use std::fmt;
use std::io;

use crate::disk::Disk;

pub const MAX_RECORD: usize = 64 * 1024 * 1024;
const HEADER: usize = 8;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Corrupt { offset: u64, reason: &'static str },
    TooLarge(usize),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "wal io error: {e}"),
            WalError::Corrupt { offset, reason } => write!(f, "wal corrupt at byte {offset}: {reason}"),
            WalError::TooLarge(n) => write!(f, "wal record of {n} bytes exceeds the limit"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

impl From<WalError> for io::Error {
    fn from(e: WalError) -> Self {
        match e {
            WalError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        }
    }
}

pub struct Wal<D: Disk> {
    disk: D,
    records: Vec<Vec<u8>>,
    recovered_tail: u64,
    appended: u64,
    synced: u64,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncTicket {
    appended: u64,
    len: u64,
    generation: u64,
}

impl<D: Disk> fmt::Debug for Wal<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wal")
            .field("recovered", &self.records.len())
            .field("appended", &self.appended)
            .field("synced", &self.synced)
            .finish()
    }
}

enum Frame<'a> {
    Record(&'a [u8], usize),
    Torn,
    Corrupt(&'static str),
}

fn frame(bytes: &[u8]) -> Frame<'_> {
    if bytes.len() < HEADER {
        return Frame::Torn;
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().expect("four bytes")) as usize;
    let crc = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes"));
    if len > MAX_RECORD || bytes.len() < HEADER + len {
        return Frame::Torn;
    }
    let payload = &bytes[HEADER..HEADER + len];
    if crc32fast::hash(payload) != crc {
        return if bytes.len() == HEADER + len { Frame::Torn } else { Frame::Corrupt("checksum mismatch") };
    }
    Frame::Record(payload, HEADER + len)
}

impl<D: Disk> Wal<D> {
    pub fn open(mut disk: D) -> Result<Self, WalError> {
        let bytes = disk.read_all()?;
        let mut records = Vec::new();
        let mut offset = 0usize;
        loop {
            match frame(&bytes[offset..]) {
                Frame::Record(payload, size) => {
                    records.push(payload.to_vec());
                    offset += size;
                }
                Frame::Torn => break,
                Frame::Corrupt(reason) => return Err(WalError::Corrupt { offset: offset as u64, reason }),
            }
        }
        let recovered_tail = (bytes.len() - offset) as u64;
        if recovered_tail > 0 {
            disk.truncate(offset as u64)?;
        }
        let count = records.len() as u64;
        Ok(Self { disk, records, recovered_tail, appended: count, synced: count, generation: 0 })
    }

    pub fn records(&self) -> &[Vec<u8>] {
        &self.records
    }

    pub fn take_records(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.records)
    }

    pub fn recovered_tail(&self) -> u64 {
        self.recovered_tail
    }

    fn frame(payload: &[u8], out: &mut Vec<u8>) -> Result<(), WalError> {
        if payload.len() > MAX_RECORD {
            return Err(WalError::TooLarge(payload.len()));
        }
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        out.extend_from_slice(payload);
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        let mut buf = Vec::with_capacity(HEADER + payload.len());
        Self::frame(payload, &mut buf)?;
        self.disk.append(&buf)?;
        self.appended += 1;
        Ok(self.appended)
    }

    pub fn replace(&mut self, payloads: &[Vec<u8>]) -> Result<(), WalError> {
        let mut buf = Vec::new();
        for p in payloads {
            Self::frame(p, &mut buf)?;
        }
        self.disk.replace(&buf)?;
        self.synced = self.appended;
        self.generation += 1;
        Ok(())
    }

    pub fn begin_sync(&self) -> Result<Option<(SyncTicket, crate::disk::Syncer)>, WalError> {
        if self.synced >= self.appended {
            return Ok(None);
        }
        let ticket = SyncTicket { appended: self.appended, len: self.disk.len(), generation: self.generation };
        Ok(Some((ticket, self.disk.syncer()?)))
    }

    pub fn finish_sync(&mut self, ticket: SyncTicket) -> u64 {
        if ticket.generation == self.generation {
            self.disk.mark_synced(ticket.len);
        }
        self.synced = self.synced.max(ticket.appended);
        self.synced
    }

    pub fn sync(&mut self) -> Result<u64, WalError> {
        if self.synced < self.appended {
            self.disk.sync()?;
            self.synced = self.appended;
        }
        Ok(self.synced)
    }

    pub fn appended(&self) -> u64 {
        self.appended
    }

    pub fn synced(&self) -> u64 {
        self.synced
    }

    pub fn into_disk(self) -> D {
        self.disk
    }

    pub fn disk_len(&self) -> u64 {
        self.disk.len()
    }
}
