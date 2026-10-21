use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use quorium_core::kv::{KvState, RowKey};

use crate::mem::{Checkpoint, FileSnapshotStore, KvSnapshotMeta, Rows, Storage};

const CACHE_BYTES: u64 = 64 << 20;
const APPLIED: &[u8] = b"applied";

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

type Handle = Arc<Mutex<Option<Keyspace>>>;

fn keyspace(handle: &Handle) -> io::Result<Keyspace> {
    handle.lock().unwrap().clone().ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "state database is closed"))
}

#[derive(Clone)]
pub struct FjallDb {
    keyspace: Handle,
}

impl FjallDb {
    pub fn open(path: &Path) -> io::Result<Self> {
        let keyspace =
            Config::new(path).cache_size(CACHE_BYTES).max_write_buffer_size(CACHE_BYTES).open().map_err(other)?;
        Ok(Self { keyspace: Arc::new(Mutex::new(Some(keyspace))) })
    }

    pub fn close(&self) {
        self.keyspace.lock().unwrap().take();
    }

    pub fn shard(&self, shard: u64) -> FjallStorage {
        FjallStorage {
            keyspace: self.keyspace.clone(),
            rows: format!("shard-{shard}-rows"),
            meta: format!("shard-{shard}-meta"),
            legacy: None,
            written: Arc::new(AtomicU64::new(0)),
            rewrite: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct FjallStorage {
    keyspace: Handle,
    rows: String,
    meta: String,
    legacy: Option<PathBuf>,
    written: Arc<AtomicU64>,
    rewrite: Arc<AtomicBool>,
}

impl FjallStorage {
    pub fn migrating_from(mut self, legacy: &Path) -> Self {
        self.legacy = Some(legacy.to_path_buf());
        self
    }

    pub fn rows_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    fn partitions(&self) -> io::Result<(Keyspace, PartitionHandle, PartitionHandle)> {
        let keyspace = keyspace(&self.keyspace)?;
        let rows = keyspace.open_partition(&self.rows, PartitionCreateOptions::default()).map_err(other)?;
        let meta = keyspace.open_partition(&self.meta, PartitionCreateOptions::default()).map_err(other)?;
        Ok((keyspace, rows, meta))
    }

    fn load_rows(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>> {
        let (_, rows, meta) = self.partitions()?;
        let Some(applied) = meta.get(APPLIED).map_err(other)? else {
            return Ok(None);
        };
        let meta: KvSnapshotMeta =
            postcard::from_bytes(&applied).map_err(|_| invalid("state checkpoint is undecodable"))?;
        let mut out = Vec::new();
        for row in rows.iter() {
            let (k, v) = row.map_err(other)?;
            let key = RowKey::from_bytes(&k).map_err(|_| invalid("state row key is undecodable"))?;
            out.push((key, v.to_vec()));
        }
        let state = KvState::from_rows(out).map_err(|_| invalid("state rows are undecodable"))?;
        Ok(Some((meta, state)))
    }

    pub fn __corrupt_meta_row(&self) -> io::Result<()> {
        let (keyspace, rows, _) = self.partitions()?;
        rows.insert(RowKey::Meta.to_bytes(), [0xffu8, 0xff]).map_err(other)?;
        keyspace.persist(PersistMode::SyncAll).map_err(other)
    }
}

impl Storage for FjallStorage {
    fn save(&self, checkpoint: Checkpoint<'_>) -> io::Result<()> {
        let (keyspace, rows, meta) = self.partitions()?;
        let mut batch = keyspace.batch().durability(Some(PersistMode::SyncAll));
        let mut written = 0;
        let rewrite = matches!(checkpoint.rows, Rows::All(_));
        match checkpoint.rows {
            Rows::All(all) => {
                let keep: BTreeSet<Vec<u8>> = all.iter().map(|(k, _)| k.to_bytes()).collect();
                for key in rows.keys() {
                    let key = key.map_err(other)?;
                    if !keep.contains(key.as_ref()) {
                        batch.remove(&rows, key);
                    }
                }
                for (key, value) in all {
                    batch.insert(&rows, key.to_bytes(), value.as_slice());
                    written += 1;
                }
            }
            Rows::Changed(changed) => {
                for (key, value) in changed {
                    match value {
                        Some(value) => batch.insert(&rows, key.to_bytes(), value.as_slice()),
                        None => batch.remove(&rows, key.to_bytes()),
                    }
                    written += 1;
                }
            }
        }
        batch.insert(&meta, APPLIED, postcard::to_allocvec(checkpoint.meta).map_err(other)?);
        batch.commit().map_err(other)?;
        self.written.fetch_add(written, Ordering::Relaxed);
        if rewrite {
            self.rewrite.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    fn wants_rows(&self) -> bool {
        true
    }

    fn needs_full_rewrite(&self) -> bool {
        self.rewrite.load(Ordering::Relaxed)
    }

    fn load(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>> {
        if let Some(found) = self.load_rows()? {
            return Ok(Some(found));
        }
        match &self.legacy {
            Some(dir) if dir.exists() => {
                let found = FileSnapshotStore::new(dir)?.load()?;
                self.rewrite.store(found.is_some(), Ordering::Relaxed);
                Ok(found)
            }
            _ => Ok(None),
        }
    }
}
