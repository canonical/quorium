use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use quorium_core::kv::{KvState, RowKey};
use redb::{Database, Durability, ReadableTable, TableDefinition, TableError};

use crate::mem::{Checkpoint, FileSnapshotStore, KvSnapshotMeta, Rows, Storage};

const CACHE_BYTES: usize = 64 << 20;
const APPLIED: &[u8] = b"applied";

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

type Handle = Arc<Mutex<Option<Arc<Database>>>>;

fn database(handle: &Handle) -> io::Result<Arc<Database>> {
    handle.lock().unwrap().clone().ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "state database is closed"))
}

#[derive(Clone)]
pub struct StateDb {
    db: Handle,
}

impl StateDb {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::builder().set_cache_size(CACHE_BYTES).create(path).map_err(other)?;
        Ok(Self { db: Arc::new(Mutex::new(Some(Arc::new(db)))) })
    }

    pub fn close(&self) {
        self.db.lock().unwrap().take();
    }

    pub fn shard(&self, shard: u64) -> RedbStorage {
        RedbStorage {
            db: self.db.clone(),
            rows: format!("shard-{shard}-rows"),
            meta: format!("shard-{shard}-meta"),
            legacy: None,
            written: Arc::new(AtomicU64::new(0)),
            rewrite: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct RedbStorage {
    db: Handle,
    rows: String,
    meta: String,
    legacy: Option<PathBuf>,
    written: Arc<AtomicU64>,
    rewrite: Arc<AtomicBool>,
}

impl RedbStorage {
    pub fn migrating_from(mut self, legacy: &Path) -> Self {
        self.legacy = Some(legacy.to_path_buf());
        self
    }

    pub fn rows_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    fn rows_table(&self) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
        TableDefinition::new(&self.rows)
    }

    fn meta_table(&self) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
        TableDefinition::new(&self.meta)
    }

    fn load_rows(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>> {
        let txn = database(&self.db)?.begin_read().map_err(other)?;
        let meta_table = match txn.open_table(self.meta_table()) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(other(e)),
        };
        let Some(applied) = meta_table.get(APPLIED).map_err(other)? else {
            return Ok(None);
        };
        let meta: KvSnapshotMeta =
            postcard::from_bytes(applied.value()).map_err(|_| invalid("state checkpoint is undecodable"))?;
        let rows_table = txn.open_table(self.rows_table()).map_err(other)?;
        let mut rows = Vec::new();
        for row in rows_table.iter().map_err(other)? {
            let (k, v) = row.map_err(other)?;
            let key = RowKey::from_bytes(k.value()).map_err(|_| invalid("state row key is undecodable"))?;
            rows.push((key, v.value().to_vec()));
        }
        let state = KvState::from_rows(rows).map_err(|_| invalid("state rows are undecodable"))?;
        Ok(Some((meta, state)))
    }

    pub fn __corrupt_meta_row(&self) -> io::Result<()> {
        let txn = database(&self.db)?.begin_write().map_err(other)?;
        {
            let mut rows = txn.open_table(self.rows_table()).map_err(other)?;
            rows.insert(RowKey::Meta.to_bytes().as_slice(), [0xffu8, 0xff].as_slice()).map_err(other)?;
        }
        txn.commit().map_err(other)
    }
}

impl Storage for RedbStorage {
    fn save(&self, checkpoint: Checkpoint<'_>) -> io::Result<()> {
        let mut txn = database(&self.db)?.begin_write().map_err(other)?;
        txn.set_durability(Durability::Immediate);
        let mut written = 0;
        let rewrite = matches!(checkpoint.rows, Rows::All(_));
        if rewrite {
            txn.delete_table(self.rows_table()).map_err(other)?;
        }
        {
            let mut rows = txn.open_table(self.rows_table()).map_err(other)?;
            match checkpoint.rows {
                Rows::All(all) => {
                    for (key, value) in all {
                        rows.insert(key.to_bytes().as_slice(), value.as_slice()).map_err(other)?;
                        written += 1;
                    }
                }
                Rows::Changed(changed) => {
                    for (key, value) in changed {
                        let encoded = key.to_bytes();
                        match value {
                            Some(value) => {
                                rows.insert(encoded.as_slice(), value.as_slice()).map_err(other)?;
                            }
                            None => {
                                rows.remove(encoded.as_slice()).map_err(other)?;
                            }
                        }
                        written += 1;
                    }
                }
            }
            let mut meta = txn.open_table(self.meta_table()).map_err(other)?;
            let applied = postcard::to_allocvec(checkpoint.meta).map_err(other)?;
            meta.insert(APPLIED, applied.as_slice()).map_err(other)?;
        }
        txn.commit().map_err(other)?;
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
