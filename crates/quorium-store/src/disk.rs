use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub type Syncer = Box<dyn FnOnce() -> io::Result<()> + Send>;

pub trait Disk: Send + 'static {
    fn read_all(&mut self) -> io::Result<Vec<u8>>;
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
    fn syncer(&self) -> io::Result<Syncer>;
    fn mark_synced(&mut self, upto: u64);
    fn truncate(&mut self, len: u64) -> io::Result<()>;
    fn replace(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemDisk {
    data: Vec<u8>,
    durable: usize,
}

impl MemDisk {
    pub fn crash(mut self) -> Self {
        self.data.truncate(self.durable);
        self
    }

    pub fn chop(&mut self, len: u64) {
        self.data.truncate(len as usize);
        self.durable = self.durable.min(self.data.len());
    }

    pub fn flip_bit(&mut self, bit: u64) {
        self.data[(bit / 8) as usize] ^= 1 << (bit % 8);
    }

    pub fn durable_len(&self) -> u64 {
        self.durable as u64
    }
}

impl Disk for MemDisk {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.data.clone())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.durable = self.data.len();
        Ok(())
    }

    fn syncer(&self) -> io::Result<Syncer> {
        Ok(Box::new(|| Ok(())))
    }

    fn mark_synced(&mut self, upto: u64) {
        self.durable = self.durable.max((upto as usize).min(self.data.len()));
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.data.truncate(len as usize);
        self.durable = self.durable.min(self.data.len());
        Ok(())
    }

    fn replace(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.data = bytes.to_vec();
        self.durable = self.data.len();
        Ok(())
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

#[derive(Debug)]
pub struct FileDisk {
    file: File,
    len: u64,
    path: std::path::PathBuf,
}

impl FileDisk {
    pub fn open(path: &Path) -> io::Result<Self> {
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        if !existed && let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            File::open(dir)?.sync_all()?;
        }
        let len = file.metadata()?.len();
        Ok(Self { file, len, path: path.to_path_buf() })
    }
}

impl Disk for FileDisk {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.len as usize);
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_to_end(&mut out)?;
        Ok(out)
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(self.len))?;
        self.file.write_all(bytes)?;
        self.len += bytes.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn syncer(&self) -> io::Result<Syncer> {
        let file = self.file.try_clone()?;
        Ok(Box::new(move || file.sync_data()))
    }

    fn mark_synced(&mut self, _upto: u64) {}

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.file.sync_all()?;
        self.len = len;
        Ok(())
    }

    fn replace(&mut self, bytes: &[u8]) -> io::Result<()> {
        let tmp = self.path.with_extension("compact");
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            File::open(dir)?.sync_all()?;
        }
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.len = bytes.len() as u64;
        Ok(())
    }

    fn len(&self) -> u64 {
        self.len
    }
}
