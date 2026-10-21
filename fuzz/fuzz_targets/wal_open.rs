#![no_main]

use libfuzzer_sys::fuzz_target;
use quorium_store::disk::{Disk, MemDisk};
use quorium_store::wal_log::SharedWal;

fuzz_target!(|data: &[u8]| {
    let mut disk = MemDisk::default();
    if disk.append(data).is_ok() && disk.sync().is_ok() {
        let _ = SharedWal::<openraft_rt_tokio::TokioRuntime, MemDisk>::open(disk);
    }
});
