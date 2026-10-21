use proptest::prelude::*;
use quorium_core::kv::KvState;
use quorium_store::disk::{Disk, MemDisk};
use quorium_store::wal::Wal;
use quorium_store::wal_log::SharedWal;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn wal_open_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let mut disk = MemDisk::default();
        disk.append(&bytes).unwrap();
        disk.sync().unwrap();
        let _ = Wal::open(disk);
    }

    #[test]
    fn shared_wal_open_never_panics_on_arbitrary_records(records in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..64), 0..16)) {
        let mut wal = Wal::open(MemDisk::default()).unwrap();
        for r in &records {
            wal.append(r).unwrap();
        }
        wal.sync().unwrap();
        let disk = wal.into_disk();
        let _ = SharedWal::<openraft_rt_tokio::TokioRuntime, MemDisk>::open(disk);
    }

    #[test]
    fn snapshot_decoding_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = KvState::from_bytes(&bytes);
    }
}
