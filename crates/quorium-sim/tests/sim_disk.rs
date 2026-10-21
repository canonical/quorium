use quorium_sim::disk::{DiskFaults, SimDisk};
use quorium_sim::rng::SimRng;
use quorium_sim::sim_disk::SimStorage;
use quorium_store::wal::Wal;

#[test]
fn wal_on_sim_disk_recovers_a_prefix_after_torn_writes() {
    for seed in 0..100 {
        let faults = DiskFaults { torn_write: 1.0, ..DiskFaults::none() };
        let mut wal = Wal::open(SimStorage::new(SimDisk::new(faults), SimRng::new(seed))).unwrap();
        wal.append(b"synced").unwrap();
        wal.sync().unwrap();
        wal.append(b"in-flight-1").unwrap();
        wal.append(b"in-flight-2").unwrap();
        let disk = wal.into_disk().crash();
        let wal = Wal::open(disk).unwrap();
        let recs = wal.records();
        assert_eq!(recs[0], b"synced".to_vec());
        let expected = [b"synced".to_vec(), b"in-flight-1".to_vec(), b"in-flight-2".to_vec()];
        assert!(expected.starts_with(recs), "seed {seed}: {recs:?}");
    }
}

#[test]
fn lost_fsync_drops_acknowledged_records() {
    let faults = DiskFaults { lost_fsync: 1.0, ..DiskFaults::none() };
    let mut wal = Wal::open(SimStorage::new(SimDisk::new(faults), SimRng::new(1))).unwrap();
    wal.append(b"acked").unwrap();
    wal.sync().unwrap();
    let wal = Wal::open(wal.into_disk().crash()).unwrap();
    assert!(wal.records().is_empty());
}

#[test]
fn full_disk_surfaces_as_an_error() {
    let faults = DiskFaults { capacity: Some(16), ..DiskFaults::none() };
    let mut wal = Wal::open(SimStorage::new(SimDisk::new(faults), SimRng::new(1))).unwrap();
    assert!(wal.append(&[0u8; 64]).is_err());
}
