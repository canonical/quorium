use quorium_sim::disk::{DiskError, DiskFaults, SimDisk};
use quorium_sim::rng::SimRng;

#[test]
fn synced_data_survives_crash() {
    let mut d = SimDisk::new(DiskFaults::none());
    let mut r = SimRng::new(1);
    d.append(b"hello").unwrap();
    d.fsync(&mut r).unwrap();
    d.append(b" world").unwrap();
    d.crash(&mut r);
    assert_eq!(d.read(), b"hello");
}

#[test]
fn reads_see_unsynced_writes_before_crash() {
    let mut d = SimDisk::new(DiskFaults::none());
    d.append(b"abc").unwrap();
    assert_eq!(d.read(), b"abc");
    assert_eq!(d.durable_len(), 0);
}

#[test]
fn torn_write_keeps_a_prefix_of_unsynced_tail() {
    let faults = DiskFaults { torn_write: 1.0, ..DiskFaults::none() };
    for seed in 0..50 {
        let mut d = SimDisk::new(faults.clone());
        let mut r = SimRng::new(seed);
        d.append(b"base").unwrap();
        d.fsync(&mut SimRng::new(0)).unwrap();
        d.append(b"tail-bytes").unwrap();
        d.crash(&mut r);
        let data = d.read();
        assert!(data.starts_with(b"base"));
        assert!(b"basetail-bytes".starts_with(data));
    }
}

#[test]
fn lost_fsync_reports_success_but_loses_data() {
    let faults = DiskFaults { lost_fsync: 1.0, ..DiskFaults::none() };
    let mut d = SimDisk::new(faults);
    let mut r = SimRng::new(1);
    d.append(b"data").unwrap();
    assert!(d.fsync(&mut r).is_ok());
    d.crash(&mut r);
    assert!(d.read().is_empty());
}

#[test]
fn full_disk_rejects_appends() {
    let faults = DiskFaults { capacity: Some(4), ..DiskFaults::none() };
    let mut d = SimDisk::new(faults);
    assert!(d.append(b"1234").is_ok());
    assert_eq!(d.append(b"5"), Err(DiskError::Full));
}

#[test]
fn bit_flip_corrupts_exactly_one_bit() {
    let faults = DiskFaults { bit_flip: 1.0, ..DiskFaults::none() };
    let mut d = SimDisk::new(faults);
    let mut r = SimRng::new(4);
    d.append(&[0u8; 16]).unwrap();
    d.fsync(&mut r).unwrap();
    d.crash(&mut r);
    let ones: u32 = d.read().iter().map(|b| b.count_ones()).sum();
    assert_eq!(ones, 1);
}

#[test]
fn truncate_discards_tail() {
    let mut d = SimDisk::new(DiskFaults::none());
    let mut r = SimRng::new(1);
    d.append(b"abcdef").unwrap();
    d.truncate(3);
    d.fsync(&mut r).unwrap();
    d.crash(&mut r);
    assert_eq!(d.read(), b"abc");
}
