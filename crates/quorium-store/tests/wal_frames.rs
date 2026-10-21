use proptest::prelude::*;
use quorium_store::disk::{Disk, FileDisk, MemDisk};
use quorium_store::wal::{Wal, WalError};

fn records(wal: &mut Wal<MemDisk>) -> Vec<Vec<u8>> {
    wal.records().to_vec()
}

#[test]
fn appended_records_are_read_back_after_sync_and_reopen() {
    let mut wal = Wal::open(MemDisk::default()).unwrap();
    wal.append(b"one").unwrap();
    wal.append(b"two").unwrap();
    wal.sync().unwrap();
    let disk = wal.into_disk().crash();
    let mut wal = Wal::open(disk).unwrap();
    assert_eq!(records(&mut wal), vec![b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn unsynced_records_are_lost_on_crash() {
    let mut wal = Wal::open(MemDisk::default()).unwrap();
    wal.append(b"kept").unwrap();
    wal.sync().unwrap();
    wal.append(b"lost").unwrap();
    let wal = Wal::open(wal.into_disk().crash()).unwrap();
    assert_eq!(wal.records(), &[b"kept".to_vec()]);
}

#[test]
fn torn_tail_is_truncated_and_appends_continue() {
    let mut wal = Wal::open(MemDisk::default()).unwrap();
    wal.append(b"first").unwrap();
    wal.append(b"second-record").unwrap();
    wal.sync().unwrap();
    let mut disk = wal.into_disk();
    let len = disk.len();
    disk.chop(len - 3);
    let mut wal = Wal::open(disk).unwrap();
    assert_eq!(wal.records(), &[b"first".to_vec()]);
    assert_eq!(wal.recovered_tail(), 8 + 13 - 3);
    wal.append(b"third").unwrap();
    wal.sync().unwrap();
    let wal = Wal::open(wal.into_disk().crash()).unwrap();
    assert_eq!(wal.records(), &[b"first".to_vec(), b"third".to_vec()]);
}

#[test]
fn bit_flip_in_middle_record_is_detected() {
    let mut wal = Wal::open(MemDisk::default()).unwrap();
    wal.append(b"aaaa").unwrap();
    wal.append(b"bbbb").unwrap();
    wal.append(b"cccc").unwrap();
    wal.sync().unwrap();
    let mut disk = wal.into_disk();
    disk.flip_bit(12 * 8 + 9 * 8 + 1);
    let err = Wal::open(disk).unwrap_err();
    assert!(matches!(err, WalError::Corrupt { offset: 12, .. }), "{err:?}");
}

#[test]
fn record_size_is_bounded() {
    let mut wal = Wal::open(MemDisk::default()).unwrap();
    let big = vec![0u8; quorium_store::wal::MAX_RECORD + 1];
    assert!(matches!(wal.append(&big), Err(WalError::TooLarge(_))));
}

#[test]
fn file_disk_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let mut wal = Wal::open(FileDisk::open(&path).unwrap()).unwrap();
    wal.append(b"durable").unwrap();
    wal.sync().unwrap();
    drop(wal);
    let wal = Wal::open(FileDisk::open(&path).unwrap()).unwrap();
    assert_eq!(wal.records(), &[b"durable".to_vec()]);
}

proptest! {
    #[test]
    fn round_trips_any_records(recs in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..200), 0..20)) {
        let mut wal = Wal::open(MemDisk::default()).unwrap();
        for r in &recs {
            wal.append(r).unwrap();
        }
        wal.sync().unwrap();
        let wal = Wal::open(wal.into_disk().crash()).unwrap();
        prop_assert_eq!(wal.records(), &recs[..]);
    }

    #[test]
    fn any_truncation_recovers_a_prefix(recs in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..50), 1..10), cut in 0usize..2000) {
        let mut wal = Wal::open(MemDisk::default()).unwrap();
        for r in &recs {
            wal.append(r).unwrap();
        }
        wal.sync().unwrap();
        let mut disk = wal.into_disk();
        let keep = (cut as u64).min(disk.len());
        disk.chop(keep);
        let wal = Wal::open(disk).unwrap();
        prop_assert!(recs.starts_with(wal.records()));
    }
}
