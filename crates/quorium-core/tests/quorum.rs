use quorium_core::quorum::{has_quorum, majority};

#[test]
fn majority_of_cluster_sizes() {
    assert_eq!(majority(1), 1);
    assert_eq!(majority(2), 2);
    assert_eq!(majority(3), 2);
    assert_eq!(majority(4), 3);
    assert_eq!(majority(5), 3);
}

#[test]
fn quorum_counts_only_voters() {
    let voters = [1u64, 2, 3];
    assert!(has_quorum(&voters, &[1, 2]));
    assert!(!has_quorum(&voters, &[1, 9]));
    assert!(!has_quorum(&voters, &[1, 1]));
    assert!(has_quorum(&voters, &[3, 2, 1]));
}
