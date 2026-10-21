use xtask::mutation::{Score, evaluate_score};

#[test]
fn parses_cargo_mutants_summary() {
    let json =
        r#"{"total_mutants": 10, "missed": 2, "caught": 6, "timeout": 1, "unviable": 1, "success": 0, "outcomes": []}"#;
    let score = Score::from_outcomes(json).unwrap();
    assert_eq!(score, Score { killed: 7, missed: 2 });
    assert!((score.ratio() - 7.0 / 9.0).abs() < 1e-9);
}

#[test]
fn floor_is_enforced() {
    let score = Score { killed: 79, missed: 21 };
    assert!(evaluate_score(score, 0.8).is_err());
    let score = Score { killed: 80, missed: 20 };
    assert!(evaluate_score(score, 0.8).is_ok());
}

#[test]
fn no_viable_mutants_fails_closed() {
    assert!(evaluate_score(Score { killed: 0, missed: 0 }, 0.8).is_err());
    assert!(Score::from_outcomes("not json").is_err());
}

#[test]
fn scores_from_shards_add_up() {
    let a = Score { killed: 7, missed: 2 };
    let b = Score { killed: 3, missed: 0 };
    assert_eq!(a + b, Score { killed: 10, missed: 2 });
    assert_eq!([a, b].into_iter().sum::<Score>(), Score { killed: 10, missed: 2 });
}
