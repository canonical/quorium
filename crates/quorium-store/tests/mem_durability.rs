use std::time::Duration;

use openraft::entry::RaftEntry;
use openraft::impls::{Entry, Vote};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::mem::MemLog;

type C = Types<TokioRuntime>;

fn blank(term: u64, index: u64) -> <C as RaftTypeConfig>::Entry {
    Entry::new_blank(LogId::new(CommittedLeaderId::new(term), index))
}

#[test]
fn crash_keeps_only_flushed_entries_and_vote() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::with_flush_delay(Duration::from_secs(3600));
        log.save_vote(&Vote::new(1, 0)).await.unwrap();
        log.append([blank(1, 1)], IOFlushed::noop()).await.unwrap();
        let mut reader = log.get_log_reader().await;
        assert_eq!(reader.try_get_log_entries(0..10).await.unwrap().len(), 1);
        let mut recovered = log.crash();
        assert!(recovered.get_log_state().await.unwrap().last_log_id.is_none());
        assert_eq!(recovered.get_log_reader().await.read_vote().await.unwrap(), Some(Vote::new(1, 0)));
    });
}

#[test]
fn immediate_flush_survives_crash() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::default();
        log.append([blank(1, 1), blank(1, 2)], IOFlushed::noop()).await.unwrap();
        let mut recovered = log.crash();
        assert_eq!(recovered.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(2));
    });
}

#[test]
fn delayed_flush_becomes_durable_after_delay() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::with_flush_delay(Duration::from_millis(5));
        log.append([blank(1, 1)], IOFlushed::noop()).await.unwrap();
        TokioRuntime::sleep(Duration::from_millis(50)).await;
        let mut recovered = log.crash();
        assert_eq!(recovered.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
    });
}

#[test]
fn old_handle_is_fenced_after_crash() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::with_flush_delay(Duration::from_millis(5));
        let mut recovered = log.crash();
        assert!(log.append([blank(1, 1)], IOFlushed::noop()).await.is_err());
        TokioRuntime::sleep(Duration::from_millis(50)).await;
        assert!(recovered.get_log_state().await.unwrap().last_log_id.is_none());
    });
}

#[test]
fn forget_vote_drops_the_durable_vote() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::default();
        log.save_vote(&Vote::new(3, 1)).await.unwrap();
        log.append([blank(1, 1)], IOFlushed::noop()).await.unwrap();
        let mut recovered = log.crash().forget_vote();
        assert_eq!(recovered.get_log_reader().await.read_vote().await.unwrap(), None);
        assert_eq!(recovered.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
    });
}

#[test]
fn lost_fsync_acknowledges_but_loses_entries_on_crash() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::default().with_lost_fsync(true);
        log.append([blank(1, 1)], IOFlushed::noop()).await.unwrap();
        assert_eq!(log.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
        let mut recovered = log.crash();
        assert!(recovered.get_log_state().await.unwrap().last_log_id.is_none());
    });
}

#[test]
fn committed_ahead_of_durable_log_is_not_recovered() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::with_flush_delay(Duration::from_secs(3600));
        log.append([blank(1, 1)], IOFlushed::noop()).await.unwrap();
        log.save_committed(Some(LogId::new(CommittedLeaderId::new(1), 1))).await.unwrap();
        let mut recovered = log.crash();
        assert_eq!(recovered.read_committed().await.unwrap(), None);
    });
}

#[test]
fn committed_covered_by_durable_log_is_recovered() {
    TokioRuntime::run(async {
        let mut log: MemLog<C> = MemLog::default();
        log.append([blank(1, 1), blank(1, 2)], IOFlushed::noop()).await.unwrap();
        let committed = Some(LogId::new(CommittedLeaderId::new(1), 2));
        log.save_committed(committed).await.unwrap();
        let mut recovered = log.crash();
        assert_eq!(recovered.read_committed().await.unwrap(), committed);
    });
}
