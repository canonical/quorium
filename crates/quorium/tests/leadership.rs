use std::time::{Duration, Instant};

use futures::StreamExt;
use quorium::LeadershipEvent;
use quorium::testing::Cluster;

async fn wait_until(limit: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failover_is_visible_in_events_status_and_stats() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.expect("a leader");
    let survivors: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
    let stable = |c: &Cluster| survivors.iter().all(|i| c.node(*i).is_stable(Duration::from_millis(500)));
    assert!(wait_until(Duration::from_secs(10), || stable(&cluster)).await);
    let status = cluster.node(survivors[0]).status();
    assert!(status.leader_age.is_some_and(|age| age >= Duration::from_millis(500)), "{status:?}");
    assert!(cluster.node(leader).status().quorum_contact_age.is_some_and(|age| age < Duration::from_secs(1)));
    let before: Vec<_> = survivors.iter().map(|i| cluster.node(*i).stats()).collect();
    let mut events =
        futures::stream::select_all(survivors.iter().map(|i| Box::pin(cluster.node(*i).leadership_events())));
    let old = cluster.node(leader).status().id;
    cluster.kill(leader).await;
    let mut seen = Vec::new();
    let mut new_leader = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(200), events.next()).await else { continue };
        if let LeadershipEvent::LeaderChanged { leader: Some(l), shard: 0, .. } = event
            && l != old
        {
            new_leader = Some(l);
        }
        seen.push(event);
        let unreachable =
            seen.iter().any(|e| matches!(e, LeadershipEvent::PeerUnreachable { peer, .. } if *peer == old));
        if new_leader.is_some() && unreachable {
            break;
        }
    }
    let new_leader = new_leader.unwrap_or_else(|| panic!("no new leader in {seen:?}"));
    assert!(seen.iter().any(|e| matches!(e, LeadershipEvent::ElectionStarted { shard: 0, .. })), "{seen:?}");
    assert!(
        seen.iter().any(|e| matches!(e, LeadershipEvent::PeerUnreachable { peer, .. } if *peer == old)),
        "{seen:?}"
    );
    assert!(!stable(&cluster));
    let after: Vec<_> = survivors.iter().map(|i| cluster.node(*i).stats()).collect();
    assert!(after.iter().zip(&before).all(|(a, b)| a.leader_changes > b.leader_changes));
    assert!(after.iter().map(|s| s.elections_started).sum::<u64>() > before.iter().map(|s| s.elections_started).sum());
    assert!(after.iter().map(|s| s.rpc_failures).sum::<u64>() > before.iter().map(|s| s.rpc_failures).sum());
    assert!(
        wait_until(Duration::from_secs(5), || survivors
            .iter()
            .all(|i| cluster.node(*i).status().leader == Some(new_leader)))
        .await
    );
    cluster.shutdown().await;
}
