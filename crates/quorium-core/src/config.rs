use std::time::Duration;

use openraft::Config;

use crate::ladder::LadderConfig;

pub fn ladder_for(heartbeat: Duration, ladder: &LadderConfig) -> LadderConfig {
    LadderConfig { base: ladder.base.max(heartbeat * 3), ..ladder.clone() }
}

pub fn raft_config(heartbeat: Duration, ladder: &LadderConfig) -> Config {
    let base = ladder.base.as_millis() as u64;
    let heartbeat = (heartbeat.as_millis() as u64).clamp(1, (base / 3).max(1));
    let lease = (heartbeat * 2).min(base.saturating_sub(2)).max(heartbeat + 1);
    Config {
        cluster_name: "quorium".into(),
        heartbeat_interval: heartbeat,
        election_timeout_min: lease,
        election_timeout_max: lease + 1,
        enable_elect: false,
        enable_pre_vote: Some(true),
        enable_leader_restore: Some(false),
        ..Default::default()
    }
    .validate()
    .expect("quorium raft config is valid")
}
