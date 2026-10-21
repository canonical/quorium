use rig::{Event, Region, boot_cleanup_commands, check_bank, check_sales, parse_events, log_limit_commands, netem_commands, one_way_ms, partition_commands};

#[test]
fn bank_reads_must_conserve_the_total_and_stay_non_negative() {
    let good = vec![Event::BankRead { total: 1000, min: 0 }, Event::BankRead { total: 1000, min: 40 }];
    assert_eq!(check_bank(&good, 1000), Ok(2));
    let leaked = vec![Event::BankRead { total: 990, min: 1 }];
    assert!(check_bank(&leaked, 1000).unwrap_err().contains("990"));
    let negative = vec![Event::BankRead { total: 1000, min: -5 }];
    assert!(check_bank(&negative, 1000).unwrap_err().contains("-5"));
    assert!(check_bank(&[Event::Sale { units: 1 }], 1000).is_err());
}

#[test]
fn acknowledged_sales_never_exceed_stock() {
    let events = vec![Event::Refill { units: 3 }, Event::Sale { units: 2 }, Event::Sale { units: 1 }];
    assert_eq!(check_sales(&events), Ok((3, 3)));
    let over = vec![Event::Refill { units: 2 }, Event::Sale { units: 2 }, Event::Sale { units: 1 }];
    assert!(check_sales(&over).unwrap_err().contains("3 of 2"));
}

#[test]
fn events_are_json_lines() {
    let line = serde_json::to_string(&Event::Sale { units: 4 }).unwrap();
    assert_eq!(serde_json::from_str::<Event>(&line).unwrap(), Event::Sale { units: 4 });
}

#[test]
fn latency_is_symmetric_and_small_within_a_region() {
    use Region::*;
    assert_eq!(one_way_ms(A, B), 15);
    assert_eq!(one_way_ms(B, A), 15);
    assert_eq!(one_way_ms(A, C), 30);
    assert_eq!(one_way_ms(C, B), 25);
    assert_eq!(one_way_ms(C, C), 0);
}

#[test]
fn netem_sends_each_peer_through_its_region_band() {
    let peers = vec![("10.0.0.2".to_string(), Region::B), ("10.0.0.3".to_string(), Region::C)];
    let cmds = netem_commands(Region::A, &peers, 0);
    assert_eq!(
        cmds[0],
        "tc qdisc replace dev eth0 root handle 1: prio bands 6 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1"
    );
    assert!(cmds.contains(&"tc qdisc replace dev eth0 parent 1:5 handle 50: netem delay 15ms 1ms".to_string()));
    assert!(cmds.contains(&"tc qdisc replace dev eth0 parent 1:6 handle 60: netem delay 30ms 1ms".to_string()));
    assert!(cmds.contains(
        &"tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 match ip dst 10.0.0.2/32 flowid 1:5".to_string()
    ));
    assert!(cmds.contains(
        &"tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 match ip dst 10.0.0.3/32 flowid 1:6".to_string()
    ));
    let lossy = netem_commands(Region::A, &peers, 5);
    assert!(
        lossy.contains(&"tc qdisc replace dev eth0 parent 1:5 handle 50: netem delay 15ms 1ms loss 5%".to_string())
    );
}

#[test]
fn partitions_drop_both_directions() {
    let cmds = partition_commands(&["10.0.0.4".to_string()]);
    assert_eq!(
        cmds,
        vec!["iptables -A INPUT -s 10.0.0.4 -j DROP".to_string(), "iptables -A OUTPUT -d 10.0.0.4 -j DROP".to_string()]
    );
}

#[test]
fn new_types_are_written_only_after_every_node_upgrades() {
    use rig::check_upgrade;
    let ok = vec![Event::Ledger { ok: false, at_ms: 10 }, Event::Ledger { ok: true, at_ms: 50 }];
    assert_eq!(check_upgrade(&ok, 40), Ok((0, 1)));
    let early = vec![Event::Ledger { ok: true, at_ms: 10 }];
    assert!(check_upgrade(&early, 40).unwrap_err().contains("before every node"));
    let never = vec![Event::Ledger { ok: false, at_ms: 50 }];
    assert!(check_upgrade(&never, 40).unwrap_err().contains("no write"));
}

#[test]
fn container_logs_are_capped_and_not_duplicated_to_syslog() {
    let cmds = log_limit_commands(64);
    let script = cmds.join(" && ");
    assert!(script.contains("SystemMaxUse=64M"));
    assert!(script.contains("RuntimeMaxUse=64M"));
    assert!(script.contains("ForwardToSyslog=no"));
    assert!(script.contains("systemctl disable --now rsyslog"));
    assert!(script.contains("journalctl --vacuum-size=64M"));
    let restart = cmds.iter().position(|c| c.contains("restart systemd-journald")).unwrap();
    let vacuum = cmds.iter().position(|c| c.contains("vacuum")).unwrap();
    assert!(restart < vacuum);
}

#[test]
fn kill_times_come_from_the_first_line_in_milliseconds() {
    use rig::kill_time_ms;
    assert_eq!(kill_time_ms("1792510224874\n"), Some(1_792_510_224_874));
    assert_eq!(kill_time_ms("1792510224874\nFailed to kill unit\n"), Some(1_792_510_224_874));
    assert_eq!(kill_time_ms("date: invalid\n"), None);
    assert_eq!(kill_time_ms(""), None);
}

#[test]
fn container_boots_leave_no_apparmor_cache_debris() {
    let script = boot_cleanup_commands().join(" && ");
    assert!(script.contains("systemctl mask apparmor.service"));
    assert!(script.contains("find /var/cache/apparmor -type f -name '*-??????' -mmin +5 -delete"));
}

#[test]
fn event_logs_survive_records_run_together_or_cut_off() {
    let text = "{\"BankRead\":{\"total\":1000,\"min\":16}}{\"BankRead\":{\"total\":1000,\"min\":21}}\n\
                {\"Sale\":{\"units\":1}}\n\
                {\"Sale\":{\"un";
    let events = parse_events(text).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[2], Event::Sale { units: 1 });
}

#[test]
fn malformed_records_inside_a_log_are_errors() {
    assert!(parse_events("{\"Sale\":{\"units\":1}}\nnot json\n{\"Sale\":{\"units\":1}}\n").is_err());
    assert!(parse_events("{\"Sale\":{\"un\n{\"Sale\":{\"units\":1}}\n").is_err());
}
