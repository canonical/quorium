# Known limitations

Quorium `0.1.0-alpha.1` is an early release. These are the limitations known today.

## Escrow can strand units after a crash

A node that crashes while asking the cluster for a grant can lose track of that grant. The grant's units then stay outside the pool: they are never sold twice, but they are never sold at all. A counter can therefore stop short of its full stock. A fix that journals each grant request before sending it is in progress.

## Failover takes longer than half a second

On real processes with fast disks and a 100 ms heartbeat, failover takes 577 ms at the median and 650 ms at the 99th percentile. Most of it is failure detection, which takes about three heartbeats. On slow network block storage, where an fsync takes 100 ms or more, failover can take several seconds.

## The consensus engine is a pre-release

Quorium depends on openraft `0.10.0-alpha.37`, pinned exactly. Consensus safety rests on it, and it has not reached a stable release.

## The API will change

Names and signatures may change before 1.0, without deprecation. Pin the exact version.

## No transactions across entities

A write is atomic for one entity only. Writes that span entities must be composed and compensated by your code.

## All data on every voter, in memory

Every voter stores every shard, and entity state is served from memory. The dataset must fit in one node's memory.

## Backups are limited to 16 MiB per shard

`export` sends each shard's state in one message. Larger shards cannot be backed up; use more shards.

## Testing on one physical host

The multi-region test rig emulates regions with containers on one machine. Real cross-machine clock behaviour, network card failures and disk power-loss semantics are approximated, not reproduced. Power loss is covered by the simulator's disk model.

## Upgrades from an earlier release are untested

The upgrade mechanism is tested with builds that add state types and methods. This is the first release, so no upgrade from a published version has been tested yet.
