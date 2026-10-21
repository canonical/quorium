# Stored and wire formats

Every format below has a fixture written by an earlier build and decoded by today's code in CI. Fixtures are protected paths. Regenerate one only for a new format version, and never overwrite an old version's fixture.

| Format | Version | Where | Fixture and test |
|---|---|---|---|
| Write-ahead log framing (length, CRC32, payload) and record encoding | record format 1 | `data_dir/wal` | `quorium-store/tests/fixtures/wal-v1.bin`, `fixtures.rs::wal_format_is_stable` |
| Full state snapshot (openraft snapshot payload, backups, range moves) | 4; 3 and 2 still read | snapshot messages, `.qbak` archives | `state-v4.bin`, `snapshot_format_is_stable`; `state-v3.bin`, `state-v2.bin` |
| State rows (row keys and values in the state database) | row format 2; 1 still read | `state.redb`, `state.fjall` | `rows-v2.bin`, `row_format_is_stable`; `rows-v1.bin` |
| redb database file | redb 2.x | `data_dir/state.redb` | `state-v1.redb`, `redb_files_written_today_stay_readable` |
| Escrow journal | 1, with records appended in 2026-10 | `data_dir/escrow.journal` | `quorium/tests/fixtures/escrow-journal-v1.bin`, `journal_fixture.rs` |
| Peer messages (envelope, requests, responses) | envelope format 1 | QUIC and TCP between nodes and clients | `quorium-net/tests/fixtures/wire-v1.bin`, `wire_fixtures.rs` |
| Legacy snapshot files | 1 | `data_dir/state/shard-N/snapshot` | migrated into `state.redb` at startup, covered by `redb_state.rs` |

## Rules

- Byte fields use `serde_bytes`. Their encoding is identical to a plain `Vec<u8>` sequence, so switching between the two never changes a format.
- New enum variants are only appended at the end. Postcard encodes variants by index.
- Adding a field to a stored struct is a format change. Postcard has no optional trailing fields. Bump the version, keep a decoder for the old version, and add a fixture for the new one.
- `EscrowView` is also stored inside user entities that use `Escrow<T>`, so changing it needs an entity migration as well.
- A node refuses to start if stored state contains an entity type the binary does not register.
- When state in an older snapshot or row format is decoded, the range snapshots it carries are rewritten in today's format. These are frozen ranges and cached `Extracted` replies. Replicas that loaded old state and replicas that applied the same entries afresh then hold identical bytes.

## Changes in format 4 and row format 2

Shard state gained the escrow holder map: per escrow and holder node, the running total settled through, the units held, the peer that copies the holder's sales, and whether the grant was reclaimed. Older state decodes with an empty map.

New enum variants, appended:

- `EscrowAction::SettleThrough`, `Reclaim`, `Close` and `GrantVia`;
- `Request::EscrowSold`;
- escrow journal records `Copied`, `GrantedVia`, `PeerSold`, `Fenced` and `Reclaimed`.
