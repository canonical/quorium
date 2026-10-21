# Releasing

Quorium is published to crates.io as six crates sharing one version, set in `[workspace.package]` of the root `Cargo.toml`:

| Order | Crate | Depends on |
|---|---|---|
| 1 | `quorium-core` | |
| 2 | `quorium-macros` | |
| 3 | `quorium-store` | `quorium-core` |
| 4 | `quorium-net` | `quorium-core` |
| 5 | `quorium-sim` | `quorium-core`, `quorium-store` |
| 6 | `quorium` | all of the above |

`quorium-sim` is published because the `testing` feature of `quorium` depends on it. `xtask`, `rig` and `shop` set `publish = false`.

Internal dependencies are pinned exactly (`=0.1.0-alpha.1`), so the six crates are always used at the same version. `crates/quorium-sim/spec` and the `LICENSE-*` files in each crate are symlinks to the repository root; cargo packages the files they point to.

## Steps

1. Bump `version` in `[workspace.package]` and the six `version = "=..."` pins in `[workspace.dependencies]`.
2. Update the known issues in `crates/quorium/README.md`, which is the page crates.io shows.
3. Commit and push to `https://github.com/canonical/quorium`.
4. Log in once with `cargo login`, or set `CARGO_REGISTRY_TOKEN`.
5. Dry run: `scripts/publish.sh`. It checks that all six crates share the version, runs `cargo test --workspace --release`, then `cargo publish --workspace --dry-run`.
6. Publish: `scripts/publish.sh --execute`. It refuses a dirty tree or a commit not on origin, publishes in the order above, skips crates already published at this version so a failed run can be resumed, and creates the tag `v<version>`.
7. Push the tag: `git push origin v<version>`.

`--skip-tests` skips step 5's test suite. `--allow-dirty` is accepted for dry runs only.

A published version cannot be deleted, only yanked with `cargo yank --version <v> <crate>`.
