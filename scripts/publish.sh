#!/usr/bin/env bash
set -euo pipefail

CRATES=(quorium-core quorium-macros quorium-store quorium-net quorium-sim quorium)
EXCLUDED=(xtask rig shop)

usage() {
    cat <<EOF
Usage: scripts/publish.sh [--execute] [--allow-dirty] [--skip-tests]

Publishes the Quorium crates to crates.io in dependency order:
  ${CRATES[*]}

Without --execute it only runs a dry run of every crate.
  --execute       publish for real; needs a clean tree pushed to origin and a crates.io token
  --allow-dirty   allow uncommitted changes (dry run only)
  --skip-tests    skip the release test suite
EOF
}

execute=0
allow_dirty=0
skip_tests=0
for arg in "$@"; do
    case "$arg" in
        --execute) execute=1 ;;
        --allow-dirty) allow_dirty=1 ;;
        --skip-tests) skip_tests=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $arg" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ $execute == 1 && $allow_dirty == 1 ]]; then
    echo "--allow-dirty is only allowed for a dry run" >&2
    exit 2
fi

cd "$(git rev-parse --show-toplevel)"

fail() {
    echo "error: $*" >&2
    exit 1
}

version=$(cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "quorium"))')
echo "Quorium version: $version"

for crate in "${CRATES[@]}"; do
    v=$(cargo metadata --no-deps --format-version 1 \
        | python3 -c "import json,sys; print(next(p['version'] for p in json.load(sys.stdin)['packages'] if p['name'] == '$crate'))")
    [[ $v == "$version" ]] || fail "$crate is at $v, expected $version"
done

dirty_flag=()
if [[ $allow_dirty == 1 ]]; then
    dirty_flag=(--allow-dirty)
else
    [[ -z $(git status --porcelain) ]] || fail "the working tree has uncommitted changes"
fi

if [[ $execute == 1 ]]; then
    git fetch --quiet origin
    [[ -n $(git branch -r --contains HEAD) ]] || fail "HEAD $(git rev-parse --short HEAD) is not on any branch of origin; push it first"
    if [[ -z ${CARGO_REGISTRY_TOKEN:-} && ! -s ${CARGO_HOME:-$HOME/.cargo}/credentials.toml ]]; then
        fail "no crates.io token; run 'cargo login' or set CARGO_REGISTRY_TOKEN"
    fi
fi

published() {
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' \
        -A "quorium-publish-script (https://github.com/canonical/quorium)" \
        "https://crates.io/api/v1/crates/$1/$2")
    [[ $code == 200 ]]
}

if [[ $skip_tests == 0 ]]; then
    echo "Running the release test suite"
    cargo test --workspace --release
fi

exclude_flags=()
for crate in "${EXCLUDED[@]}"; do
    exclude_flags+=(--exclude "$crate")
done

echo "Dry run of every crate"
cargo publish --workspace --dry-run "${exclude_flags[@]}" "${dirty_flag[@]}"

if [[ $execute == 0 ]]; then
    echo "Dry run passed. Run again with --execute to publish $version."
    exit 0
fi

for crate in "${CRATES[@]}"; do
    if published "$crate" "$version"; then
        echo "$crate $version is already on crates.io, skipping"
        continue
    fi
    echo "Publishing $crate $version"
    cargo publish -p "$crate"
done

tag="v$version"
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "Tag $tag already exists"
else
    git tag -a "$tag" -m "Quorium $version"
    echo "Created tag $tag; push it with: git push origin $tag"
fi

echo "Published Quorium $version"
