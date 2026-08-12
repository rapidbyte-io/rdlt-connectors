#!/usr/bin/env sh
# Print the rdlt revision Cargo.lock resolved for the rdlt git
# dependencies — the ONE revision anything in this repository may
# `cargo install` an rdlt binary (the certifier CLI, the release rdlt
# CLI) from, so a spawned bin always matches the libraries the suites
# link. Read from the rdlt-certify [[package]] block; every rdlt git
# dep resolves to the same revision because they share one git source.
#
# Under the README's two-checkout [patch] dev loop (an uncommitted
# .cargo/config.toml redirecting the rdlt git deps to a local
# checkout), cargo strips the `source =` line from the patched
# packages, so no locked revision exists. The fallback is a `rev`
# pinned on the workspace manifest's own git spec; where neither names
# one, refuse loudly — the gate of record never runs patched.
#
# Mirrors resolve_locked_rev in the file connector's
# tests/cases/test_certify_wire.rs, which pins all three arms.
set -eu
cd "$(dirname "$0")/.."

rev=$(awk '
    /^name = "rdlt-certify"$/ { inside = 1; next }
    inside && /^\[\[package\]\]/ { exit }
    inside && /^source = / {
        sub(/.*#/, ""); sub(/".*/, ""); print; exit
    }
' Cargo.lock)
if [ -n "$rev" ]; then
    echo "$rev"
    exit 0
fi

rev=$(sed -n 's/^rdlt-certify *=.*rev *= *"\([^"]*\)".*/\1/p' Cargo.toml)
if [ -n "$rev" ]; then
    echo "$rev"
    exit 0
fi

echo "locked-rdlt-rev: the rdlt-certify lock entry carries no git source — the \
two-checkout [patch] dev loop is active (an uncommitted .cargo/config.toml \
redirects the rdlt git dependencies to a local checkout, and cargo strips the \
source line from patched packages) and the workspace manifest pins no 'rev', \
so there is no locked revision to install from. The gate of record never runs \
patched: delete the patch, let cargo restore Cargo.lock, and run again." >&2
exit 1
