# runfusehq/openraft — maintenance notes

This is a fork of [`databendlabs/openraft`](https://github.com/databendlabs/openraft) maintained by [runfusehq](https://github.com/runfusehq). It exists to carry the `Trigger::tick()` patch that fuse needs for its per-node raft scheduler (Tier 3 of the per-node substrate PRD — see fuse-internal `docs/exploration/per-node-substrate-crdb-parallel-prd.md` §5).

## What's forked

- Branch `fuse-0.9.25` — off upstream tag `v0.9.25`, carries one patch commit adding `pub async fn tick(&self) -> Result<(), Fatal<C::NodeId>>` on `Trigger<C>`. Sends `Notify::Tick { i: 0 }` via a `tx_notify` clone stashed on `RaftInner`. Composes with `Raft::runtime_config().tick(false)` — external ticks work when the internal timer is off.
- Everything else stays in lockstep with upstream `v0.9.25`.

Fuse's workspace pins to this branch:

```toml
[workspace.dependencies]
openraft = { git = "https://github.com/runfusehq/openraft", branch = "fuse-0.9.25", version = "0.9.25" }
openraft-macros = { git = "https://github.com/runfusehq/openraft", branch = "fuse-0.9.25", version = "0.9.25" }
```

The `version = "0.9.25"` field preserves semver signals for downstream tooling but Cargo resolves against the git branch tip.

## Rebasing on a new upstream 0.9.x tag

Upstream's 0.9.x cadence has run ~1 tag per 3 months over the past year (observed via `git log --tags --simplify-by-decoration --pretty="%h %d"` on `databendlabs/openraft` `release-0.9`). When a new `v0.9.z` lands:

```bash
cd /tmp/openraft-fork-rebase
git clone https://github.com/runfusehq/openraft.git
cd openraft

# Add upstream if not present, fetch the new tag
git remote add upstream https://github.com/databendlabs/openraft.git
git fetch upstream --tags

# Cherry-pick our tick patch onto the new tag
git checkout -b fuse-0.9.z v0.9.z
git cherry-pick <patch-sha-on-fuse-0.9.25>  # find via: git log --oneline fuse-0.9.25 | head

# Run upstream's test suite + our added test to prove nothing broke
cargo test -p openraft
cargo test --test client_api trigger_tick

# If green: push, update the branch pin in fuse's Cargo.toml
git push origin fuse-0.9.z
```

**Expected cost per rebase:** 15–30 minutes when the cherry-pick is clean, longer if `src/raft/trigger.rs`, `src/raft/raft_inner.rs`, `src/raft/mod.rs`, or `src/core/notify.rs` moved under the new tag.

**Conflict shapes to expect:**

- `raft_inner.rs`: the `pub(in crate::raft) tx_notify` field. If upstream added another `Notify`-adjacent field or reordered, resolve by inserting our field alongside.
- `mod.rs`: the `tx_notify: tx_notify.clone()` change on the `RaftCore` construction site plus the `tx_notify,` addition on the `RaftInner` construction site.
- `trigger.rs`: additive — new method under `impl Trigger`.

Upstream tests for `Trigger::heartbeat()` and the metrics-wait harness serve as regression coverage for the composition. Our own regression test lives at `tests/tests/client_api/t14_trigger_tick.rs`.

## Retirement conditions

Retire the fork when either:

- **(a)** Upstream drmingdrmer lands an equivalent `Trigger::tick()` in a future 0.9.x tag. Watch the upstream `release-0.9` branch for PRs touching `src/raft/trigger.rs`. Once available upstream, drop the fork pin from fuse's `Cargo.toml`.
- **(b)** fuse bumps its openraft dependency to 0.10.x. Re-evaluate patch applicability at that time — 0.10 may have restructured the tick pipeline.

Neither is a hard commitment today. The fork stands as long as fuse needs the external tick surface and upstream does not provide it.

## Ownership

The fork is owned by the `runfusehq` GitHub org (Runfuse Inc). Maintenance follows the org's normal rotation — no single-person namespace risk. This document is the primary handoff; anyone with `runfusehq/openraft` push access can pick up a rebase from this file alone.
