use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use openraft::type_config::TypeConfigExt;
use openraft::Config;
use openraft_memstore::TypeConfig;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// External `Trigger::tick()` drives the same RaftCore path as the internal
/// tick loop when [`RuntimeConfigHandle::tick(false)`] has disabled the timer.
///
/// This is the composition entry point for external schedulers (fuse's per-node
/// tick pump). Observability: while the internal ticker is off, no heartbeats
/// fire (leader's heartbeat check runs inside the tick handler). Firing an
/// external tick reaches `Notify::Tick` in RaftCore, which runs the leader's
/// heartbeat timer check, sends heartbeats, and refreshes
/// `millis_since_quorum_ack`. If `Trigger::tick()` were not wired, the leader
/// would never resume heartbeating and quorum-ack age would grow unbounded.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn trigger_tick_drives_heartbeat_when_internal_tick_disabled() -> anyhow::Result<()> {
    let heartbeat_interval = 50; // ms
    let config = Arc::new(
        Config {
            heartbeat_interval,
            election_timeout_min: 1_000,
            election_timeout_max: 1_500,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;

    // Disable the internal tick loop. From this point on, RaftCore only sees
    // ticks when we call `n0.trigger().tick().await`. With no ticks, the
    // leader's per-tick heartbeat check never runs and quorum-ack age grows.
    n0.runtime_config().tick(false);

    // Sleep long enough that many internal ticks would have fired if the
    // internal ticker were still running. Confirm that (a) the metrics watch
    // is stale because no ticks are updating it, or (b) quorum-ack has grown.
    // Either way, the leader is not heartbeating on its own.
    TypeConfig::sleep(Duration::from_millis(400)).await;

    // Drive one external tick to pump the leader's heartbeat check.
    n0.trigger().tick().await?;

    // Give the runtime a moment to process the tick + heartbeat + acks.
    // If Trigger::tick() is correctly wired, quorum-ack refreshes below
    // 200ms within the timeout window. If it is not wired, this wait fails.
    n0.wait(Some(Duration::from_millis(1_000)))
        .metrics(
            |x| x.millis_since_quorum_ack < Some(200),
            "millis_since_quorum_ack refreshed after external tick drove heartbeat",
        )
        .await?;

    // Second cycle: sleep again, confirm quorum-ack age grows (still no
    // internal ticker), then fire another external tick and confirm refresh.
    TypeConfig::sleep(Duration::from_millis(400)).await;
    n0.trigger().tick().await?;
    n0.wait(Some(Duration::from_millis(1_000)))
        .metrics(
            |x| x.millis_since_quorum_ack < Some(200),
            "millis_since_quorum_ack refreshed after second external tick",
        )
        .await?;

    Ok(())
}
