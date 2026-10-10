use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreemap;
use maplit::btreeset;
use openraft::testing::log_id;
use openraft::Config;
use openraft_memstore::BlockOperation;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// The leader learns each follower's last applied log id from its append-entries and heartbeat
/// responses (fuse fork: `AppendEntriesResponse::Success { applied }`), and reports it as
/// `RaftMetrics::replication_applied`. A follower reports none.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn leader_reports_followers_applied_log_ids() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: true,
            heartbeat_interval: 50,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing a 3-voter cluster");
    let mut log_index = router.new_cluster(btreeset! {0, 1, 2}, btreeset! {}).await?;

    tracing::info!(log_index, "--- write and let the followers apply");
    log_index += router.client_request_many(0, "x", 10).await? as u64;

    let last = Some(log_id(1, 0, log_index));
    router
        .wait(&0, timeout())
        .metrics(
            |m| m.replication_applied == Some(btreemap! {1 => last, 2 => last}),
            "the leader reports both followers applied through the last write",
        )
        .await?;

    // Each report carries when the leader sent the request it answered: on the leader's clock,
    // not in the future, and moving on with every heartbeat.
    let leader = router.get_raft_handle(&0)?;
    let base = leader.clock_base();
    let sent = |m: &openraft::RaftMetrics<u64, ()>| m.replication_applied_sent_since_clock_base.clone();
    let first = sent(&leader.metrics().borrow()).expect("a leader reports when each report was sent");
    assert_eq!(first.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    for (id, at) in &first {
        assert!(base + *at <= openraft::TokioInstant::now(), "target {id}: sent in the future");
    }
    router
        .wait(&0, timeout())
        .metrics(
            |m| m.replication_applied_sent_since_clock_base.as_ref().is_some_and(|s| s.iter().all(|(id, at)| *at > first[id])),
            "heartbeats move each target's report send time on",
        )
        .await?;

    let follower = router.get_raft_handle(&1)?.metrics().borrow().clone();
    assert_eq!(follower.replication_applied, None, "a follower reports no followers' applied log ids");
    assert_eq!(follower.replication_applied_sent_since_clock_base, None, "nor when they were sent");
    Ok(())
}

/// A target that has applied nothing yet reports so (`Success { applied: None }`), and the leader
/// records it as `None`, with when it was sampled, rather than leaving the target absent: an
/// absent target reads as unheard, though it answers every heartbeat (fuse fork).
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn leader_records_a_target_that_has_applied_nothing() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: true,
            heartbeat_interval: 50,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    tracing::info!(log_index, "--- a learner whose applies are held");
    router.new_raft_node(1).await;
    {
        let (_log, sm) = router.get_storage_handle(&1)?;
        sm.storage_mut().await.set_blocking(BlockOperation::DelayApply, Duration::from_millis(3_000));
    }
    router.add_learner(0, 1).await?;

    router
        .wait(&0, timeout())
        .metrics(
            |m| {
                m.replication_applied.as_ref().is_some_and(|a| a.get(&1) == Some(&None))
                    && m.replication_applied_sent_since_clock_base.as_ref().is_some_and(|s| s.contains_key(&1))
            },
            "the leader records the learner as having applied nothing, with when it was sampled",
        )
        .await?;
    assert_eq!(router.get_raft_handle(&1)?.metrics().borrow().last_applied, None, "the learner applied nothing");
    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
