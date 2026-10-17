use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use openraft::Config;
use openraft::RaftLogReader;
use openraft::ServerState;
use openraft::Vote;
use openraft::raft::VoteRequest;

use crate::fixtures::RaftRouter;
use crate::fixtures::log_id;
use crate::fixtures::ut_harness;

/// Fuse fork: a member that granted a candidate's vote and holds an empty log can still
/// initialize, and its vote never goes back.
///
/// - Members `{0,1,2}`. The `lost` member campaigned at `(1, lost)`, both others granted it, and
///   it was lost before its first entry reached either of them.
/// - `init` initializes: it keeps the granted `(1, lost)` vote and campaigns at `(2, init)`, which
///   the other survivor grants. The group forms with the two survivors.
/// - Every member's first log id is `((0,0),0)`, the canonical one.
///
/// Run with the lost member below and above the initializing one: upstream 0.10's initialize
/// writes `(0, init)`, and a `(1, init)` would still regress a granted `(1, lost)` when
/// `init < lost`.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn initialize_after_granted_vote_lost_member_below() -> anyhow::Result<()> {
    form_after_granted_vote(0, 1, 2).await
}

#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn initialize_after_granted_vote_lost_member_above() -> anyhow::Result<()> {
    form_after_granted_vote(2, 0, 1).await
}

async fn form_after_granted_vote(lost: u64, init: u64, other: u64) -> anyhow::Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    // The lost member is never created: every RPC to it fails.
    router.new_raft_node(init).await;
    router.new_raft_node(other).await;

    let granted = Vote::new(1, lost);

    tracing::info!("--- both survivors grant ({granted})");
    for id in [init, other] {
        let n = router.get_raft_handle(&id)?;
        let resp = n.vote(VoteRequest::new(granted, None)).await?;
        assert!(resp.is_granted_to(&granted), "node {id} must grant {granted}: {resp:?}");

        let inited = n.is_initialized().await?;
        assert!(!inited, "a granted vote with an empty log is not initialized");
    }

    tracing::info!("--- node {init} initializes: allowed, and campaigns above the granted vote");
    {
        let n = router.get_raft_handle(&init)?;
        n.initialize(btreeset! {0,1,2}).await?;

        n.wait(timeout()).state(ServerState::Leader, "init forms the group").await?;
        n.wait(timeout()).vote(Vote::new_committed(2, init), "campaigned at (T+1, self)").await?;
        // Index 0 is the membership entry, index 1 the new leader's blank entry.
        n.wait(timeout()).applied_index(Some(1), "leader applied its blank entry").await?;
    }

    tracing::info!("--- node {other} follows the new leader");
    {
        let n = router.get_raft_handle(&other)?;
        n.wait(timeout()).applied_index(Some(1), "follower applied").await?;
        n.wait(timeout()).current_leader(init, "follower sees the leader").await?;
    }

    tracing::info!("--- every member holds the canonical first log id and a vote above the granted one");
    for id in [init, other] {
        let (mut sto, _sm) = router.get_storage_handle(&id)?;
        let entries = sto.try_get_log_entries(0..2).await?;
        assert_eq!(log_id(0, 0, 0), entries[0].log_id, "node {id}: canonical first log id");
        assert_eq!(log_id(2, init, 1), entries[1].log_id, "node {id}: the new leader's blank entry");

        let saved = sto.read_vote().await?.unwrap();
        assert!(saved > granted, "node {id}: saved vote {saved} must be above the granted {granted}");
    }

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3_000))
}
