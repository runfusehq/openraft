use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use openraft::Config;
use openraft::RPCTypes;
use openraft::RaftLogReader;
use openraft::ServerState;
use openraft::Vote;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::NetworkError;
use openraft::errors::RPCError;
use openraft::raft::VoteRequest;
use openraft::storage::RaftLogStorage;
use openraft::type_config::TypeConfigExt;
use openraft_memstore::TypeConfig;

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

/// Fuse fork: in a group of 5 voters, a member that initializes after a granted vote cannot be
/// elected over a committed entry.
///
/// - Node 0 forms the group and every member grants its `(1, 0)` vote, but its appends never
///   reach nodes 3 and 4, so the group commits entries on nodes 0, 1 and 2 only. Node 0 is then
///   lost.
/// - Node 3 holds the granted vote and an empty log, so it initializes and campaigns at
///   `(2, 3)` with the index-0 entry as its last log. Nodes 1 and 2 hold the committed entries
///   and refuse it, so it wins no quorum.
/// - Node 1 is elected instead, and every survivor ends with the committed entries.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn initialize_after_granted_vote_cannot_elect_over_a_committed_entry() -> anyhow::Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    for id in 0..5 {
        router.new_raft_node(id).await;
    }

    router
        .set_rpc_pre_hook(RPCTypes::AppendEntries, |_router, _req, _from, to| {
            let res = if to == 3 || to == 4 {
                Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(format!(
                    "blocked: append-entries to {to}"
                ))))
            } else {
                Ok(())
            };
            Box::pin(futures::future::ready(res))
        })
        .await;

    tracing::info!("--- node 0 forms the group; its entries commit on nodes 0, 1 and 2 only");
    {
        let n0 = router.get_raft_handle(&0)?;
        n0.initialize(btreeset! {0,1,2,3,4}).await?;
        n0.wait(timeout()).state(ServerState::Leader, "node 0 leads").await?;
        n0.wait(timeout()).applied_index(Some(1), "node 0's blank entry commits").await?;
        router.client_request(0, "foo", 1).await?;
        for id in [0, 1, 2] {
            router.wait(&id, timeout()).log_index(Some(2), "the committed entries").await?;
        }
        for id in [3, 4] {
            let (mut sto, _sm) = router.get_storage_handle(&id)?;
            assert_eq!(None, sto.get_log_state().await?.last_log_id, "node {id}: an empty log");
            assert_eq!(Some(Vote::new(1, 0)), sto.read_vote().await?, "node {id}: granted node 0's vote");
        }
    }
    let committed = {
        let (mut sto, _sm) = router.get_storage_handle(&1)?;
        sto.try_get_log_entries(0..3).await?.into_iter().map(|e| e.log_id).collect::<Vec<_>>()
    };

    router.remove_node(0);
    router.rpc_pre_hook(RPCTypes::AppendEntries, None).await;

    tracing::info!("--- node 3 initializes after its granted vote, but nodes 1 and 2 refuse it");
    {
        let n3 = router.get_raft_handle(&3)?;
        n3.initialize(btreeset! {0,1,2,3,4}).await?;
        n3.wait(timeout()).vote(Vote::new(2, 3), "node 3 campaigns at (T+1, self)").await?;
        // Long enough for every vote reply to arrive.
        TypeConfig::sleep(Duration::from_millis(500)).await;
        assert_ne!(ServerState::Leader, n3.metrics().borrow_watched().state, "node 3 won no quorum");
    }

    tracing::info!("--- node 1 is elected, and every survivor ends with the committed entries");
    {
        let n1 = router.get_raft_handle(&1)?;
        for _ in 0..5 {
            n1.trigger().elect(false).await?;
            if n1.wait(Some(Duration::from_millis(500))).state(ServerState::Leader, "node 1 leads").await.is_ok() {
                break;
            }
        }
        n1.wait(timeout()).state(ServerState::Leader, "node 1 leads").await?;
        let last = n1.metrics().borrow_watched().last_log_index;
        for id in [1, 2, 3, 4] {
            router.wait(&id, timeout()).log_index(last, "the new leader's log").await?;
            let (mut sto, _sm) = router.get_storage_handle(&id)?;
            let held = sto.try_get_log_entries(0..3).await?.into_iter().map(|e| e.log_id).collect::<Vec<_>>();
            assert_eq!(committed, held, "node {id}: the committed entries survive");
        }
    }

    Ok(())
}
