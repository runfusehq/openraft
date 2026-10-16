use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::ServerState;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// With Pre-Vote enabled (fuse fork; backport of upstream openraft `0f6bb9a3`), a follower cut
/// off from the others does not inflate its term, and rejoins as a follower of the same leader.
///
/// Without Pre-Vote it raises its term at every election timeout, and the higher term deposes the
/// healthy leader when it reconnects. With Pre-Vote it never reaches a quorum of grants: an
/// unreachable peer counts as no grant.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn pre_vote_keeps_an_isolated_follower_from_inflating_its_term() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_pre_vote: true,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- create cluster of 0,1,2; node 0 leads");
    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    let n1 = router.get_raft_handle(&1)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is leader").await?;
    n1.wait(timeout()).state(ServerState::Follower, "node 1 is follower").await?;
    let term = n0.metrics().borrow().current_term;

    tracing::info!("--- isolate node 1 for many election timeouts");
    router.set_network_error(1, true);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(term, n1.metrics().borrow().current_term, "node 1 did not raise its term while isolated");
    // The rounds that kept it from raising the term show on its Pre-Vote counters.
    let stats = n1.pre_vote_stats();
    assert!(stats.rounds_started >= 2, "node 1 pre-voted at its election timeouts: {stats:?}");
    assert!(stats.replies_network_error > 0, "every peer was unreachable: {stats:?}");
    assert_eq!((0, 0), (stats.replies_granted, stats.elections_started), "{stats:?}");
    assert_eq!(Some(term + 1), stats.last_round.map(|r| r.term), "a round asks for the next term");

    tracing::info!("--- reconnect node 1: the leader keeps leading in its term");
    router.set_network_error(1, false);
    n1.wait(timeout()).current_leader(0, "node 1 follows node 0 again").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 still leads").await?;
    assert_eq!(term, n0.metrics().borrow().current_term, "the leader's term did not change");
    assert_eq!(term, n1.metrics().borrow().current_term);

    Ok(())
}

/// With Pre-Vote enabled, losing the leader still elects a new one: the two followers grant each
/// other's Pre-Vote once the old leader's lease has expired.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn pre_vote_elects_a_new_leader_when_the_leader_is_lost() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_pre_vote: true,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is leader").await?;
    let term = n0.metrics().borrow().current_term;

    tracing::info!("--- isolate the leader, node 0");
    router.set_network_error(0, true);

    let n1 = router.get_raft_handle(&1)?;
    let n2 = router.get_raft_handle(&2)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (m1, m2) = (n1.metrics().borrow().clone(), n2.metrics().borrow().clone());
        if m1.state == ServerState::Leader || m2.state == ServerState::Leader {
            assert!(m1.current_term > term || m2.current_term > term);
            let (s1, s2) = (n1.pre_vote_stats(), n2.pre_vote_stats());
            assert!(s1.elections_started + s2.elections_started >= 1, "a granted round started the election: {s1:?} {s2:?}");
            assert!(s1.served_granted + s2.served_granted >= 1, "and one of them granted it: {s1:?} {s2:?}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no new leader among 1 and 2: n1 {:?} term {}, n2 {:?} term {}", m1.state, m1.current_term, m2.state, m2.current_term);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// The contrast: with Pre-Vote off (the default) the same isolated follower raises its term, so
/// the test above exercises Pre-Vote and not some other quiescence.
#[async_entry::test(worker_threads = 8, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn without_pre_vote_an_isolated_follower_inflates_its_term() -> Result<()> {
    let config = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(config.clone());

    router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    let n1 = router.get_raft_handle(&1)?;
    n0.wait(timeout()).state(ServerState::Leader, "node 0 is leader").await?;
    n1.wait(timeout()).state(ServerState::Follower, "node 1 is follower").await?;
    let term = n0.metrics().borrow().current_term;

    router.set_network_error(1, true);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(n1.metrics().borrow().current_term > term, "node 1 raised its term while isolated");

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(3000))
}
