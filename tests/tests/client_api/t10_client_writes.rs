use std::sync::Arc;

use anyhow::Result;
use futures::prelude::*;
use maplit::btreeset;
use openraft::error::ClientWriteError;
use openraft::raft::ClientWriteResponse;
use openraft::CommittedLeaderId;
use openraft::Config;
use openraft::LogId;
use openraft::SnapshotPolicy;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;
use openraft_memstore::TypeConfig;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// - create a stable 3-node cluster.
/// - write a lot of data to it.
/// - assert that the cluster stayed stable and has all of the expected data.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn client_writes() -> Result<()> {
    let config = Arc::new(
        Config {
            snapshot_policy: SnapshotPolicy::LogsSinceLast(500),
            // The write load is heavy in this test, need a relatively long timeout.
            election_timeout_min: 500,
            election_timeout_max: 1000,
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    // Write a bunch of data and assert that the cluster stayes stable.
    let leader = router.leader().expect("leader not found");
    let mut clients = futures::stream::FuturesUnordered::new();
    clients.push(router.client_request_many(leader, "0", 100));
    clients.push(router.client_request_many(leader, "1", 100));
    clients.push(router.client_request_many(leader, "2", 100));
    clients.push(router.client_request_many(leader, "3", 100));
    clients.push(router.client_request_many(leader, "4", 100));
    clients.push(router.client_request_many(leader, "5", 100));
    while clients.next().await.is_some() {}

    log_index += 100 * 6;
    router.wait_for_log(&btreeset![0, 1, 2], Some(log_index), None, "sync logs").await?;

    router
        .assert_storage_state(
            1,
            log_index,
            Some(0),
            LogId::new(CommittedLeaderId::new(1, 0), log_index),
            Some(((499..600).into(), 1)),
        )
        .await?;

    Ok(())
}

/// Test Raft::client_write_ff,
///
/// Manually receive the client-write response via the returned `Responder::Receiver`
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn client_write_ff() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster");
    let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let _ = log_index;

    let n0 = router.get_raft_handle(&0)?;

    let resp_rx = n0.client_write_ff(ClientRequest::make_request("foo", 2)).await?;
    let got: ClientWriteResponse<TypeConfig> = resp_rx.await??;
    assert_eq!(None, got.response().0.as_deref());

    let resp_rx = n0.client_write_ff(ClientRequest::make_request("foo", 3)).await?;
    let got: ClientWriteResponse<TypeConfig> = resp_rx.await??;
    assert_eq!(Some("request-2"), got.response().0.as_deref());

    Ok(())
}

/// Test Raft::client_write_many:
///
/// - the leader appends the group at consecutive log indexes, in order, and answers each write;
/// - a follower answers every write of a group with ForwardToLeader and appends nothing.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn client_write_many() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;

    tracing::info!(log_index, "--- leader appends a group of three");
    let rxs = n0
        .client_write_many(vec![
            ClientRequest::make_request("foo", 2),
            ClientRequest::make_request("foo", 3),
            ClientRequest::make_request("foo", 4),
        ])
        .await?;
    assert_eq!(3, rxs.len());
    let mut prev = None;
    for (i, rx) in rxs.into_iter().enumerate() {
        let got: ClientWriteResponse<TypeConfig> = rx.await??;
        assert_eq!(log_index + 1 + i as u64, got.log_id().index);
        assert_eq!(prev.as_deref(), got.response().0.as_deref());
        prev = Some(format!("request-{}", i + 2));
    }
    log_index += 3;
    router.wait_for_log(&btreeset![0, 1, 2], Some(log_index), None, "group replicated").await?;

    tracing::info!(log_index, "--- an empty group sends nothing");
    assert!(n0.client_write_many(vec![]).await?.is_empty());

    tracing::info!(log_index, "--- a follower forwards every write of a group");
    let n1 = router.get_raft_handle(&1)?;
    let rxs = n1
        .client_write_many(vec![
            ClientRequest::make_request("bar", 1),
            ClientRequest::make_request("bar", 2),
        ])
        .await?;
    for rx in rxs {
        let err = rx.await?.expect_err("a follower does not accept writes");
        let ClientWriteError::ForwardToLeader(fwd) = err else {
            panic!("expected ForwardToLeader, got {:?}", err)
        };
        assert_eq!(Some(0), fwd.leader_id);
    }
    router.wait_for_log(&btreeset![0, 1, 2], Some(log_index), None, "nothing appended").await?;

    Ok(())
}
