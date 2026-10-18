use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::errors::ForwardReason;
use openraft::errors::PreconditionFailed;
use openraft::impls::ProgressResponder;
use openraft::raft::ClientWriteResult;
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::OneshotReceiverOf;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;
use openraft_memstore::TypeConfig;

use crate::fixtures::MemRaft;
use crate::fixtures::RaftRouter;
use crate::fixtures::log_id;
use crate::fixtures::ut_harness;

type Rx = OneshotReceiverOf<TypeConfig, ClientWriteResult<TypeConfig>>;

/// Write `name` required at log index `index` (fuse fork `WriteRequest::at_index`), fire and
/// forget; its result's receiver.
async fn write_at(raft: &MemRaft, name: &str, serial: u64, index: u64) -> Result<Rx> {
    let (responder, rx) = ProgressResponder::complete_only();
    raft.write(ClientRequest::make_request(name, serial)).at_index(index).responder(responder).await?;
    Ok(rx)
}

fn mismatch(result: ClientWriteResult<TypeConfig>) -> (u64, u64) {
    match result {
        Err(ClientWriteError::PreconditionFailed(PreconditionFailed::NextLogIndexMismatch { expected, actual })) => (expected, actual),
        other => panic!("expected NextLogIndexMismatch, got {other:?}"),
    }
}

/// A write required at the index it gets is appended there; one required at any other index is
/// refused with `NextLogIndexMismatch` and takes no index, so the next write is checked against
/// the index it would really get (fuse fork).
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn a_write_at_another_index_is_refused_and_takes_no_index() -> Result<()> {
    let config = Arc::new(Config { enable_tick: false, ..Default::default() }.validate()?);
    let mut router = RaftRouter::new(config.clone());
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;

    // At the next index: appended there.
    let ok = write_at(&n0, "a", 1, log_index + 1).await?.await??;
    log_index += 1;
    assert_eq!(log_id(1, 0, log_index), ok.log_id);

    // Ahead of it, then behind it: both refused, the log unchanged.
    assert_eq!((log_index + 2, log_index + 1), mismatch(write_at(&n0, "ahead", 2, log_index + 2).await?.await?));
    assert_eq!((log_index, log_index + 1), mismatch(write_at(&n0, "behind", 3, log_index).await?.await?));
    assert_eq!(Some(log_index), n0.metrics().borrow_watched().last_log_index);

    // Queued together: each is checked against the index it would get after the ones before it,
    // whether or not the core takes them as one batch.
    let wrong = write_at(&n0, "wrong", 4, log_index + 2).await?;
    let first = write_at(&n0, "first", 5, log_index + 1).await?;
    let second = write_at(&n0, "second", 6, log_index + 2).await?;
    assert_eq!((log_index + 2, log_index + 1), mismatch(wrong.await?));
    assert_eq!(log_id(1, 0, log_index + 1), first.await??.log_id);
    assert_eq!(log_id(1, 0, log_index + 2), second.await??.log_id);
    Ok(())
}

/// The fuse pipeline's case: a leader whose lease expired refuses the write cut for index `i`
/// (`LeaseExpired`, no index taken); writes queued behind it were cut for `i + 1`, `i + 2`. When
/// the lease comes back before the core takes them, each is refused on its own check rather than
/// appended one index below the index it was cut for.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn writes_cut_behind_a_lease_refusal_are_refused_when_the_lease_returns() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 20,
            election_timeout_min: 100,
            election_timeout_max: 200,
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    let i = log_index + 1;

    router.set_unreachable(1, true);
    router.set_unreachable(2, true);
    TypeConfig::sleep(Duration::from_millis(config.election_timeout_max)).await;

    let refused = write_at(&n0, "b1", 1, i).await?.await?;
    let Err(ClientWriteError::ForwardToLeader(forward)) = refused else {
        panic!("expected LeaseExpired, got {refused:?}");
    };
    assert_eq!(ForwardReason::LeaseExpired, forward.reason);

    router.set_unreachable(1, false);
    router.set_unreachable(2, false);
    let refresh_started = TypeConfig::now();
    n0.trigger().heartbeat().await?;
    n0.wait(Some(Duration::from_secs(3))).leader_with_quorum_acked(Some(refresh_started), "leader lease recovered").await?;

    assert_eq!((i + 1, i), mismatch(write_at(&n0, "b2", 2, i + 1).await?.await?));
    assert_eq!((i + 2, i), mismatch(write_at(&n0, "b3", 3, i + 2).await?.await?));
    assert_eq!(Some(log_index), n0.metrics().borrow_watched().last_log_index, "nothing appended");

    // The batch cut again for `i` is appended at `i`.
    assert_eq!(log_id(1, 0, i), write_at(&n0, "b1-again", 4, i).await?.await??.log_id);
    Ok(())
}
