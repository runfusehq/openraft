use std::sync::Arc;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::errors::PreconditionFailed;
use openraft::impls::ProgressResponder;
use openraft::raft::ClientWriteResult;
use openraft::type_config::alias::OneshotReceiverOf;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;
use openraft_memstore::TypeConfig;

use crate::fixtures::MemRaft;
use crate::fixtures::RaftRouter;
use crate::fixtures::log_id;
use crate::fixtures::ut_harness;

type Rx = OneshotReceiverOf<TypeConfig, ClientWriteResult<TypeConfig>>;

/// Write `entries` (name, required log index) as one batch (fuse fork `Raft::write_batch`); each
/// entry's result receiver, in order.
async fn write_batch(raft: &MemRaft, entries: &[(&str, u64)]) -> Result<Vec<Rx>> {
    let mut batch = raft.write_batch();
    let mut rxs = Vec::new();
    for (serial, &(name, index)) in entries.iter().enumerate() {
        let (responder, rx) = ProgressResponder::complete_only();
        batch.push(ClientRequest::make_request(name, serial as u64), Some(responder), Some(index));
        rxs.push(rx);
    }
    batch.await?;
    Ok(rxs)
}

/// A batch's entries are appended at the indexes they name, in one write of the leader's log; an
/// entry required at another index is refused with `NextLogIndexMismatch` and takes no index, so
/// the next entry of the batch is checked against the index it would really get (fuse fork).
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn a_batch_is_one_leader_append_and_each_entry_is_checked_at_its_index() -> Result<()> {
    let config = Arc::new(Config { enable_tick: false, ..Default::default() }.validate()?);
    let mut router = RaftRouter::new(config.clone());
    let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
    let n0 = router.get_raft_handle(&0)?;
    let (store, _) = router.get_storage_handle(&0)?;
    let i = log_index + 1;

    store.take_append_count();
    let rxs = write_batch(&n0, &[("a", i), ("b", i + 1), ("c", i + 2)]).await?;
    for (k, rx) in rxs.into_iter().enumerate() {
        assert_eq!(log_id(1, 0, i + k as u64), rx.await??.log_id);
    }
    assert_eq!(1, store.take_append_count(), "the batch took one leader append");

    // The middle entry names an index ahead of the one it would get: refused, and the one after
    // it takes the index the refused one would have.
    let j = i + 3;
    let mut rxs = write_batch(&n0, &[("d", j), ("ahead", j + 5), ("e", j + 1)]).await?.into_iter();
    assert_eq!(log_id(1, 0, j), rxs.next().unwrap().await??.log_id);
    match rxs.next().unwrap().await? {
        Err(ClientWriteError::PreconditionFailed(PreconditionFailed::NextLogIndexMismatch { expected, actual })) => {
            assert_eq!((j + 5, j + 1), (expected, actual))
        }
        other => panic!("expected NextLogIndexMismatch, got {other:?}"),
    }
    assert_eq!(log_id(1, 0, j + 1), rxs.next().unwrap().await??.log_id);
    assert_eq!(1, store.take_append_count(), "the kept entries took one leader append");

    // An empty batch sends nothing.
    n0.write_batch().await?;
    assert_eq!(Some(j + 1), n0.metrics().borrow_watched().last_log_index);
    assert_eq!(0, store.take_append_count());
    Ok(())
}
