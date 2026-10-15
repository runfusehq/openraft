#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::error::RPCError;
use openraft::error::Timeout;
use openraft::raft::AppendEntriesRequest;
use openraft::Config;
use openraft::RPCTypes;

use crate::fixtures::init_default_ut_tracing;
use crate::fixtures::RaftRouter;

/// (fuse fork) An AppendEntries RPC that carried entries and timed out is retried with at most
/// half as many entries, so a follower that cannot take the whole payload within the deadline
/// still catches up; once the hint has been used up, the full payload resumes.
///
/// In this test node-1 times out every AppendEntries RPC that carries more than 2 entries.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn append_entries_timeout_halves_payload() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster of 1 node");
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    let n = 20u64;
    tracing::info!(log_index, "--- write {} entries to leader", n);
    {
        log_index += router.client_request_many(0, "0", n as usize).await?;
        router.wait(&0, timeout()).applied_index(Some(log_index), format!("{} writes", n)).await?;
    }

    // (entries carried, timed out) of every non-empty AppendEntries RPC to node-1.
    let sent: Arc<Mutex<Vec<(usize, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let limit: Arc<Mutex<usize>> = Arc::new(Mutex::new(2));

    let (s, l) = (sent.clone(), limit.clone());
    router.set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, req, from, target| {
        let r: AppendEntriesRequest<_> = req.try_into().unwrap();
        if target != 1 || r.entries.is_empty() {
            return Ok(());
        }
        let late = r.entries.len() > *l.lock().unwrap();
        s.lock().unwrap().push((r.entries.len(), late));
        if late {
            return Err(RPCError::Timeout(Timeout {
                action: RPCTypes::AppendEntries,
                id: from,
                target,
                timeout: Duration::from_millis(50),
            }));
        }
        Ok(())
    });

    tracing::info!(log_index, "--- add node-1 as learner; it times out appends of more than 2 entries");
    {
        router.new_raft_node(1).await;
        router.add_learner(0, 1).await?;
        log_index += 1;
        router.wait(&1, timeout()).applied_index(Some(log_index), "node-1 caught up").await?;
    }

    {
        let sent = sent.lock().unwrap();
        assert!(sent.iter().any(|(_, late)| *late), "a catch-up append timed out: {:?}", sent);
        for w in sent.windows(2) {
            let ((before, timed_out), (after, _)) = (w[0], w[1]);
            if timed_out {
                assert!(after <= std::cmp::max(1, before / 2), "after {} entries timed out, sent {}: {:?}", before, after, sent);
            }
        }
    }

    tracing::info!(log_index, "--- the hint expires: a later catch-up is sent whole");
    {
        *limit.lock().unwrap() = usize::MAX;
        // Use the hint up with single-entry appends.
        for _ in 0..12 {
            log_index += router.client_request_many(0, "0", 1).await?;
            router.wait(&1, timeout()).applied_index(Some(log_index), "single write").await?;
        }
        sent.lock().unwrap().clear();
        router.set_unreachable(1, true);
        log_index += router.client_request_many(0, "0", 10).await?;
        router.set_unreachable(1, false);
        router.wait(&1, timeout()).applied_index(Some(log_index), "10 writes while node-1 was unreachable").await?;
        let sent = sent.lock().unwrap();
        assert!(sent.iter().any(|(len, _)| *len > 2), "the full payload resumed: {:?}", sent);
    }

    Ok(())
}

/// (fuse fork) A heartbeat carries no entries: one that times out leaves the entries hint as it
/// is, so the next catch-up is not held to one entry per append.
#[async_entry::test(worker_threads = 4, init = "init_default_ut_tracing()", tracing_span = "debug")]
async fn a_timed_out_heartbeat_does_not_shrink_the_payload() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

    let heartbeat_timed_out = Arc::new(Mutex::new(false));
    let sent: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let (h, s) = (heartbeat_timed_out.clone(), sent.clone());
    router.set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, req, from, target| {
        let r: AppendEntriesRequest<_> = req.try_into().unwrap();
        if target != 1 {
            return Ok(());
        }
        if r.entries.is_empty() {
            let mut h = h.lock().unwrap();
            if !*h {
                *h = true;
                return Err(RPCError::Timeout(Timeout {
                    action: RPCTypes::AppendEntries,
                    id: from,
                    target,
                    timeout: Duration::from_millis(50),
                }));
            }
            return Ok(());
        }
        s.lock().unwrap().push(r.entries.len());
        Ok(())
    });

    tracing::info!(log_index, "--- one heartbeat to node-1 times out");
    {
        router.get_raft_handle(&0)?.trigger().heartbeat().await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !*heartbeat_timed_out.lock().unwrap() {
            assert!(std::time::Instant::now() < deadline, "the heartbeat was sent");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    tracing::info!(log_index, "--- the next catch-up goes out whole");
    {
        router.set_unreachable(1, true);
        log_index += router.client_request_many(0, "0", 10).await?;
        sent.lock().unwrap().clear();
        router.set_unreachable(1, false);
        router.wait(&1, timeout()).applied_index(Some(log_index), "10 writes").await?;
        let sent = sent.lock().unwrap();
        // A heartbeat timeout that set the hint would hold this catch-up to one entry per append
        // for the hint's 10 uses: about ten appends. Unshrunk it takes one or two (the first may
        // be a range computed before node-1 went unreachable).
        assert!(sent.len() <= 3, "the catch-up went in a few appends, not one entry each: {:?}", sent);
    }

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
