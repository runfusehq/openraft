//! Pre-Vote counters (fuse fork), read with [`Raft::pre_vote_stats`](crate::Raft::pre_vote_stats).
//!
//! A member that keeps pre-voting and failing looks idle on [`RaftMetrics`](crate::RaftMetrics):
//! its term and vote do not move. These counters tell the two apart, and say why the rounds fail,
//! from both sides: the rounds this node started and how its peers answered, and the Pre-Votes
//! it answered and on what grounds.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const NONE: u64 = u64::MAX;

/// Why this node answered a Pre-Vote as it did. Checked in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreVoteVerdict {
    /// Refused: this node follows a leader whose lease has not expired.
    LeaderLease,
    /// Refused: this node leads and a quorum acked it within the lease.
    LeaderQuorumAcked,
    /// Refused: the candidate's last log id is behind this node's.
    ShorterLog,
    /// Refused: the candidate's vote is not greater than or equal to this node's.
    LowerVote,
    Granted,
}

/// How a peer answered one of this node's Pre-Votes, as this node can tell from the reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreVoteReply {
    Granted,
    /// Refused, and the reply carries a vote this request does not reach.
    RefusedByVote,
    /// Refused, and the reply carries a longer log than this node's.
    RefusedByLog,
    /// Refused with neither: the peer still has a live leader (its lease, or it leads).
    RefusedLiveLeader,
    /// An answer to an earlier round, or after the round ended: ignored.
    Stale,
    /// No answer within the election timeout.
    Timeout,
    /// The network returned an error (unreachable, decode, remote error).
    NetworkError,
}

/// Shared between the [`Raft`](crate::Raft) handle and its core: the core writes, readers load.
#[derive(Debug)]
pub(crate) struct PreVoteStats {
    rounds_started: AtomicU64,
    elections_started: AtomicU64,
    last_round_term: AtomicU64,
    /// Milliseconds after the raft's clock base, or `NONE`.
    last_round_started_ms: AtomicU64,
    last_round_granted: AtomicU64,
    last_round_refused: AtomicU64,
    replies: [AtomicU64; 7],
    served: [AtomicU64; 5],
}

impl Default for PreVoteStats {
    fn default() -> Self {
        Self {
            rounds_started: AtomicU64::new(0),
            elections_started: AtomicU64::new(0),
            last_round_term: AtomicU64::new(NONE),
            last_round_started_ms: AtomicU64::new(NONE),
            last_round_granted: AtomicU64::new(0),
            last_round_refused: AtomicU64::new(0),
            replies: Default::default(),
            served: Default::default(),
        }
    }
}

impl PreVoteStats {
    /// A round at `term` was sent `started_ms` after the clock base.
    pub(crate) fn round_started(&self, term: u64, started_ms: u64) {
        self.rounds_started.fetch_add(1, Ordering::Relaxed);
        self.last_round_granted.store(0, Ordering::Relaxed);
        self.last_round_refused.store(0, Ordering::Relaxed);
        self.last_round_term.store(term, Ordering::Relaxed);
        self.last_round_started_ms.store(started_ms, Ordering::Relaxed);
    }

    /// A quorum would grant: the real election started.
    pub(crate) fn election_started(&self) {
        self.elections_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reply(&self, reply: PreVoteReply) {
        self.replies[reply as usize].fetch_add(1, Ordering::Relaxed);
        match reply {
            PreVoteReply::Granted => self.last_round_granted.fetch_add(1, Ordering::Relaxed),
            PreVoteReply::RefusedByVote | PreVoteReply::RefusedByLog | PreVoteReply::RefusedLiveLeader => {
                self.last_round_refused.fetch_add(1, Ordering::Relaxed)
            }
            PreVoteReply::Stale | PreVoteReply::Timeout | PreVoteReply::NetworkError => 0,
        };
    }

    pub(crate) fn served(&self, verdict: PreVoteVerdict) {
        self.served[verdict as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Read every counter; `now_ms` is milliseconds after the clock base.
    pub(crate) fn snapshot(&self, now_ms: u64) -> PreVoteStatsSnapshot {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let reply = |r: PreVoteReply| load(&self.replies[r as usize]);
        let served = |v: PreVoteVerdict| load(&self.served[v as usize]);
        let started = load(&self.last_round_started_ms);
        PreVoteStatsSnapshot {
            rounds_started: load(&self.rounds_started),
            elections_started: load(&self.elections_started),
            last_round: (started != NONE).then(|| PreVoteRound {
                term: load(&self.last_round_term),
                millis_since_start: now_ms.saturating_sub(started),
                granted: load(&self.last_round_granted),
                refused: load(&self.last_round_refused),
            }),
            replies_granted: reply(PreVoteReply::Granted),
            replies_refused_by_vote: reply(PreVoteReply::RefusedByVote),
            replies_refused_by_log: reply(PreVoteReply::RefusedByLog),
            replies_refused_live_leader: reply(PreVoteReply::RefusedLiveLeader),
            replies_stale: reply(PreVoteReply::Stale),
            replies_timeout: reply(PreVoteReply::Timeout),
            replies_network_error: reply(PreVoteReply::NetworkError),
            served_granted: served(PreVoteVerdict::Granted),
            served_refused_leader_lease: served(PreVoteVerdict::LeaderLease),
            served_refused_leader_quorum_acked: served(PreVoteVerdict::LeaderQuorumAcked),
            served_refused_shorter_log: served(PreVoteVerdict::ShorterLog),
            served_refused_lower_vote: served(PreVoteVerdict::LowerVote),
        }
    }
}

/// This node's most recent Pre-Vote round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteRound {
    /// The term the round asked for: this node's term + 1.
    pub term: u64,
    pub millis_since_start: u64,
    /// Peers that granted so far (this node's own grant not counted).
    pub granted: u64,
    /// Peers that refused so far.
    pub refused: u64,
}

/// Pre-Vote counters since the raft started (fuse fork).
///
/// `replies_*`: the answers to the Pre-Votes this node sent. A refusal's cause is read off the
/// reply (its vote, then its log), so it is inferred; `served_*` are the exact grounds on which
/// this node answered others.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreVoteStatsSnapshot {
    pub rounds_started: u64,
    /// Rounds a quorum would grant, which started a real election.
    pub elections_started: u64,
    pub last_round: Option<PreVoteRound>,
    pub replies_granted: u64,
    pub replies_refused_by_vote: u64,
    pub replies_refused_by_log: u64,
    pub replies_refused_live_leader: u64,
    /// Answers to an earlier round, or after the round ended: ignored.
    pub replies_stale: u64,
    pub replies_timeout: u64,
    pub replies_network_error: u64,
    pub served_granted: u64,
    pub served_refused_leader_lease: u64,
    pub served_refused_leader_quorum_acked: u64,
    pub served_refused_shorter_log: u64,
    pub served_refused_lower_vote: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_round_resets_its_own_tallies_and_keeps_the_totals() {
        let s = PreVoteStats::default();
        assert_eq!(None, s.snapshot(0).last_round);

        s.round_started(3, 100);
        s.reply(PreVoteReply::Granted);
        s.reply(PreVoteReply::RefusedLiveLeader);
        s.reply(PreVoteReply::Timeout);
        s.round_started(3, 400);
        s.reply(PreVoteReply::RefusedByLog);
        s.reply(PreVoteReply::Stale);
        s.served(PreVoteVerdict::LeaderLease);

        let snap = s.snapshot(1000);
        assert_eq!(
            Some(PreVoteRound {
                term: 3,
                millis_since_start: 600,
                granted: 0,
                refused: 1
            }),
            snap.last_round
        );
        assert_eq!(2, snap.rounds_started);
        assert_eq!(1, snap.replies_granted);
        assert_eq!(1, snap.replies_refused_live_leader);
        assert_eq!(1, snap.replies_refused_by_log);
        assert_eq!(1, snap.replies_timeout);
        assert_eq!(1, snap.replies_stale);
        assert_eq!(1, snap.served_refused_leader_lease);
        assert_eq!(0, snap.served_granted);
    }
}
