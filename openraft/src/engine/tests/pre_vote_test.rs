//! Pre-Vote (fuse fork; backport of upstream openraft `0f6bb9a3`): a round that asks whether a
//! quorum *would* grant a vote changes no state until a quorum would; only then does the real
//! election raise the term.

use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::progress::Progress;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m123() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1,2,3}], None)
}

fn m1() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {1}], None)
}

/// Node 1, a follower of node 2's term-2 leadership whose lease has expired, with log up to
/// `log_id(2, 2, 5)`.
fn follower(m: Membership<u64, ()>) -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_millis(300), Vote::new_committed(2, 2));
    eng.state.server_state = ServerState::Follower;
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 2, 5)]);
    eng.state.membership_state.set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 1, 1)), m)));
    eng.output.take_commands();
    eng
}

#[test]
fn test_pre_elect_changes_nothing_and_sends_a_next_term_pre_vote() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    let utime = eng.state.vote_last_modified();

    eng.pre_elect();

    assert_eq!(Vote::new_committed(2, 2), *eng.state.vote_ref(), "the term is not raised");
    assert_eq!(utime, eng.state.vote_last_modified(), "the lease clock is not touched");
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert!(eng.candidate_ref().is_none());
    assert_eq!(&Vote::new(3, 1), eng.pre_candidate_ref().unwrap().vote_ref());
    assert_eq!(
        vec![Command::SendPreVote {
            vote_req: VoteRequest::new(Vote::new(3, 1), Some(log_id(2, 2, 5))),
        }],
        eng.output.take_commands(),
        "no SaveVote: a Pre-Vote persists nothing"
    );
    Ok(())
}

#[test]
fn test_pre_elect_single_voter_elects_at_once() -> anyhow::Result<()> {
    let mut eng = follower(m1());

    eng.pre_elect();

    assert_eq!(Vote::new(3, 1), *eng.state.vote_ref());
    assert!(eng.pre_candidate_ref().is_none(), "the real election replaced the round");
    assert!(eng.candidate_ref().is_some());
    assert!(eng.output.take_commands().iter().any(|c| matches!(c, Command::SaveVote { .. })));
    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejected_by_a_live_leader_lease() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.state.vote.update(TokioInstant::now(), Vote::new_committed(2, 2));
    let utime = eng.state.vote_last_modified();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(3, 3), Some(log_id(2, 2, 9))));

    assert_eq!(VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 5)), false), resp);
    assert_eq!(Vote::new_committed(2, 2), *eng.state.vote_ref());
    assert_eq!(utime, eng.state.vote_last_modified());
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejected_by_a_shorter_log() -> anyhow::Result<()> {
    let mut eng = follower(m123());

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(3, 3), Some(log_id(2, 2, 4))));

    assert_eq!(VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 5)), false), resp);
    assert_eq!(Vote::new_committed(2, 2), *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

#[test]
fn test_handle_pre_vote_req_granted_without_changing_state() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    let utime = eng.state.vote_last_modified();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(3, 3), Some(log_id(2, 2, 5))));

    assert_eq!(VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 5)), true), resp);
    assert_eq!(Vote::new_committed(2, 2), *eng.state.vote_ref(), "a granted Pre-Vote adopts nothing");
    assert_eq!(utime, eng.state.vote_last_modified());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len(), "no SaveVote");
    Ok(())
}

#[test]
fn test_handle_pre_vote_req_rejects_a_vote_lower_than_its_own() -> anyhow::Result<()> {
    let mut eng = follower(m123());

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(1, 3), Some(log_id(2, 2, 5))));

    assert!(!resp.vote_granted);
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

#[test]
fn test_a_pre_vote_quorum_starts_the_real_election() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 5)), true));

    assert_eq!(Vote::new(3, 1), *eng.state.vote_ref(), "the term is raised only now");
    assert!(eng.pre_candidate_ref().is_none());
    assert_eq!(&Vote::new(3, 1), eng.candidate_ref().unwrap().vote_ref());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    let commands = eng.output.take_commands();
    assert!(commands.iter().any(|c| matches!(c, Command::SaveVote { vote } if *vote == Vote::new(3, 1))));
    assert!(commands.iter().any(|c| matches!(c, Command::SendVote { .. })));
    Ok(())
}

#[test]
fn test_a_pre_vote_refused_at_the_same_vote_changes_nothing_and_notes_a_greater_log() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 9)), false));

    assert_eq!(Vote::new_committed(2, 2), *eng.state.vote_ref());
    assert!(eng.is_there_greater_log());
    assert!(eng.pre_candidate_ref().is_some(), "the round stays open; the core restarts it later");
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

/// Node 1 lags at term 2 with the longer log; node 3 was forced to campaign at term 3 and lost,
/// so it sits at an uncommitted (3,3) with a shorter log. Node 3 refuses node 1's Pre-Vote at
/// (3,1) by vote, and node 1 refuses node 3's at (4,3) by log. Unless node 1 learns term 3 from
/// the refusal, its next round asks at (3,1) again and the two refuse each other for good.
#[test]
fn test_a_pre_vote_refused_by_a_greater_vote_adopts_its_uncommitted_form() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(3, VoteResponse::new(Vote::new(3, 3), Some(log_id(2, 2, 4)), false));

    assert_eq!(Vote::new(3, 3), *eng.state.vote_ref(), "the responder's term is learned");
    assert!(!eng.state.vote_ref().is_committed());
    assert!(eng.pre_candidate_ref().is_none(), "the round at (3,1) is over");
    assert!(!eng.is_there_greater_log());
    assert!(eng.output.take_commands().iter().any(|c| matches!(c, Command::SaveVote { vote } if *vote == Vote::new(3, 3))));

    // The next round asks at term 4, which node 3 grants: node 1's log is longer.
    eng.pre_elect();
    assert_eq!(&Vote::new(4, 1), eng.pre_candidate_ref().unwrap().vote_ref());
    Ok(())
}

/// A refusal carrying a committed greater vote (a leader at a later term) is adopted only in its
/// non-committed form, as a refused real vote is: a refusal is not proof that leader holds a
/// quorum for this node to follow.
#[test]
fn test_a_pre_vote_refused_by_a_committed_greater_vote_adopts_it_uncommitted() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new_committed(4, 2), Some(log_id(4, 2, 9)), false));

    assert_eq!(Vote::new(4, 2), *eng.state.vote_ref());
    assert!(eng.is_there_greater_log());
    assert!(eng.pre_candidate_ref().is_none());
    Ok(())
}

#[test]
fn test_a_round_in_flight_is_dropped_by_following_a_leader() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.vote_handler().update_vote(&Vote::new_committed(3, 3))?;

    assert!(eng.pre_candidate_ref().is_none());
    // A late grant from the dropped round starts nothing.
    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new_committed(3, 3), Some(log_id(2, 2, 5)), true));
    assert_eq!(Vote::new_committed(3, 3), *eng.state.vote_ref());
    assert!(eng.candidate_ref().is_none());
    Ok(())
}

/// The fork re-derives a follower's state on every tick (`Engine::leader_step_down` ->
/// `update_internal_server_state` -> `become_following`). That must not cancel a Pre-Vote round
/// in flight, or its replies arrive stale every time and no follower is ever elected.
#[test]
fn test_a_round_in_flight_survives_the_per_tick_following_refresh() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.pre_elect();
    eng.output.take_commands();

    eng.vote_handler().update_internal_server_state();
    eng.leader_step_down();

    assert_eq!(&Vote::new(3, 1), eng.pre_candidate_ref().unwrap().vote_ref());
    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new_committed(2, 2), Some(log_id(2, 2, 5)), true));
    assert_eq!(Vote::new(3, 1), *eng.state.vote_ref(), "the round's quorum still elects");
    Ok(())
}

/// A leader that a quorum acked within the lease refuses a Pre-Vote: its own vote time is not
/// renewed by its followers' acks, so the vote-lease rule alone would let it grant, and a member
/// that rejoins after a partition could unseat it.
#[test]
fn test_a_leader_with_recent_quorum_acks_refuses_a_pre_vote() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_millis(300), Vote::new_committed(3, 1));
    eng.testing_new_leader().clock_progress.increase_to(&2, Some(TokioInstant::now())).ok();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(4, 3), Some(log_id(2, 2, 5))));

    assert!(!resp.vote_granted, "a leader a quorum keeps acking does not grant");
    assert_eq!(Vote::new_committed(3, 1), *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());
    Ok(())
}

/// The contrast: a leader no quorum has acked within the lease grants like any node, so a cluster
/// whose leader lost its quorum still elects.
#[test]
fn test_a_leader_without_recent_quorum_acks_grants_a_pre_vote() -> anyhow::Result<()> {
    let mut eng = follower(m123());
    eng.state.vote = UTime::new(TokioInstant::now() - Duration::from_millis(300), Vote::new_committed(3, 1));
    eng.testing_new_leader();

    let resp = eng.handle_pre_vote_req(VoteRequest::new(Vote::new(4, 3), Some(log_id(2, 2, 5))));

    assert!(resp.vote_granted);
    Ok(())
}
