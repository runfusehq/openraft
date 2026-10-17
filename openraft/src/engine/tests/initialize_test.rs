use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::Vote;
use crate::batch::Batch;
use crate::core::ServerState;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::entry::RaftEntry;
use crate::errors::InitializeError;
use crate::errors::NotAllowed;
use crate::errors::NotInMembers;
use crate::raft_state::LogStateReader;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::EntryOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

#[test]
fn test_initialize_single_node() -> anyhow::Result<()> {
    let eng = || {
        let mut eng = Engine::<UTConfig>::testing_default(0);
        eng.state.enable_validation(false); // Disable validation for incomplete state

        eng.state.server_state = eng.calc_server_state();
        eng
    };

    let m1 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1}], []);

    tracing::info!("--- ok: init empty node 1 with membership(1,2)");
    tracing::info!("--- expect OK result, check output commands and state changes");
    {
        let mut eng = eng();
        eng.config.id = 1;

        // The first log id uses the lowest voter's id as leader
        let log_id0 = log_id(0, 1, 0);

        eng.initialize(m1())?;

        assert_eq!(Some(log_id0), eng.state.get_log_id(0));
        assert_eq!(None, eng.state.get_log_id(1));
        assert_eq!(Some(&log_id0), eng.state.last_log_id());

        assert_eq!(ServerState::Leader, eng.state.server_state);
        assert_eq!(&m1(), eng.state.membership_state.effective().membership());

        assert_eq!(
            vec![
                //
                Command::AppendEntries {
                    committed_vote: Vote::new_with_default_term(1).to_committed(),
                    entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m1())]),
                },
            ],
            eng.output.take_commands()
        );
    }
    Ok(())
}

#[test]
fn test_initialize() -> anyhow::Result<()> {
    let eng = || {
        let mut eng = Engine::<UTConfig>::testing_default(0);
        eng.state.enable_validation(false); // Disable validation for incomplete state

        eng.state.server_state = eng.calc_server_state();
        eng
    };

    let m12 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1,2}], []);

    tracing::info!("--- ok: init empty node 1 with membership(1,2)");
    tracing::info!("--- expect OK result, check output commands and state changes");
    {
        let mut eng = eng();
        eng.config.id = 1;

        // The first log id uses the lowest voter's id as leader
        let log_id0 = log_id(0, 1, 0);

        eng.initialize(m12())?;

        assert_eq!(Some(log_id0), eng.state.get_log_id(0));
        assert_eq!(None, eng.state.get_log_id(1));
        assert_eq!(Some(&log_id0), eng.state.last_log_id());

        assert_eq!(ServerState::Leader, eng.state.server_state);
        assert_eq!(&m12(), eng.state.membership_state.effective().membership());

        assert_eq!(
            vec![
                //
                Command::AppendEntries {
                    committed_vote: Vote::new_with_default_term(1).to_committed(),
                    entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m12())]),
                },
            ],
            eng.output.take_commands()
        );
    }

    // For error cases, use a pre-existing log with arbitrary log ID
    let existing_log_id = log_id(0, 0, 0);

    tracing::info!("--- not allowed because of last_log_id");
    {
        let mut eng = eng();
        eng.state.log_ids = LogIdList::new(None, vec![existing_log_id]);

        assert_eq!(
            Err(InitializeError::NotAllowed(NotAllowed {
                last_log_id: Some(existing_log_id),
                vote: Vote::new(0, 0),
            })),
            eng.initialize(m12())
        );
    }

    tracing::info!("--- not allowed because of a committed vote");
    {
        let mut eng = eng();
        eng.config.id = 1;
        eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new_committed(1, 2));

        assert_eq!(
            Err(InitializeError::NotAllowed(NotAllowed {
                last_log_id: None,
                vote: Vote::new_committed(1, 2),
            })),
            eng.initialize(m12())
        );
    }

    tracing::info!("--- not allowed because of last_log_id, even with a granted vote");
    {
        let mut eng = eng();
        eng.config.id = 1;
        eng.state.log_ids = LogIdList::new(None, vec![existing_log_id]);
        eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new(1, 2));

        assert_eq!(
            Err(InitializeError::NotAllowed(NotAllowed {
                last_log_id: Some(existing_log_id),
                vote: Vote::new(1, 2),
            })),
            eng.initialize(m12())
        );
    }

    tracing::info!("--- node id 0 is not in membership");
    {
        let mut eng = eng();

        assert_eq!(
            Err(InitializeError::NotInMembers(NotInMembers {
                node_id: 0,
                membership: m12()
            })),
            eng.initialize(m12())
        );
    }

    Ok(())
}

/// Fuse fork: every member writes the same first log id, `((0, lowest voter), 0)`, not
/// `((0, self), 0)`.
#[test]
fn test_initialize_writes_the_canonical_first_log_id_on_every_member() -> anyhow::Result<()> {
    let m123 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1,2,3}], []);

    for id in [1, 2, 3] {
        let mut eng = Engine::<UTConfig>::testing_default(id);
        eng.state.enable_validation(false); // Disable validation for incomplete state
        eng.state.server_state = eng.calc_server_state();

        eng.initialize(m123())?;

        assert_eq!(Some(&log_id(0, 1, 0)), eng.state.last_log_id(), "node {id}");
        assert_eq!(
            vec![Command::AppendEntries {
                committed_vote: Vote::new_with_default_term(id).to_committed(),
                entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m123())]),
            }],
            eng.output.take_commands(),
            "node {id}"
        );
    }

    Ok(())
}

/// Fuse fork: a member that granted `(T, c)` and holds an empty log initializes. It keeps the
/// vote, appends the canonical first entry, and the election that follows runs at `(T+1, self)`.
/// The vote never decreases, including when `self < c`, where `(T, self)` would regress.
#[test]
fn test_initialize_keeps_a_granted_vote_and_campaigns_above_it() -> anyhow::Result<()> {
    let m123 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1,2,3}], []);

    // (granted vote, initializing node): the candidate below and above the node.
    for (granted, id) in [(Vote::new(1, 1), 2), (Vote::new(1, 3), 2), (Vote::new(5, 3), 1)] {
        let mut eng = Engine::<UTConfig>::testing_default(id);
        eng.state.enable_validation(false); // Disable validation for incomplete state
        eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), granted);
        eng.state.server_state = eng.calc_server_state();

        eng.initialize(m123())?;

        assert_eq!(&granted, eng.state.vote_ref(), "{granted} @{id}: the granted vote is kept");
        assert_eq!(Some(&log_id(0, 1, 0)), eng.state.last_log_id(), "{granted} @{id}");
        assert_eq!(&m123(), eng.state.membership_state.effective().membership());
        assert_eq!(
            vec![Command::AppendEntries {
                committed_vote: granted.to_committed(),
                entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m123())]),
            }],
            eng.output.take_commands(),
            "{granted} @{id}: the append is labeled with the held vote"
        );

        eng.elect();

        let campaign = Vote::new(granted.leader_id.term + 1, id);
        assert_eq!(&campaign, eng.state.vote_ref(), "{granted} @{id}: campaigns at (T+1, self)");
        assert!(campaign > granted, "{granted} @{id}: the vote never decreases");
        assert_eq!(ServerState::Candidate, eng.state.server_state);

        let commands = eng.output.take_commands();
        assert!(
            commands.contains(&Command::SaveVote { vote: campaign }),
            "{granted} @{id}: {commands:?}"
        );
        assert!(
            commands.iter().any(|c| matches!(c, Command::SendVote { vote_req }
                if vote_req.vote == campaign && vote_req.last_log_id == Some(log_id(0, 1, 0)))),
            "{granted} @{id}: {commands:?}"
        );
    }

    Ok(())
}
