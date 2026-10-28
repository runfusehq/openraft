use std::future::IntoFuture;

use openraft_macros::since;

use crate::RaftTypeConfig;
use crate::base::BoxFuture;
use crate::batch::Batch;
use crate::core::raft_msg::RaftMsg;
use crate::entry::RaftPayload;
use crate::errors::Fatal;
#[cfg(feature = "runtime-stats")]
use crate::raft::api::app::propose_at_now;
use crate::raft::raft_inner::RaftInner;
use crate::raft::responder::core_responder::CoreResponder;
use crate::type_config::alias::CommittedLeaderIdOf;
use crate::type_config::alias::PayloadOf;
use crate::type_config::alias::WriteResponderOf;

/// Builder for a group of client writes submitted to Raft as one message (fuse fork).
///
/// Returned by [`Raft::write_batch()`]. Each [`push`](Self::push) adds one entry, with its own
/// responder and, as [`WriteRequest::at_index`], the log index it must be appended at. Awaiting
/// the batch sends every entry to the core in one `ClientWrite`, so the leader assigns their log
/// ids, and appends them to its log, in one write, however the core's message queue is drained.
/// Each entry is checked and answered on its own: one refused at its index takes no index, and
/// the next is checked against the index it would really get.
///
/// An empty batch sends nothing.
///
/// # Examples
///
/// ```ignore
/// let mut batch = raft.write_batch();
/// for (i, data) in group {
///     batch.push(data, Some(responder_for(i)), Some(i));
/// }
/// batch.await?;
/// ```
///
/// [`Raft::write_batch()`]: crate::Raft::write_batch
/// [`WriteRequest::at_index`]: crate::raft::WriteRequest::at_index
#[since(version = "0.10.0")]
pub struct WriteBatch<'a, C>
where C: RaftTypeConfig
{
    pub(in crate::raft) inner: &'a RaftInner<C>,
    pub(in crate::raft) payloads: Vec<PayloadOf<C>>,
    pub(in crate::raft) responders: Vec<Option<CoreResponder<C>>>,
    pub(in crate::raft) expected_leader: Option<CommittedLeaderIdOf<C>>,
    pub(in crate::raft) expected_indexes: Vec<Option<u64>>,
}

impl<'a, C> WriteBatch<'a, C>
where C: RaftTypeConfig
{
    /// Add an entry: its data, the responder its result goes to, and the log index it must be
    /// appended at (see [`WriteRequest::at_index`]).
    ///
    /// [`WriteRequest::at_index`]: crate::raft::WriteRequest::at_index
    #[since(version = "0.10.0")]
    pub fn push(&mut self, app_data: C::D, responder: Option<WriteResponderOf<C>>, at_index: Option<u64>) -> &mut Self {
        self.payloads.push(C::Payload::normal(app_data));
        self.responders.push(responder.map(CoreResponder::UserDefined));
        self.expected_indexes.push(at_index);
        self
    }

    /// Execute every write of the batch only if the current leader matches the expected leader,
    /// as [`WriteRequest::with_leader`].
    ///
    /// [`WriteRequest::with_leader`]: crate::raft::WriteRequest::with_leader
    #[since(version = "0.10.0")]
    pub fn with_leader(mut self, expected_leader: impl Into<CommittedLeaderIdOf<C>>) -> Self {
        self.expected_leader = Some(expected_leader.into());
        self
    }

    /// The number of entries pushed.
    #[since(version = "0.10.0")]
    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    /// Whether no entry was pushed.
    #[since(version = "0.10.0")]
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}

impl<'a, C> IntoFuture for WriteBatch<'a, C>
where C: RaftTypeConfig
{
    type Output = Result<(), Fatal<C>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if self.payloads.is_empty() {
                return Ok(());
            }
            self.inner
                .send_msg(RaftMsg::ClientWrite {
                    payloads: Batch::of(self.payloads),
                    responders: Batch::of(self.responders),
                    expected_leader: self.expected_leader,
                    expected_indexes: Batch::of(self.expected_indexes),
                    #[cfg(feature = "runtime-stats")]
                    proposed_at: propose_at_now::<C>(),
                })
                .await
        })
    }
}
