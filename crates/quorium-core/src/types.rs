use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use openraft::impls::{BasicNode, BoxedErrorSource, Entry, EntryPayload, InlineBatch, ProgressResponder, Vote};
use openraft::vote::RaftLeaderId;
use openraft::{AsyncRuntime, OptionalSend, RaftTypeConfig};

use crate::kv::{Command, Reply};

pub type NodeId = u64;
pub type LeaderId = openraft::impls::leader_id_std::LeaderId<u64, NodeId>;
pub type CommittedLeaderId = <LeaderId as RaftLeaderId>::Committed;
pub type LogId = openraft::LogId<CommittedLeaderId>;
pub type KvVote = Vote<LeaderId>;
pub type KvEntry = Entry<CommittedLeaderId, EntryPayload<Command, NodeId, BasicNode>>;

pub struct Types<RT>(PhantomData<fn() -> RT>);

impl<RT> fmt::Debug for Types<RT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Types")
    }
}

impl<RT> Clone for Types<RT> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<RT> Copy for Types<RT> {}

impl<RT> Default for Types<RT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<RT> PartialEq for Types<RT> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<RT> Eq for Types<RT> {}

impl<RT> PartialOrd for Types<RT> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<RT> Ord for Types<RT> {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<RT: AsyncRuntime> RaftTypeConfig for Types<RT> {
    type D = Command;
    type R = Reply;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Term = u64;
    type LeaderId = LeaderId;
    type Vote = Vote<LeaderId>;
    type Payload = EntryPayload<Command, NodeId, BasicNode>;
    type Entry = Entry<CommittedLeaderId, Self::Payload>;
    type AsyncRuntime = RT;
    type Responder<T: OptionalSend + 'static> = ProgressResponder<Self, T>;
    type Batch<T: OptionalSend + 'static> = InlineBatch<T>;
    type ErrorSource = BoxedErrorSource;
}
