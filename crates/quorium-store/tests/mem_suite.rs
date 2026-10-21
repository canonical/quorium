use openraft::testing::log::{StoreBuilder, Suite};
use openraft::{AsyncRuntime, StorageError};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::types::Types;
use quorium_store::mem::{KvStateMachine, MemLog};

type C = Types<TokioRuntime>;

struct Builder;

impl StoreBuilder<C, MemLog<C>, KvStateMachine, ()> for Builder {
    async fn build(&self) -> Result<((), MemLog<C>, KvStateMachine), StorageError<C>> {
        Ok(((), MemLog::default(), KvStateMachine::default()))
    }
}

#[test]
fn passes_openraft_storage_suite() {
    TokioRuntime::run(async {
        Suite::test_all(Builder).await.unwrap();
    });
}
