use openraft::RaftTypeConfig;
use openraft_rt_tokio::TokioRuntime;
use quorium_core::types::Types;

fn assert_config<C: RaftTypeConfig>() {}

#[test]
fn types_form_a_raft_config_for_any_runtime() {
    assert_config::<Types<TokioRuntime>>();
    assert_eq!(Types::<TokioRuntime>::default(), Types::<TokioRuntime>::default());
}

#[test]
fn types_debug_and_order_are_trivial() {
    let a = Types::<TokioRuntime>::default();
    assert_eq!(format!("{a:?}"), "Types");
    assert_eq!(a.partial_cmp(&a), Some(std::cmp::Ordering::Equal));
    assert_eq!(a.clone(), a);
}
