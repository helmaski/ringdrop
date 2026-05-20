//! Server-layer IPC contract test using [`InMemoryRegistry`].
//!
//! Verifies that `DaemonServer<R>` behaves correctly with a non-redb registry.
//! The full behavioural contract is defined in [`common::daemon_contract`] and
//! shared with the redb-backed variant in `daemon.rs`.
//!
//! [`InMemoryRegistry`]: iroh_rings::InMemoryRegistry

mod common;

#[tokio::test]
async fn daemon_contract_holds_with_in_memory_registry() {
    common::daemon_contract(common::TestDaemon::start_mem().await).await;
}
