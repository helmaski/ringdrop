mod common;

use ringdrop::daemon::client::DaemonClient;
use ringdrop::daemon::protocol::{EventKind, Op};

#[tokio::test]
async fn node_id_returns_a_non_empty_string() {
    let daemon = common::TestDaemon::start().await;
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::NodeId, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert_eq!(lines.len(), 1, "expected exactly one line for NodeId");
    assert!(!lines[0].is_empty(), "node ID must not be empty");
    daemon.shutdown().await;
}

#[tokio::test]
async fn ring_new_creates_ring_visible_in_list() {
    let daemon = common::TestDaemon::start().await;
    daemon
        .client
        .run(Op::RingNew {
            name: "friends".into(),
        })
        .await
        .unwrap();
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::RingList, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert!(
        lines.iter().any(|l| l.contains("friends")),
        "ring list should include the newly created ring; got: {lines:?}"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn blob_list_on_empty_store_prints_expected_message() {
    let daemon = common::TestDaemon::start().await;
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::BlobList, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert_eq!(lines, vec!["No blobs in local store."]);
    daemon.shutdown().await;
}

#[tokio::test]
async fn ring_add_self_is_rejected_via_daemon() {
    let daemon = common::TestDaemon::start().await;

    let mut node_id = String::new();
    daemon
        .client
        .send(Op::NodeId, |event| {
            if let EventKind::Line { text } = event.kind {
                node_id = text;
            }
        })
        .await
        .unwrap();

    daemon
        .client
        .run(Op::RingNew {
            name: "test".into(),
        })
        .await
        .unwrap();

    let err = daemon
        .client
        .run(Op::RingAdd {
            ring: "test".into(),
            peer: node_id,
            nickname: None,
        })
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("yourself"),
        "expected 'yourself' in error message; got: {err}"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn shutdown_stops_the_server() {
    let common::TestDaemon {
        port,
        client,
        handle,
        ..
    } = common::TestDaemon::start().await;
    client.run(Op::Shutdown).await.unwrap();
    handle.await.unwrap();
    assert!(
        !DaemonClient::new(port).is_running().await,
        "server should no longer be reachable after shutdown"
    );
}
