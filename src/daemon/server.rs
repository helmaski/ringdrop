use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tracing::{error, info};
use uuid::Uuid;

use crate::core::{Node, ShareTicket};
use crate::daemon::protocol::{Event, Op, Request};
use crate::util::{parse_hash, parse_peer_id};
use iroh_rings::{RedbRegistry, Registry, OPEN_RING_NAME};

pub struct DaemonServer {
    node: Arc<Node<RedbRegistry>>,
    listener: TcpListener,
    shutdown: Arc<Notify>,
}

impl DaemonServer {
    pub async fn bind(node: Node<RedbRegistry>, port: u16) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| anyhow::anyhow!("cannot bind to port {port}: {e}"))?;
        info!(port, "daemon listening");
        Ok(Self {
            node: Arc::new(node),
            listener,
            shutdown: Arc::new(Notify::new()),
        })
    }

    pub fn local_port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

    pub async fn run(self) -> Result<()> {
        let mut tasks: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    let (stream, addr) = result?;
                    info!(%addr, "connection accepted");
                    let node = Arc::clone(&self.node);
                    let shutdown = Arc::clone(&self.shutdown);
                    tasks.spawn(async move {
                        if let Err(e) = handle_connection(stream, node, shutdown).await {
                            error!("connection error: {e:#}");
                        }
                    });
                }
                _ = self.shutdown.notified() => {
                    info!("shutdown requested, draining in-flight requests");
                    break;
                }
            }
        }

        // Give in-flight requests up to 30s to finish cleanly, then abort.
        let drain = async { while tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(Duration::from_secs(30), drain)
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }

        Arc::try_unwrap(self.node)
            .unwrap_or_else(|_| panic!("all connection tasks completed"))
            .shutdown()
            .await
    }
}

async fn handle_connection(
    stream: TcpStream,
    node: Arc<Node<RedbRegistry>>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }

    let req: Request = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(e) => {
            emit(&mut writer, &Event::error(Uuid::new_v4(), e.to_string())).await;
            return Ok(());
        }
    };

    let req_id = req.req_id;
    let (tx, mut rx) = mpsc::channel::<Event>(32);

    tokio::spawn(dispatch(req.op, req_id, node, tx, shutdown));

    while let Some(event) = rx.recv().await {
        emit(&mut writer, &event).await;
    }

    Ok(())
}

async fn emit(writer: &mut (impl AsyncWriteExt + Unpin), event: &Event) {
    if let Ok(json) = serde_json::to_string(event) {
        let _ = writer.write_all(format!("{json}\n").as_bytes()).await;
    }
}

async fn send(tx: &mpsc::Sender<Event>, event: Event) {
    let _ = tx.send(event).await;
}

async fn dispatch(
    op: Op,
    req_id: Uuid,
    node: Arc<Node<RedbRegistry>>,
    tx: mpsc::Sender<Event>,
    shutdown: Arc<Notify>,
) {
    if let Op::Shutdown = op {
        send(&tx, Event::done(req_id)).await;
        shutdown.notify_one();
        return;
    }

    match handle_op(op, req_id, &node, &tx).await {
        Ok(()) => {}
        Err(e) => send(&tx, Event::error(req_id, e.to_string())).await,
    }
}

async fn handle_op(
    op: Op,
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
) -> Result<()> {
    match op {
        Op::NodeId => {
            send(tx, Event::line(req_id, node.endpoint.id().to_string())).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::Import { path, rings, open } => {
            handle_import(req_id, node, tx, path, rings, open).await?;
        }
        Op::BlobList => {
            handle_blob_list(req_id, node, tx).await?;
        }
        Op::BlobRemove { target } => {
            handle_blob_remove(req_id, node, tx, target).await?;
        }
        Op::Tag {
            target,
            rings,
            open,
        } => {
            handle_tag(req_id, node, tx, target, rings, open).await?;
        }
        Op::Tags { target } => {
            handle_tags(req_id, node, tx, target).await?;
        }
        Op::RingNew { name } => {
            let lines = ring_new_lines(&node.registry, &name)?;
            send_lines(tx, req_id, &lines).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::RingList => {
            let lines = ring_list_lines(&node.registry)?;
            send_lines(tx, req_id, &lines).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::RingAdd {
            ring,
            peer,
            nickname,
        } => {
            let lines = ring_add_lines(
                &node.registry,
                node.endpoint.id(),
                &ring,
                &peer,
                nickname.as_deref(),
            )?;
            send_lines(tx, req_id, &lines).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::RingRemove { ring, peer } => {
            let lines = ring_remove_lines(&node.registry, &ring, &peer)?;
            send_lines(tx, req_id, &lines).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::RingMembers { ring } => {
            let lines = ring_members_lines(&node.registry, &ring)?;
            send_lines(tx, req_id, &lines).await;
            send(tx, Event::done(req_id)).await;
        }
        Op::Receive {
            ticket,
            dest,
            force_overwrite,
        } => {
            handle_receive(req_id, node, tx, ticket, dest, force_overwrite).await?;
        }
        Op::Shutdown => unreachable!("handled before handle_op"),
    }
    Ok(())
}

async fn send_lines(tx: &mpsc::Sender<Event>, req_id: Uuid, lines: &[String]) {
    for line in lines {
        send(tx, Event::line(req_id, line.clone())).await;
    }
}

// ── blob handlers ─────────────────────────────────────────────────────────────

async fn handle_import(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    path: PathBuf,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    let (hash, format) = node.import_path(&path).await?;

    let effective_rings: Vec<String> = if open {
        vec![OPEN_RING_NAME.to_owned()]
    } else {
        rings
    };

    if effective_rings.is_empty() {
        let existing = node.registry.list_resource_rings(*hash.as_bytes())?;
        if existing.is_empty() {
            send(
                tx,
                Event::line(
                    req_id,
                    "Warning: not tagged — this blob won't be served to any peer.",
                ),
            )
            .await;
            send(tx, Event::line(req_id, "Tag it with:")).await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop tag {hash} --ring <ring-name>")),
            )
            .await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop tag {hash} --open")),
            )
            .await;
        } else {
            send(tx, Event::line(req_id, "Already tagged:")).await;
            for r in &existing {
                if r.is_open() {
                    send(
                        tx,
                        Event::line(
                            req_id,
                            format!("  {} (open — publicly accessible)", r.as_str()),
                        ),
                    )
                    .await;
                } else {
                    send(tx, Event::line(req_id, format!("  {}", r.as_str()))).await;
                }
            }
        }
    } else {
        for ring in &effective_rings {
            node.registry.add_ring_to_resource(*hash.as_bytes(), ring)?;
            if ring == OPEN_RING_NAME {
                send(
                    tx,
                    Event::line(req_id, "Tagged as open (publicly accessible)"),
                )
                .await;
            } else {
                send(
                    tx,
                    Event::line(req_id, format!("Tagged with ring '{ring}'")),
                )
                .await;
            }
        }
    }

    let display_name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let ticket = node.make_ticket(hash, format, display_name);
    let ticket_str = ticket.to_uri()?;

    send(tx, Event::line(req_id, "\nTicket:")).await;
    send(tx, Event::line(req_id, format!("  {ticket_str}\n"))).await;
    send(tx, Event::line(req_id, "Peers receive with:")).await;
    send(
        tx,
        Event::line(req_id, format!("  rdrop receive {ticket_str}")),
    )
    .await;
    send(tx, Event::done(req_id)).await;
    Ok(())
}

async fn handle_blob_list(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
) -> Result<()> {
    let blobs = node.list_blobs().await?;
    if blobs.is_empty() {
        send(tx, Event::line(req_id, "No blobs in local store.")).await;
    } else {
        send(tx, Event::line(req_id, format!("{} blobs:", blobs.len()))).await;
        for (hash, format, name) in blobs {
            let rings = node.registry.list_resource_rings(*hash.as_bytes())?;
            let ticket = node.make_ticket(hash, format, Some(name.clone()));
            let ticket_str = ticket.to_uri()?;
            send(tx, Event::line(req_id, format!("\n  {hash}  ({name})"))).await;
            if rings.is_empty() {
                send(
                    tx,
                    Event::line(req_id, "    no rings:  (inaccessible for all peers)"),
                )
                .await;
            } else {
                let names: Vec<_> = rings.iter().map(|r| r.as_str().to_owned()).collect();
                send(
                    tx,
                    Event::line(req_id, format!("    rings:  {}", names.join(", "))),
                )
                .await;
            }
            send(tx, Event::line(req_id, format!("    ticket: {ticket_str}"))).await;
        }
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

async fn handle_blob_remove(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    target: String,
) -> Result<()> {
    let path = PathBuf::from(&target);
    let hash = if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        hash
    } else {
        parse_hash(&target)?
    };
    node.registry.remove_ring_from_resource(*hash.as_bytes())?;
    node.delete_blob(hash).await?;
    send(tx, Event::line(req_id, format!("Removed {hash}"))).await;
    send(
        tx,
        Event::line(req_id, "Disk space will be reclaimed on the next GC cycle."),
    )
    .await;
    send(tx, Event::done(req_id)).await;
    Ok(())
}

// ── tag handlers ──────────────────────────────────────────────────────────────

async fn handle_tag(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    target: String,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    let path = PathBuf::from(&target);
    let hash = if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        hash
    } else {
        parse_hash(&target)?
    };
    for ring in &rings {
        node.registry.add_ring_to_resource(*hash.as_bytes(), ring)?;
        send(
            tx,
            Event::line(req_id, format!("Tagged {hash} with ring '{ring}'")),
        )
        .await;
    }
    if open {
        node.registry
            .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME)?;
        send(
            tx,
            Event::line(
                req_id,
                format!("Tagged {hash} as open (publicly accessible)"),
            ),
        )
        .await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

async fn handle_tags(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    target: String,
) -> Result<()> {
    let path = PathBuf::from(&target);
    let hash = if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        hash
    } else {
        parse_hash(&target)?
    };
    let rings = node.registry.list_resource_rings(*hash.as_bytes())?;
    if rings.is_empty() {
        send(
            tx,
            Event::line(
                req_id,
                format!("{hash}: no rings (access denied to all peers)"),
            ),
        )
        .await;
    } else {
        send(
            tx,
            Event::line(req_id, format!("{}: {} rings:", hash, rings.len())),
        )
        .await;
        for ring in &rings {
            if ring.is_open() {
                send(
                    tx,
                    Event::line(
                        req_id,
                        format!("  {}  (open — publicly accessible)", ring.as_str()),
                    ),
                )
                .await;
            } else {
                send(tx, Event::line(req_id, format!("  {}", ring.as_str()))).await;
            }
        }
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

// ── ring helpers (pure, unit-tested) ─────────────────────────────────────────

fn ring_new_lines(registry: &impl Registry, name: &str) -> Result<Vec<String>> {
    registry.create_ring(name)?;
    Ok(vec![
        format!("Ring created: {name}"),
        format!("Add peers: rdrop ring add {name} <peer-id>"),
    ])
}

fn ring_list_lines(registry: &impl Registry) -> Result<Vec<String>> {
    let rings = registry.list_rings()?;
    let mut out = vec![format!("{} rings:", rings.len())];
    for r in rings {
        if r.is_open() {
            out.push(format!(
                "  {}  — publicly accessible (no membership required)",
                r.as_str()
            ));
        } else {
            let members = registry.list_ring_peers(r.as_str())?;
            out.push(format!("  {}  ({} members)", r.as_str(), members.len()));
        }
    }
    Ok(out)
}

fn ring_add_lines(
    registry: &impl Registry,
    public_id: iroh::EndpointId,
    ring: &str,
    peer: &str,
    nickname: Option<&str>,
) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring has no membership list — everyone is welcome by default.".to_owned(),
        ]);
    }
    let peer_id = parse_peer_id(peer)?;
    if peer_id == public_id {
        anyhow::bail!("cannot add yourself to a ring");
    }
    registry.add_peer_to_ring(ring, peer_id, nickname)?;
    let line = match nickname {
        Some(nick) => format!("Added {peer_id} ({nick}) to ring {ring}"),
        None => format!("Added {peer_id} to ring {ring}"),
    };
    Ok(vec![line])
}

fn ring_remove_lines(registry: &impl Registry, ring: &str, peer: &str) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring has no membership list to remove from.".to_owned()
        ]);
    }
    let peer_id = parse_peer_id(peer)?;
    registry.remove_peer_from_ring(ring, peer_id)?;
    Ok(vec![format!("Removed {peer_id} from ring {ring}")])
}

fn ring_members_lines(registry: &impl Registry, ring: &str) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring is public — any peer may access blobs tagged with it.".to_owned(),
        ]);
    }
    let members = registry.list_ring_peers(ring)?;
    if members.is_empty() {
        return Ok(vec![
            format!("Ring '{ring}' has no members yet."),
            format!("Add peers: rdrop ring add {ring} <peer-id>"),
            "Peers print their peer-id with: rdrop id".to_owned(),
        ]);
    }
    let mut out = vec![format!("Ring '{ring}' — {} members:", members.len())];
    for (peer, nick) in members {
        match nick {
            Some(n) => out.push(format!("  {peer}  ({n})")),
            None => out.push(format!("  {peer}")),
        }
    }
    Ok(out)
}

// ── receive handler ───────────────────────────────────────────────────────────

async fn handle_receive(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    ticket_str: String,
    dest: PathBuf,
    force_overwrite: bool,
) -> Result<()> {
    let ticket = ShareTicket::from_uri(&ticket_str)?;
    let hash_hex = ticket.hash().to_string();

    let dest_path = if dest.is_dir() {
        dest.join(ticket.name.as_deref().unwrap_or(hash_hex.as_str()))
    } else {
        dest.clone()
    };
    if dest_path.exists() && !force_overwrite {
        anyhow::bail!(
            "destination '{}' already exists; \
             use --dest to choose a different location or --force-overwrite to replace it",
            dest_path.display()
        );
    }

    send(
        tx,
        Event::line(
            req_id,
            format!(
                "Fetching {} from {}{}",
                ticket.hash(),
                ticket.peer_id(),
                ticket
                    .name
                    .as_deref()
                    .map(|n| format!(" ({n})"))
                    .unwrap_or_default()
            ),
        ),
    )
    .await;
    send(
        tx,
        Event::line(req_id, format!("Destination: {}", dest_path.display())),
    )
    .await;
    send(
        tx,
        Event::line(
            req_id,
            "(If interrupted, re-run this command to resume from where it stopped.)",
        ),
    )
    .await;

    // Progress events are emitted by a separate task so they don't block the
    // download future — `on_progress` is `Fn` (not async), so it can't await
    // the channel send directly.
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();
    let on_progress = move |done: u64, total: u64| {
        let _ = progress_tx.send((done, total));
    };

    let event_tx = tx.clone();
    let progress_task = tokio::spawn(async move {
        while let Some((done, total)) = progress_rx.recv().await {
            let _ = event_tx.send(Event::progress(req_id, done, total)).await;
        }
    });

    let result = node
        .download_with_progress(&ticket, &dest_path, on_progress)
        .await;

    // on_progress has been dropped (download finished), so progress_tx is gone
    // and progress_rx will return None — awaiting the task is instant.
    let _ = progress_task.await;

    match result {
        Ok(()) => {
            send(tx, Event::line(req_id, "Transfer complete.")).await;
            send(tx, Event::done(req_id)).await;
            Ok(())
        }
        Err(e) => {
            let mut msg = format!("Transfer failed: {e:#}");
            if e.to_string().contains("access denied") {
                let public_id = node.endpoint.id();
                msg.push_str(&format!(
                    "\n\nYour peer-id: {public_id}\n\
                     Ask the file owner to run:\n  rdrop ring add <ring-name> {public_id}"
                ));
            }
            anyhow::bail!(msg)
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_rings::RedbRegistry;
    use tempfile::TempDir;

    fn setup(dir: &TempDir) -> (RedbRegistry, iroh::EndpointId) {
        let cfg = crate::config::Config::load_or_create(dir.path()).unwrap();
        let public_id = cfg.public_id();
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).unwrap();
        (registry, public_id)
    }

    #[test]
    fn ring_add_self_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (registry, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();

        let err = ring_add_lines(
            &registry,
            public_id,
            "friends",
            &public_id.to_string(),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("yourself"));
    }

    #[test]
    fn ring_add_to_open_ring_does_not_add_member() {
        let dir = TempDir::new().unwrap();
        let (registry, public_id) = setup(&dir);
        let peer = iroh::SecretKey::generate().public();

        ring_add_lines(
            &registry,
            public_id,
            OPEN_RING_NAME,
            &peer.to_string(),
            None,
        )
        .unwrap();

        assert_eq!(registry.list_ring_peers(OPEN_RING_NAME).unwrap().len(), 0);
    }
}
