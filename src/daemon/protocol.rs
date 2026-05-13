use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A request sent from a CLI client (or GUI) to the daemon over TCP.
///
/// Each connection carries exactly one `Request` (newline-terminated JSON),
/// followed by a stream of [`Event`]s from the daemon until [`Event::Done`]
/// or [`Event::Error`].
///
/// `req_id` is echoed back on every response event, allowing a persistent
/// connection to multiplex concurrent requests (e.g. a GUI importing several
/// files simultaneously).
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_id: Option<Uuid>,
    #[serde(flatten)]
    pub op: Op,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    NodeId,
    Import {
        path: PathBuf,
        rings: Vec<String>,
        open: bool,
    },
    BlobList,
    BlobRemove {
        target: String,
    },
    Tag {
        target: String,
        rings: Vec<String>,
        open: bool,
    },
    Tags {
        target: String,
    },
    RingNew {
        name: String,
    },
    RingList,
    RingAdd {
        ring: String,
        peer: String,
        nickname: Option<String>,
    },
    RingRemove {
        ring: String,
        peer: String,
    },
    RingMembers {
        ring: String,
    },
    Receive {
        ticket: String,
        dest: PathBuf,
        force_overwrite: bool,
    },
    Shutdown,
}

/// An event streamed from the daemon to the client.
///
/// The daemon sends one or more events per request, always ending with
/// [`Event::Done`] or [`Event::Error`]. `req_id` matches the value sent in
/// the originating [`Request`], enabling multiplexed connections.
#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_id: Option<Uuid>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// A line of text that would have been printed to stdout.
    Line { text: String },
    /// Download/upload progress for long-running transfers.
    Progress { done: u64, total: u64 },
    /// The request completed successfully.
    Done,
    /// The request failed; no further events follow.
    Error { message: String },
}

impl Event {
    pub fn line(req_id: Option<Uuid>, text: impl Into<String>) -> Self {
        Self {
            req_id,
            kind: EventKind::Line { text: text.into() },
        }
    }

    pub fn progress(req_id: Option<Uuid>, done: u64, total: u64) -> Self {
        Self {
            req_id,
            kind: EventKind::Progress { done, total },
        }
    }

    pub fn done(req_id: Option<Uuid>) -> Self {
        Self {
            req_id,
            kind: EventKind::Done,
        }
    }

    pub fn error(req_id: Option<Uuid>, message: impl Into<String>) -> Self {
        Self {
            req_id,
            kind: EventKind::Error {
                message: message.into(),
            },
        }
    }
}
