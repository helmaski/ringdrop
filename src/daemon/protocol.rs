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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_id: Option<Uuid>,
    #[serde(flatten)]
    pub op: Op,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_id: Option<Uuid>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// Line of text to be printed to stdout (by a console process) or rendered (by a GUI).
    Line { text: String },
    /// Download/upload progress indicator for long-running transfers.
    Progress { done: u64, total: u64 },
    /// Signal of request completed successfully; no further events will follow for this req_id.
    Done,
    /// Signal of request failed; no further events will follow for this req_id.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_node_id_serializes_to_snake_case_tag() {
        assert_eq!(
            serde_json::to_string(&Op::NodeId).unwrap(),
            r#"{"op":"node_id"}"#
        );
    }

    #[test]
    fn op_blob_list_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&Op::BlobList).unwrap(),
            r#"{"op":"blob_list"}"#
        );
    }

    #[test]
    fn op_ring_new_serializes_with_name_field() {
        let json = serde_json::to_string(&Op::RingNew {
            name: "friends".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"op":"ring_new","name":"friends"}"#);
    }

    #[test]
    fn request_with_no_req_id_omits_field() {
        let req = Request {
            req_id: None,
            op: Op::NodeId,
        };
        assert_eq!(serde_json::to_string(&req).unwrap(), r#"{"op":"node_id"}"#);
    }

    #[test]
    fn request_with_req_id_includes_field() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let req = Request {
            req_id: Some(id),
            op: Op::BlobList,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","op":"blob_list"}"#
        );
    }

    #[test]
    fn event_done_with_no_req_id_omits_field() {
        assert_eq!(
            serde_json::to_string(&Event::done(None)).unwrap(),
            r#"{"type":"done"}"#
        );
    }

    #[test]
    fn event_line_serializes_type_and_text() {
        let json = serde_json::to_string(&Event::line(None, "hello world")).unwrap();
        assert_eq!(json, r#"{"type":"line","text":"hello world"}"#);
    }

    #[test]
    fn event_progress_serializes_correctly() {
        let json = serde_json::to_string(&Event::progress(None, 50, 100)).unwrap();
        assert_eq!(json, r#"{"type":"progress","done":50,"total":100}"#);
    }

    #[test]
    fn event_error_serializes_correctly() {
        let json = serde_json::to_string(&Event::error(None, "something went wrong")).unwrap();
        assert_eq!(json, r#"{"type":"error","message":"something went wrong"}"#);
    }

    #[test]
    fn event_with_req_id_includes_field() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let json = serde_json::to_string(&Event::line(Some(id), "hi")).unwrap();
        assert_eq!(
            json,
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"line","text":"hi"}"#
        );
    }

    #[test]
    fn request_round_trips_through_json() {
        let id = Uuid::new_v4();
        let original = Request {
            req_id: Some(id),
            op: Op::RingNew {
                name: "work".into(),
            },
        };
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn event_round_trips_through_json() {
        let id = Uuid::new_v4();
        let original = Event::progress(Some(id), 42, 100);
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }
}
