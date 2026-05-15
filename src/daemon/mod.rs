pub mod client;
pub mod pid;
pub mod protocol;
pub mod server;

/// Maximum byte length of a single IPC request or response line
/// in the wire protocol.
pub(crate) const MAX_LINE_BYTES: usize = 512 * 1024;
