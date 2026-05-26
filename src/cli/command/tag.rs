use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

pub(crate) async fn run_tag(
    target: String,
    rings: Vec<String>,
    open: bool,
    data_dir: &Path,
) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::Tag {
            target,
            rings,
            open,
        })
        .await
}

pub(crate) async fn run_untag(
    target: String,
    rings: Vec<String>,
    open: bool,
    all: bool,
    data_dir: &Path,
) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::Untag {
            target,
            rings,
            open,
            all,
        })
        .await
}
