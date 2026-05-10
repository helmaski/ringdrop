//! `rdrop` — ringdrop CLI
//!
//! # Usage
//!
//! ```text
//! # Print your peer-id so others can add you to their rings
//! rdrop id
//!
//! # Manage rings
//! rdrop ring new friends               # create a ring named "friends"
//! rdrop ring list                      # list all rings
//! rdrop ring add friends <peer-id>     # add a peer to a ring
//! rdrop ring members friends
//!
//! # Import a file and get a ticket (shortcut)
//! rdrop import file.txt                       # untagged — warns until tagged
//! rdrop import file.txt --open                # publicly accessible
//! rdrop import file.txt --ring friends        # restrict to a ring
//! rdrop import file.txt --ring friends --ring work  # multiple rings
//!
//! # Manage blobs
//! rdrop blob import file.txt --ring friends
//! rdrop blob list
//! rdrop blob remove file.txt
//! rdrop blob remove <hash>
//!
//! # Re-tag a blob at any time
//! rdrop tag file.txt --ring friends
//! rdrop tag <hash> --open
//!
//! # Start serving all authorised blobs
//! rdrop share
//!
//! # Receive — resumes automatically if interrupted
//! rdrop receive rdrop://ABCDEF... [--dest ./downloads]
//! ```

mod command;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use crate::util::default_data_dir;
use command::Cmd;

#[derive(Parser)]
#[command(
    name = "rdrop",
    about = "P2P streamed file transfer with ring-based access control.\n\
             Built on iroh and bao protocols.",
    version
)]
struct Cli {
    /// Directory for blob store + registry (default: ~/.ringdrop)
    #[arg(long, env = "RINGDROP_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    let default_filter = if matches!(cli.command, Cmd::Share) {
        "ringdrop=info"
    } else {
        "warn"
    };

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .with_target(false)
        .compact()
        .init();

    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    match cli.command {
        Cmd::Ring(cmd) => command::ring::run(cmd, &data_dir)?,
        Cmd::Blob(cmd) => command::blob::run(cmd, &data_dir).await?,
        Cmd::Import { path, rings, open } => {
            command::blob::run_import(path, rings, open, &data_dir).await?;
        }
        Cmd::Share => command::share::run(&data_dir).await?,
        Cmd::Receive {
            ticket,
            dest,
            force_overwrite,
        } => {
            command::receive::run(&ticket, dest, force_overwrite, &data_dir).await?;
        }
        Cmd::Tag {
            target,
            rings,
            open,
        } => {
            command::tag::run_tag(target, rings, open, &data_dir).await?;
        }
        Cmd::Tags { target } => command::tag::run_tags(target, &data_dir).await?,
        Cmd::Id => command::id::run(&data_dir)?,
    }

    Ok(())
}
