use std::path::Path;

use anyhow::{Context, Result};

use crate::config::Config;

pub fn run(data_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let cfg = Config::load_or_create(data_dir).context("loading config")?;
    println!("{}", cfg.public_id());
    Ok(())
}
