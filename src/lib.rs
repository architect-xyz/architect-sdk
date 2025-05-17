use anyhow::{bail, Result};
use std::path::{Path, PathBuf};

#[cfg(feature = "grpc")]
pub mod client;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod marketdata;
pub mod order_id_allocator;
#[cfg(feature = "grpc")]
pub mod symbology;
pub mod synced;

#[cfg(feature = "grpc")]
pub use client::Architect;
pub use marketdata::MarketdataSource;

/// Canonical config file location resolution.
pub fn locate_config(from_arg: Option<impl AsRef<Path>>) -> Result<PathBuf> {
    const FROM_ENV: &[&str] = &["ARCHITECT_CFG"];
    const DEFAULT_CONFIG: Option<&str> = Some("~/.architect/config.yml");
    if let Some(path) = from_arg {
        return Ok(path.as_ref().to_owned());
    }
    for env in FROM_ENV {
        if let Ok(path) = std::env::var(env) {
            return Ok(path.into());
        }
    }
    if let Some(path) = DEFAULT_CONFIG {
        if let Some(path) = path.strip_prefix("~/") {
            if let Some(home_dir) = dirs::home_dir() {
                return Ok(home_dir.join(path));
            }
        } else {
            return Ok(path.to_owned().into());
        }
    }
    bail!("config file was required but not specified");
}

#[cfg(feature = "yaml")]
pub fn load_config(from_arg: Option<impl AsRef<Path>>) -> Result<api::Config> {
    use anyhow::Context;
    let path = locate_config(from_arg)?;
    let config_s = std::fs::read_to_string(&path)
        .with_context(|| format!("while reading file: {}", path.display()))?;
    let config: api::Config = serde_yaml::from_str(&config_s)
        .with_context(|| format!("while parsing file: {}", path.display()))?;
    Ok(config)
}
