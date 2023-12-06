use crate::Paths;
use anyhow::{Context, Result};
use api::Config;
use log::debug;
use netidx::{
    config::Config as NetidxConfig,
    publisher::{BindCfg, Publisher, PublisherBuilder},
    subscriber::{DesiredAuth, Subscriber},
};
use std::{fs, ops::Deref, path::PathBuf, sync::Arc};
use tokio::task;

/// Common data and functionality shared by most everything in the core,
/// derived from the config.  
#[derive(Debug, Clone)]
pub struct Common(pub Arc<CommonInner>);

impl Deref for Common {
    type Target = CommonInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Common {
    pub async fn from_config(config_path: Option<PathBuf>, f: Config) -> Result<Self> {
        let config = f.clone();
        let netidx_config = task::block_in_place(|| match &f.netidx_config {
            None => {
                debug!("loading default netidx config");
                NetidxConfig::load_default()
            }
            Some(p) => {
                debug!("loading netidx config from {}", p.display());
                NetidxConfig::load(p)
            }
        })?;
        let desired_auth = match f.desired_auth {
            None => netidx_config.default_auth(),
            Some(a) => a,
        };
        let bind_config = match f.bind_config {
            None => {
                // a small sin (using env vars) to avoid a big one (having to split configs for Docker)
                if let Ok(b) = std::env::var("OVERRIDE_BIND_CFG_FOR_DOCKER") {
                    debug!("using bind config override: {b}");
                    b.parse::<BindCfg>()?
                } else {
                    netidx_config.default_bind_config.clone()
                }
            }
            Some(b) => b.parse::<BindCfg>()?,
        };
        debug!("creating publisher");
        let publisher = PublisherBuilder::new(netidx_config.clone())
            .slack(f.publisher_slack.unwrap_or(3))
            .bind_cfg(Some(bind_config.clone()))
            .desired_auth(desired_auth.clone())
            .build()
            .await?;
        debug!("creating subscriber");
        let local_base = f.local_base;
        let hosted_base = f.hosted_base;
        let subscriber = Subscriber::new(netidx_config.clone(), desired_auth.clone())?;
        Ok(Self(Arc::new(CommonInner {
            config_path,
            config,
            netidx_config,
            netidx_config_path: f.netidx_config,
            desired_auth,
            bind_config,
            publisher_slack: f.publisher_slack,
            publisher,
            subscriber,
            paths: Paths { hosted_base, local_base },
        })))
    }

    async fn from_file<P: AsRef<std::path::Path>>(path: Option<P>) -> Result<Self> {
        let config_path = match path {
            Some(path) => path.as_ref().into(),
            None => Config::default_path()?,
        };
        debug!("loading config from {:?}", config_path);
        let f: Config =
            serde_yaml::from_slice(&fs::read(&config_path).with_context(|| {
                format!("no core config found at {}", config_path.display())
            })?)?;
        Self::from_config(Some(config_path), f).await
    }

    /// Load from the specified file
    pub async fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::from_file(Some(path)).await
    }

    /// Load from the default location
    pub async fn load_default() -> Result<Self> {
        Self::from_file(None::<&str>).await
    }
}

#[derive(Debug)]
pub struct CommonInner {
    /// The path to the config that was loaded
    pub config_path: Option<PathBuf>,
    /// A copy of the raw config file
    pub config: Config,
    /// The netidx config used to build the publisher and subscriber
    pub netidx_config: NetidxConfig,
    /// The location of the netidx config that was used, if not the default
    pub netidx_config_path: Option<PathBuf>,
    /// The netidx authentication mechanism
    pub desired_auth: DesiredAuth,
    /// The netidx publisher bind config
    pub bind_config: BindCfg,
    // CR alee: document this
    pub publisher_slack: Option<usize>,
    /// The netidx publisher object
    pub publisher: Publisher,
    /// The netidx subscriber object
    pub subscriber: Subscriber,
    /// The path map
    pub paths: Paths,
}
