use crate::{admin_stats::AdminStats, tls, ChannelDriverBuilder, Paths};
use anyhow::{anyhow, Context, Result};
use api::{symbology::CptyId, ComponentId, Config, UserId};
use fxhash::{FxHashMap, FxHashSet};
use log::debug;
use netidx::{
    config::Config as NetidxConfig,
    path::Path,
    publisher::{BindCfg, Publisher, PublisherBuilder},
    subscriber::{DesiredAuth, Subscriber},
};
use once_cell::sync::OnceCell;
use std::{fs, ops::Deref, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{sync::Mutex, task};
use url::Url;

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
        let subscriber = Subscriber::new(netidx_config.clone(), desired_auth.clone())?;
        let mut component_kind = FxHashMap::default();
        let local_components: FxHashSet<ComponentId> =
            f.local.keys().map(|k| *k).collect();
        for (com, (kind, _)) in f.local.iter() {
            component_kind.insert(*com, kind.clone());
        }
        let mut remote_components: FxHashMap<ComponentId, Path> = FxHashMap::default();
        for (path, coms) in f.remote.iter() {
            for (com, kind) in coms.iter() {
                remote_components.insert(*com, path.clone());
                component_kind.insert(*com, kind.clone());
            }
        }
        let mut use_local_marketdata = FxHashSet::default();
        for cpty in f.use_local_marketdata {
            use_local_marketdata.insert(CptyId::from_str(&cpty)?);
        }
        let mut use_legacy_marketdata = FxHashSet::default();
        for cpty in f.use_legacy_marketdata {
            use_legacy_marketdata.insert(CptyId::from_str(&cpty)?);
        }
        let mut use_legacy_hist_marketdata = FxHashSet::default();
        for cpty in f.use_legacy_hist_marketdata {
            use_legacy_hist_marketdata.insert(CptyId::from_str(&cpty)?);
        }
        let mut external_marketdata = FxHashMap::default();
        for (cpty, url) in f.external_marketdata.iter() {
            external_marketdata.insert(CptyId::from_str(&cpty)?, Url::parse(url)?);
        }
        let identity = tls::netidx_tls_identity(&netidx_config)
            .map(|(_, identity)| {
                let cert = tls::netidx_tls_identity_certificate(identity)?;
                let subj = tls::subject_name(&cert)?
                    .ok_or_else(|| anyhow!("missing subject name"))?;
                let uid: UserId = subj.parse()?;
                Ok::<_, anyhow::Error>(uid)
            })
            .transpose()?;
        Ok(Self(Arc::new(CommonInner {
            config_path,
            config,
            identity,
            netidx_config,
            netidx_config_path: f.netidx_config,
            desired_auth,
            bind_config,
            publisher_slack: f.publisher_slack,
            publisher,
            subscriber,
            paths: Paths {
                hosted_base: f.hosted_base,
                local_base: f.local_base,
                core_base: f.core_base,
                local_components,
                remote_components,
                component_kind,
                use_local_symbology: f.use_local_symbology,
                use_local_licensedb: f.use_local_licensedb,
                use_local_marketdata,
                use_legacy_marketdata,
                use_legacy_hist_marketdata,
            },
            stats: OnceCell::new(),
            external_symbology: Mutex::new(FxHashMap::default()),
            external_marketdata,
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

    pub fn get_local_component_of_kind(&self, kind: &str) -> Option<ComponentId> {
        for (com, (k, _)) in self.config.local.iter() {
            if k == kind {
                return Some(*com);
            }
        }
        None
    }

    pub fn find_local_component_of_kind(&self, kind: &str) -> Result<ComponentId> {
        self.get_local_component_of_kind(kind)
            .ok_or_else(|| anyhow!("no local component of kind: {}", kind))
    }

    pub fn get_component_of_kind(&self, kind: &str) -> Option<ComponentId> {
        if let Some(com) = self.get_local_component_of_kind(kind) {
            return Some(com);
        }
        for (_, coms) in self.config.remote.iter() {
            for (com, k) in coms.iter() {
                if k == kind {
                    return Some(*com);
                }
            }
        }
        None
    }

    pub fn find_component_of_kind(&self, kind: &str) -> Result<ComponentId> {
        self.get_component_of_kind(kind)
            .ok_or_else(|| anyhow!("no component of kind: {}", kind))
    }

    pub fn all_components(&self, filter: impl Fn(&str) -> bool) -> Vec<ComponentId> {
        let mut res = Vec::new();
        for (com, (k, _)) in self.config.local.iter() {
            if filter(k) {
                res.push(*com);
            }
        }
        for (_, coms) in self.config.remote.iter() {
            for (com, k) in coms.iter() {
                if filter(k) {
                    res.push(*com);
                }
            }
        }
        res
    }

    pub fn all_components_of_kind(&self, kind: &str) -> Vec<ComponentId> {
        let mut res = Vec::new();
        for (com, (k, _)) in self.config.local.iter() {
            if k == kind {
                res.push(*com);
            }
        }
        for (_, coms) in self.config.remote.iter() {
            for (com, k) in coms.iter() {
                if k == kind {
                    res.push(*com);
                }
            }
        }
        res
    }

    /// Convenience function to initialize symbology from [Common]
    pub async fn start_symbology(
        &self,
        write: bool,
    ) -> Option<crate::symbology::client::Client> {
        if self.config.no_symbology {
            return None;
        }
        let client = crate::symbology::client::Client::start(
            self.subscriber.clone(),
            self.paths.sym(),
            None,
            write,
        )
        .await;
        Some(client)
    }

    pub async fn start_external_symbology(&self) -> Result<()> {
        let mut external_symbology = self.external_symbology.lock().await;
        for (cpty, url) in self.config.external_marketdata.iter() {
            let cpty: CptyId = cpty.parse()?;
            let url: Url = url.parse()?;
            let client =
                crate::symbology::external_client::ExternalSymbologyClient::start(url);
            client.synced().wait_synced(None).await?;
            external_symbology.insert(cpty, client);
        }
        Ok(())
    }

    pub fn channel_driver(&self) -> ChannelDriverBuilder {
        ChannelDriverBuilder::new(self)
    }
}

#[derive(Debug)]
pub struct CommonInner {
    /// The path to the config that was loaded
    pub config_path: Option<PathBuf>,
    /// A copy of the raw config file
    pub config: Config,
    /// Self-identification (certificate subject if TLS, or None)
    pub identity: Option<UserId>,
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
    /// Optional admin_stats support
    pub stats: OnceCell<AdminStats>,
    /// External symbology subscriptions
    pub external_symbology: Mutex<
        FxHashMap<CptyId, crate::symbology::external_client::ExternalSymbologyClient>,
    >,
    /// External marketdata source config
    pub external_marketdata: FxHashMap<CptyId, Url>,
}
