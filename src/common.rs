use crate::{admin_stats::StatCmd, Paths};
use anyhow::{anyhow, bail, Context, Result};
use api::{symbology::CptyId, ComponentId, Config};
use fxhash::{FxHashMap, FxHashSet};
use log::debug;
use netidx::{
    config::Config as NetidxConfig,
    path::Path,
    publisher::{BindCfg, Publisher, PublisherBuilder},
    subscriber::{DesiredAuth, Subscriber},
};
use once_cell::sync::OnceCell;
use openssl::{pkey::Private, rsa::Rsa, x509::X509};
use std::{fs, ops::Deref, path::PathBuf, str::FromStr, sync::Arc};
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
        let subscriber = Subscriber::new(netidx_config.clone(), desired_auth.clone())?;
        let local_components: FxHashSet<ComponentId> =
            f.local.keys().map(|k| *k).collect();
        let mut remote_components: FxHashMap<ComponentId, Path> = FxHashMap::default();
        for (path, coms) in f.remote.iter() {
            for (com, _) in coms.iter() {
                remote_components.insert(*com, path.clone());
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
            paths: Paths {
                hosted_base: f.hosted_base,
                local_base: f.local_base,
                core_base: f.core_base,
                local_components,
                remote_components,
                use_local_symbology: f.use_local_symbology,
                use_local_userdb: f.use_local_userdb,
                use_local_marketdata,
                use_legacy_marketdata,
            },
            stats: OnceCell::new(),
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

    /// Get the architect tls identity
    pub fn get_tls_identity(
        &self,
    ) -> Result<(&netidx::config::Tls, &netidx::config::TlsIdentity)> {
        let tls =
            self.netidx_config.tls.as_ref().ok_or_else(|| anyhow!("no tls config"))?;
        let identity = tls
            .identities
            .get("xyz.architect.")
            .ok_or_else(|| anyhow!("architect.xyz identity not found"))?;
        Ok((tls, identity))
    }

    // CR-someday alee: switch all of this to rustls?
    /// Load and decrypt the private key from the configured identity
    /// Note this does blocking operations, so within an async context
    /// call it with block_in_place
    pub fn load_private_key(&self) -> Result<Rsa<Private>> {
        use pkcs8::{
            der::{pem::PemLabel, zeroize::Zeroize},
            EncryptedPrivateKeyInfo, PrivateKeyInfo, SecretDocument,
        };
        let (tls, identity) = self.get_tls_identity()?;
        let path = &identity.private_key;
        debug!("reading key from {}", path);
        let pem = fs::read_to_string(&path)?;
        let (label, doc) = match SecretDocument::from_pem(&pem) {
            Ok((label, doc)) => (label, doc),
            Err(e) => bail!("failed to load pem {}, error: {}", path, e),
        };
        debug!("key label is {}", label);
        if label == EncryptedPrivateKeyInfo::PEM_LABEL {
            if !EncryptedPrivateKeyInfo::try_from(doc.as_bytes()).is_ok() {
                bail!("encrypted key malformed")
            }
            debug!("decrypting key");
            // try password-protected
            let mut password = netidx::tls::load_key_password(
                tls.askpass.as_ref().map(|s| s.as_str()),
                &path,
            )?;
            let pkey = Rsa::private_key_from_pem_passphrase(
                pem.as_bytes(),
                password.as_bytes(),
            )?;
            password.zeroize();
            Ok(pkey)
        } else if label == PrivateKeyInfo::PEM_LABEL {
            Ok(Rsa::private_key_from_pem(pem.as_bytes())?)
        } else {
            bail!("unknown key type")
        }
    }

    /// Load the public key/certificate from the configured
    /// identity. Note this does blocking operations, so within an
    /// async context call it with block_in_place
    pub fn load_certificate(&self) -> Result<X509> {
        let (_, identity) = self.get_tls_identity()?;
        Ok(X509::from_pem(&fs::read(&identity.certificate)?)?)
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

    /// Convenience function to initialize symbology from [Common]
    pub async fn start_symbology(&self, write: bool) -> crate::symbology::client::Client {
        crate::symbology::client::Client::start(
            self.subscriber.clone(),
            self.paths.sym(),
            None,
            write,
        )
        .await
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
    /// Optional admin_stats support
    pub(crate) stats: OnceCell<futures::channel::mpsc::UnboundedSender<(Path, StatCmd)>>,
}
