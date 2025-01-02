//! Useful/safe/Architect-tuned abstractions for gRPC clients

use anyhow::{anyhow, Context, Result};
use api::HumanDuration;
use clap::Args;
use log::error;
use serde::{Deserialize, Serialize};
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    str::FromStr,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use url::Url;

const ARCHITECT_CA: &[u8] = include_bytes!("ca.crt");

#[derive(Debug, Clone, Serialize, Deserialize, Args)]
pub struct GrpcClientConfig {
    /// Connect timeout, e.g. 3s for three seconds
    #[serde(default)]
    #[arg(long)]
    pub connect_timeout: Option<HumanDuration>,
    /// Specify that TLS client auth should be used.  If a TLS identity
    /// isn't explicitly provided, look for the Architect license file
    /// in the default directory.
    #[serde(default)]
    #[arg(long)]
    pub tls_client: bool,
    /// Specify a certificate to present for TLS client certificate auth;
    /// the corresponding private key file will be looked for at the same
    /// path but with .key extension, unless explicitly overriden.
    #[serde(default)]
    #[arg(long)]
    pub tls_identity: Option<PathBuf>,
    /// Explicitly specify the private key for TLS client certificate
    /// auth, if not at the canonical location.
    #[serde(default)]
    #[arg(long)]
    pub tls_identity_key: Option<PathBuf>,
    // CR alee: for localhost, prefer `dangerous_accept_any_tls_for_localhost`;
    // implementing requires a change in tonic
    /// Manually specify an additional root certificate to verify server TLS
    #[serde(default)]
    #[arg(long = "ca")]
    pub dangerous_additional_ca_certificate: Option<PathBuf>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Self::default_connect_timeout(),
            tls_client: false,
            tls_identity: None,
            tls_identity_key: None,
            dangerous_additional_ca_certificate: None,
        }
    }
}

impl GrpcClientConfig {
    fn default_connect_timeout() -> Option<HumanDuration> {
        Some(chrono::Duration::seconds(3).into())
    }

    /// Open a gRPC channel to the given endpoint URL
    pub async fn connect(&self, url: &Url) -> Result<Channel> {
        let endpoint = Endpoint::try_from(url.to_string())?;
        self.connect_to(endpoint).await
    }

    /// Open a gRPC channel to the given endpoint
    pub async fn connect_to(&self, mut endpoint: Endpoint) -> Result<Channel> {
        if let Some(dur) = self.connect_timeout {
            endpoint = endpoint.connect_timeout((*dur).to_std()?);
        }
        if endpoint.uri().scheme_str() == Some("https") {
            let ca = Certificate::from_pem(ARCHITECT_CA);
            let mut tls_config =
                ClientTlsConfig::new().with_enabled_roots().ca_certificate(ca);
            if let Some(add_ca_path) = &self.dangerous_additional_ca_certificate {
                let add_ca_pem = tokio::fs::read(add_ca_path)
                    .await
                    .with_context(|| format!("reading {}", add_ca_path.display()))?;
                let add_ca = Certificate::from_pem(&add_ca_pem);
                tls_config = tls_config.ca_certificate(add_ca);
                // useful notice
                if endpoint.uri().host().is_some_and(|h| is_ip_address(h)) {
                    error!("url is not a domain name but scheme is https; this will always fail tls domain verification");
                    error!("if connecting to 127.0.0.1--use \"localhost\" instead")
                }
            }
            if self.tls_client || self.tls_identity.is_some() {
                if let Some(cert_path) = &self.tls_identity {
                    let identity = grpc_tls_identity_from_pem_files(
                        cert_path,
                        self.tls_identity_key.as_ref(),
                    )
                    .await?;
                    tls_config = tls_config.identity(identity);
                } else {
                    // try the Architect/netidx default location
                    let mut cert_path = dirs::config_dir().ok_or_else(|| {
                        anyhow!("could not determine default config directory")
                    })?;
                    cert_path.push("netidx");
                    cert_path.push("license.crt");
                    let identity = grpc_tls_identity_from_pem_files(
                        cert_path,
                        self.tls_identity_key.as_ref(),
                    )
                    .await?;
                    tls_config = tls_config.identity(identity);
                }
            }
            endpoint = endpoint.tls_config(tls_config)?;
        }
        let channel = endpoint.connect().await?;
        Ok(channel)
    }
}

fn is_ip_address(host: &str) -> bool {
    IpAddr::from_str(host).is_ok()
}

/// Canonical reading of a TLS identity from file for tonic/gRPC
///
/// If `pkey_path` isn't provided, look for it where `cert_path`
/// is but with the `.key` extension.
pub async fn grpc_tls_identity_from_pem_files(
    cert_path: impl AsRef<Path>,
    pkey_path: Option<impl AsRef<Path>>,
) -> Result<tonic::transport::Identity> {
    let cert_path = cert_path.as_ref().to_owned();
    let pkey_path = match pkey_path {
        Some(path) => path.as_ref().to_owned(),
        None => cert_path.with_extension("key"),
    };
    let cert_pem = tokio::fs::read(&cert_path)
        .await
        .with_context(|| format!("reading {}", cert_path.display()))?;
    let pkey_pem = tokio::fs::read(&pkey_path)
        .await
        .with_context(|| format!("reading {}", pkey_path.display()))?;
    Ok(tonic::transport::Identity::from_pem(&cert_pem, &pkey_pem))
}
