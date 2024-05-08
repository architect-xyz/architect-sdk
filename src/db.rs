//! Database utilities

use anyhow::Result;
use api::MaybeSecret;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::rustls::{self, OwnedTrustAnchor};
use zeroize::Zeroizing;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub enum PostgresDbFlavor {
    #[default]
    Postgres,
    Timescale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresDbConfig {
    pub pg_user: String,
    pub pg_pass: MaybeSecret<String>,
    pub pg_host: String,
    pub pg_port: u32,
    pub pg_database: String,
    #[serde(default)]
    pub pg_flavor: PostgresDbFlavor,
}

impl PostgresDbConfig {
    pub fn connection_string(
        &self,
        decrypt_secret: impl FnOnce(&MaybeSecret<String>) -> Result<Zeroizing<String>>,
    ) -> Result<String> {
        let pg_pass = decrypt_secret(&self.pg_pass)?;
        Ok(format!(
            "host={} port={} user={} password={} dbname={}",
            self.pg_host, self.pg_port, self.pg_user, *pg_pass, self.pg_database
        ))
    }

    pub fn is_timescale(&self) -> bool {
        matches!(self.pg_flavor, PostgresDbFlavor::Timescale)
    }
}

/// Generates basic tls config, using Mozilla root certs
pub fn postgres_tls() -> Result<MakeRustlsConnect> {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS
            .0
            .iter()
            .map(|c| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    c.subject,
                    c.spki,
                    c.name_constraints,
                )
            })
            .collect_vec(),
    };
    let config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}
