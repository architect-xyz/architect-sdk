//! Utilities for working with TLS, TLS certificates, and netidx TLS

use anyhow::{bail, Result};
use log::debug;
use openssl::{pkey::Private, rsa::Rsa, x509::X509};

pub fn subject_name(certificate: &X509) -> Result<Option<String>> {
    certificate
        .subject_name()
        .entries()
        .next()
        .map(|ner| Ok::<_, anyhow::Error>(ner.data().as_utf8()?.to_string()))
        .transpose()
}

/// Get the architect.xyz tls identity from netidx; returns None
/// if no TLS configuration or no TLS architect.xyz identity.
pub fn netidx_tls_identity(
    config: &netidx::config::Config,
) -> Option<(&netidx::config::Tls, &netidx::config::TlsIdentity)> {
    let tls = config.tls.as_ref()?;
    let identity = tls.identities.get("xyz.architect.")?;
    Some((tls, identity))
}

// CR-someday alee: switch all of this to rustls?
/// Load and decrypt the private key from the configured identity
/// Note this does blocking operations, so within an async context
/// call it with block_in_place
pub fn netidx_tls_identity_privkey(
    tls: &netidx::config::Tls,
    identity: &netidx::config::TlsIdentity,
) -> Result<Rsa<Private>> {
    use pkcs8::{
        der::{pem::PemLabel, zeroize::Zeroize},
        EncryptedPrivateKeyInfo, PrivateKeyInfo, SecretDocument,
    };
    let path = &identity.private_key;
    debug!("reading key from {}", path);
    let pem = std::fs::read_to_string(&path)?;
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
        let pkey =
            Rsa::private_key_from_pem_passphrase(pem.as_bytes(), password.as_bytes())?;
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
pub fn netidx_tls_identity_certificate(
    identity: &netidx::config::TlsIdentity,
) -> Result<X509> {
    Ok(X509::from_pem(&std::fs::read(&identity.certificate)?)?)
}
