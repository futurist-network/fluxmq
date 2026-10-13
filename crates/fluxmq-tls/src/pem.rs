//! PEM loading (rustls-pki-types' parser): certificate chains, private keys (PKCS#8, PKCS#1,
//! SEC1) and root stores.

use std::path::Path;

use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::{Result, TlsError};

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.display().to_string(),
        source,
    })
}

/// Every `CERTIFICATE` block of a PEM file, in order (leaf first for a chain).
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let bytes = read(path)?;
    certs_from_pem(&bytes).map_err(|e| TlsError::Pem(format!("{}: {e}", path.display())))
}

/// Every `CERTIFICATE` block of PEM bytes; errors when there is none.
pub fn certs_from_pem(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("invalid PEM certificate: {e}"))?;
    if certs.is_empty() {
        return Err("no CERTIFICATE found in PEM".into());
    }
    Ok(certs)
}

/// The first private key of a PEM file.
pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let bytes = read(path)?;
    key_from_pem(&bytes).map_err(|e| TlsError::Pem(format!("{}: {e}", path.display())))
}

/// The first private key in PEM bytes.
pub fn key_from_pem(pem: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| match e {
        rustls_pki_types::pem::Error::NoItemsFound => "no PRIVATE KEY found in PEM".to_string(),
        e => format!("invalid PEM private key: {e}"),
    })
}

/// A root store with every certificate of a PEM bundle.
pub fn load_roots(path: &Path) -> Result<RootCertStore> {
    let bytes = read(path)?;
    roots_from_pem(&bytes).map_err(|e| TlsError::Pem(format!("{}: {e}", path.display())))
}

/// A root store with every certificate of PEM bytes; errors when none is usable.
pub fn roots_from_pem(pem: &[u8]) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certs_from_pem(pem)?);
    if added == 0 {
        return Err(format!(
            "no usable CA certificate ({ignored} unparsable) in PEM bundle"
        ));
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_garbage() {
        assert!(certs_from_pem(b"hello").is_err());
        assert!(key_from_pem(b"hello").is_err());
        assert!(roots_from_pem(b"").is_err());
        let err = load_certs(Path::new("/nonexistent/cert.pem")).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/cert.pem"));
    }
}
