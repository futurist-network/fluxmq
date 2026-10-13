//! Certificate facts for status / alerts ([`CertificateInfo`]) and peer identities.

use std::net::IpAddr;

use fluxmq_proto::tls::CertificateInfo;
use rustls_pki_types::CertificateDer;
use sha2::{Digest, Sha256};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

/// Subject, issuer, SANs, validity and SHA-256 fingerprint of a DER certificate.
pub fn certificate_info(der: &CertificateDer<'_>) -> Result<CertificateInfo, String> {
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| format!("invalid X.509 certificate: {e}"))?;
    let mut sans = Vec::new();
    if let Ok(Some(ext)) = cert.subject_alternative_name() {
        for name in &ext.value.general_names {
            match name {
                GeneralName::DNSName(d) => sans.push((*d).to_string()),
                GeneralName::IPAddress(b) => {
                    if let Some(ip) = ip_from_bytes(b) {
                        sans.push(ip.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    let ms = |ts: i64| u64::try_from(ts).unwrap_or(0).saturating_mul(1000);
    Ok(CertificateInfo {
        subject: cert.subject().to_string(),
        issuer: cert.issuer().to_string(),
        sans,
        not_before_ms: ms(cert.validity().not_before.timestamp()),
        not_after_ms: ms(cert.validity().not_after.timestamp()),
        sha256_fingerprint: fingerprint(der),
    })
}

/// Lowercase hex SHA-256 of the DER bytes.
pub fn fingerprint(der: &CertificateDer<'_>) -> String {
    let digest = Sha256::digest(der.as_ref());
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Subject common name of a DER certificate.
pub fn common_name(der: &CertificateDer<'_>) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(der.as_ref()).ok()?;
    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string)
}

/// Common name of the leaf of a peer chain (`rustls` `peer_certificates()`).
pub fn peer_common_name(chain: Option<&[CertificateDer<'_>]>) -> Option<String> {
    chain.and_then(|c| c.first()).and_then(common_name)
}

fn ip_from_bytes(b: &[u8]) -> Option<IpAddr> {
    match b.len() {
        4 => Some(IpAddr::from(<[u8; 4]>::try_from(b).ok()?)),
        16 => Some(IpAddr::from(<[u8; 16]>::try_from(b).ok()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev;

    #[test]
    fn extracts_fields() {
        let ca = dev::DevCa::generate("Test CA").unwrap();
        let leaf = ca
            .issue(
                "broker-1",
                &["localhost".into(), "127.0.0.1".into()],
                dev::Usage::Server,
            )
            .unwrap();
        let der = crate::pem::certs_from_pem(leaf.cert_pem.as_bytes()).unwrap();
        let info = certificate_info(&der[0]).unwrap();
        assert!(info.subject.contains("CN=broker-1"), "{}", info.subject);
        assert!(info.issuer.contains("Test CA"), "{}", info.issuer);
        assert_eq!(info.sans, vec!["localhost", "127.0.0.1"]);
        assert!(info.not_after_ms > info.not_before_ms);
        assert_eq!(info.sha256_fingerprint.len(), 64);
        assert_eq!(common_name(&der[0]).as_deref(), Some("broker-1"));
        assert_eq!(peer_common_name(Some(&der)).as_deref(), Some("broker-1"));
        assert!(certificate_info(&CertificateDer::from(vec![1, 2, 3])).is_err());
    }
}
