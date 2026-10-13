//! Development certificates (`fluxmq tls gen-dev`): a throwaway CA plus server, node and
//! client certificates signed by it. **Never use these in production** — the CA key sits next
//! to the certificates.
//!
//! Layout written by [`generate`] into `--out`:
//!
//! | file | use |
//! |---|---|
//! | `ca.pem` / `ca-key.pem` | the development CA (`--tls-client-ca`, `--cluster-tls-ca`, SDK `ca`) |
//! | `server.pem` / `server-key.pem` | client + HTTP ports (`--tls-cert/--tls-key`), SANs = `--hosts` |
//! | `client.pem` / `client-key.pem` | an mTLS client identity (CN `client`) |
//! | `node-<id>.pem` / `node-<id>-key.pem` | per node for `--cluster-tls-cert/key` (CN = node id, SANs = `--hosts`) |
//!
//! These names are part of the contract (`fluxmq_proto::tls`).

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};

use crate::{Result, TlsError};

/// Validity of leaf certificates (kept under Apple's 825-day TLS limit).
pub const LEAF_VALIDITY_DAYS: i64 = 397;
/// Validity of the development CA.
pub const CA_VALIDITY_DAYS: i64 = 3650;

/// What a leaf certificate is for (sets extended key usage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    Server,
    Client,
    /// Cluster node: serves and connects (server + client auth).
    Node,
}

/// A generated certificate and its private key, PEM-encoded.
#[derive(Clone)]
pub struct Pair {
    pub cert_pem: String,
    pub key_pem: String,
}

impl std::fmt::Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pair").finish_non_exhaustive()
    }
}

/// An in-memory development CA.
pub struct DevCa {
    pub pair: Pair,
    issuer: Issuer<'static, KeyPair>,
}

fn rc(e: rcgen::Error) -> TlsError {
    TlsError::Config(format!("certificate generation: {e}"))
}

fn dn(cn: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "FluxMQ development");
    dn.push(DnType::CommonName, cn);
    dn
}

impl DevCa {
    pub fn generate(common_name: &str) -> Result<DevCa> {
        let key = KeyPair::generate().map_err(rc)?;
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(rc)?;
        params.distinguished_name = dn(common_name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let now = OffsetDateTime::now_utc();
        params.not_before = now - Duration::days(1);
        params.not_after = now + Duration::days(CA_VALIDITY_DAYS);
        let cert = params.self_signed(&key).map_err(rc)?;
        let pair = Pair {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        };
        Ok(DevCa {
            pair,
            issuer: Issuer::new(params, key),
        })
    }

    /// Loads an existing CA (certificate + key PEM, e.g. `ca.pem` / `ca-key.pem` written by
    /// [`generate`]) to issue more certificates from it.
    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<DevCa> {
        let key = KeyPair::from_pem(key_pem).map_err(rc)?;
        let issuer = Issuer::from_ca_cert_pem(cert_pem, key).map_err(rc)?;
        Ok(DevCa {
            pair: Pair {
                cert_pem: cert_pem.to_string(),
                key_pem: key_pem.to_string(),
            },
            issuer,
        })
    }

    /// Issues a leaf valid for [`LEAF_VALIDITY_DAYS`] from now.
    pub fn issue(&self, common_name: &str, sans: &[String], usage: Usage) -> Result<Pair> {
        let now = OffsetDateTime::now_utc();
        self.issue_with_validity(
            common_name,
            sans,
            usage,
            now - Duration::days(1),
            now + Duration::days(LEAF_VALIDITY_DAYS),
        )
    }

    /// Issues a leaf with an explicit validity window (tests: expiring certificates).
    pub fn issue_with_validity(
        &self,
        common_name: &str,
        sans: &[String],
        usage: Usage,
        not_before: OffsetDateTime,
        not_after: OffsetDateTime,
    ) -> Result<Pair> {
        let key = KeyPair::generate().map_err(rc)?;
        let mut params = CertificateParams::new(sans.to_vec()).map_err(rc)?;
        params.distinguished_name = dn(common_name);
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = match usage {
            Usage::Server => vec![ExtendedKeyUsagePurpose::ServerAuth],
            Usage::Client => vec![ExtendedKeyUsagePurpose::ClientAuth],
            Usage::Node => vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        };
        params.use_authority_key_identifier_extension = true;
        params.not_before = not_before;
        params.not_after = not_after;
        let cert = params.signed_by(&key, &self.issuer).map_err(rc)?;
        Ok(Pair {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        })
    }
}

/// `fluxmq tls gen-dev` options.
#[derive(Debug, Clone, Default)]
pub struct GenDevOptions {
    pub out: PathBuf,
    /// Node ids that get a `node-<id>` certificate.
    pub nodes: Vec<String>,
    /// DNS names / IPs in the server and node certificates' SANs.
    pub hosts: Vec<String>,
    /// Overwrite existing files.
    pub force: bool,
    /// Leaf validity in days from now (default [`LEAF_VALIDITY_DAYS`]); short values make
    /// expiring certificates for alert tests.
    pub valid_days: Option<u32>,
    /// Issue the leaves from the existing `ca.pem` / `ca-key.pem` in `out` instead of a new CA
    /// (rotate or re-issue certificates without re-distributing the CA). The CA files are
    /// left untouched.
    pub reuse_ca: bool,
}

/// Writes the development CA and certificates; returns the written paths.
pub fn generate(opts: &GenDevOptions) -> Result<Vec<PathBuf>> {
    let hosts: Vec<String> = if opts.hosts.is_empty() {
        vec!["localhost".into(), "127.0.0.1".into()]
    } else {
        opts.hosts.clone()
    };
    for n in &opts.nodes {
        if n.is_empty()
            || !n
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        {
            return Err(TlsError::Config(format!(
                "invalid node id '{n}' (letters, digits, '-', '_', '.')"
            )));
        }
    }
    std::fs::create_dir_all(&opts.out).map_err(|source| TlsError::Io {
        path: opts.out.display().to_string(),
        source,
    })?;
    let mut planned: Vec<(String, Pair)> = Vec::new();
    let ca = if opts.reuse_ca {
        let [crt, key] = file_names(&opts.out, "ca");
        let read = |p: &Path| {
            std::fs::read_to_string(p).map_err(|source| TlsError::Io {
                path: p.display().to_string(),
                source,
            })
        };
        DevCa::from_pem(&read(&crt)?, &read(&key)?)?
    } else {
        let ca = DevCa::generate("FluxMQ Development CA")?;
        planned.push(("ca".into(), ca.pair.clone()));
        ca
    };
    let days = i64::from(opts.valid_days.unwrap_or(LEAF_VALIDITY_DAYS as u32));
    if days < 1 {
        return Err(TlsError::Config("--valid-days must be at least 1".into()));
    }
    let now = OffsetDateTime::now_utc();
    let issue = |cn: &str, sans: &[String], usage: Usage| {
        ca.issue_with_validity(
            cn,
            sans,
            usage,
            now - Duration::days(1),
            now + Duration::days(days),
        )
    };
    planned.push((
        "server".into(),
        issue("fluxmq-server", &hosts, Usage::Server)?,
    ));
    planned.push(("client".into(), issue("client", &[], Usage::Client)?));
    for n in &opts.nodes {
        planned.push((format!("node-{n}"), issue(n, &hosts, Usage::Node)?));
    }
    if !opts.force {
        for (name, _) in &planned {
            for p in file_names(&opts.out, name) {
                if p.exists() {
                    return Err(TlsError::Config(format!(
                        "{} already exists (use --force to overwrite)",
                        p.display()
                    )));
                }
            }
        }
    }
    let mut written = Vec::new();
    for (name, pair) in planned {
        let [crt, key] = file_names(&opts.out, &name);
        write_file(&crt, pair.cert_pem.as_bytes(), false)?;
        write_file(&key, pair.key_pem.as_bytes(), true)?;
        written.push(crt);
        written.push(key);
    }
    Ok(written)
}

/// `<name>.pem` and `<name>-key.pem` (contract file names).
pub fn file_names(dir: &Path, name: &str) -> [PathBuf; 2] {
    [
        dir.join(format!("{name}.pem")),
        dir.join(format!("{name}-key.pem")),
    ]
}

/// Writes atomically (temp file + rename), keys with mode 0600.
pub fn write_file(path: &Path, bytes: &[u8], secret: bool) -> Result<()> {
    let io = |source| TlsError::Io {
        path: path.display().to_string(),
        source,
    };
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    std::fs::write(&tmp, bytes).map_err(io)?;
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).map_err(io)?;
    }
    #[cfg(not(unix))]
    let _ = secret;
    std::fs::rename(&tmp, path).map_err(io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_layout_and_refuses_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let opts = GenDevOptions {
            out: dir.path().to_path_buf(),
            nodes: vec!["n1".into(), "n2".into()],
            hosts: vec![],
            force: false,
            valid_days: None,
            reuse_ca: false,
        };
        let files = generate(&opts).unwrap();
        assert_eq!(files.len(), 10);
        for f in [
            "ca.pem",
            "ca-key.pem",
            "server.pem",
            "server-key.pem",
            "client.pem",
            "client-key.pem",
            "node-n1.pem",
            "node-n1-key.pem",
            "node-n2.pem",
            "node-n2-key.pem",
        ] {
            assert!(dir.path().join(f).exists(), "{f}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("ca-key.pem"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let node = crate::pem::load_certs(&dir.path().join("node-n1.pem")).unwrap();
        let info = crate::info::certificate_info(&node[0]).unwrap();
        assert!(info.subject.contains("CN=n1"));
        assert_eq!(info.sans, vec!["localhost", "127.0.0.1"]);
        let client = crate::pem::load_certs(&dir.path().join("client.pem")).unwrap();
        assert_eq!(
            crate::info::common_name(&client[0]).as_deref(),
            Some("client")
        );
        assert!(generate(&opts).is_err());
        assert!(
            generate(&GenDevOptions {
                force: true,
                ..opts.clone()
            })
            .is_ok()
        );
        assert!(
            generate(&GenDevOptions {
                nodes: vec!["bad/id".into()],
                force: true,
                ..opts
            })
            .is_err()
        );
    }

    /// `--valid-days` shortens the leaves; `--reuse-ca` re-issues them from the existing CA
    /// (the CA files stay byte-identical, the new leaves chain to it).
    #[test]
    fn short_validity_and_reused_ca() {
        let dir = tempfile::tempdir().unwrap();
        let opts = GenDevOptions {
            out: dir.path().to_path_buf(),
            nodes: vec!["n2".into()],
            hosts: vec![],
            force: false,
            valid_days: None,
            reuse_ca: false,
        };
        generate(&opts).unwrap();
        let ca_before = std::fs::read(dir.path().join("ca.pem")).unwrap();
        let files = generate(&GenDevOptions {
            force: true,
            valid_days: Some(5),
            reuse_ca: true,
            ..opts.clone()
        })
        .unwrap();
        assert_eq!(files.len(), 6, "CA not rewritten");
        assert_eq!(std::fs::read(dir.path().join("ca.pem")).unwrap(), ca_before);
        let node = crate::pem::load_certs(&dir.path().join("node-n2.pem")).unwrap();
        let info = crate::info::certificate_info(&node[0]).unwrap();
        let days_left = (info.not_after_ms - info.not_before_ms) / 86_400_000;
        assert_eq!(days_left, 6, "1 day back-dated + 5 days");
        let ca = crate::pem::load_certs(&dir.path().join("ca.pem")).unwrap();
        let ca_info = crate::info::certificate_info(&ca[0]).unwrap();
        assert_eq!(info.issuer, ca_info.subject);
        assert!(
            generate(&GenDevOptions {
                force: true,
                valid_days: Some(0),
                ..opts
            })
            .is_err()
        );
    }
}
