//! [`ReloadableCert`]: a PEM certificate chain + private key served to rustls through
//! `ResolvesServerCert` (servers) and `ResolvesClientCert` (cluster clients), re-read when the
//! files change.
//!
//! * [`ReloadableCert::check`] compares a stamp (mtime, size, inode) of both files with the
//!   stamp of the last attempt and reloads when it differs — the 30 s watch calls it.
//! * [`ReloadableCert::reload`] re-reads unconditionally (SIGHUP).
//!
//! A reload that fails (unreadable file, bad PEM, key not matching the certificate — e.g. the
//! certificate was replaced but the key not yet) keeps serving the previous pair and records
//! the error; the next change of either file is tried again. New handshakes pick up a new pair
//! immediately; established connections are not touched.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use fluxmq_proto::tls::CertificateInfo;
use parking_lot::RwLock;
use rustls::SignatureScheme;
use rustls::client::ResolvesClientCert;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::{Result, TlsError, info, pem, provider};

/// Result of a reload attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// Files unchanged (or re-read with an identical certificate).
    Unchanged,
    /// A new certificate is being served.
    Reloaded,
    /// The files changed but could not be loaded; the previous certificate stays.
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
    ino: u64,
}

fn stamp(path: &Path) -> Option<FileStamp> {
    let m = std::fs::metadata(path).ok()?;
    #[cfg(unix)]
    let ino = std::os::unix::fs::MetadataExt::ino(&m);
    #[cfg(not(unix))]
    let ino = 0;
    Some(FileStamp {
        modified: m.modified().ok(),
        len: m.len(),
        ino,
    })
}

struct State {
    key: Arc<CertifiedKey>,
    info: CertificateInfo,
    loaded_at_ms: u64,
    last_error: Option<String>,
    /// Stamp of the files at the last load attempt (successful or not).
    seen: (Option<FileStamp>, Option<FileStamp>),
}

/// A certificate/key pair that can be swapped at runtime.
pub struct ReloadableCert {
    cert_path: PathBuf,
    key_path: PathBuf,
    state: RwLock<State>,
}

impl std::fmt::Debug for ReloadableCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableCert")
            .field("cert", &self.cert_path)
            .field("key", &self.key_path)
            .finish_non_exhaustive()
    }
}

fn load_pair(cert: &Path, key: &Path) -> Result<(Arc<CertifiedKey>, CertificateInfo)> {
    let chain = pem::load_certs(cert)?;
    let der_key = pem::load_key(key)?;
    let info = info::certificate_info(&chain[0])
        .map_err(|e| TlsError::Pem(format!("{}: {e}", cert.display())))?;
    let ck = CertifiedKey::from_der(chain, der_key, &provider())
        .map_err(|e| TlsError::Config(format!("{} / {}: {e}", cert.display(), key.display())))?;
    Ok((Arc::new(ck), info))
}

impl ReloadableCert {
    /// Loads the pair; fails when it can't be loaded (startup must not run without it).
    pub fn load(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        let seen = (stamp(&cert_path), stamp(&key_path));
        let (key, info) = load_pair(&cert_path, &key_path)?;
        Ok(Arc::new(Self {
            cert_path,
            key_path,
            state: RwLock::new(State {
                key,
                info,
                loaded_at_ms: fluxmq_proto::now_ms(),
                last_error: None,
                seen,
            }),
        }))
    }

    pub fn cert_path(&self) -> &Path {
        &self.cert_path
    }

    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// The pair currently served.
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.state.read().key.clone()
    }

    /// Facts about the certificate currently served.
    pub fn info(&self) -> CertificateInfo {
        self.state.read().info.clone()
    }

    /// Last successful (re)load, ms since epoch.
    pub fn loaded_at_ms(&self) -> u64 {
        self.state.read().loaded_at_ms
    }

    /// Last reload error, cleared by the next successful reload.
    pub fn last_error(&self) -> Option<String> {
        self.state.read().last_error.clone()
    }

    /// Reloads when either file changed since the last attempt.
    pub fn check(&self) -> ReloadOutcome {
        let now = (stamp(&self.cert_path), stamp(&self.key_path));
        if now == self.state.read().seen {
            return ReloadOutcome::Unchanged;
        }
        self.attempt(now)
    }

    /// Re-reads both files unconditionally.
    pub fn reload(&self) -> ReloadOutcome {
        let now = (stamp(&self.cert_path), stamp(&self.key_path));
        self.attempt(now)
    }

    fn attempt(&self, seen: (Option<FileStamp>, Option<FileStamp>)) -> ReloadOutcome {
        let loaded = load_pair(&self.cert_path, &self.key_path);
        let mut st = self.state.write();
        st.seen = seen;
        match loaded {
            Ok((key, info)) => {
                let changed = info.sha256_fingerprint != st.info.sha256_fingerprint
                    || key.key.public_key() != st.key.key.public_key();
                st.last_error = None;
                if !changed {
                    return ReloadOutcome::Unchanged;
                }
                st.key = key;
                st.info = info;
                st.loaded_at_ms = fluxmq_proto::now_ms();
                ReloadOutcome::Reloaded
            }
            Err(e) => {
                let msg = e.to_string();
                st.last_error = Some(msg.clone());
                ReloadOutcome::Failed(msg)
            }
        }
    }
}

impl ResolvesServerCert for ReloadableCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

impl ResolvesClientCert for ReloadableCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev::{DevCa, Usage, write_file};

    fn write_pair(dir: &Path, ca: &DevCa, cn: &str) {
        let p = ca.issue(cn, &["localhost".into()], Usage::Server).unwrap();
        write_file(&dir.join("c.pem"), p.cert_pem.as_bytes(), false).unwrap();
        write_file(&dir.join("k.pem"), p.key_pem.as_bytes(), true).unwrap();
    }

    #[test]
    fn reloads_on_change_and_keeps_old_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let ca = DevCa::generate("CA").unwrap();
        write_pair(dir.path(), &ca, "one");
        let rc = ReloadableCert::load(dir.path().join("c.pem"), dir.path().join("k.pem")).unwrap();
        assert!(rc.info().subject.contains("one"));
        assert_eq!(rc.check(), ReloadOutcome::Unchanged);

        // Rotation (rename-replace changes the inode, so the stamp differs even within the
        // same mtime tick).
        write_pair(dir.path(), &ca, "two");
        assert_eq!(rc.check(), ReloadOutcome::Reloaded);
        assert!(rc.info().subject.contains("two"));
        assert_eq!(rc.check(), ReloadOutcome::Unchanged);

        // Broken key: the old pair stays and the error is recorded.
        write_file(&dir.path().join("k.pem"), b"garbage", true).unwrap();
        assert!(matches!(rc.check(), ReloadOutcome::Failed(_)));
        assert!(rc.info().subject.contains("two"));
        assert!(rc.last_error().is_some());
        // Not retried until the files change again.
        assert_eq!(rc.check(), ReloadOutcome::Unchanged);

        // Mismatched key (certificate of a different pair).
        let other = ca.issue("three", &[], Usage::Server).unwrap();
        let mine = ca.issue("four", &[], Usage::Server).unwrap();
        write_file(&dir.path().join("c.pem"), other.cert_pem.as_bytes(), false).unwrap();
        write_file(&dir.path().join("k.pem"), mine.key_pem.as_bytes(), true).unwrap();
        assert!(matches!(rc.check(), ReloadOutcome::Failed(_)));
        assert!(rc.info().subject.contains("two"));

        // Fixed: next change succeeds and clears the error.
        write_pair(dir.path(), &ca, "five");
        assert_eq!(rc.reload(), ReloadOutcome::Reloaded);
        assert!(rc.last_error().is_none());
        assert!(rc.info().subject.contains("five"));
        assert_eq!(rc.reload(), ReloadOutcome::Unchanged);
    }

    #[test]
    fn load_fails_without_files() {
        assert!(ReloadableCert::load("/nonexistent/c.pem", "/nonexistent/k.pem").is_err());
    }
}
