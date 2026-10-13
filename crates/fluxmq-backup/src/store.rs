//! Backup targets: a [`BackupTarget`] becomes an `object_store` handle plus a key prefix.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use fluxmq_proto::backup::BackupTarget;
use fluxmq_proto::notify::TestResult;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::{BackupError, Result};

/// An opened backup target. Cheap to clone. `Debug` shows only [`Target::describe`] (never
/// credentials).
#[derive(Clone)]
pub struct Target {
    pub store: Arc<dyn ObjectStore>,
    prefix: String,
    /// Human-readable description without secrets (`s3://bucket/prefix`, `file:///path`).
    pub describe: String,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target({})", self.describe)
    }
}

impl Target {
    /// Opens `target` (no network I/O; errors surface on first use).
    pub fn open(target: &BackupTarget) -> Result<Target> {
        match target {
            BackupTarget::S3 {
                endpoint,
                region,
                bucket,
                prefix,
                access_key_id,
                secret_access_key,
                path_style,
            } => {
                if bucket.trim().is_empty() {
                    return Err(BackupError::Invalid("S3 bucket is required".into()));
                }
                let region = if region.trim().is_empty() {
                    "us-east-1"
                } else {
                    region.trim()
                };
                let mut b = AmazonS3Builder::new()
                    .with_bucket_name(bucket.trim())
                    .with_region(region)
                    .with_virtual_hosted_style_request(!path_style);
                if !access_key_id.is_empty() {
                    b = b
                        .with_access_key_id(access_key_id)
                        .with_secret_access_key(secret_access_key);
                }
                if let Some(ep) = endpoint.as_deref().map(str::trim).filter(|e| !e.is_empty()) {
                    let ep = ep.trim_end_matches('/');
                    let Some((scheme, host)) = ep.split_once("://") else {
                        return Err(BackupError::Invalid(format!(
                            "S3 endpoint must start with http:// or https:// (got '{ep}')"
                        )));
                    };
                    if scheme != "http" && scheme != "https" {
                        return Err(BackupError::Invalid(format!(
                            "unsupported S3 endpoint scheme '{scheme}'"
                        )));
                    }
                    b = b.with_allow_http(scheme == "http");
                    // Virtual-hosted style with a custom endpoint needs the bucket in the host.
                    b = if *path_style {
                        b.with_endpoint(ep)
                    } else {
                        b.with_endpoint(format!("{scheme}://{}.{host}", bucket.trim()))
                    };
                }
                let store = b
                    .build()
                    .map_err(|e| BackupError::Invalid(format!("S3 target: {e}")))?;
                let prefix = clean_prefix(prefix);
                let describe = match &prefix[..] {
                    "" => format!("s3://{}", bucket.trim()),
                    p => format!("s3://{}/{p}", bucket.trim()),
                };
                Ok(Target {
                    store: Arc::new(store),
                    prefix,
                    describe,
                })
            }
            BackupTarget::Filesystem { path } => {
                if path.trim().is_empty() {
                    return Err(BackupError::Invalid("filesystem path is required".into()));
                }
                std::fs::create_dir_all(path)?;
                let fs = LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| BackupError::Invalid(format!("filesystem target: {e}")))?
                    .with_automatic_cleanup(true);
                Ok(Target {
                    store: Arc::new(fs),
                    prefix: String::new(),
                    describe: format!("file://{path}"),
                })
            }
        }
    }

    /// Wraps any store (tests: `InMemory`).
    pub fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Target {
        let prefix = clean_prefix(prefix);
        Target {
            store,
            describe: format!("store:{prefix}"),
            prefix,
        }
    }

    pub fn in_memory() -> Target {
        Self::from_store(Arc::new(InMemory::new()), "")
    }

    /// Full object path of `rel` (a `/`-separated key relative to the prefix).
    pub fn path(&self, rel: &str) -> Path {
        if self.prefix.is_empty() {
            Path::from(rel)
        } else {
            Path::from(format!("{}/{rel}", self.prefix))
        }
    }

    /// Write + read + delete of a probe object (`POST /api/backups/test-target`).
    pub async fn probe(&self) -> TestResult {
        let started = Instant::now();
        let res = self.probe_inner().await;
        let (error, error_detail) = match res {
            Ok(()) => (None, None),
            Err(e) => {
                let detail = error_chain(&e);
                (Some(short_probe_error(&detail)), Some(detail))
            }
        };
        TestResult {
            ok: error.is_none(),
            latency_ms: started.elapsed().as_millis() as u64,
            error,
            error_detail,
        }
    }

    async fn probe_inner(&self) -> Result<()> {
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).map_err(|e| BackupError::Other(e.to_string()))?;
        let name: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
        let path = self.path(&format!("probe/{name}.txt"));
        let body = Bytes::from(format!("fluxmq backup probe {name}"));
        self.store
            .put(&path, PutPayload::from(body.clone()))
            .await
            .map_err(|e| BackupError::Other(format!("write failed: {e}")))?;
        let got = self
            .store
            .get(&path)
            .await
            .map_err(|e| BackupError::Other(format!("read failed: {e}")))?
            .bytes()
            .await
            .map_err(|e| BackupError::Other(format!("read failed: {e}")))?;
        self.store
            .delete(&path)
            .await
            .map_err(|e| BackupError::Other(format!("delete failed: {e}")))?;
        if got != body {
            return Err(BackupError::Other("read back different content".into()));
        }
        Ok(())
    }
}

fn clean_prefix(p: &str) -> String {
    p.split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// Error with its source chain (deduplicated).
pub fn error_chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        let cs = c.to_string();
        if !s.contains(&cs) {
            s.push_str(": ");
            s.push_str(&cs);
        }
        cur = c.source();
    }
    s
}

/// Short, human class of a probe failure (`access denied`, `bucket not found`,
/// `connection refused`, `timeout`, `TLS error`, `DNS error`, …); falls back to the first
/// clause of the message.
pub fn short_probe_error(detail: &str) -> String {
    use fluxmq_proto::api::short_error;
    let d = detail.to_ascii_lowercase();
    if d.contains("accessdenied") || d.contains("access denied") || d.contains("403") {
        return "access denied".into();
    }
    if d.contains("invalidaccesskeyid") || d.contains("signaturedoesnotmatch") {
        return "invalid credentials".into();
    }
    if d.contains("nosuchbucket") {
        return "bucket not found".into();
    }
    if d.contains("permission denied") {
        return "permission denied".into();
    }
    if d.contains("error sending request") {
        // object_store drops the transport cause (refused / DNS / TLS): the endpoint is
        // unreachable.
        return short_error::CONNECTION_FAILED.into();
    }
    match short_error::classify(detail, false, false) {
        short_error::REQUEST_FAILED | short_error::CONNECTION_FAILED => {
            let first: String = detail.split(": ").take(2).collect::<Vec<_>>().join(": ");
            first.chars().take(120).collect()
        }
        class => class.to_string(),
    }
}

#[cfg(test)]
mod short_tests {
    use super::short_probe_error;

    #[test]
    fn probe_errors_are_short() {
        assert_eq!(
            short_probe_error(
                "write failed: Generic S3 error: Error performing PUT http://x in 2s - HTTP error: error sending request"
            ),
            "connection failed"
        );
        assert_eq!(
            short_probe_error("write failed: <Code>NoSuchBucket</Code>"),
            "bucket not found"
        );
        assert_eq!(
            short_probe_error("write failed: 403 AccessDenied"),
            "access denied"
        );
    }
}
