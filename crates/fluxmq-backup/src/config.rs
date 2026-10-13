//! Backup configuration: validation, secret redaction and the uniform secret update rule
//! (`fluxmq_proto::notify` docs): reads return [`REDACTED`] for every secret that is set;
//! on update `REDACTED` keeps the stored value, `""` removes it, anything else replaces it.
//! Secrets: the S3 `secret_access_key` and the `encryption_key`.

use fluxmq_proto::backup::{BackupConfig, BackupTarget};
use fluxmq_proto::notify::REDACTED;

use crate::crypto::Key;
use crate::{BackupError, Result};

pub const MIN_INTERVAL_SECS: u32 = 300;
pub const DEFAULT_INTERVAL_SECS: u32 = 3600;
pub const DEFAULT_KEEP_LAST: u32 = 24;
pub const DEFAULT_KEEP_DAYS: u32 = 7;

/// Rejects out-of-range or incomplete settings.
pub fn validate(c: &BackupConfig) -> Result<()> {
    if c.interval_secs < MIN_INTERVAL_SECS {
        return Err(BackupError::Invalid(format!(
            "interval_secs must be >= {MIN_INTERVAL_SECS}"
        )));
    }
    if c.keep_last == 0 {
        return Err(BackupError::Invalid("keep_last must be >= 1".into()));
    }
    validate_target(&c.target)?;
    if let Some(k) = &c.encryption_key {
        Key::parse(k)?;
    }
    Ok(())
}

pub fn validate_target(t: &BackupTarget) -> Result<()> {
    match t {
        BackupTarget::S3 {
            bucket,
            access_key_id,
            secret_access_key,
            endpoint,
            ..
        } => {
            if bucket.trim().is_empty() {
                return Err(BackupError::Invalid("S3 bucket is required".into()));
            }
            if access_key_id.is_empty() != secret_access_key.is_empty() {
                return Err(BackupError::Invalid(
                    "access_key_id and secret_access_key must be set together".into(),
                ));
            }
            if let Some(ep) = endpoint.as_deref().filter(|e| !e.trim().is_empty())
                && !(ep.starts_with("http://") || ep.starts_with("https://"))
            {
                return Err(BackupError::Invalid(
                    "S3 endpoint must start with http:// or https://".into(),
                ));
            }
        }
        BackupTarget::Filesystem { path } => {
            if path.trim().is_empty() {
                return Err(BackupError::Invalid("filesystem path is required".into()));
            }
        }
    }
    Ok(())
}

/// Copy safe to return from the API.
pub fn redact(c: &BackupConfig) -> BackupConfig {
    let mut c = c.clone();
    c.target = redact_target(&c.target);
    if c.encryption_key.is_some() {
        c.encryption_key = Some(REDACTED.to_string());
    }
    c
}

pub fn redact_target(t: &BackupTarget) -> BackupTarget {
    let mut t = t.clone();
    if let BackupTarget::S3 {
        secret_access_key, ..
    } = &mut t
        && !secret_access_key.is_empty()
    {
        *secret_access_key = REDACTED.to_string();
    }
    t
}

/// Applies the secret rule to an incoming target against the stored one.
pub fn merge_target(new: BackupTarget, stored: Option<&BackupTarget>) -> Result<BackupTarget> {
    let mut new = new;
    if let BackupTarget::S3 {
        secret_access_key, ..
    } = &mut new
        && secret_access_key == REDACTED
    {
        match stored {
            Some(BackupTarget::S3 {
                secret_access_key: old,
                ..
            }) => *secret_access_key = old.clone(),
            _ => {
                return Err(BackupError::Invalid(
                    "secret_access_key is redacted but no stored value exists".into(),
                ));
            }
        }
    }
    Ok(new)
}

/// Applies the secret rule to an incoming config against the stored one.
pub fn merge(new: BackupConfig, stored: Option<&BackupConfig>) -> Result<BackupConfig> {
    let mut new = new;
    new.target = merge_target(new.target, stored.map(|s| &s.target))?;
    new.encryption_key = match new.encryption_key.as_deref() {
        Some(REDACTED) => match stored.and_then(|s| s.encryption_key.clone()) {
            Some(k) => Some(k),
            None => {
                return Err(BackupError::Invalid(
                    "encryption_key is redacted but no stored value exists".into(),
                ));
            }
        },
        Some("") | None => None,
        Some(k) => Some(k.trim().to_string()),
    };
    Ok(new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(secret: &str) -> BackupTarget {
        BackupTarget::S3 {
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: "us-east-1".into(),
            bucket: "b".into(),
            prefix: "p".into(),
            access_key_id: "AK".into(),
            secret_access_key: secret.into(),
            path_style: true,
        }
    }

    fn cfg(secret: &str, key: Option<&str>) -> BackupConfig {
        BackupConfig {
            enabled: true,
            target: s3(secret),
            interval_secs: 3600,
            keep_last: 3,
            keep_days: 0,
            include_messages: true,
            encryption_key: key.map(str::to_string),
        }
    }

    #[test]
    fn redaction_and_merge_rule() {
        let key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        let stored = cfg("SECRET", Some(key));
        validate(&stored).unwrap();
        let r = redact(&stored);
        assert!(!serde_json::to_string(&r).unwrap().contains("SECRET"));
        assert!(!serde_json::to_string(&r).unwrap().contains(key));
        // Echo keeps both.
        assert_eq!(merge(r.clone(), Some(&stored)).unwrap(), stored);
        // "" removes the encryption key.
        let mut upd = r.clone();
        upd.encryption_key = Some(String::new());
        assert_eq!(merge(upd, Some(&stored)).unwrap().encryption_key, None);
        // New value replaces.
        let mut upd = r;
        upd.target = s3("NEW");
        assert_eq!(merge(upd, Some(&stored)).unwrap().target, s3("NEW"));
        // Redacted without stored value is rejected.
        assert!(merge(cfg(REDACTED, None), None).is_err());
        // Validation.
        let mut bad = stored.clone();
        bad.interval_secs = 10;
        assert!(validate(&bad).is_err());
        let mut bad = stored;
        bad.encryption_key = Some("short".into());
        assert!(validate(&bad).is_err());
    }
}
