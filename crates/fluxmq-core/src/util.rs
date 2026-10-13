//! Small filesystem and time helpers.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// fsyncs a directory so that renames/creates inside it are durable.
pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Atomically replaces `path` with `bytes`: write tmp, fsync, rename, fsync dir.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("path has no parent"))?;
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| io::Error::other("path has no file name"))?
        .to_os_string();
    tmp_name.push(".tmp");
    let tmp = dir.join(tmp_name);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir)
}

/// Microseconds since the Unix epoch.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub use fluxmq_proto::now_ms;
