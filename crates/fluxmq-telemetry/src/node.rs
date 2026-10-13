//! Node resource sampling via `sysinfo` (blocking — run it on `spawn_blocking`).
//!
//! * CPU and memory are the **broker process's** (`cpu_percent` normalized to 0–100 across all
//!   cores, `memory_bytes` = RSS); `memory_total_bytes` is the machine's total memory.
//! * Disk is the filesystem that holds the data directory (longest mount-point prefix).

use std::path::{Path, PathBuf};

use sysinfo::{Disks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// One resource sample.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct NodeSample {
    pub cpu_percent: f32,
    pub memory_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
}

pub struct NodeSampler {
    sys: System,
    disks: Disks,
    pid: Option<Pid>,
    data_dir: PathBuf,
    cores: f32,
}

impl NodeSampler {
    pub fn new(data_dir: PathBuf) -> Self {
        let data_dir = std::fs::canonicalize(&data_dir).unwrap_or(data_dir);
        Self {
            sys: System::new(),
            disks: Disks::new_with_refreshed_list(),
            pid: sysinfo::get_current_pid().ok(),
            data_dir,
            cores: std::thread::available_parallelism().map_or(1.0, |n| n.get() as f32),
        }
    }

    /// Blocking. The first CPU reading is 0 (sysinfo needs two refreshes).
    pub fn sample(&mut self) -> NodeSample {
        self.sys.refresh_memory();
        let mut out = NodeSample {
            memory_total_bytes: self.sys.total_memory(),
            ..Default::default()
        };
        if let Some(pid) = self.pid {
            self.sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );
            if let Some(p) = self.sys.process(pid) {
                out.cpu_percent = (p.cpu_usage() / self.cores).clamp(0.0, 100.0);
                out.memory_bytes = p.memory();
            }
        }
        self.disks.refresh(true);
        if let Some((total, avail)) = disk_for(
            &self.data_dir,
            self.disks
                .list()
                .iter()
                .map(|d| (d.mount_point(), d.total_space(), d.available_space())),
        ) {
            out.disk_total_bytes = total;
            out.disk_used_bytes = total.saturating_sub(avail);
        }
        out
    }
}

/// Picks the disk whose mount point is the longest prefix of `path`: `(total, available)`.
pub fn disk_for<'a>(
    path: &Path,
    disks: impl Iterator<Item = (&'a Path, u64, u64)>,
) -> Option<(u64, u64)> {
    disks
        .filter(|(mp, _, _)| path.starts_with(mp))
        .max_by_key(|(mp, _, _)| mp.as_os_str().len())
        .map(|(_, total, avail)| (total, avail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_mount_prefix_wins() {
        let disks = [
            (Path::new("/"), 100, 50),
            (Path::new("/data"), 1000, 100),
            (Path::new("/dat"), 7, 7),
        ];
        let r = disk_for(
            Path::new("/data/fluxmq"),
            disks.iter().map(|(p, t, a)| (*p, *t, *a)),
        );
        assert_eq!(r, Some((1000, 100)));
        let r = disk_for(
            Path::new("/var/x"),
            disks.iter().map(|(p, t, a)| (*p, *t, *a)),
        );
        assert_eq!(r, Some((100, 50)));
    }

    #[test]
    fn samples_this_process() {
        let mut s = NodeSampler::new(std::env::temp_dir());
        let a = s.sample();
        assert!(a.memory_total_bytes > 0);
        assert!(a.memory_bytes > 0);
        assert!(a.disk_total_bytes >= a.disk_used_bytes);
    }
}
