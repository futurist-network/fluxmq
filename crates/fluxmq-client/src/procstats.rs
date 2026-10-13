//! Cheap process CPU / memory sampling for `ReportStats`.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Re-sample at most this often, however many consumers report.
const MIN_REFRESH: Duration = Duration::from_millis(900);

struct Monitor {
    sys: System,
    pid: Option<Pid>,
    last: Option<Instant>,
    cpu: Option<f32>,
    mem: Option<u64>,
}

fn monitor() -> &'static Mutex<Monitor> {
    static MON: OnceLock<Mutex<Monitor>> = OnceLock::new();
    MON.get_or_init(|| {
        Mutex::new(Monitor {
            sys: System::new(),
            pid: sysinfo::get_current_pid().ok(),
            last: None,
            cpu: None,
            mem: None,
        })
    })
}

fn sample_blocking() -> (Option<f32>, Option<u64>) {
    let mut m = monitor().lock();
    let Some(pid) = m.pid else {
        return (None, None);
    };
    if m.last.is_some_and(|t| t.elapsed() < MIN_REFRESH) {
        return (m.cpu, m.mem);
    }
    let first = m.last.is_none();
    m.sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );
    m.last = Some(Instant::now());
    let (cpu, mem) = match m.sys.process(pid) {
        // CPU usage needs two samples; the first one is always 0.
        Some(p) => ((!first).then(|| p.cpu_usage()), Some(p.memory())),
        None => (None, None),
    };
    m.cpu = cpu;
    m.mem = mem;
    (cpu, mem)
}

/// Process CPU (percent of one core, like `top`) and resident memory in bytes.
pub(crate) async fn sample() -> (Option<f32>, Option<u64>) {
    tokio::task::spawn_blocking(sample_blocking)
        .await
        .unwrap_or((None, None))
}
