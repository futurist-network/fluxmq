//! CPU profile of the running broker (`--features profiling`):
//! `GET /api/debug/pprof/profile?seconds=30&hz=99&format=svg|folded` (admin).
//!
//! Sampling is signal-based (`SIGPROF` timer), so it works where `perf` is unavailable
//! (containers, WSL). One profile at a time; the request blocks for `seconds`.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::extract::Query;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::error::{ApiErr, ApiResult};

static RUNNING: AtomicBool = AtomicBool::new(false);

const MAX_SECONDS: u64 = 300;
const MAX_HZ: i32 = 999;

#[derive(Deserialize)]
pub struct ProfileQuery {
    seconds: Option<u64>,
    hz: Option<i32>,
    format: Option<String>,
}

pub async fn profile(Query(q): Query<ProfileQuery>) -> ApiResult<Response> {
    let seconds = q.seconds.unwrap_or(30).clamp(1, MAX_SECONDS);
    let hz = q.hz.unwrap_or(99).clamp(1, MAX_HZ);
    let svg = match q.format.as_deref().unwrap_or("svg") {
        "svg" => true,
        "folded" => false,
        other => return Err(ApiErr::bad_request(format!("unknown format {other:?}"))),
    };
    if RUNNING.swap(true, Ordering::AcqRel) {
        return Err(ApiErr::conflict("a profile is already running"));
    }
    let result = tokio::task::spawn_blocking(move || record(seconds, hz, svg)).await;
    RUNNING.store(false, Ordering::Release);
    let body = result
        .map_err(|e| ApiErr::internal(format!("profiler task failed: {e}")))?
        .map_err(|e| ApiErr::internal(format!("profiler: {e}")))?;
    let content_type = if svg {
        "image/svg+xml"
    } else {
        "text/plain; charset=utf-8"
    };
    Ok(([(header::CONTENT_TYPE, content_type)], body).into_response())
}

fn record(seconds: u64, hz: i32, svg: bool) -> Result<Vec<u8>, pprof::Error> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(hz)
        // Unwinding inside these can deadlock; libc stays sampled (memcpy, syscalls).
        .blocklist(&["libgcc", "pthread", "vdso"])
        .build()?;
    std::thread::sleep(Duration::from_secs(seconds));
    let report = guard.report().build()?;
    if svg {
        let mut out = Vec::new();
        report.flamegraph(&mut out)?;
        return Ok(out);
    }
    // Folded stacks (`thread;root;…;leaf count`), the input format of flamegraph tools.
    let mut out = String::new();
    for (frames, count) in &report.data {
        out.push_str(&frames.thread_name);
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                let name = symbol.name();
                // The sampler's own frames (signal handler, unwinder) sit on top of every stack.
                if name.contains("pprof::") || name.starts_with("backtrace::") {
                    continue;
                }
                let _ = write!(out, ";{name}");
            }
        }
        let _ = writeln!(out, " {count}");
    }
    Ok(out.into_bytes())
}
