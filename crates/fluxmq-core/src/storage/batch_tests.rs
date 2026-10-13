//! v0.7 storage tests: batch envelopes, mixed segments, torn batches, CRC, index seeks,
//! compression, retention, crash with fsync, v0.6 upgrade, unsynced reads.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use fluxmq_proto::batch::{self, Compression, RecordsSection, codec};
use fluxmq_proto::model::PublishMessage;

use super::entry::{self, decode_chunk};
use super::log::{CommitMark, DEFAULT_CACHE_BYTES, LogConfig, PartitionLog, PartitionWriter};
use super::record::Record;

fn cfg(segment_bytes: u64) -> LogConfig {
    LogConfig {
        segment_bytes,
        sync: true,
        roll_jitter: false,
        cache_bytes: DEFAULT_CACHE_BYTES,
    }
}

fn pm(i: u64) -> PublishMessage {
    let mut headers = BTreeMap::new();
    if i.is_multiple_of(3) {
        headers.insert("h".to_string(), format!("v{i}"));
    }
    PublishMessage {
        key: (i % 4 == 1).then(|| format!("k{i}")),
        headers,
        payload: Bytes::from(format!("payload-{i:06}")),
    }
}

fn section(from: u64, n: u64, c: Compression) -> RecordsSection {
    let mut body = Vec::new();
    for (idx, i) in (from..from + n).enumerate() {
        let m = pm(i);
        batch::encode_record(
            &mut body,
            idx as u32,
            m.key.as_deref(),
            &m.headers,
            &m.payload,
        );
    }
    let (compression, body) = match codec::compress(c, &body) {
        Some(z) => (c, z),
        None => (Compression::None, body),
    };
    RecordsSection {
        compression,
        record_count: n as u32,
        body: Bytes::from(body),
    }
}

fn commit(w: &mut PartitionWriter) {
    w.sync().unwrap();
    w.publish();
}

fn collect(log: &PartitionLog, start: u64) -> Vec<Record> {
    let mut out = Vec::new();
    log.scan(start, |r| {
        out.push(r);
        ControlFlow::Continue(())
    })
    .unwrap();
    out
}

/// Checks offsets 0..n are present and carry `pm(i)`'s content.
fn check_all(log: &PartitionLog, n: u64) {
    let all = collect(log, 0);
    assert_eq!(all.len() as u64, n);
    for (i, r) in all.iter().enumerate() {
        let want = pm(i as u64);
        assert_eq!(r.offset, i as u64);
        assert_eq!(r.payload, want.payload, "offset {i}");
        assert_eq!(r.key, want.key, "offset {i}");
        assert_eq!(r.headers, want.headers, "offset {i}");
    }
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut logs: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    logs.sort();
    logs
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// Every kind of entry: legacy records, message envelopes, producer batches (all codecs).
fn write_mixed(w: &mut PartitionWriter) -> u64 {
    let mut n = 0;
    for i in 0..5 {
        w.append_legacy(1000 + i, pm(i)).unwrap();
    }
    n += 5;
    w.append_messages(2000, (n..n + 40).map(pm).collect())
        .unwrap();
    n += 40;
    commit(w);
    for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let (base, count) = w.append_batch(3000, &section(n, 50, c)).unwrap();
        assert_eq!((base, count), (n, 50));
        n += 50;
    }
    w.append_legacy(4000, pm(n)).unwrap();
    n += 1;
    commit(w);
    n
}

#[test]
fn mixed_segment_reads_reopen_and_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    let n = write_mixed(&mut w);
    assert_eq!(log.high(), n);
    assert_eq!(log.segment_count(), 1, "one mixed segment");
    check_all(&log, n);
    // Cache and disk agree, for every offset.
    for o in 0..n {
        let want = pm(o);
        let m = log.get(o).unwrap().unwrap();
        assert_eq!((m.offset, &m.payload), (o, &want.payload));
    }
    let batch = log.read_batch(3, 100, usize::MAX).unwrap();
    assert_eq!(batch.len(), 100);
    assert!(
        batch
            .iter()
            .enumerate()
            .all(|(i, m)| m.offset == 3 + i as u64)
    );
    // Raw reads: whole entries, legacy flagged.
    let c = log.read_envelopes(0, 1 << 20).unwrap().unwrap();
    assert_eq!((c.first_base, c.next_offset), (0, n));
    assert!(!c.batches_only);
    let msgs: Vec<_> = decode_chunk(0, &c.bytes).collect::<Result<_, _>>().unwrap();
    assert_eq!(msgs.len() as u64, n);
    let c = log.read_envelopes(70, 1).unwrap().unwrap();
    assert_eq!(
        (c.first_base, c.next_offset),
        (45, 95),
        "the first producer batch"
    );
    assert!(c.batches_only);
    drop(w);
    // Reopen: recovery validates both CRC kinds; the index works for both.
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    assert_eq!(log.high(), n);
    check_all(&log, n);
    for at in [0, 4, 5, 44, 45, 70, 94, 95, 120, 144, 145, 194, n - 1] {
        assert_eq!(collect(&log, at)[0].offset, at);
    }
    // Disk path (cache is empty after reopen).
    let c = log.read_envelopes(100, 1).unwrap().unwrap();
    assert_eq!((c.first_base, c.next_offset), (95, 145));
    // Follower copy, verbatim, with different segment sizes.
    let d2 = tempfile::tempdir().unwrap();
    let (dst, mut wd) = PartitionLog::open(d2.path(), 0, cfg(700)).unwrap();
    while let Some(c) = log.read_raw(wd.next_offset(), 300).unwrap() {
        assert_eq!(c.base_offset, wd.next_offset());
        assert_eq!(wd.append_raw(&c.bytes).unwrap(), c.base_offset + c.count);
        commit(&mut wd);
    }
    check_all(&dst, n);
    // Continue appending after reopen.
    w.append_messages(5000, (n..n + 10).map(pm).collect())
        .unwrap();
    commit(&mut w);
    check_all(&log, n + 10);
}

#[test]
fn torn_batch_is_truncated_as_a_unit_at_every_byte() {
    let src = tempfile::tempdir().unwrap();
    let last_start;
    let last_len;
    {
        let (log, mut w) = PartitionLog::open(src.path(), 0, cfg(1 << 20)).unwrap();
        w.append_messages(1000, (0..20).map(pm).collect()).unwrap();
        commit(&mut w);
        last_start = log.storage_bytes();
        w.append_batch(2000, &section(20, 30, Compression::Lz4))
            .unwrap();
        commit(&mut w);
        last_len = log.storage_bytes() - last_start;
    }
    let file = log_files(src.path())[0].clone();
    let name = file.file_name().unwrap().to_owned();
    for cut in 0..last_len {
        let d = tempfile::tempdir().unwrap();
        copy_dir(src.path(), d.path());
        OpenOptions::new()
            .write(true)
            .open(d.path().join(&name))
            .unwrap()
            .set_len(last_start + cut)
            .unwrap();
        let (log, mut w) = PartitionLog::open(d.path(), 0, cfg(1 << 20)).unwrap();
        assert_eq!(log.high(), 20, "cut at {cut}: torn batch removed whole");
        assert_eq!(log.storage_bytes(), last_start, "cut at {cut}");
        check_all(&log, 20);
        // Appending continues at the batch's offset.
        let (base, _) = w
            .append_batch(3000, &section(20, 30, Compression::None))
            .unwrap();
        assert_eq!(base, 20);
        commit(&mut w);
        check_all(&log, 50);
    }
}

#[test]
fn crc_corruption_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let mid;
    {
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
        w.append_messages(1000, (0..20).map(pm).collect()).unwrap();
        commit(&mut w);
        mid = log.storage_bytes();
        w.append_messages(1001, (20..40).map(pm).collect()).unwrap();
        w.append_messages(1002, (40..60).map(pm).collect()).unwrap();
        commit(&mut w);
    }
    let file = log_files(dir.path())[0].clone();
    // Flip one payload byte inside the second envelope.
    let mut bytes = fs::read(&file).unwrap();
    bytes[mid as usize + 60] ^= 0x40;
    fs::write(&file, &bytes).unwrap();
    // A reader of the visible log refuses it (the CRC is checked before decoding).
    {
        let bytes = Bytes::from(bytes[mid as usize..].to_vec());
        assert!(decode_chunk(0, &bytes).next().unwrap().is_err());
        assert!(entry::split_entries(&bytes, 20).is_err());
    }
    // Recovery of the active segment truncates from the corrupt envelope on.
    let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    assert_eq!(log.high(), 20);
    check_all(&log, 20);
}

#[test]
fn index_seeks_inside_batches() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(64 * 1024)).unwrap();
    // Producer batches of 300 messages (~6 KiB, never split): index entries point at
    // envelopes, several envelopes per segment.
    let mut n = 0;
    for _ in 0..20 {
        w.append_batch(1000 + n, &section(n, 300, Compression::None))
            .unwrap();
        n += 300;
        commit(&mut w);
    }
    assert!(log.segment_count() > 2);
    drop(w);
    let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(64 * 1024)).unwrap();
    for at in [0, 1, 299, 300, 301, 1234, 2999, 3000, 4567, n - 2, n - 1] {
        let r = collect(&log, at);
        assert_eq!(r[0].offset, at);
        assert_eq!(r.len() as u64, n - at);
        assert_eq!(log.get(at).unwrap().unwrap().payload, pm(at).payload);
        let c = log.read_envelopes(at, 1).unwrap().unwrap();
        assert!(c.first_base <= at && at < c.next_offset, "{at}");
        assert_eq!(c.first_base % 300, 0);
        assert_eq!(c.next_offset - c.first_base, 300);
    }
    // Time seek lands on the batch holding the time (one timestamp per batch).
    assert_eq!(log.offset_for_time(1000 + 600).unwrap(), 600);
    assert!(log.read_envelopes(n, 1).unwrap().is_none());
}

#[test]
fn compressed_batches_roundtrip_everywhere() {
    for c in [Compression::Lz4, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
        let sec = section(0, 500, c);
        assert_eq!(sec.compression, c, "compressible");
        w.append_batch(1000, &sec).unwrap();
        commit(&mut w);
        // Stored as received: the envelope body is the compressed section.
        let raw = log.cached_envelopes(0, 1 << 20).unwrap();
        assert_eq!(&raw.bytes[batch::ENVELOPE_HEADER_LEN..], &sec.body[..]);
        // Cache (lazy decode), scan, raw decode all see the messages.
        assert_eq!(log.cached(321).unwrap().payload, pm(321).payload);
        check_all(&log, 500);
        drop(w);
        let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
        check_all(&log, 500);
        let raw = log.read_envelopes(250, 1 << 20).unwrap().unwrap();
        let msgs: Vec<_> = decode_chunk(0, &raw.bytes)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(msgs[499].payload, pm(499).payload);
    }
}

#[test]
fn retention_removes_whole_segments_of_batches() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(8 * 1024)).unwrap();
    let mut n = 0;
    for _ in 0..30 {
        w.append_batch(1000, &section(n, 50, Compression::None))
            .unwrap();
        n += 50;
        commit(&mut w);
    }
    let segs = log.segment_count();
    assert!(segs > 3);
    let (removed, _) = log.remove_oldest_while(|s| s.next_offset() <= 700).unwrap();
    assert!(removed > 0);
    let low = log.low();
    assert!(low <= 700 && low % 50 == 0, "low {low} is a batch boundary");
    assert_eq!(collect(&log, 0).len() as u64, n - low);
    assert_eq!(collect(&log, 0)[0].offset, low);
    // Reading below low is out of range for replication.
    assert!(log.read_raw(0, 100).is_err());
}

#[test]
fn crash_with_fsync_keeps_every_acked_batch() {
    for tear in [0u64, 1, 17, 33, 100, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let acked;
        {
            let (_log, mut w) = PartitionLog::open(dir.path(), 0, cfg(16 * 1024)).unwrap();
            let mut n = 0;
            for _ in 0..10 {
                w.append_batch(1000, &section(n, 40, Compression::Lz4))
                    .unwrap();
                n += 40;
            }
            let a = w.prepare_commit(true).unwrap();
            run_ops(&a);
            w.publish_mark(a);
            acked = n;
            // Written (and maybe rolled), never fsynced: the crash tears the tail.
            for _ in 0..10 {
                w.append_batch(2000, &section(n, 40, Compression::None))
                    .unwrap();
                n += 40;
            }
            let _b = w.prepare_commit(true).unwrap();
        }
        let last = log_files(dir.path()).last().unwrap().clone();
        let len = fs::metadata(&last).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&last)
            .unwrap()
            .set_len(len.saturating_sub(tear))
            .unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(16 * 1024)).unwrap();
        let high = log.high();
        assert!(
            high >= acked,
            "tear {tear}: acked {acked} lost (high {high})"
        );
        assert_eq!(high % 40, 0, "only whole batches survive");
        check_all(&log, high);
        w.append_batch(3000, &section(high, 40, Compression::None))
            .unwrap();
        commit(&mut w);
        check_all(&log, high + 40);
    }
}

fn run_ops(m: &CommitMark) {
    for op in &m.ops {
        crate::fsync_pool::run_chain(std::slice::from_ref(op)).unwrap();
    }
}

#[test]
fn v06_data_dir_opens_and_continues_with_batches() {
    let dir = tempfile::tempdir().unwrap();
    // A v0.6 partition: legacy records only, several segments (the legacy write path is
    // byte-for-byte the v0.6 encoder).
    {
        let (_log, mut w) = PartitionLog::open(dir.path(), 0, cfg(2048)).unwrap();
        for i in 0..200 {
            w.append_legacy(1000 + i, pm(i)).unwrap();
            if i % 7 == 0 {
                commit(&mut w);
            }
        }
        commit(&mut w);
        w.close().unwrap();
    }
    assert!(log_files(dir.path()).len() > 3);
    // Check the fixture really is v0.6: no entry has bit 31.
    for f in log_files(dir.path()) {
        let bytes = Bytes::from(fs::read(&f).unwrap());
        let mut at = 0;
        while at < bytes.len() {
            let m = entry::parse_meta(&bytes[at..], true).unwrap();
            assert!(!m.is_batch());
            at += m.len;
        }
    }
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(2048)).unwrap();
    assert_eq!(log.high(), 200);
    check_all(&log, 200);
    w.append_messages(5000, (200..260).map(pm).collect())
        .unwrap();
    w.append_batch(5001, &section(260, 40, Compression::Zstd))
        .unwrap();
    commit(&mut w);
    check_all(&log, 300);
    drop(w);
    let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(2048)).unwrap();
    check_all(&log, 300);
    let c = log.read_envelopes(150, 1).unwrap().unwrap();
    assert_eq!((c.first_base, c.next_offset), (150, 151));
    assert!(!c.batches_only);
}

#[test]
fn batch_writes_off_writes_legacy_records() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    log.set_batch_writes(false);
    w.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    assert_eq!(
        w.append_batch(1000, &section(10, 20, Compression::Lz4))
            .unwrap(),
        (10, 20)
    );
    commit(&mut w);
    let c = log.read_envelopes(0, 1 << 20).unwrap().unwrap();
    assert_eq!(c.next_offset, 30);
    assert!(!c.batches_only);
    let bytes = c.bytes;
    let mut at = 0;
    while at < bytes.len() {
        let m = entry::parse_meta(&bytes[at..], true).unwrap();
        assert!(!m.is_batch(), "legacy only");
        at += m.len;
    }
    check_all(&log, 30);
}

#[test]
fn truncate_inside_envelope_rounds_down() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    w.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    w.append_messages(1001, (10..20).map(pm).collect()).unwrap();
    commit(&mut w);
    w.truncate(15).unwrap();
    assert_eq!((w.next_offset(), log.high()), (10, 10));
    check_all(&log, 10);
    w.append_messages(1002, (10..12).map(pm).collect()).unwrap();
    commit(&mut w);
    check_all(&log, 12);
    drop(w);
    let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    check_all(&log, 12);
}

#[test]
fn unsynced_reads_and_poison_on_failed_rollback_point() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    w.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    commit(&mut w);
    assert_eq!((log.high(), log.written_high()), (10, 10));
    w.append_messages(1001, (10..20).map(pm).collect()).unwrap();
    assert_eq!(log.written_high(), 10, "not written yet");
    let mark = w.prepare_commit(true).unwrap();
    assert_eq!((log.high(), log.written_high()), (10, 20));
    assert!(log.read_envelopes(10, 1 << 20).unwrap().is_none());
    let c = log.read_envelopes_unsynced(10, 1 << 20).unwrap().unwrap();
    assert_eq!((c.first_base, c.next_offset), (10, 20));
    assert!(log.read_envelopes_unsynced(20, 1).unwrap().is_none());
    // The fsync "fails": rollback lowers written_high and, because a follower may hold
    // 10..20, poisons the writer.
    drop(mark);
    w.rollback().unwrap();
    assert_eq!((log.high(), log.written_high()), (10, 10));
    assert!(w.append_messages(1002, vec![pm(10)]).is_err());

    // Without an unsynced read, a rollback keeps the writer usable (single node).
    let d2 = tempfile::tempdir().unwrap();
    let (log2, mut w2) = PartitionLog::open(d2.path(), 0, cfg(1 << 20)).unwrap();
    w2.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    let _m = w2.prepare_commit(true).unwrap();
    w2.rollback().unwrap();
    w2.append_messages(1000, (0..5).map(pm).collect()).unwrap();
    commit(&mut w2);
    check_all(&log2, 5);
}

#[tokio::test]
async fn written_notify_wakes_on_write() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    let l2 = log.clone();
    let waiter = tokio::spawn(async move {
        loop {
            let n = l2.written_notify().notified();
            tokio::pin!(n);
            n.as_mut().enable();
            if l2.written_high() >= 3 {
                return l2.written_high();
            }
            n.await;
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    w.append_messages(1000, (0..3).map(pm).collect()).unwrap();
    w.flush().unwrap();
    let got = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, 3);
}

#[test]
fn injected_fsync_failure_fails_the_commit_once() {
    let dir = tempfile::tempdir().unwrap();
    let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
    w.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    log.fail_next_fsync_for_tests();
    let m = w.prepare_commit(true).unwrap();
    let chains = crate::fsync_pool::chains_for(m.ops);
    let err = chains
        .iter()
        .map(|c| crate::fsync_pool::run_chain(c))
        .find_map(Result::err)
        .expect("injected failure");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
    w.rollback().unwrap();
    assert_eq!(log.high(), 0);
    // One-shot: the retry succeeds.
    w.append_messages(1000, (0..10).map(pm).collect()).unwrap();
    let m = w.prepare_commit(true).unwrap();
    for c in crate::fsync_pool::chains_for(m.ops.clone()) {
        crate::fsync_pool::run_chain(&c).unwrap();
    }
    w.publish_mark(m);
    check_all(&log, 10);
}
