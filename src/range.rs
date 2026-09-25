use std::{
    fs::File,
    io::{Error, ErrorKind, Result as IoResult},
    os::unix::fs::FileExt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use crc32fast::Hasher;
use futures_util::StreamExt;
use reqwest::header::{CONTENT_RANGE, RANGE};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    debug_println,
    http::{RangePrecondition, validate_content_range, validate_identity_encoding},
};

#[derive(Debug)]
pub(crate) enum Event {
    ChunkStarted(usize),
    ChunkDone(usize),
    Error(String),
}

#[derive(Debug)]
pub(crate) enum SchedToWorker {
    Assign(Vec<usize>),
    StealRequest { requester: usize },
    Stop,
}

#[derive(Debug)]
pub(crate) enum WorkerToSched {
    NeedWork {
        worker: usize,
    },
    Status {
        worker: usize,
        remaining_blocks: usize,
    },
    StealReply {
        victim: usize,
        requester: usize,
        stolen: Option<Vec<usize>>,
    },
}

pub(crate) fn block_byte_range(idx: usize, bytes_per_cell: u64, total_len: u64) -> (u64, u64) {
    let start = (idx as u64) * bytes_per_cell;
    let end = (start + bytes_per_cell - 1).min(total_len.saturating_sub(1));
    (start, end)
}

pub(crate) fn checksum_file_range(file: &File, start: u64, end: u64) -> anyhow::Result<u32> {
    if end < start {
        anyhow::bail!("invalid checksum range {start}-{end}");
    }
    let mut hasher = Hasher::new();
    let mut offset = start;
    let mut remaining = end - start + 1;
    let mut buffer = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let wanted = remaining.min(buffer.len() as u64) as usize;
        let read = read_exact_at(file, &mut buffer[..wanted], offset)?;
        if read == 0 {
            anyhow::bail!("unexpected EOF while checksumming output at byte {offset}");
        }
        hasher.update(&buffer[..read]);
        offset += read as u64;
        remaining -= read as u64;
    }
    Ok(hasher.finalize())
}

fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> IoResult<usize> {
    let mut total = 0;
    while total < buffer.len() {
        match file.read_at(&mut buffer[total..], offset + total as u64) {
            Ok(0) => break,
            Ok(read) => total += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    if total == 0 && !buffer.is_empty() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "short file read"));
    }
    Ok(total)
}

async fn checksum_block(file: Arc<File>, start: u64, end: u64) -> anyhow::Result<u32> {
    tokio::task::spawn_blocking(move || checksum_file_range(&file, start, end))
        .await
        .context("checksum task failed")?
}

pub(crate) async fn scheduler_loop(
    worker_count: usize,
    _active_cells: usize,
    to_workers: Vec<mpsc::UnboundedSender<SchedToWorker>>,
    to_sched_rx: &mut mpsc::UnboundedReceiver<WorkerToSched>,
    done: Arc<Mutex<Vec<bool>>>,
    initial_work: Vec<usize>,
    cancel: CancellationToken,
) {
    let mut remaining: Vec<usize> = vec![0; worker_count];
    let mut owner_assigned = false;

    while !cancel.is_cancelled() {
        // If everything is done, ask all workers to stop and exit scheduler.
        {
            let bm = done.lock().await;
            if bm.iter().all(|d| *d) {
                for tx in &to_workers {
                    let _ = tx.send(SchedToWorker::Stop);
                }
                break;
            }
        }

        let msg = tokio::select! {
            _ = cancel.cancelled() => break,
            m = to_sched_rx.recv() => m,
        };
        let Some(msg) = msg else {
            break;
        };
        match msg {
            WorkerToSched::NeedWork { worker } => {
                if worker >= worker_count {
                    continue;
                }

                // Ensure thread 0 is the initial owner (start 1 thread first).
                if !owner_assigned {
                    if worker == 0 {
                        owner_assigned = true;
                        remaining[0] = initial_work.len();
                        let _ = to_workers[0].send(SchedToWorker::Assign(initial_work.clone()));
                    }
                    continue;
                }

                // Find a victim with most remaining blocks.
                let mut best: Option<(usize, usize)> = None;
                for (tid, &remaining_blocks) in remaining.iter().enumerate() {
                    if tid == worker {
                        continue;
                    }
                    if remaining_blocks > best.map(|(_, count)| count).unwrap_or(0) {
                        best = Some((tid, remaining_blocks));
                    }
                }
                if let Some((victim, remaining_blocks)) = best
                    && remaining_blocks >= 2
                {
                    let _ =
                        to_workers[victim].send(SchedToWorker::StealRequest { requester: worker });
                }
            }
            WorkerToSched::Status {
                worker,
                remaining_blocks,
            } => {
                if worker >= worker_count {
                    continue;
                }
                remaining[worker] = remaining_blocks;
            }
            WorkerToSched::StealReply {
                victim,
                requester,
                stolen,
            } => {
                if victim >= worker_count || requester >= worker_count {
                    continue;
                }
                if let Some(blocks) = stolen {
                    remaining[requester] = blocks.len();
                    // victim should send Status itself; we don't force-update victim here
                    let _ = to_workers[requester].send(SchedToWorker::Assign(blocks));
                }
            }
        }
    }
}

pub(crate) fn find_all_undone_blocks(done: &[bool]) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, d) in done.iter().enumerate() {
        if !*d {
            out.push(i);
        }
    }
    out
}

pub(crate) struct WorkerContext {
    pub(crate) active_cells: usize,
    pub(crate) bytes_per_cell: u64,
    pub(crate) total_len: u64,
    pub(crate) client: reqwest::Client,
    pub(crate) url: String,
    pub(crate) debug: bool,
    pub(crate) file: Arc<std::fs::File>,
    pub(crate) done: Arc<Mutex<Vec<bool>>>,
    pub(crate) block_checksums: Arc<Mutex<Vec<Option<u32>>>>,
    pub(crate) meta_dirty: Arc<AtomicU64>,
    pub(crate) completed_bytes: Arc<AtomicU64>,
    pub(crate) received_bytes: Arc<AtomicU64>,
    pub(crate) global_received: Option<Arc<AtomicU64>>,
    pub(crate) active_conns: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
    pub(crate) last_progress: Arc<Mutex<Instant>>,
    pub(crate) last_error: Arc<Mutex<String>>,
    pub(crate) cancel: CancellationToken,
    pub(crate) evt_tx: mpsc::UnboundedSender<Event>,
    pub(crate) per_read_timeout: Duration,
    pub(crate) backoff: Duration,
    pub(crate) precondition: RangePrecondition,
}

pub(crate) async fn worker_loop(
    tid: usize,
    context: WorkerContext,
    mut to_worker_rx: mpsc::UnboundedReceiver<SchedToWorker>,
    to_sched_tx: mpsc::UnboundedSender<WorkerToSched>,
    enabled_rx: &mut watch::Receiver<u64>,
) {
    let WorkerContext {
        active_cells,
        bytes_per_cell,
        total_len,
        client,
        url,
        debug,
        file,
        done,
        block_checksums,
        meta_dirty,
        completed_bytes,
        received_bytes,
        global_received,
        active_conns,
        errors,
        last_progress,
        last_error,
        cancel,
        evt_tx,
        per_read_timeout,
        backoff,
        precondition,
    } = context;
    use std::collections::VecDeque;
    let mut queue: VecDeque<usize> = VecDeque::new();
    let mut current: Option<usize> = None;
    let mut started_current = false;
    let mut pos: u64 = 0; // next file offset to write for current block
    let mut requested_work = false;
    let mut stash: Vec<u8> = Vec::new(); // overread buffer

    // Keep queue sorted/deduped; block counts are small (<= a few thousand), so O(n log n) is fine.
    fn push_blocks(queue: &mut VecDeque<usize>, mut blocks: Vec<usize>) {
        if blocks.is_empty() {
            return;
        }
        let mut all: Vec<usize> = queue.iter().copied().collect();
        all.append(&mut blocks);
        all.sort_unstable();
        all.dedup();
        *queue = all.into();
    }

    fn remaining_blocks_count(queue: &VecDeque<usize>, current: Option<usize>) -> usize {
        queue.len() + current.map(|_| 1).unwrap_or(0)
    }

    // Select next unfinished block (skips blocks already marked done).
    async fn pop_next_undone(
        queue: &mut VecDeque<usize>,
        done: &Arc<Mutex<Vec<bool>>>,
        active_cells: usize,
    ) -> Option<usize> {
        loop {
            let b = queue.pop_front()?;
            if b >= active_cells {
                continue;
            }
            let bm = done.lock().await;
            if bm.get(b).copied().unwrap_or(true) {
                continue;
            }
            return Some(b);
        }
    }

    // Steal tail-half of remaining blocks (excluding current).
    async fn steal_tail_half_blocks(
        queue: &mut VecDeque<usize>,
        done: &Arc<Mutex<Vec<bool>>>,
        active_cells: usize,
    ) -> Option<Vec<usize>> {
        // Filter any done/out-of-range blocks (best-effort cleanup).
        {
            let bm = done.lock().await;
            let mut kept: Vec<usize> = Vec::with_capacity(queue.len());
            for &b in queue.iter() {
                if b < active_cells && !bm.get(b).copied().unwrap_or(true) {
                    kept.push(b);
                }
            }
            kept.sort_unstable();
            kept.dedup();
            *queue = kept.into();
        }

        let rem = queue.len();
        if rem < 2 {
            return None;
        }
        let steal = rem / 2; // second half (tail)
        if steal == 0 {
            return None;
        }
        let mut stolen = Vec::with_capacity(steal);
        for _ in 0..steal {
            if let Some(x) = queue.pop_back() {
                stolen.push(x);
            }
        }
        stolen.sort_unstable();
        if stolen.is_empty() {
            None
        } else {
            Some(stolen)
        }
    }

    while !cancel.is_cancelled() {
        // Dynamic enabling: only start "stealing/working" once enabled_count > tid.
        while (*enabled_rx.borrow() as usize) <= tid {
            let wait = tokio::select! {
                _ = cancel.cancelled() => return,
                r = enabled_rx.changed() => r,
            };
            if wait.is_err() {
                return;
            }
        }

        // Ask scheduler for work if idle.
        if current.is_none() && queue.is_empty() {
            // If we were reduced below enabled_count, retire on idle (don't steal).
            if (*enabled_rx.borrow() as usize) <= tid {
                return;
            }
            if !requested_work {
                let _ = to_sched_tx.send(WorkerToSched::NeedWork { worker: tid });
                requested_work = true;
            }
            // If no work arrives, back off and ask again (keeps the worker responsive but not busy-looping).
            let msg = tokio::select! {
                _ = cancel.cancelled() => break,
                message = to_worker_rx.recv() => {
                    let Some(message) = message else { break };
                    message
                },
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    requested_work = false;
                    continue;
                },
            };
            match msg {
                SchedToWorker::Assign(blocks) => {
                    push_blocks(&mut queue, blocks);
                    requested_work = false;
                }
                SchedToWorker::Stop => break,
                SchedToWorker::StealRequest { requester } => {
                    let _ = to_sched_tx.send(WorkerToSched::StealReply {
                        victim: tid,
                        requester,
                        stolen: None,
                    });
                }
            }
            continue;
        }

        // Ensure we have a current block.
        if current.is_none() {
            current = pop_next_undone(&mut queue, &done, active_cells).await;
            started_current = false;
            stash.clear();
            if let Some(b) = current {
                let (p, _) = block_byte_range(b, bytes_per_cell, total_len);
                pos = p;
            } else {
                continue;
            }
        }

        let cur = current.unwrap();
        let (cur_start, _cur_end) = block_byte_range(cur, bytes_per_cell, total_len);
        if pos < cur_start {
            pos = cur_start;
        }

        let _ = to_sched_tx.send(WorkerToSched::Status {
            worker: tid,
            remaining_blocks: remaining_blocks_count(&queue, current),
        });

        // Open-ended stream from current position; we decide when to close based on block continuity.
        'conn: loop {
            if cancel.is_cancelled() {
                break;
            }
            let range_header = format!("bytes={}-", pos);
            debug_println(
                debug,
                format!(
                    "worker {tid}: connect pos={pos} range={range_header} current={current:?} qlen={}",
                    queue.len()
                ),
            );
            // Apply per-thread timeout to connection setup / request send as well (DNS/connect/TLS/headers).
            let resp = tokio::time::timeout(
                per_read_timeout,
                client
                    .get(&url)
                    .header(RANGE, range_header)
                    .header(precondition.name.clone(), precondition.value.clone())
                    .send(),
            )
            .await;
            let resp = match resp {
                Ok(r) => r,
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    *last_error.lock().await = format!(
                        "thread timeout: request setup/send exceeded {:.0}s",
                        per_read_timeout.as_secs_f64()
                    );
                    debug_println(
                        debug,
                        format!(
                            "worker {tid}: timeout during request setup/send (>{:.0}s), will retry",
                            per_read_timeout.as_secs_f64()
                        ),
                    );
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    continue 'conn;
                }
            };
            let resp = match resp {
                Ok(rsp) => rsp,
                Err(e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    *last_error.lock().await = format!("request error: {e}");
                    debug_println(debug, format!("worker {tid}: request error: {e}"));
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    continue 'conn;
                }
            };
            debug_println(
                debug,
                format!("worker {tid}: response status={}", resp.status()),
            );
            if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                let _ = evt_tx.send(Event::Error(format!(
                    "server did not return 206 Partial Content for bytes={}- (status={})",
                    pos,
                    resp.status()
                )));
                debug_println(
                    debug,
                    format!(
                        "worker {tid}: expected 206 Partial Content, got status={} (pos={pos})",
                        resp.status()
                    ),
                );
                cancel.cancel();
                break;
            }
            if let Err(e) =
                validate_content_range(resp.headers().get(CONTENT_RANGE), pos, total_len)
            {
                let _ = evt_tx.send(Event::Error(format!("{e:#}")));
                debug_println(debug, format!("worker {tid}: invalid Content-Range: {e:#}"));
                cancel.cancel();
                break;
            }
            if let Err(e) = validate_identity_encoding(resp.headers()) {
                let _ = evt_tx.send(Event::Error(format!("{e:#}")));
                debug_println(
                    debug,
                    format!("worker {tid}: invalid Content-Encoding: {e:#}"),
                );
                cancel.cancel();
                break;
            }
            let mut stream = resp.bytes_stream();
            struct ActiveGuard {
                ctr: Arc<AtomicU64>,
            }
            impl ActiveGuard {
                fn new(ctr: Arc<AtomicU64>) -> Self {
                    ctr.fetch_add(1, Ordering::Relaxed);
                    Self { ctr }
                }
            }
            impl Drop for ActiveGuard {
                fn drop(&mut self) {
                    self.ctr.fetch_sub(1, Ordering::Relaxed);
                }
            }
            // A worker is considered "active" as long as it's inside the read/write loop for a connection.
            let _active_guard = ActiveGuard::new(active_conns.clone());

            // Inner read loop
            let mut last_worker_progress = Instant::now();
            loop {
                if cancel.is_cancelled() {
                    break 'conn;
                }

                tokio::select! {
                            _ = cancel.cancelled() => break 'conn,
                            msg = to_worker_rx.recv() => {
                                match msg {
                                    Some(SchedToWorker::StealRequest { requester }) => {
                                        let stolen = steal_tail_half_blocks(&mut queue, &done, active_cells).await;
                                        let _ = to_sched_tx.send(WorkerToSched::StealReply { victim: tid, requester, stolen });
                                        let _ = to_sched_tx.send(WorkerToSched::Status { worker: tid, remaining_blocks: remaining_blocks_count(&queue, current) });
                                    }
                                    Some(SchedToWorker::Assign(blocks)) => {
                                        push_blocks(&mut queue, blocks);
                                    }
                                    Some(SchedToWorker::Stop) | None => break 'conn,
                                }
                            }
                            next = tokio::time::timeout(
                                per_read_timeout.saturating_sub(last_worker_progress.elapsed()),
                                stream.next()
                            ) => {
                                let item = match next {
                                    Ok(v) => v,
                                    Err(_) => {
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        *last_error.lock().await = format!("thread timeout: no bytes received for {:.0}s", per_read_timeout.as_secs_f64());
                                        if !backoff.is_zero() { tokio::time::sleep(backoff).await; }
                                        break; // reopen connection at current pos
                                    }
                                };
                                let bytes = match item {
                                    Some(Ok(b)) => b,
                                    Some(Err(e)) => {
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        *last_error.lock().await = format!("bytes stream error: {e}");
                                        if !backoff.is_zero() { tokio::time::sleep(backoff).await; }
                                        break;
                                    }
                                    None => {
                                        // EOF: if we're done with all work, exit; otherwise retry.
                                        if current.is_none() && queue.is_empty() {
                                            break 'conn;
                                        }
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        *last_error.lock().await = "unexpected EOF".to_string();
                                        if !backoff.is_zero() { tokio::time::sleep(backoff).await; }
                                        break;
                                    }
                                };
                                if bytes.is_empty() {
                                    continue;
                                }

                                last_worker_progress = Instant::now();
                                received_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                                if let Some(g) = global_received.as_ref() {
                                    g.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                                }
                                *last_progress.lock().await = Instant::now();
                                stash.extend_from_slice(&bytes);

                                // Consume stash across current and possibly subsequent contiguous blocks.
                                loop {
                                    if stash.is_empty() {
                                        break;
                                    }
                                    let Some(cur) = current else { stash.clear(); break; };

                                    let (cur_start, cur_end) = block_byte_range(cur, bytes_per_cell, total_len);
                                    if pos < cur_start { pos = cur_start; }

                                    // If current is already complete, mark done and advance.
                                    if pos > cur_end {
                                        let checksum = match checksum_block(file.clone(), cur_start, cur_end).await {
                                            Ok(checksum) => checksum,
                                            Err(error) => {
                                                let _ = evt_tx.send(Event::Error(format!("failed checksumming block {cur}: {error:#}")));
                                                cancel.cancel();
                                                break 'conn;
                                            }
                                        };
                                        {
                                            let mut checksums = block_checksums.lock().await;
                                            checksums[cur] = Some(checksum);
                                        }
                                        {
                                            let mut bm = done.lock().await;
                                            if cur < bm.len() && !bm[cur] {
                                                bm[cur] = true;
                                                meta_dirty.fetch_add(1, Ordering::Relaxed);
                                                completed_bytes.fetch_add(cur_end - cur_start + 1, Ordering::Relaxed);
                                            }
                                        }
                                        let _ = evt_tx.send(Event::ChunkDone(cur));
                                        current = pop_next_undone(&mut queue, &done, active_cells).await;
                                        started_current = false;
                                        if let Some(n) = current {
                                            let (p, _) = block_byte_range(n, bytes_per_cell, total_len);
                                            pos = p;
                                        }
                                        continue;
                                    }

                                    let need = (cur_end + 1 - pos) as usize;
                                    let take = need.min(stash.len());
                                    if take == 0 {
                                        break;
                                    }

                                    if !started_current {
                                        let _ = evt_tx.send(Event::ChunkStarted(cur));
                                        started_current = true;
                                    }

                                    let f = file.clone();
                                    let data = stash[..take].to_vec();
                                    let off = pos;
                                    let write_result = tokio::task::spawn_blocking(move || {
                                        f.write_all_at(&data, off)
                                            .context("failed writing to output file")?;
                                        Ok::<(), anyhow::Error>(())
                                    }).await;
                                    match write_result {
                                        Ok(Ok(())) => {}
                                        Ok(Err(e)) => {
                                            let _ = evt_tx.send(Event::Error(format!("{e:#}")));
                                            cancel.cancel();
                                            break 'conn;
                                        }
                                        Err(e) => {
                                            let _ = evt_tx.send(Event::Error(format!("write task failed: {e}")));
                                            cancel.cancel();
                                            break 'conn;
                                        }
                                    }

                                    pos += take as u64;
                                    stash.drain(..take);

                                    if pos == cur_end + 1 {
                                        // Completed this block.
                                        let checksum = match checksum_block(file.clone(), cur_start, cur_end).await {
                                            Ok(checksum) => checksum,
                                            Err(error) => {
                                                let _ = evt_tx.send(Event::Error(format!("failed checksumming block {cur}: {error:#}")));
                                                cancel.cancel();
                                                break 'conn;
                                            }
                                        };
                                        {
                                            let mut checksums = block_checksums.lock().await;
                                            checksums[cur] = Some(checksum);
                                        }
                                        {
                                            let mut bm = done.lock().await;
                                            if cur < bm.len() && !bm[cur] {
                                                bm[cur] = true;
                                                meta_dirty.fetch_add(1, Ordering::Relaxed);
                                                completed_bytes.fetch_add(cur_end - cur_start + 1, Ordering::Relaxed);
                                            }
                                        }
                                        let _ = evt_tx.send(Event::ChunkDone(cur));
                                        started_current = false;

                                        // Decide next block behavior with overread.
                                        let next = pop_next_undone(&mut queue, &done, active_cells).await;
                                        if let Some(nb) = next {
                                            if nb == cur + 1 {
                                                // continuous: keep connection and use overread.
                                                current = Some(nb);
                                                let (p, _) = block_byte_range(nb, bytes_per_cell, total_len);
                                                pos = p; // should match cur_end+1
                                                continue;
                                            } else {
                                                // discontinuous: discard overread, close connection, restart at next block.
                                                stash.clear();
                                                current = Some(nb);
                                                let (p, _) = block_byte_range(nb, bytes_per_cell, total_len);
                                                pos = p;
                                                break 'conn;
                                            }
                                        } else {
                                            // No more work.
                                            stash.clear();
                                            current = None;
                                            break 'conn;
                                        }
                                    }
                                }
                            }

                // Loop back to either continue with current block (reopened connection) or ask for more work.
                        }
            }
        }

        requested_work = false;
    }
}
