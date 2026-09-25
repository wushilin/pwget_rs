use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::IsTerminal,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use crossterm::{
    cursor,
    event::{self, Event as CEvent, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend, layout::Rect};
use reqwest::header::{ACCEPT_ENCODING, HeaderValue};
use tokio::io::AsyncWriteExt;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

mod batch;
mod cli;
mod http;
mod meta;
mod output;
mod progress;
mod range;

use crate::batch::parse_url_list;
use crate::cli::{Cli, parse_extra_headers};
use crate::http::{
    StreamFallbackReason, probe_len_and_ranges, range_precondition, validate_identity_encoding,
};
use crate::meta::{Meta, load_meta_if_present, write_meta_atomic};
use crate::output::{
    default_output_path, ensure_output_parent, filename_from_content_disposition, resolve_save_as,
    sanitize_output_filename,
};
use crate::progress::{
    CellState, ProgressStats, RollingSpeed, format_noui_status_line, format_stream_status_line,
    human_bytes, render_grid, render_status,
};
use crate::range::{
    Event, SchedToWorker, WorkerContext, WorkerToSched, block_byte_range, checksum_file_range,
    find_all_undone_blocks, scheduler_loop, worker_loop,
};

pub(crate) fn debug_println(enabled: bool, msg: impl AsRef<str>) {
    if !enabled {
        return;
    }
    let msg = msg.as_ref();
    let mut stderr = std::io::stderr();
    if stderr.is_terminal() {
        let _ = write!(stderr, "\r\x1b[2K[debug] {msg}\r\n");
        let _ = stderr.flush();
    } else {
        eprintln!("[debug] {msg}");
    }
}

const MIN_TICK: Duration = Duration::from_millis(33);

async fn shutdown_signal() -> Result<()> {
    // Always handle Ctrl-C.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    #[cfg(unix)]
    {
        let mut term =
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
        let mut hup = signal(SignalKind::hangup()).context("failed to install SIGHUP handler")?;
        tokio::select! {
            _ = &mut ctrl_c => {},
            _ = term.recv() => {},
            _ = hup.recv() => {},
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct DownloadReport {
    url: String,
    output_path: PathBuf,
    total_bytes: u64,
    session_downloaded: u64,
    elapsed: Duration,
}

#[derive(Debug, Clone)]
struct BatchProgress {
    total_tasks: usize,
    done_tasks: Arc<AtomicU64>,
    // Bytes downloaded by completed successful tasks (optional bookkeeping).
    total_downloaded: Arc<AtomicU64>,
    global_received: Arc<AtomicU64>,
    active_downloads: Arc<AtomicU64>,
}

type OutputClaims = Arc<Mutex<HashMap<PathBuf, String>>>;

#[derive(Clone, Default)]
struct DownloadOptions {
    force_noui: bool,
    batch: Option<BatchProgress>,
    quiet_summary: bool,
    output_override: Option<PathBuf>,
    output_claims: Option<OutputClaims>,
}

fn confirm_overwrite(path: &Path, yes: bool) -> Result<()> {
    if yes {
        return Ok(());
    }
    eprintln!("output file already exists: {}", path.display());
    if !std::io::stdin().is_terminal() {
        bail!("refusing to overwrite non-interactively; rerun with --yes to proceed");
    }
    eprint!("Overwrite? [y/N]: ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).ok();
    let ans = line.trim().to_ascii_lowercase();
    if ans == "y" || ans == "yes" {
        Ok(())
    } else {
        bail!("aborted by user");
    }
}

struct TermGuard;
impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
        let mut stdout = std::io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen, cursor::Show);
    }
}

struct RawModeGuard;
impl RawModeGuard {
    fn new() -> Result<Self> {
        terminal::enable_raw_mode().context("failed to enable raw mode")?;
        Ok(Self)
    }
}
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
    }
}

fn cancellation_error(events: &mut mpsc::UnboundedReceiver<Event>) -> anyhow::Error {
    while let Ok(event) = events.try_recv() {
        if let Event::Error(message) = event {
            return anyhow!(message);
        }
    }
    anyhow!("cancelled")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.hard_limit > 20 {
        eprintln!(
            "warning: -N {} is high (> 20) and may spam the server / get you rate-limited.",
            cli.hard_limit
        );
    }

    let (ua, mut extra_headers) = parse_extra_headers(&cli)?;
    extra_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    if cli.debug {
        debug_println(true, "debug enabled: forcing --noui (disables TUI)");
        debug_println(
            true,
            format!(
                "http config: ua={:?}  extra_headers=[{}]",
                ua.as_deref().unwrap_or("pwget/0.1"),
                extra_headers
                    .iter()
                    .map(|(k, _)| format!("{k}=<redacted>"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }

    // Shared HTTP client.
    let mut builder = reqwest::Client::builder()
        .default_headers(extra_headers)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        // Make connection setup (DNS/TCP/TLS) fail fast per thread timeout.
        .connect_timeout(Duration::from_secs(cli.thread_timeout_secs.max(1)))
        .pool_max_idle_per_host(cli.hard_limit.max(1) * cli.parallel.max(1));
    builder = builder.user_agent(ua.unwrap_or_else(|| "pwget/0.1".to_string()));
    let client = builder.build().context("failed to build HTTP client")?;

    // Batch mode (-T): force --noui and require -d.
    if let Some(list_path) = cli.url_list.as_ref() {
        if cli.meta.is_some() {
            bail!("--meta is not supported with -T/--urllist (each URL uses its own OUTPUT.meta)");
        }
        let dir = cli
            .download_dir
            .as_ref()
            .ok_or_else(|| anyhow!("-T requires -d"))?
            .clone();
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("failed to create download dir {}", dir.display()))?;

        let text = tokio::fs::read_to_string(list_path)
            .await
            .with_context(|| format!("failed to read urllist {}", list_path.display()))?;
        let items = parse_url_list(&text, cli.list_format).with_context(|| {
            format!(
                "failed parsing urllist {} as {:?}",
                list_path.display(),
                cli.list_format
            )
        })?;
        if items.is_empty() {
            bail!("urllist is empty: {}", list_path.display());
        }

        let batch = BatchProgress {
            total_tasks: items.len(),
            done_tasks: Arc::new(AtomicU64::new(0)),
            total_downloaded: Arc::new(AtomicU64::new(0)),
            global_received: Arc::new(AtomicU64::new(0)),
            active_downloads: Arc::new(AtomicU64::new(0)),
        };
        let batch_start = Instant::now();
        let mut failed: Vec<(String, String)> = Vec::new();

        eprintln!(
            "batch: {} urls, saving into {} (noui mode)",
            batch.total_tasks,
            dir.display()
        );

        let parallel = cli.parallel.max(1);
        let sem = Arc::new(tokio::sync::Semaphore::new(parallel));
        let output_claims: OutputClaims = Arc::new(Mutex::new(HashMap::new()));

        // Global batch status reporter (single-line).
        let status_cancel = CancellationToken::new();
        let status_cancel2 = status_cancel.clone();
        let batch2 = batch.clone();
        let status_handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            let mut last_total = 0u64;
            let start = Instant::now();
            loop {
                tokio::select! {
                    _ = status_cancel2.cancelled() => break,
                    _ = tick.tick() => {}
                }
                let done = batch2.done_tasks.load(Ordering::Relaxed) as usize;
                let remaining = batch2.total_tasks.saturating_sub(done);
                let active = batch2.active_downloads.load(Ordering::Relaxed);
                let total_dl = batch2.global_received.load(Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64().max(0.001);
                let inst_bps = {
                    let cur = total_dl;
                    let delta = cur.saturating_sub(last_total);
                    last_total = cur;
                    // 250ms tick
                    (delta as f64) / 0.25
                };
                let avg_bps = (total_dl as f64) / elapsed;
                let line = format!(
                    "tasks {done}/{total}  remaining {remaining}  active {active}  downloaded {}  speed {}/s  avg {}/s",
                    human_bytes(total_dl as f64),
                    human_bytes(inst_bps),
                    human_bytes(avg_bps),
                    total = batch2.total_tasks,
                );
                let mut stderr = std::io::stderr();
                let _ = write!(stderr, "\r\x1b[2K{line}");
                let _ = stderr.flush();
            }
            // newline after exiting
            eprintln!();
        });

        let mut joinset = tokio::task::JoinSet::new();
        for item in items {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let cli_ref = cli.clone();
            let client = client.clone();
            let dir = dir.clone();
            let batch = batch.clone();
            let output_claims = output_claims.clone();
            joinset.spawn(async move {
                let _permit = permit;
                let output_override = match item.save_as.as_deref() {
                    Some(sa) => match resolve_save_as(&dir, sa) {
                        Ok(p) => Some(p),
                        Err(e) => return (item.url, Err(e)),
                    },
                    None => None,
                };
                if let Some(parent) = output_override.as_deref().and_then(Path::parent)
                    && let Err(error) = tokio::fs::create_dir_all(parent).await
                {
                    return (
                        item.url,
                        Err(anyhow!(error)
                            .context(format!("failed to create parent dir {}", parent.display()))),
                    );
                }
                let res = run_one_download(
                    &cli_ref,
                    client,
                    item.url.clone(),
                    Some(dir),
                    DownloadOptions {
                        force_noui: true,
                        batch: Some(batch.clone()),
                        quiet_summary: true,
                        output_override,
                        output_claims: Some(output_claims),
                    },
                )
                .await;
                (item.url, res)
            });
        }

        while let Some(res) = joinset.join_next().await {
            match res {
                Ok((_url, Ok(rep))) => {
                    batch
                        .total_downloaded
                        .fetch_add(rep.session_downloaded, Ordering::Relaxed);
                    batch.done_tasks.fetch_add(1, Ordering::Relaxed);
                }
                Ok((url, Err(e))) => {
                    failed.push((url, format!("{e:#}")));
                    batch.done_tasks.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    failed.push(("<join_error>".to_string(), format!("{e:#}")));
                    batch.done_tasks.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        status_cancel.cancel();
        let _ = status_handle.await;

        let elapsed = batch_start.elapsed().as_secs_f64().max(0.001);
        let total_dl = batch.global_received.load(Ordering::Relaxed);
        let avg_bps = (total_dl as f64) / elapsed;

        let mut out = String::new();
        out.push_str(&format!(
            "summary: done {}/{}  downloaded {}  time {:.2}s  avg_speed {}/s\n",
            batch.total_tasks,
            batch.total_tasks,
            human_bytes(total_dl as f64),
            elapsed,
            human_bytes(avg_bps)
        ));
        let failed_count = failed.len();
        if failed_count > 0 {
            out.push_str(&format!("failed downloads ({}):\n", failed.len()));
            for (u, err) in failed {
                out.push_str(&format!("- {u}\n  {err}\n"));
            }
        }

        // Ensure CRLF + clear-line, since batch mode uses an in-place status line.
        if std::io::stderr().is_terminal() {
            let mut stderr = std::io::stderr();
            let _ = write!(stderr, "\r\x1b[2K\r\n{}", out.replace('\n', "\r\n"));
            let _ = stderr.flush();
        } else {
            eprintln!("{out}");
        }
        if failed_count > 0 {
            bail!("{failed_count} batch download(s) failed");
        }
        return Ok(());
    }

    let url = cli
        .url
        .clone()
        .ok_or_else(|| anyhow!("missing URL (or use -T for batch mode)"))?;
    let _rep = run_one_download(
        &cli,
        client.clone(),
        url,
        cli.download_dir.clone(),
        DownloadOptions::default(),
    )
    .await?;
    Ok(())
}

async fn run_one_download(
    cli: &Cli,
    client: reqwest::Client,
    url: String,
    download_dir: Option<PathBuf>,
    options: DownloadOptions,
) -> Result<DownloadReport> {
    let DownloadOptions {
        force_noui,
        batch,
        quiet_summary,
        output_override,
        output_claims,
    } = options;
    // Batch bookkeeping: track how many downloads are currently active.
    struct ActiveDlGuard {
        ctr: Arc<AtomicU64>,
    }
    impl Drop for ActiveDlGuard {
        fn drop(&mut self) {
            self.ctr.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _active_dl_guard = batch.as_ref().map(|b| {
        b.active_downloads.fetch_add(1, Ordering::Relaxed);
        ActiveDlGuard {
            ctr: b.active_downloads.clone(),
        }
    });

    // In batch / non-interactive environments we may not have a real TTY; fall back to a sane size.
    let (cols, rows) = terminal::size().unwrap_or((80, 24));
    // Avoid drawing into the terminal's last column to prevent line-wrapping artifacts that
    // can show up as "blank/black rows" in some terminals.
    let usable_cols = cols.saturating_sub(1);
    if rows < 2 || usable_cols < 1 {
        bail!("terminal is too small (need at least 2 cols and 2 rows)");
    }
    let grid_rows_live = (rows - 1) as usize;
    let grid_cols_live = usable_cols as usize;
    let cell_capacity_live = grid_rows_live * grid_cols_live;

    let probe = probe_len_and_ranges(&client, &url, cli.debug).await?;
    let ranges_ok = probe.ranges_ok;

    debug_println(
        cli.debug,
        format!(
            "probe result: len={:?} ranges_ok={ranges_ok} content_disposition={:?}",
            probe.len, probe.content_disposition
        ),
    );

    let output_user_specified = cli.output.is_some() || output_override.is_some();
    let output_path = if let Some(p) = output_override.clone() {
        p
    } else if let Some(p) = cli.output.clone() {
        p
    } else if let Some(fname) = probe
        .content_disposition
        .as_deref()
        .and_then(filename_from_content_disposition)
    {
        PathBuf::from(fname)
    } else {
        default_output_path(&url)
    };
    let output_path = sanitize_output_filename(&output_path, output_user_specified);
    let output_path = if !output_user_specified {
        if let Some(dir) = download_dir.as_ref() {
            dir.join(&output_path)
        } else {
            output_path
        }
    } else {
        output_path
    };

    if let Some(claims) = output_claims.as_ref() {
        let mut claims = claims.lock().await;
        if let Some(first_url) = claims.get(&output_path) {
            bail!(
                "batch output collision: {url} and {first_url} both resolve to {}",
                output_path.display()
            );
        }
        claims.insert(output_path.clone(), url.clone());
    }

    if probe.len == Some(0) {
        if std::fs::metadata(&output_path).is_ok() {
            if batch.is_some() && !cli.yes {
                bail!(
                    "output exists: {} (rerun with --yes to overwrite in batch mode)",
                    output_path.display()
                );
            }
            confirm_overwrite(&output_path, cli.yes)?;
        }
        ensure_output_parent(&output_path).await?;
        tokio::fs::write(&output_path, [])
            .await
            .with_context(|| format!("failed to create empty output: {}", output_path.display()))?;
        let meta_path = cli
            .meta
            .clone()
            .unwrap_or_else(|| Meta::meta_path_for_output(&output_path));
        match tokio::fs::remove_file(&meta_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(anyhow!(error).context(format!(
                    "failed removing stale metadata {}",
                    meta_path.display()
                )));
            }
        }
        if !quiet_summary {
            eprintln!(
                "saved: {}\n  total_bytes: 0B\n  session_downloaded: 0B\n  time: 0.00s\n  avg_speed: 0B/s",
                output_path.display()
            );
        }
        return Ok(DownloadReport {
            url,
            output_path,
            total_bytes: 0,
            session_downloaded: 0,
            elapsed: Duration::ZERO,
        });
    }

    let precondition = range_precondition(&probe);

    let stream_reason = if probe.len.is_none() {
        Some(StreamFallbackReason::UnknownLength)
    } else if !ranges_ok {
        Some(StreamFallbackReason::RangesUnavailable)
    } else if precondition.is_none() {
        Some(StreamFallbackReason::MissingValidator)
    } else {
        None
    };

    if let Some(stream_reason) = stream_reason {
        if cli.meta.is_some() {
            bail!(
                "--meta requires a known Content-Length, byte range support, and a stable validator"
            );
        }
        debug_println(
            cli.debug,
            format!(
                "falling back to sequential stream mode: {} (no resume/grid range download)",
                stream_reason.label()
            ),
        );
        return run_stream_download(
            cli,
            client,
            url,
            output_path,
            StreamOptions {
                len_hint: probe.len,
                reason: stream_reason,
                batch,
                quiet_summary,
            },
        )
        .await;
    }

    let len = probe.len.unwrap();

    let meta_path = cli
        .meta
        .clone()
        .unwrap_or_else(|| Meta::meta_path_for_output(&output_path));

    // Load meta (if present) to resume; otherwise compute layout from current terminal.
    let mut meta: Option<Meta> = load_meta_if_present(&meta_path).await?;
    if let Some(m) = meta.as_mut() {
        // Validate against current run
        if m.version != 2 && m.version != 3 {
            bail!("unsupported meta version: {}", m.version);
        }
        if m.total_len != len {
            bail!(
                "meta total size mismatch: meta has {}, but server reports {}",
                m.total_len,
                len
            );
        }
        if m.blocks_used == 0 {
            bail!("meta has invalid blocks_used=0");
        }
        if m.done.len() != m.blocks_used {
            bail!(
                "meta done bitmap length mismatch: done={} blocks_used={}",
                m.done.len(),
                m.blocks_used
            );
        }
        if !m.block_checksums.is_empty() && m.block_checksums.len() != m.blocks_used {
            bail!(
                "meta checksum length mismatch: checksums={} blocks_used={}",
                m.block_checksums.len(),
                m.blocks_used
            );
        }
        if m.block_checksums.is_empty() {
            m.block_checksums.resize(m.blocks_used, None);
        }
        if Path::new(&m.output) != output_path {
            bail!(
                "meta output mismatch: meta is for {}, current output is {}",
                m.output,
                output_path.display()
            );
        }
        let output_metadata = std::fs::metadata(&output_path).with_context(|| {
            format!(
                "cannot resume because output file is missing or inaccessible: {}",
                output_path.display()
            )
        })?;
        if !output_metadata.is_file() {
            bail!(
                "cannot resume because output is not a regular file: {}",
                output_path.display()
            );
        }
        if output_metadata.len() != len {
            bail!(
                "cannot resume because output size is {}, expected {}: {}",
                output_metadata.len(),
                len,
                output_path.display()
            );
        }
        // Legacy metadata has no checksums, so completed blocks must be fetched again.
        // For current metadata, detect same-length corruption before trusting a done bit.
        let checksum_file = OpenOptions::new()
            .read(true)
            .open(&output_path)
            .with_context(|| format!("failed opening resume output {}", output_path.display()))?;
        for idx in 0..m.blocks_used {
            if !m.done[idx] {
                continue;
            }
            let (start, end) = block_byte_range(idx, len.div_ceil(m.blocks_used as u64), len);
            let Some(expected) = m.block_checksums[idx] else {
                m.done[idx] = false;
                continue;
            };
            let actual = checksum_file_range(&checksum_file, start, end)?;
            if actual != expected {
                m.done[idx] = false;
                m.block_checksums[idx] = None;
            }
        }
        m.version = 3;
        if let Some(meta_url) = m.url.as_ref()
            && meta_url != &url
        {
            bail!("meta URL mismatch: meta is for {meta_url}, current URL is {url}");
        }
        if let Some(meta_etag) = m.etag.as_ref()
            && probe.etag.as_ref() != Some(meta_etag)
        {
            bail!(
                "meta ETag mismatch: meta has {:?}, server reports {:?}",
                meta_etag,
                probe.etag
            );
        }
        if let Some(meta_last_modified) = m.last_modified.as_ref()
            && probe.last_modified.as_ref() != Some(meta_last_modified)
        {
            bail!(
                "meta Last-Modified mismatch: meta has {:?}, server reports {:?}",
                meta_last_modified,
                probe.last_modified
            );
        }

        // If the terminal size differs from the original `blocks_used`, we can still render:
        // we linearly scale real blocks into the available visual cells.
        if std::io::stdout().is_terminal()
            && !cli.noui
            && !force_noui
            && cell_capacity_live != m.blocks_used
        {
            eprintln!(
                "note: resume metadata has blocks_used={}, but current terminal has {} usable cells (excluding last row/col).",
                m.blocks_used, cell_capacity_live
            );
            eprintln!("      The block UI will be scaled to fit the current terminal.");
        }
    }

    // Compute layout (new run vs resume)
    let (
        _grid_rows,
        _grid_cols,
        cell_count,
        bytes_per_cell,
        active_cells,
        _threads_to_use,
        done_bitmap,
    ) = if let Some(m) = meta.as_ref() {
        let bytes_per_cell = len.div_ceil(m.blocks_used as u64);
        if bytes_per_cell == 0 {
            bail!("computed bytes_per_cell is 0 (unexpected)");
        }
        let mut done = m.done.clone();
        // Normalize blocks beyond EOF to done=true.
        for (idx, is_done) in done.iter_mut().enumerate() {
            let (start, _) = block_byte_range(idx, bytes_per_cell, len);
            if start >= len {
                *is_done = true;
            }
        }
        let active_cells = done
            .iter()
            .enumerate()
            .take(m.blocks_used)
            .take_while(|(idx, _)| {
                let (start, _) = block_byte_range(*idx, bytes_per_cell, len);
                start < len
            })
            .count();
        (
            grid_rows_live,
            grid_cols_live,
            m.blocks_used,
            bytes_per_cell,
            active_cells,
            cli.threads.max(1),
            done,
        )
    } else {
        let cell_count = cell_capacity_live;
        let bytes_per_cell = len.div_ceil(cell_count as u64);
        if bytes_per_cell == 0 {
            bail!("computed bytes_per_cell is 0 (unexpected)");
        }
        let mut active_cells: usize = 0;
        for idx in 0..cell_count {
            let start = (idx as u64) * bytes_per_cell;
            if start >= len {
                break;
            }
            active_cells += 1;
        }
        if active_cells == 0 {
            bail!("no active cells were generated (unexpected)");
        }
        (
            grid_rows_live,
            grid_cols_live,
            cell_count,
            bytes_per_cell,
            active_cells,
            cli.threads.max(1),
            {
                // done bitmap spans all blocks used; blocks beyond EOF are marked done.
                let mut d = vec![false; cell_count];
                for is_done in d.iter_mut().skip(active_cells) {
                    *is_done = true;
                }
                d
            },
        )
    };

    // If output exists and we're NOT resuming, confirm overwrite.
    // In batch mode, avoid interactive prompts: require --yes or fail this URL.
    if meta.is_none() && std::fs::metadata(&output_path).is_ok() {
        if batch.is_some() && !cli.yes {
            bail!(
                "output exists: {} (rerun with --yes to overwrite in batch mode)",
                output_path.display()
            );
        }
        confirm_overwrite(&output_path, cli.yes)?;
    }

    ensure_output_parent(&output_path).await?;

    // Prepare output file (pre-allocate to allow pwrite).
    let mut open = OpenOptions::new();
    open.create(true).read(true).write(true);
    if meta.is_none() {
        open.truncate(true);
    }
    let file = open
        .open(&output_path)
        .with_context(|| format!("failed to open output file: {}", output_path.display()))?;
    file.set_len(len)
        .with_context(|| format!("failed to set output file length to {len}"))?;
    let file = Arc::new(file);

    // Initial state (resume-aware)
    let mut initial_states = vec![CellState::Done; cell_count]; // cells beyond EOF start as done
    for (idx, state) in initial_states.iter_mut().enumerate().take(active_cells) {
        *state = if done_bitmap.get(idx).copied().unwrap_or(false) {
            CellState::Done
        } else {
            CellState::Pending
        };
    }

    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel::<Event>();
    let cancel = CancellationToken::new();

    // Completed-bytes (logical progress) is derived from done blocks (resume-safe).
    let already_completed = done_bitmap
        .iter()
        .enumerate()
        .filter(|(_, d)| **d)
        .map(|(idx, _)| {
            let (start, end) = block_byte_range(idx, bytes_per_cell, len);
            if start >= len { 0 } else { end - start + 1 }
        })
        .sum::<u64>();
    let completed_bytes = Arc::new(AtomicU64::new(already_completed));
    // Received/written bytes in this run (throughput metric; may include redundant re-downloads).
    let received_bytes = Arc::new(AtomicU64::new(0));
    // Number of workers currently inside the connection read/write loop.
    let active_conns = Arc::new(AtomicU64::new(0));

    let errors = Arc::new(AtomicU64::new(0));
    let last_progress = Arc::new(Mutex::new(Instant::now()));
    let last_error = Arc::new(Mutex::new(String::new()));
    let meta_dirty = Arc::new(AtomicU64::new(0)); // bump on changes
    let initial_work = find_all_undone_blocks(&done_bitmap);
    let done = Arc::new(Mutex::new(done_bitmap.clone()));
    let block_checksums = Arc::new(Mutex::new(
        meta.as_ref()
            .map(|m| m.block_checksums.clone())
            .unwrap_or_else(|| vec![None; cell_count]),
    ));

    let worker_count = cli.hard_limit.max(1).min(active_cells);
    // Dynamically enable workers: start with -n (clamped), increase with Ctrl+A, reduce with Ctrl+R.
    let initial_enabled = (cli.threads.max(1)).min(worker_count);
    let (enabled_tx, enabled_rx) = watch::channel::<u64>(initial_enabled as u64);
    let enabled_rx_main = enabled_tx.subscribe();
    let global_timeout = Duration::from_secs(cli.timeout_secs.max(1));
    let thread_timeout = Duration::from_secs(cli.thread_timeout_secs.max(1));
    let thread_backoff = Duration::from_secs(cli.thread_backoff_secs);

    // Cooperative scheduler channels
    let (to_sched_tx, mut to_sched_rx) = mpsc::unbounded_channel::<WorkerToSched>();
    let mut to_workers: Vec<mpsc::UnboundedSender<SchedToWorker>> =
        Vec::with_capacity(worker_count);

    let mut handles = Vec::with_capacity(worker_count + 1);

    // Spawn workers
    let url0 = url.clone();
    let global_received0 = batch.as_ref().map(|b| b.global_received.clone());
    let debug0 = cli.debug;
    for tid in 0..worker_count {
        let (to_worker_tx, to_worker_rx) = mpsc::unbounded_channel::<SchedToWorker>();
        to_workers.push(to_worker_tx);

        let evt_tx = evt_tx.clone();
        let cancel = cancel.clone();
        let completed_bytes = completed_bytes.clone();
        let received_bytes = received_bytes.clone();
        let global_received = global_received0.clone();
        let active_conns = active_conns.clone();
        let errors = errors.clone();
        let last_progress = last_progress.clone();
        let last_error = last_error.clone();
        let file = file.clone();
        let url = url0.clone();
        let debug = debug0;
        let client = client.clone();
        let meta_dirty = meta_dirty.clone();
        let done = done.clone();
        let block_checksums = block_checksums.clone();
        let to_sched_tx = to_sched_tx.clone();
        let mut enabled_rx = enabled_rx.clone();
        let precondition = precondition
            .clone()
            .expect("range mode requires a precondition");
        let context = WorkerContext {
            active_cells,
            bytes_per_cell,
            total_len: len,
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
            per_read_timeout: thread_timeout,
            backoff: thread_backoff,
            precondition,
        };

        handles.push(tokio::spawn(async move {
            worker_loop(tid, context, to_worker_rx, to_sched_tx, &mut enabled_rx).await
        }));
    }

    // Spawn scheduler (thread0 initially owns all blocks; others steal from it; fallback assigns
    // longest undone run if it detects gaps)
    let cancel_sched = cancel.clone();
    let sched_txs = to_workers.clone();
    let sched_done = done.clone();
    let sched_initial = initial_work.clone();
    handles.push(tokio::spawn(async move {
        scheduler_loop(
            worker_count,
            active_cells,
            sched_txs,
            &mut to_sched_rx,
            sched_done,
            sched_initial,
            cancel_sched,
        )
        .await
    }));

    drop(evt_tx);

    // Enter TUI
    let mut states = initial_states;
    let total_bytes = len;
    // Padded cells beyond EOF are green in the visual grid, but aren't work blocks.
    let total_cells = active_cells;
    let start_time = Instant::now();
    let mut last_draw = Instant::now() - MIN_TICK;
    let mut rolling_speed = RollingSpeed::default();
    let mut tick = tokio::time::interval(MIN_TICK);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let use_tui = std::io::stdout().is_terminal() && !cli.noui && !cli.debug;
    let use_tui = use_tui && !force_noui;
    let mut meta_save_tick = tokio::time::interval(Duration::from_secs(5));
    let mut last_dirty_seen = meta_dirty.load(Ordering::Relaxed);
    let mut enabled_count: u64 = initial_enabled as u64;

    // Create meta on new run.
    if meta.is_none() {
        meta = Some(Meta {
            version: 3,
            output: output_path.display().to_string(),
            url: Some(url.clone()),
            etag: probe.etag.clone(),
            last_modified: probe.last_modified.clone(),
            total_len: len,
            blocks_used: cell_count,
            done: done_bitmap.clone(),
            block_checksums: vec![None; cell_count],
        });
        write_meta_atomic(&meta_path, meta.as_ref().unwrap()).await?;
    }

    if use_tui {
        terminal::enable_raw_mode().context("failed to enable raw mode")?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen, cursor::Hide)
            .context("failed to enter alternate screen")?;
        let _guard = TermGuard;

        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).context("failed to init terminal backend")?;
        terminal.clear().ok();

        loop {
            tokio::select! {
                _ = tick.tick() => {},
                _ = meta_save_tick.tick() => {
                    let dirty = meta_dirty.load(Ordering::Relaxed);
                    if dirty != last_dirty_seen {
                        if let Some(m) = meta.as_mut() {
                            m.done = done.lock().await.clone();
                            m.block_checksums = block_checksums.lock().await.clone();
                            write_meta_atomic(&meta_path, m).await.ok();
                        }
                        last_dirty_seen = dirty;
                    }
                }
                _ = &mut shutdown => {
                    cancel.cancel();
                    break;
                }
                maybe = evt_rx.recv() => {
                    if let Some(evt) = maybe {
                        match evt {
                            Event::ChunkStarted(i) => {
                                if i < states.len() {
                                    states[i] = CellState::InProgress;
                                }
                            }
                            Event::ChunkDone(i) => {
                                if i < states.len() {
                                    states[i] = CellState::Done;
                                }
                            }
                            Event::Error(msg) => {
                                cancel.cancel();
                                return Err(anyhow!(msg));
                            }
                        }
                    }
                }
            }

            // Key handling:
            // - Ctrl+I (often sent as Tab): enable one more worker (up to -N)
            // - Ctrl+R: reduce enabled workers by 1 (down to 1); workers retire when they next become idle
            // - Ctrl+C: cancel immediately (raw mode often disables SIGINT)
            let mut quit_now = false;
            while event::poll(Duration::from_millis(0)).unwrap_or(false) {
                if let Ok(CEvent::Key(k)) = event::read()
                    && k.kind == KeyEventKind::Press
                {
                    let inc = (k.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(k.code, KeyCode::Char('i') | KeyCode::Char('I')))
                        || matches!(k.code, KeyCode::Tab);
                    let dec = k.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(k.code, KeyCode::Char('r') | KeyCode::Char('R'));
                    let cancel_key = k.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(k.code, KeyCode::Char('c') | KeyCode::Char('C'));

                    if inc {
                        if enabled_count < worker_count as u64 {
                            enabled_count += 1;
                            let _ = enabled_tx.send(enabled_count);
                        }
                    } else if dec {
                        if enabled_count > 1 {
                            enabled_count -= 1;
                            let _ = enabled_tx.send(enabled_count);
                        }
                    } else if cancel_key {
                        quit_now = true;
                        break;
                    }
                }
            }
            if quit_now {
                cancel.cancel();
                break;
            }

            // Global "no progress" timeout
            let since_progress = { last_progress.lock().await.elapsed() };
            if since_progress > global_timeout {
                cancel.cancel();
                let err_ct = errors.load(Ordering::Relaxed);
                let last = last_error.lock().await.clone();
                return Err(anyhow!(
                    "global timeout: no bytes written for {:.0}s (errors={err_ct}){}",
                    since_progress.as_secs_f64(),
                    if last.is_empty() {
                        "".to_string()
                    } else {
                        format!(", last_error={last}")
                    }
                ));
            }

            let done_cells = states
                .iter()
                .take(active_cells)
                .filter(|s| **s == CellState::Done)
                .count();
            if done_cells == total_cells {
                break;
            }
            // Also break if the authoritative done bitmap says we're complete (prevents UI/event skew).
            if done.lock().await.iter().all(|d| *d) {
                break;
            }

            if last_draw.elapsed() >= MIN_TICK {
                let bytes = completed_bytes.load(Ordering::Relaxed).min(total_bytes);
                let err_ct = errors.load(Ordering::Relaxed);
                let rx = received_bytes.load(Ordering::Relaxed);
                let speed_bps = rolling_speed.sample(Instant::now(), rx);
                let enabled = enabled_count.min(worker_count as u64);
                let active = active_conns.load(Ordering::Relaxed);

                terminal
                    .draw(|f| {
                        let area = f.area();
                        let grid = Rect {
                            x: area.x,
                            y: area.y,
                            width: area.width.saturating_sub(1),
                            height: area.height.saturating_sub(1),
                        };
                        let status = Rect {
                            x: area.x,
                            y: area.y + area.height.saturating_sub(1),
                            width: area.width.saturating_sub(1),
                            height: 1,
                        };

                        render_grid(f, grid, &states);
                        render_status(
                            f,
                            status,
                            &url,
                            &output_path,
                            &ProgressStats {
                                bytes,
                                total_bytes,
                                done_cells,
                                total_cells,
                                enabled,
                                max_workers: worker_count,
                                active,
                                errors: err_ct,
                                since_progress,
                                speed_bps,
                            },
                        );
                    })
                    .ok();
                last_draw = Instant::now();
            }

            if cancel.is_cancelled() {
                return Err(cancellation_error(&mut evt_rx));
            }
        }
    } else {
        // Non-TTY / CI fallback: no raw mode, no alternate screen.
        //
        // If stdin is a TTY, we still support hotkeys (Ctrl+I/Tab increase, Ctrl+R reduce, Ctrl+C cancel)
        // by enabling raw mode in a small background task.
        let key_cancel = cancel.clone();
        let key_enabled_tx = enabled_tx.clone();
        let key_worker_max = worker_count as u64;
        if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
            tokio::spawn(async move {
                let _guard = RawModeGuard::new().ok()?;
                let mut enabled = *key_enabled_tx.borrow();
                loop {
                    if key_cancel.is_cancelled() {
                        break;
                    }
                    if event::poll(Duration::from_millis(100)).unwrap_or(false)
                        && let Ok(CEvent::Key(k)) = event::read()
                    {
                        if k.kind != KeyEventKind::Press {
                            continue;
                        }
                        let inc = (k.modifiers.contains(KeyModifiers::CONTROL)
                            && matches!(k.code, KeyCode::Char('i') | KeyCode::Char('I')))
                            || matches!(k.code, KeyCode::Tab);
                        let dec = k.modifiers.contains(KeyModifiers::CONTROL)
                            && matches!(k.code, KeyCode::Char('r') | KeyCode::Char('R'));
                        let cancel_key = k.modifiers.contains(KeyModifiers::CONTROL)
                            && matches!(k.code, KeyCode::Char('c') | KeyCode::Char('C'));

                        if cancel_key {
                            key_cancel.cancel();
                            break;
                        } else if inc {
                            if enabled < key_worker_max {
                                enabled += 1;
                                let _ = key_enabled_tx.send(enabled);
                            }
                        } else if dec && enabled > 1 {
                            enabled -= 1;
                            let _ = key_enabled_tx.send(enabled);
                        }
                    }
                }
                Some::<()>(())
            });
        }

        let mut slow_tick = tokio::time::interval(Duration::from_millis(250));
        let enabled_rx_main = enabled_rx_main;
        loop {
            tokio::select! {
                _ = slow_tick.tick() => {},
                _ = meta_save_tick.tick() => {
                    let dirty = meta_dirty.load(Ordering::Relaxed);
                    if dirty != last_dirty_seen {
                        if let Some(m) = meta.as_mut() {
                            m.done = done.lock().await.clone();
                            m.block_checksums = block_checksums.lock().await.clone();
                            write_meta_atomic(&meta_path, m).await.ok();
                        }
                        last_dirty_seen = dirty;
                    }
                }
                _ = &mut shutdown => {
                    cancel.cancel();
                    break;
                }
                maybe = evt_rx.recv() => {
                    if let Some(evt) = maybe {
                        match evt {
                            Event::ChunkStarted(i) => {
                                if i < states.len() {
                                    states[i] = CellState::InProgress;
                                }
                            }
                            Event::ChunkDone(i) => {
                                if i < states.len() {
                                    states[i] = CellState::Done;
                                }
                            }
                            Event::Error(msg) => {
                                cancel.cancel();
                                return Err(anyhow!(msg));
                            }
                        }
                    }
                }
            }

            let since_progress = { last_progress.lock().await.elapsed() };
            if since_progress > global_timeout {
                cancel.cancel();
                let err_ct = errors.load(Ordering::Relaxed);
                let last = last_error.lock().await.clone();
                return Err(anyhow!(
                    "global timeout: no bytes written for {:.0}s (errors={err_ct}){}",
                    since_progress.as_secs_f64(),
                    if last.is_empty() {
                        "".to_string()
                    } else {
                        format!(", last_error={last}")
                    }
                ));
            }

            let done_cells = states
                .iter()
                .take(active_cells)
                .filter(|s| **s == CellState::Done)
                .count();
            if done_cells == total_cells {
                break;
            }
            if done.lock().await.iter().all(|d| *d) {
                break;
            }

            if batch.is_none() && last_draw.elapsed() >= Duration::from_millis(250) {
                let bytes = completed_bytes.load(Ordering::Relaxed).min(total_bytes);
                let err_ct = errors.load(Ordering::Relaxed);
                let rx = received_bytes.load(Ordering::Relaxed);
                let speed_bps = rolling_speed.sample(Instant::now(), rx);
                let enabled = (*enabled_rx_main.borrow()).min(worker_count as u64);
                let active = active_conns.load(Ordering::Relaxed);
                // Use CR + clear-line so we update a single status line (some terminals treat '\n'
                // as "move down" without returning to column 0).
                let line = format_noui_status_line(&ProgressStats {
                    bytes,
                    total_bytes,
                    done_cells,
                    total_cells,
                    enabled,
                    max_workers: worker_count,
                    active,
                    errors: err_ct,
                    since_progress,
                    speed_bps,
                });
                let mut stderr = std::io::stderr();
                let _ = write!(stderr, "\r\x1b[2K{line}");
                let _ = stderr.flush();
                last_draw = Instant::now();
            }

            if cancel.is_cancelled() {
                return Err(cancellation_error(&mut evt_rx));
            }
        }
        // Finish the in-place status line cleanly (avoid extra newlines in batch mode).
        if batch.is_none() {
            eprintln!();
        }
    }

    cancel.cancel();
    for h in handles {
        let _ = h.await;
    }

    // On success, remove metadata file (download is complete).
    // Always flush meta once at shutdown if not complete.
    let completed_all = done.lock().await.iter().all(|d| *d);
    if completed_all {
        let _ = tokio::fs::remove_file(&meta_path).await;
    } else if let Some(m) = meta.as_mut() {
        m.done = done.lock().await.clone();
        m.block_checksums = block_checksums.lock().await.clone();
        write_meta_atomic(&meta_path, m).await.ok();
    }

    if completed_all && !quiet_summary {
        // In --noui mode, force-print a final 100% status line at completion.
        if !use_tui {
            let rx = received_bytes.load(Ordering::Relaxed);
            let speed_bps = rolling_speed.sample(Instant::now(), rx);
            let enabled = (*enabled_tx.borrow()).min(worker_count as u64);
            let line = format_noui_status_line(&ProgressStats {
                bytes: total_bytes,
                total_bytes,
                done_cells: total_cells,
                total_cells,
                enabled,
                max_workers: worker_count,
                active: 0,
                errors: errors.load(Ordering::Relaxed),
                since_progress: Duration::ZERO,
                speed_bps,
            });
            let mut stderr = std::io::stderr();
            let _ = write!(stderr, "\r\x1b[2K{line}\r\n");
            let _ = stderr.flush();
        }

        let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
        let session_bytes = received_bytes.load(Ordering::Relaxed);
        let avg_bps = (session_bytes as f64) / elapsed;
        let summary = format!(
            "saved: {}\n  total_bytes: {}\n  session_downloaded: {}\n  time: {:.2}s\n  avg_speed: {}/s",
            output_path.display(),
            human_bytes(total_bytes as f64),
            human_bytes(session_bytes as f64),
            elapsed,
            human_bytes(avg_bps)
        );
        // Some terminals treat '\n' as "move down" without returning to column 0. For non-TUI runs,
        // force CRLF + clear-line so the summary always starts at column 0.
        if !use_tui && std::io::stderr().is_terminal() {
            let mut stderr = std::io::stderr();
            let _ = write!(stderr, "\r\x1b[2K\r\n{}\r\n", summary.replace('\n', "\r\n"));
            let _ = stderr.flush();
        } else {
            eprintln!("{summary}");
        }
    }

    Ok(DownloadReport {
        url,
        output_path,
        total_bytes,
        session_downloaded: received_bytes.load(Ordering::Relaxed),
        elapsed: start_time.elapsed(),
    })
}

struct StreamOptions {
    len_hint: Option<u64>,
    reason: StreamFallbackReason,
    batch: Option<BatchProgress>,
    quiet_summary: bool,
}

async fn run_stream_download(
    cli: &Cli,
    client: reqwest::Client,
    url: String,
    output_path: PathBuf,
    options: StreamOptions,
) -> Result<DownloadReport> {
    let StreamOptions {
        len_hint,
        reason,
        batch,
        quiet_summary,
    } = options;
    if std::fs::metadata(&output_path).is_ok() {
        if batch.is_some() && !cli.yes {
            bail!(
                "output exists: {} (rerun with --yes to overwrite in batch mode)",
                output_path.display()
            );
        }
        confirm_overwrite(&output_path, cli.yes)?;
    }

    ensure_output_parent(&output_path).await?;

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to start stream download: {url}"))?;
    if !resp.status().is_success() {
        bail!("stream download failed: server returned {}", resp.status());
    }
    validate_identity_encoding(resp.headers())?;

    let total_hint = len_hint.or_else(|| resp.content_length());
    let part_path = PathBuf::from(format!("{}.part", output_path.display()));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&part_path)
        .await
        .with_context(|| format!("failed to open temp output file: {}", part_path.display()))?;

    let mut stream = resp.bytes_stream();
    let start = Instant::now();
    let mut last_progress = Instant::now();
    let mut last_draw = Instant::now() - Duration::from_millis(250);
    let mut received = 0u64;
    let mut rolling_speed = RollingSpeed::default();
    let mut read_timeouts = 0u64;
    let global_timeout = Duration::from_secs(cli.timeout_secs.max(1));
    let read_timeout = Duration::from_secs(cli.thread_timeout_secs.max(1));
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let next = tokio::select! {
            _ = &mut shutdown => {
                bail!("cancelled");
            }
            next = tokio::time::timeout(read_timeout, stream.next()) => next,
        };
        let item = match next {
            Ok(v) => v,
            Err(_) => {
                read_timeouts += 1;
                let idle = last_progress.elapsed();
                if batch.is_none() && last_draw.elapsed() >= Duration::from_millis(250) {
                    let speed = rolling_speed.sample(Instant::now(), received);
                    let line = format_stream_status_line(
                        received,
                        total_hint,
                        speed,
                        idle,
                        read_timeouts,
                        reason,
                    );
                    let mut stderr = std::io::stderr();
                    let _ = write!(stderr, "\r\x1b[2K{line}");
                    let _ = stderr.flush();
                    last_draw = Instant::now();
                }
                if idle > global_timeout {
                    bail!(
                        "stream timeout: no bytes received for {:.0}s ({}, read_timeouts={read_timeouts})",
                        idle.as_secs_f64(),
                        reason.label()
                    );
                }
                continue;
            }
        };
        let Some(item) = item else {
            break;
        };
        let bytes = item.context("stream download read error")?;
        if bytes.is_empty() {
            continue;
        }
        read_timeouts = 0;
        file.write_all(&bytes)
            .await
            .context("failed writing stream download to output file")?;
        received += bytes.len() as u64;
        if let Some(b) = batch.as_ref() {
            b.global_received
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        last_progress = Instant::now();

        if batch.is_none() && last_draw.elapsed() >= Duration::from_millis(250) {
            let speed = rolling_speed.sample(Instant::now(), received);
            let line = format_stream_status_line(
                received,
                total_hint,
                speed,
                last_progress.elapsed(),
                read_timeouts,
                reason,
            );
            let mut stderr = std::io::stderr();
            let _ = write!(stderr, "\r\x1b[2K{line}");
            let _ = stderr.flush();
            last_draw = Instant::now();
        }
    }
    file.flush()
        .await
        .context("failed flushing stream download output file")?;
    drop(file);

    if let Some(total) = total_hint
        && received != total
    {
        bail!(
            "stream ended after {}, expected {}",
            human_bytes(received as f64),
            human_bytes(total as f64)
        );
    }

    tokio::fs::rename(&part_path, &output_path)
        .await
        .with_context(|| {
            format!(
                "failed renaming temp output {} -> {}",
                part_path.display(),
                output_path.display()
            )
        })?;

    if batch.is_none() {
        eprintln!();
    }

    if !quiet_summary {
        let elapsed = start.elapsed().as_secs_f64().max(0.001);
        let avg_bps = received as f64 / elapsed;
        eprintln!(
            "saved: {}\n  total_bytes: {}\n  session_downloaded: {}\n  time: {:.2}s\n  avg_speed: {}/s\n  mode: stream ({})",
            output_path.display(),
            total_hint
                .map(|n| human_bytes(n as f64))
                .unwrap_or_else(|| "unknown".to_string()),
            human_bytes(received as f64),
            elapsed,
            human_bytes(avg_bps),
            reason.label()
        );
    }

    Ok(DownloadReport {
        url,
        output_path,
        total_bytes: total_hint.unwrap_or(received),
        session_downloaded: received,
        elapsed: start.elapsed(),
    })
}
