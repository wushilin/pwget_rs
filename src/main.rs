use std::{
    fs::OpenOptions,
    io::IsTerminal,
    io::Write,
    os::unix::fs::FileExt,
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
use percent_encoding::percent_decode_str;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::Paragraph,
};
use reqwest::header::{
    ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH,
    CONTENT_RANGE, ETAG, HeaderMap, HeaderValue, LAST_MODIFIED, RANGE,
};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;
use url::Url;

mod cli;
mod meta;

use crate::cli::{Cli, ListFormat, parse_extra_headers};
use crate::meta::{MetaV2, load_meta_if_present, write_meta_atomic};

fn debug_println(enabled: bool, msg: impl AsRef<str>) {
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
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ProbeInfo {
    len: Option<u64>,
    ranges_ok: bool,
    content_disposition: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum StreamFallbackReason {
    UnknownLength,
    RangesUnavailable,
}

impl StreamFallbackReason {
    fn label(self) -> &'static str {
        match self {
            StreamFallbackReason::UnknownLength => "unknown content length",
            StreamFallbackReason::RangesUnavailable => "byte ranges unavailable",
        }
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

#[derive(Debug, Clone, Deserialize)]
struct UrlListJsonEntry {
    url: String,
    #[serde(default)]
    save_as: Option<String>,
}

#[derive(Debug, Clone)]
struct BatchItem {
    url: String,
    save_as: Option<String>,
}

fn resolve_save_as(download_dir: &Path, save_as: &str) -> Result<PathBuf> {
    let p = PathBuf::from(save_as.trim());
    if p.as_os_str().is_empty() {
        bail!("save_as is empty");
    }
    if p.is_absolute() {
        bail!(
            "save_as must be relative, got absolute path: {}",
            p.display()
        );
    }
    for comp in p.components() {
        match comp {
            std::path::Component::ParentDir => {
                bail!("save_as must not contain '..': {}", p.display());
            }
            std::path::Component::CurDir => {}
            std::path::Component::Normal(_) => {}
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                bail!("save_as must be a relative path: {}", p.display());
            }
        }
    }
    Ok(download_dir.join(p))
}

fn parse_urllist(text: &str, fmt: ListFormat) -> Result<Vec<BatchItem>> {
    match fmt {
        ListFormat::Plain => {
            let items: Vec<BatchItem> = text
                .lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty())
                .filter(|l| !l.starts_with('#'))
                .map(|url| BatchItem {
                    url: url.to_string(),
                    save_as: None,
                })
                .collect();
            Ok(items)
        }
        ListFormat::Csv => {
            // Format:
            // "save_as.txt", "https://xxx/..."
            //
            // We implement a small tolerant parser (supports optional whitespace around the comma),
            // because many people write `"a.txt", "http://..."` (space before the quote), which
            // strict CSV parsers may treat as an unquoted field.
            fn parse_field(mut s: &str) -> Result<(String, &str)> {
                s = s.trim_start();
                if s.is_empty() {
                    return Ok((String::new(), ""));
                }
                if let Some(rest) = s.strip_prefix('"') {
                    let mut out = String::new();
                    let mut chars = rest.chars();
                    let mut rem = rest;
                    loop {
                        match chars.next() {
                            None => bail!("unterminated quote"),
                            Some('"') => {
                                // either end quote or escaped quote
                                if let Some(r) = rem.strip_prefix("\"\"") {
                                    out.push('"');
                                    rem = r;
                                    chars = rem.chars();
                                    continue;
                                }
                                // end quote
                                rem = &rem[1..];
                                return Ok((out, rem));
                            }
                            Some(c) => {
                                out.push(c);
                                rem = &rem[c.len_utf8()..];
                                chars = rem.chars();
                            }
                        }
                    }
                } else {
                    // unquoted field: read until comma or end
                    let mut end = s.len();
                    for (i, c) in s.char_indices() {
                        if c == ',' {
                            end = i;
                            break;
                        }
                    }
                    let field = s[..end].trim().to_string();
                    let rest = &s[end..];
                    Ok((field, rest))
                }
            }

            let mut out = Vec::new();
            for (idx, line0) in text.lines().enumerate() {
                let line = line0.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let (save_as, rest) = parse_field(line)
                    .with_context(|| format!("csv line {}: failed parsing save_as", idx + 1))?;
                let rest = rest.trim_start();
                let rest = rest
                    .strip_prefix(',')
                    .ok_or_else(|| anyhow!("csv line {}: expected ',' separator", idx + 1))?;
                let (url, _rest2) = parse_field(rest)
                    .with_context(|| format!("csv line {}: failed parsing url", idx + 1))?;
                if url.trim().is_empty() {
                    bail!("csv line {}: url is empty", idx + 1);
                }
                out.push(BatchItem {
                    url: url.trim().to_string(),
                    save_as: if save_as.trim().is_empty() {
                        None
                    } else {
                        Some(save_as.trim().to_string())
                    },
                });
            }
            Ok(out)
        }
        ListFormat::Json => {
            let entries: Vec<UrlListJsonEntry> =
                serde_json::from_str(text).context("failed to parse json url list")?;
            let out = entries
                .into_iter()
                .filter(|e| !e.url.trim().is_empty())
                .map(|e| BatchItem {
                    url: e.url,
                    save_as: e.save_as,
                })
                .collect();
            Ok(out)
        }
    }
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

fn sanitize_output_filename(p: &PathBuf, user_specified: bool) -> PathBuf {
    if user_specified {
        return p.clone();
    }
    // Keep only the last path segment to avoid directory traversal from headers/URLs.
    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
        if !name.is_empty() && name != "." && name != ".." {
            return PathBuf::from(name);
        }
    }
    PathBuf::from("download.bin")
}

fn parse_filename_from_content_disposition(cd: &str) -> Option<String> {
    // Very small parser for:
    // - filename="foo.bin"
    // - filename=foo.bin
    // - filename*=UTF-8''foo%20bar.bin
    let lower = cd.to_ascii_lowercase();

    // Prefer filename*
    if let Some(i) = lower.find("filename*=") {
        let v = cd[(i + "filename*=".len())..].trim();
        let v = v.split(';').next().unwrap_or(v).trim().trim_matches('"');
        // RFC 5987: charset'lang'%XX
        if let Some(pos) = v.find("''") {
            let enc = &v[(pos + 2)..];
            let decoded = percent_decode_str(enc).decode_utf8().ok()?;
            let s = decoded.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        } else {
            let decoded = percent_decode_str(v).decode_utf8().ok()?;
            let s = decoded.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }

    if let Some(i) = lower.find("filename=") {
        let v = cd[(i + "filename=".len())..].trim();
        let v = v.split(';').next().unwrap_or(v).trim();
        let v = v.trim_matches('"').trim_matches('\'').trim();
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CellState {
    Pending,
    InProgress,
    HalfDone,
    Done,
}

#[derive(Debug)]
enum Event {
    ChunkStarted(usize),
    ChunkDone(usize),
    Error(String),
}

#[derive(Debug)]
enum SchedToWorker {
    Assign(Vec<usize>),
    StealRequest { requester: usize },
    Stop,
}

#[derive(Debug)]
enum WorkerToSched {
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
                    .map(|(k, v)| {
                        let val = v.to_str().unwrap_or("<non-utf8>");
                        format!("{k}={val}")
                    })
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
        let items = parse_urllist(&text, cli.list_format).with_context(|| {
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
            joinset.spawn(async move {
                let _permit = permit;
                let output_override = match item.save_as.as_deref() {
                    Some(sa) => match resolve_save_as(&dir, sa) {
                        Ok(p) => Some(p),
                        Err(e) => return (item.url, Err(e)),
                    },
                    None => None,
                };
                if let Some(p) = output_override.as_ref() {
                    if let Some(parent) = p.parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            return (
                                item.url,
                                Err(anyhow!(e).context(format!(
                                    "failed to create parent dir {}",
                                    parent.display()
                                ))),
                            );
                        }
                    }
                }
                let res = run_one_download(
                    &cli_ref,
                    client,
                    item.url.clone(),
                    Some(dir),
                    /*force_noui*/ true,
                    Some(batch.clone()),
                    /*quiet_summary*/ true,
                    output_override,
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
        if !failed.is_empty() {
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
        /*force_noui*/ false,
        None,
        /*quiet_summary*/ false,
        None,
    )
    .await?;
    Ok(())
}

async fn run_one_download(
    cli: &Cli,
    client: reqwest::Client,
    url: String,
    download_dir: Option<PathBuf>,
    force_noui: bool,
    batch: Option<BatchProgress>,
    quiet_summary: bool,
    output_override: Option<PathBuf>,
) -> Result<DownloadReport> {
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
        .and_then(parse_filename_from_content_disposition)
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

    let stream_reason = if probe.len.is_none() {
        Some(StreamFallbackReason::UnknownLength)
    } else if !ranges_ok {
        Some(StreamFallbackReason::RangesUnavailable)
    } else {
        None
    };

    if let Some(stream_reason) = stream_reason {
        if cli.meta.is_some() {
            bail!("--meta requires a known Content-Length and byte range support");
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
            probe.len,
            stream_reason,
            batch,
            quiet_summary,
        )
        .await;
    }

    let len = probe.len.unwrap();
    if len == 0 {
        bail!("remote content-length is 0 (nothing to download)");
    }

    let meta_path = cli
        .meta
        .clone()
        .unwrap_or_else(|| MetaV2::meta_path_for_output(&output_path));

    // Load meta (if present) to resume; otherwise compute layout from current terminal.
    let mut meta: Option<MetaV2> = load_meta_if_present(&meta_path).await?;
    if let Some(m) = meta.as_ref() {
        // Validate against current run
        if m.version != 2 {
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
        if let Some(meta_url) = m.url.as_ref() {
            if meta_url != &url {
                bail!("meta URL mismatch: meta is for {meta_url}, current URL is {url}");
            }
        }
        if let Some(meta_etag) = m.etag.as_ref() {
            if probe.etag.as_ref() != Some(meta_etag) {
                bail!(
                    "meta ETag mismatch: meta has {:?}, server reports {:?}",
                    meta_etag,
                    probe.etag
                );
            }
        }
        if let Some(meta_last_modified) = m.last_modified.as_ref() {
            if probe.last_modified.as_ref() != Some(meta_last_modified) {
                bail!(
                    "meta Last-Modified mismatch: meta has {:?}, server reports {:?}",
                    meta_last_modified,
                    probe.last_modified
                );
            }
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
        let bytes_per_cell = (len + (m.blocks_used as u64) - 1) / (m.blocks_used as u64);
        if bytes_per_cell == 0 {
            bail!("computed bytes_per_cell is 0 (unexpected)");
        }
        let mut done = m.done.clone();
        // Normalize blocks beyond EOF to done=true.
        for idx in 0..done.len() {
            let (start, _) = block_byte_range(idx, bytes_per_cell, len);
            if start >= len {
                done[idx] = true;
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
        let bytes_per_cell = (len + (cell_count as u64) - 1) / (cell_count as u64); // ceil
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
                for idx in active_cells..cell_count {
                    d[idx] = true;
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

    // Prepare output file (pre-allocate to allow pwrite).
    let mut open = OpenOptions::new();
    open.create(true).write(true);
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
    for idx in 0..active_cells {
        initial_states[idx] = if done_bitmap.get(idx).copied().unwrap_or(false) {
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
        let to_sched_tx = to_sched_tx.clone();
        let mut enabled_rx = enabled_rx.clone();

        handles.push(tokio::spawn(async move {
            worker_loop(
                tid,
                active_cells,
                bytes_per_cell,
                len,
                client,
                url,
                debug,
                file,
                done,
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
                to_worker_rx,
                to_sched_tx,
                thread_timeout,
                thread_backoff,
                &mut enabled_rx,
            )
            .await
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
    let total_cells = cell_count;
    let start_time = Instant::now();
    let mut last_draw = Instant::now() - MIN_TICK;
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
        meta = Some(MetaV2 {
            version: 2,
            output: output_path.display().to_string(),
            url: Some(url.clone()),
            etag: probe.etag.clone(),
            last_modified: probe.last_modified.clone(),
            total_len: len,
            blocks_used: cell_count,
            done: done_bitmap.clone(),
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
                if let Ok(CEvent::Key(k)) = event::read() {
                    if k.kind == KeyEventKind::Press {
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

            let done_cells = states.iter().filter(|s| **s == CellState::Done).count();
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
                let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
                let rx = received_bytes.load(Ordering::Relaxed);
                let speed_bps = (rx as f64) / elapsed;
                let pct = (bytes as f64) * 100.0 / (total_bytes as f64);
                let enabled = enabled_count.min(worker_count as u64);
                let active = active_conns.load(Ordering::Relaxed).min(enabled);

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
                            bytes,
                            total_bytes,
                            pct,
                            speed_bps,
                            done_cells,
                            total_cells,
                            enabled as usize,
                            worker_count,
                            active,
                            err_ct,
                            since_progress,
                        );
                    })
                    .ok();
                last_draw = Instant::now();
            }

            if cancel.is_cancelled() {
                return Err(anyhow!("cancelled"));
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
                    if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                        if let Ok(CEvent::Key(k)) = event::read() {
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
                            } else if dec {
                                if enabled > 1 {
                                    enabled -= 1;
                                    let _ = key_enabled_tx.send(enabled);
                                }
                            }
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

            let done_cells = states.iter().filter(|s| **s == CellState::Done).count();
            if done_cells == total_cells {
                break;
            }
            if done.lock().await.iter().all(|d| *d) {
                break;
            }

            if batch.is_none() && last_draw.elapsed() >= Duration::from_millis(250) {
                let bytes = completed_bytes.load(Ordering::Relaxed).min(total_bytes);
                let err_ct = errors.load(Ordering::Relaxed);
                let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
                let rx = received_bytes.load(Ordering::Relaxed);
                let speed_bps = (rx as f64) / elapsed;
                let enabled = (*enabled_rx_main.borrow()).min(worker_count as u64);
                let active = active_conns.load(Ordering::Relaxed).min(enabled);
                // Use CR + clear-line so we update a single status line (some terminals treat '\n'
                // as "move down" without returning to column 0).
                let line = format_noui_status_line(
                    bytes,
                    total_bytes,
                    done_cells,
                    total_cells,
                    enabled,
                    worker_count,
                    active,
                    err_ct,
                    since_progress,
                    speed_bps,
                );
                let mut stderr = std::io::stderr();
                let _ = write!(stderr, "\r\x1b[2K{line}");
                let _ = stderr.flush();
                last_draw = Instant::now();
            }

            if cancel.is_cancelled() {
                return Err(anyhow!("cancelled"));
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
        write_meta_atomic(&meta_path, m).await.ok();
    }

    if completed_all && !quiet_summary {
        // In --noui mode, force-print a final 100% status line at completion.
        if !use_tui {
            let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
            let rx = received_bytes.load(Ordering::Relaxed);
            let speed_bps = (rx as f64) / elapsed;
            let enabled = (*enabled_tx.borrow()).min(worker_count as u64);
            let line = format_noui_status_line(
                total_bytes,
                total_bytes,
                total_cells,
                total_cells,
                enabled,
                worker_count,
                0,
                errors.load(Ordering::Relaxed),
                Duration::from_secs(0),
                speed_bps,
            );
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

async fn run_stream_download(
    cli: &Cli,
    client: reqwest::Client,
    url: String,
    output_path: PathBuf,
    len_hint: Option<u64>,
    reason: StreamFallbackReason,
    batch: Option<BatchProgress>,
    quiet_summary: bool,
) -> Result<DownloadReport> {
    if std::fs::metadata(&output_path).is_ok() {
        if batch.is_some() && !cli.yes {
            bail!(
                "output exists: {} (rerun with --yes to overwrite in batch mode)",
                output_path.display()
            );
        }
        confirm_overwrite(&output_path, cli.yes)?;
    }

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
        }
    }

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
                    let elapsed = start.elapsed().as_secs_f64().max(0.001);
                    let speed = received as f64 / elapsed;
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
            let elapsed = start.elapsed().as_secs_f64().max(0.001);
            let speed = received as f64 / elapsed;
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

    if let Some(total) = total_hint {
        if received != total {
            bail!(
                "stream ended after {}, expected {}",
                human_bytes(received as f64),
                human_bytes(total as f64)
            );
        }
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

fn block_byte_range(idx: usize, bytes_per_cell: u64, total_len: u64) -> (u64, u64) {
    let start = (idx as u64) * bytes_per_cell;
    let end = (start + bytes_per_cell - 1).min(total_len.saturating_sub(1));
    (start, end)
}

async fn scheduler_loop(
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
                for tid in 0..worker_count {
                    if tid == worker {
                        continue;
                    }
                    if remaining[tid] > best.map(|(_, r)| r).unwrap_or(0) {
                        best = Some((tid, remaining[tid]));
                    }
                }
                if let Some((victim, rem)) = best {
                    if rem >= 2 {
                        let _ = to_workers[victim]
                            .send(SchedToWorker::StealRequest { requester: worker });
                    }
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

fn find_all_undone_blocks(done: &[bool]) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, d) in done.iter().enumerate() {
        if !*d {
            out.push(i);
        }
    }
    out
}

async fn worker_loop(
    tid: usize,
    active_cells: usize,
    bytes_per_cell: u64,
    total_len: u64,
    client: reqwest::Client,
    url: String,
    debug: bool,
    file: Arc<std::fs::File>,
    done: Arc<Mutex<Vec<bool>>>,
    meta_dirty: Arc<AtomicU64>,
    completed_bytes: Arc<AtomicU64>,
    received_bytes: Arc<AtomicU64>,
    global_received: Option<Arc<AtomicU64>>,
    active_conns: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    last_progress: Arc<Mutex<Instant>>,
    last_error: Arc<Mutex<String>>,
    cancel: CancellationToken,
    evt_tx: mpsc::UnboundedSender<Event>,
    mut to_worker_rx: mpsc::UnboundedReceiver<SchedToWorker>,
    to_sched_tx: mpsc::UnboundedSender<WorkerToSched>,
    per_read_timeout: Duration,
    backoff: Duration,
    enabled_rx: &mut watch::Receiver<u64>,
) {
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
                m = to_worker_rx.recv() => Some(m),
                _ = tokio::time::sleep(Duration::from_secs(5)) => None,
            };
            match msg.flatten() {
                Some(SchedToWorker::Assign(blocks)) => {
                    push_blocks(&mut queue, blocks);
                    requested_work = false;
                }
                Some(SchedToWorker::Stop) | None => break,
                Some(SchedToWorker::StealRequest { requester }) => {
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
                client.get(&url).header(RANGE, range_header).send(),
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
                            next = tokio::time::timeout(per_read_timeout, stream.next()) => {
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
                                        // mark done
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
                                        f.write_at(&data, off).context("failed writing to output file")?;
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

async fn probe_len_and_ranges(
    client: &reqwest::Client,
    url: &str,
    debug: bool,
) -> Result<ProbeInfo> {
    // Prefer HEAD, but fall back to GET if needed.
    let mut len: Option<u64> = None;
    let mut ranges_ok = false;
    let mut cd: Option<String> = None;
    let mut etag: Option<String> = None;
    let mut last_modified: Option<String> = None;

    if let Ok(resp) = client.head(url).send().await {
        debug_println(debug, format!("probe: HEAD status={}", resp.status()));
        if resp.status().is_success() {
            len = parse_len(resp.headers().get(CONTENT_LENGTH));
            ranges_ok = parse_ranges(resp.headers().get(ACCEPT_RANGES));
            cd = resp
                .headers()
                .get(CONTENT_DISPOSITION)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            etag = resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            last_modified = resp
                .headers()
                .get(LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
    }

    if len.is_none() || !ranges_ok {
        let resp = client
            .get(url)
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .context("failed to GET for probing")?;
        debug_println(
            debug,
            format!("probe: GET bytes=0-0 status={}", resp.status()),
        );
        // A 206 implies range support. Prefer Content-Range for total length.
        if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            ranges_ok = true;
            len = len.or_else(|| {
                parse_total_len_from_content_range(
                    resp.headers().get(reqwest::header::CONTENT_RANGE),
                )
            });
        } else if resp.status().is_success() {
            ranges_ok = ranges_ok || parse_ranges(resp.headers().get(ACCEPT_RANGES));
        }
        len = len.or_else(|| parse_len(resp.headers().get(CONTENT_LENGTH)));
        if cd.is_none() {
            cd = resp
                .headers()
                .get(CONTENT_DISPOSITION)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
        if etag.is_none() {
            etag = resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
        if last_modified.is_none() {
            last_modified = resp
                .headers()
                .get(LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
    }

    Ok(ProbeInfo {
        len,
        ranges_ok,
        content_disposition: cd,
        etag,
        last_modified,
    })
}

fn parse_len(v: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    v.and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

fn parse_ranges(v: Option<&reqwest::header::HeaderValue>) -> bool {
    v.and_then(|hv| hv.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("bytes"))
        .unwrap_or(false)
}

fn parse_total_len_from_content_range(v: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    // Example: "bytes 0-0/12345" or "bytes 0-0/*"
    let s = v?.to_str().ok()?;
    let slash = s.rfind('/')?;
    let total = s[(slash + 1)..].trim();
    if total == "*" {
        None
    } else {
        total.parse::<u64>().ok()
    }
}

#[derive(Debug)]
struct ParsedContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(v: Option<&reqwest::header::HeaderValue>) -> Option<ParsedContentRange> {
    // Example: "bytes 100-199/12345"
    let s = v?.to_str().ok()?.trim();
    let rest = s.strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.trim().parse::<u64>().ok()?;
    let end = end.trim().parse::<u64>().ok()?;
    let total = match total.trim() {
        "*" => None,
        n => Some(n.parse::<u64>().ok()?),
    };
    Some(ParsedContentRange { start, end, total })
}

fn validate_content_range(
    v: Option<&reqwest::header::HeaderValue>,
    requested_start: u64,
    expected_total: u64,
) -> Result<()> {
    let cr = parse_content_range(v).ok_or_else(|| anyhow!("missing/invalid Content-Range"))?;
    if cr.start != requested_start {
        bail!(
            "Content-Range start mismatch: requested {}, got {}",
            requested_start,
            cr.start
        );
    }
    if cr.end < cr.start {
        bail!("Content-Range end is before start: {}-{}", cr.start, cr.end);
    }
    if cr.end >= expected_total {
        bail!(
            "Content-Range end {} is outside expected total {}",
            cr.end,
            expected_total
        );
    }
    if let Some(total) = cr.total {
        if total != expected_total {
            bail!(
                "Content-Range total mismatch: expected {}, got {}",
                expected_total,
                total
            );
        }
    }
    Ok(())
}

fn validate_identity_encoding(headers: &HeaderMap) -> Result<()> {
    if let Some(value) = headers.get(CONTENT_ENCODING) {
        let encoding = value.to_str().unwrap_or("<non-utf8>");
        if !encoding.eq_ignore_ascii_case("identity") {
            bail!(
                "server returned Content-Encoding {encoding:?}; byte-exact downloads require identity encoding"
            );
        }
    }
    Ok(())
}

fn default_output_path(url: &str) -> PathBuf {
    if let Ok(u) = Url::parse(url) {
        if let Some(seg) = u.path_segments().and_then(|mut it| it.next_back()) {
            if !seg.is_empty() && seg != "/" {
                return Path::new(seg).to_path_buf();
            }
        }
    }
    PathBuf::from("download.bin")
}

fn render_grid(f: &mut ratatui::Frame, area: Rect, states: &[CellState]) {
    let height = area.height as usize;
    let width = area.width as usize;
    if height == 0 || width == 0 {
        return;
    }

    let visual_cells = height.saturating_mul(width);
    let real_cells = states.len();
    if visual_cells == 0 {
        return;
    }

    let mut lines: Vec<Line> = Vec::with_capacity(height);
    // For visual cells that map to zero real blocks (can happen when V > R), inherit the previous
    // visual cell color to keep the "cursor" / progress visually consistent. If the first visual
    // cell is unmapped, treat it as Done.
    let mut prev_visual = CellState::Done;
    for row in 0..height {
        let mut spans: Vec<Span> = Vec::with_capacity(width);
        for col in 0..width {
            // IMPORTANT: derive indexing from the *current* area width so terminal resizes
            // are reflected correctly.
            let idx = row * width + col;
            // Map this visual cell to a (possibly empty) range of "real" blocks.
            // This allows resume runs to render even if the terminal size changed.
            //
            // start = floor(idx * R / V)
            // end   = floor((idx+1) * R / V)
            let start = idx.saturating_mul(real_cells) / visual_cells;
            let end = (idx.saturating_add(1)).saturating_mul(real_cells) / visual_cells;

            // If this visual cell corresponds to zero real blocks, inherit previous visual state.
            let st = if start >= end || start >= real_cells {
                prev_visual
            } else {
                // Aggregate the real block states into one visual state (scaled/condensed view).
                //
                // Rules (as requested):
                // - if any real block is being worked on (InProgress) -> orange
                // - else if some done and some not done -> yellow
                // - else if all done -> green
                // - else (all not done) -> grey
                let slice = &states[start..end.min(real_cells)];
                let total = slice.len().max(1);
                let mut done_count = 0usize;
                let mut pending_count = 0usize;
                let mut inprog_count = 0usize;
                for st in slice {
                    match st {
                        CellState::Done => {
                            done_count += 1;
                        }
                        CellState::InProgress => {
                            inprog_count += 1;
                        }
                        CellState::Pending => {
                            pending_count += 1;
                        }
                        // HalfDone is not expected in the real-block state array, but handle it defensively.
                        CellState::HalfDone => {
                            // Treat as "mixed" for safety.
                            pending_count += 1;
                        }
                    }
                }
                if inprog_count > 0 {
                    CellState::InProgress
                } else if done_count > 0 && pending_count > 0 {
                    CellState::HalfDone
                } else if done_count == total {
                    CellState::Done
                } else {
                    CellState::Pending
                }
            };
            prev_visual = st;
            let color = match st {
                CellState::Pending => Color::DarkGray,
                // "Orange" for in-progress.
                CellState::InProgress => Color::Rgb(255, 165, 0),
                // Yellow for half-done.
                CellState::HalfDone => Color::Yellow,
                CellState::Done => Color::Green,
            };
            spans.push(Span::styled("█", Style::default().fg(color)));
        }
        lines.push(Line::from(spans));
    }

    let p = Paragraph::new(lines);
    f.render_widget(p, area);
}

fn render_status(
    f: &mut ratatui::Frame,
    area: Rect,
    url: &str,
    output: &Path,
    bytes: u64,
    total: u64,
    pct: f64,
    speed_bps: f64,
    done_cells: usize,
    total_cells: usize,
    enabled_workers: usize,
    max_workers: usize,
    active: u64,
    errors: u64,
    since_progress: Duration,
) {
    let idle_workers = (enabled_workers as u64).saturating_sub(active);
    let bytes_hr = human_bytes(bytes as f64);
    let total_hr = human_bytes(total as f64);
    let status = format!(
        "{pct:6.2}%  {bytes_hr}/{total_hr}  {speed}/s  cells {done_cells}/{total_cells}  workers {enabled_workers}/{max_workers}  active {active}  idle_workers {idle_workers}  errors {errors}  idle {idle:.0}s  -> {out}  ({url})",
        speed = human_bytes(speed_bps),
        idle = since_progress.as_secs_f64(),
        out = output.display(),
    );
    let p = Paragraph::new(status).style(Style::default().fg(Color::White));
    f.render_widget(p, area);
}

fn human_bytes(bps: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bps.max(0.0);
    let mut u = 0usize;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{v:.0}{}", UNITS[u])
    } else {
        format!("{v:.3}{}", UNITS[u])
    }
}

fn format_noui_status_line(
    bytes: u64,
    total_bytes: u64,
    done_cells: usize,
    total_cells: usize,
    enabled: u64,
    max_workers: usize,
    active: u64,
    err_ct: u64,
    since_progress: Duration,
    speed_bps: f64,
) -> String {
    let bytes_hr = human_bytes(bytes as f64);
    let total_hr = human_bytes(total_bytes as f64);
    let pct = if total_bytes == 0 {
        0.0
    } else {
        ((bytes.min(total_bytes) as f64) * 100.0 / (total_bytes as f64)).min(100.0)
    };
    format!(
        "{pct:6.2}%  {bytes_hr}/{total_hr}  {}/s  cells {done_cells}/{total_cells}  workers {}/{}  active {active}  idle_workers {}  errors {err_ct}  idle {:.0}s",
        human_bytes(speed_bps),
        enabled,
        max_workers,
        enabled.saturating_sub(active),
        since_progress.as_secs_f64()
    )
}

fn format_stream_status_line(
    bytes: u64,
    total_hint: Option<u64>,
    speed_bps: f64,
    idle: Duration,
    read_timeouts: u64,
    reason: StreamFallbackReason,
) -> String {
    let bytes_hr = human_bytes(bytes as f64);
    let suffix = format!(
        "idle {:.0}s  read_timeouts {read_timeouts}  resume unavailable ({})",
        idle.as_secs_f64(),
        reason.label()
    );
    match total_hint {
        Some(total) if total > 0 => {
            let total_hr = human_bytes(total as f64);
            let pct = ((bytes.min(total) as f64) * 100.0 / (total as f64)).min(100.0);
            format!(
                "stream {pct:6.2}%  {bytes_hr}/{total_hr}  {}/s  {suffix}",
                human_bytes(speed_bps),
            )
        }
        _ => format!(
            "stream  {bytes_hr} downloaded  {}/s  total unknown  {suffix}",
            human_bytes(speed_bps),
        ),
    }
}
