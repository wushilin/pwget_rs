use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use reqwest::header::{ACCEPT_ENCODING, HeaderMap, HeaderName, HeaderValue, RANGE, USER_AGENT};

#[derive(Debug, Parser, Clone)]
#[command(
    name = "pwget",
    version,
    about = "Parallel wget-like downloader with a grid TUI"
)]
pub struct Cli {
    /// URL to download (single-file mode). Omit when using -T.
    #[arg(required_unless_present = "url_list")]
    pub url: Option<String>,

    /// Number of download threads
    #[arg(short = 'n', long = "threads", default_value_t = 5)]
    pub threads: usize,

    /// Hard limit for maximum workers (spawned + enabled via hotkeys)
    #[arg(short = 'N', long = "hard-limit", default_value_t = 20)]
    pub hard_limit: usize,

    /// Assume "yes" for prompts (overwrite file, etc.)
    #[arg(long = "yes")]
    pub yes: bool,

    /// Global timeout: if no bytes are written for this many seconds, abort
    #[arg(long = "timeout", default_value_t = 60)]
    pub timeout_secs: u64,

    /// Per-thread timeout: if a range request doesn't complete within this many seconds, retry
    #[arg(long = "ttimeout", default_value_t = 10)]
    pub thread_timeout_secs: u64,

    /// Per-thread backoff: sleep this many seconds between retries
    #[arg(long = "tbackoff", default_value_t = 3)]
    pub thread_backoff_secs: u64,

    /// Output file path (defaults to last URL path segment)
    #[arg(
        short = 'o',
        long = "output",
        conflicts_with = "download_dir",
        conflicts_with = "url_list"
    )]
    pub output: Option<PathBuf>,

    /// Download directory (used when output name is derived). Not allowed with -o.
    #[arg(short = 'd', long = "dir", conflicts_with = "output")]
    pub download_dir: Option<PathBuf>,

    /// File containing URLs (one per line). Requires -d. Forces --noui.
    #[arg(short = 'T', long = "urllist", requires = "download_dir")]
    pub url_list: Option<PathBuf>,

    /// Format of the -T/--urllist file
    #[arg(long = "list-format", value_enum, default_value_t = ListFormat::Plain)]
    pub list_format: ListFormat,

    /// Number of downloads to run in parallel in -T batch mode (each download uses its own -n/-N worker pool)
    #[arg(short = 'p', long = "parallel", default_value_t = 3)]
    pub parallel: usize,

    /// Metadata path for resume support (defaults to OUTPUT + ".meta")
    #[arg(long = "meta")]
    pub meta: Option<PathBuf>,

    /// Disable the TUI and use simple line-based progress output
    #[arg(long = "noui")]
    pub noui: bool,

    /// Override User-Agent header (otherwise defaults to pwget/0.1)
    #[arg(long = "ua", value_name = "USER_AGENT")]
    pub ua: Option<String>,

    /// Add an extra HTTP header (repeatable), in KEY=VALUE or KEY:VALUE format
    #[arg(long = "header", value_name = "KEY=VALUE|KEY:VALUE", action = clap::ArgAction::Append)]
    pub header: Vec<String>,

    /// Debug logging (forces --noui to avoid corrupting the TUI)
    #[arg(long = "debug")]
    pub debug: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum ListFormat {
    Plain,
    Csv,
    Json,
}

pub fn parse_extra_headers(cli: &Cli) -> Result<(Option<String>, HeaderMap)> {
    let mut ua: Option<String> = cli.ua.clone();
    let mut out = HeaderMap::new();
    for raw in &cli.header {
        let (k, v) = raw
            .split_once('=')
            .or_else(|| raw.split_once(':'))
            .ok_or_else(|| anyhow!("invalid --header {raw:?}: expected KEY=VALUE or KEY:VALUE"))?;
        let k = k.trim();
        let v = v.trim();
        if k.is_empty() {
            bail!("invalid --header {raw:?}: empty header key");
        }
        let name = HeaderName::from_bytes(k.as_bytes())
            .with_context(|| format!("invalid header name in --header {raw:?}"))?;
        if name == RANGE {
            bail!("--header must not set Range (pwget manages Range internally)");
        }
        if name == ACCEPT_ENCODING {
            bail!(
                "--header must not set Accept-Encoding (pwget requires byte-exact identity downloads)"
            );
        }
        if name == USER_AGENT {
            // Keep a single source of truth for UA (builder.user_agent).
            if ua.is_none() {
                ua = Some(v.to_string());
            }
            continue;
        }
        let value = HeaderValue::from_str(v)
            .with_context(|| format!("invalid header value in --header {raw:?}"))?;
        out.insert(name, value);
    }
    Ok((ua, out))
}
