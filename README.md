# pwget
Fancy, parallel downloader in terinal, written in Rust. You can visualize download progress, dynamically increase parallelism.

<img width="1728" height="953" alt="image" src="https://github.com/user-attachments/assets/3e758c1f-15a2-409a-9e4f-cd8dbfc2caf1" />

`pwget` is a parallel `wget`-like downloader written in Rust. Its differentiator is a **terminal grid UI** where each cell represents a chunk (“block”) of the file and changes color as it downloads.

It also supports:
- Cooperative **work stealing** between worker threads (idle workers steal the tail half of a busy worker’s remaining blocks)
- **Resume** via a sidecar metadata file (`.meta`)
- **Batch mode** (`-T`) with configurable concurrent downloads (`-p`)
- `http_proxy` / `https_proxy` / `no_proxy` environment variables (via `reqwest` system proxy support)

---

## Features

- **Parallel range downloads**: `-n` sets the initially enabled worker count.
- **Hard cap**: `-N` sets the maximum worker pool size for a download (default `20`).
- **Interactive worker scaling** (TTY):
  - **Ctrl+I / Tab**: increase enabled workers (up to `-N`)
  - **Ctrl+R**: reduce enabled workers (down to 1); reduced workers retire when next idle
  - **Ctrl+C**: cancel immediately (even in raw mode)
- **Timeouts**:
  - `--timeout`: global “no bytes received anywhere” timeout
  - `--ttimeout`: per-connection “no bytes received” timeout (applied to the response stream reads)
  - `--tbackoff`: backoff between retries
- **Signals**: Ctrl-C, SIGTERM, SIGHUP trigger a graceful cancel (metadata is flushed if incomplete).
- **TUI scaling on resize**: if the terminal size changes (or differs on resume), the grid is **scaled** so the UI remains usable.

---

## Install / Build

Build:

```bash
cargo build
```

Run:

```bash
cargo run -- <URL> [args...]
```

Binary output (example toolchain path shown by Cargo):
- `target/**/debug/pwget`

---

## Single download usage

Download a single URL (TUI if TTY):

```bash
pwget https://example.com/big.iso
```

Choose output explicitly (single-file mode):

```bash
pwget https://example.com/file.bin -o file.bin
```

Save into a directory (output name derived from `Content-Disposition` or URL):

```bash
pwget https://example.com/file.bin -d ./downloads
```

Disable TUI (line-based status):

```bash
pwget https://example.com/file.bin --noui
```

Worker controls:
- `-n`: initial enabled workers (default `5`)
- `-N`: hard limit max workers (default `20`)

Example:

```bash
pwget https://example.com/10GB.bin -n 5 -N 20
```

---

## Resume (`.meta`)

By default, a sidecar metadata file is stored at:
- `OUTPUT.meta`

It stores:
- total length
- blocks used
- done bitmap

On completion, the `.meta` file is removed.

---

## Batch mode (`-T`)

Batch mode downloads multiple URLs from a file. It:
- **requires `-d`** (download directory)
- runs in **noui-style** output (single global status line)
- can download multiple URLs concurrently with `-p`

Example:

```bash
pwget -T urls.txt -d ./downloads -p 3 -n 5 -N 20 --yes
```

### `--list-format` (plain/csv/json)

#### plain (default)
One URL per line (blank lines and `#...` comments ignored):

```text
https://example.com/a.bin
https://example.com/b.bin
```

#### csv
Two columns: `save_as`, `url` (quotes recommended):

```text
"a.bin", "https://example.com/a.bin"
"subdir/b.bin", "https://example.com/b.bin"
```

`save_as` is always treated as **relative to `-d`**. Parent directories are created automatically.

#### json
Array of objects:

```json
[
  {"url":"https://example.com/a.bin", "save_as":"a.bin"},
  {"url":"https://example.com/b.bin", "save_as":"subdir/b.bin"}
]
```

### Failed downloads summary
At the end of a `-T` run, `pwget` prints a summary and explicitly lists failed URLs and their errors.

---

## Output naming rules (when `-o` is not provided)

1. `Content-Disposition` filename/filename* (if present)
2. URL last path segment (before `?` / `#`)

Derived filenames are sanitized to avoid directory traversal and are saved into `-d` if provided.

---

## Overwrite prompting

If the output file exists and we’re **not** resuming, `pwget` prompts before overwriting.

To skip prompts (useful for batch runs/CI), pass:

```bash
pwget --yes ...
```

---

## Proxy environment variables

`pwget` honors common proxy environment variables (through `reqwest` system proxy support):
- `http_proxy` / `HTTP_PROXY`
- `https_proxy` / `HTTPS_PROXY`
- `no_proxy` / `NO_PROXY`

### Examples

HTTP proxy:

```bash
export http_proxy="http://proxy.example.com:3128"
pwget https://example.com/file.bin
```

HTTPS proxy:

```bash
export https_proxy="http://proxy.example.com:3128"
pwget https://example.com/file.bin
```

Proxy with username/password:

```bash
export https_proxy="http://username:password@proxy.example.com:3128"
pwget https://example.com/file.bin
```

If your password contains special characters (like `@`, `:`, `/`, `#`, or spaces), URL-encode it, e.g.:

```bash
# password: p@ss:word -> p%40ss%3Aword
export https_proxy="http://username:p%40ss%3Aword@proxy.example.com:3128"
pwget https://example.com/file.bin
```

Bypass proxy for specific hosts (comma-separated):

```bash
export https_proxy="http://proxy.example.com:3128"
export no_proxy="localhost,127.0.0.1,.internal.example.com"
pwget https://example.com/file.bin
```

---

## Notes / Requirements

- The “true parallel” downloader requires the server to support **range requests** (`206 Partial Content`).
- The UI reserves the **last terminal row** for the status line.
- If the terminal is resized, the UI grid is re-mapped on redraw.


