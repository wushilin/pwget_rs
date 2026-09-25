# Pending issues

The requested review fixes and modularization are implemented in the current worktree. Completed follow-ups are recorded here so the handoff stays useful; only FreeBSD environment validation remains open.

## High priority

- **Completed:** Resume metadata now stores CRC32 checksums per completed block and verifies them before trusting done bits. Corrupt blocks are scheduled again; older metadata without checksums is accepted conservatively by re-downloading its completed blocks. The regression test edits a block without changing the output size.

## Verification gaps

- **FreeBSD native build/runtime:** The Rust `x86_64-unknown-freebsd` standard library target is installed, but cross-compilation on this macOS host stops in `ring`'s C build because there is no FreeBSD C sysroot (`assert.h` is unavailable). Run `cargo check` and the binary tests on a FreeBSD host or CI runner to finish this verification. The crate declares Rust 1.88 due to its use of stabilized let chains.
- **Completed:** Mock tests exercise `If-Unmodified-Since` when ETag is absent, and stream fallback when both validators are absent.
- **Completed:** Empty-file output and its final summary are asserted.
- **Completed:** An invalid URL in batch mode is asserted to return a nonzero exit code.
- **Completed:** The built-in mock server can generate a seeded payload on demand. A multiworker integration test downloads 8 MiB with four workers and checks every output byte against the seed-derived generator without storing a golden payload.
- **Completed:** URL-driven scenario matrix uses one seed across multiple payload sizes and covers absent Content-Length/Last-Modified (stream fallback), a Last-Modified range download, and a range response truncated once at `breakafter=1232` (retry/reconnect). The server is organized around per-route options so deterministic delay/rate throttling and seeded intermittent hiccups can be added as further scenarios.

## Refactor follow-up

- **Optional:** `main.rs` is smaller and the HTTP, range-worker, batch parsing, output naming, and progress code have separate modules. The command orchestration and TUI event loops still live in `main.rs`; consider extracting those if further feature work makes the file difficult to navigate.
