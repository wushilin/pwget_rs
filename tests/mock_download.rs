use percent_encoding::percent_decode_str;
use std::{
    collections::HashSet,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy)]
enum ServerMode {
    Ranges,
    IgnoreRanges,
    DisconnectOnce,
    StallOnce,
    ChangeValidator,
    LastModified,
    NoValidator,
}

struct MockServer {
    address: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    requests: Arc<AtomicUsize>,
    max_active_responses: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum Payload {
    Bytes(Vec<u8>),
    Seeded { len: usize, seed: u64 },
}

impl Payload {
    fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Seeded { len, .. } => *len,
        }
    }

    fn write_range(&self, stream: &mut TcpStream, start: usize, len: usize) -> std::io::Result<()> {
        match self {
            Self::Bytes(bytes) => stream.write_all(&bytes[start..start + len]),
            Self::Seeded { seed, .. } => {
                let mut offset = start;
                let end = start + len;
                let mut buffer = vec![0; 64 * 1024];
                while offset < end {
                    let chunk_len = (end - offset).min(buffer.len());
                    for (idx, byte) in buffer[..chunk_len].iter_mut().enumerate() {
                        *byte = seeded_byte(*seed, offset + idx);
                    }
                    stream.write_all(&buffer[..chunk_len])?;
                    offset += chunk_len;
                }
                Ok(())
            }
        }
    }
}

fn seeded_byte(seed: u64, index: usize) -> u8 {
    let mut value = seed.wrapping_add((index as u64 / 8).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    (value >> ((index % 8) * 8)) as u8
}

impl MockServer {
    fn start(body: Vec<u8>, etag: &'static str, mode: ServerMode) -> Self {
        Self::start_payload(Payload::Bytes(body), etag, mode)
    }

    fn start_seeded(len: usize, seed: u64, etag: &'static str, mode: ServerMode) -> Self {
        Self::start_payload(Payload::Seeded { len, seed }, etag, mode)
    }

    fn start_payload(payload: Payload, etag: &'static str, mode: ServerMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_thread = requests.clone();
        let max_active_responses = Arc::new(AtomicUsize::new(0));
        let max_active_thread = max_active_responses.clone();
        let active_responses = Arc::new(AtomicUsize::new(0));
        let active_thread = active_responses.clone();
        let disconnected = Arc::new(AtomicBool::new(false));
        let thread = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        requests_thread.fetch_add(1, Ordering::Relaxed);
                        let payload = payload.clone();
                        let disconnected = disconnected.clone();
                        let max_active = max_active_thread.clone();
                        let active = active_thread.clone();
                        thread::spawn(move || {
                            handle_request(
                                &mut stream,
                                &payload,
                                etag,
                                mode,
                                &disconnected,
                                &max_active,
                                &active,
                            );
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
            requests,
            max_active_responses,
        }
    }

    fn url(&self) -> String {
        format!("{}/file.bin", self.address)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

/// URL-configured server used by matrix tests. Each path declares its own
/// payload size and fault knobs while all responses derive from one seed.
struct ScenarioServer {
    address: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    broken_routes: Arc<Mutex<HashSet<String>>>,
}

impl ScenarioServer {
    fn start(seed: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let broken_routes = Arc::new(Mutex::new(HashSet::new()));
        let routes_thread = broken_routes.clone();
        let thread = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let routes = routes_thread.clone();
                        thread::spawn(move || handle_scenario(&mut stream, seed, routes));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
            broken_routes,
        }
    }

    fn url(&self, route: &str) -> String {
        format!("{}{}", self.address, route)
    }
}

impl Drop for ScenarioServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn handle_scenario(stream: &mut TcpStream, seed: u64, broken_routes: Arc<Mutex<HashSet<String>>>) {
    let mut request = Vec::new();
    let mut buf = [0; 4096];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let count = stream.read(&mut buf).unwrap_or(0);
        if count == 0 {
            return;
        }
        request.extend_from_slice(&buf[..count]);
    }
    let request = String::from_utf8_lossy(&request);
    let Some(target) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
    else {
        return;
    };
    let route = percent_decode_str(target).decode_utf8_lossy().into_owned();
    let segments: Vec<_> = route.trim_matches('/').split('/').collect();
    let Some(size_text) = segments.first() else {
        return;
    };
    let size_text = size_text.strip_suffix('M').unwrap_or(size_text);
    let Ok(mut len) = size_text.parse::<usize>() else {
        return;
    };
    if segments[0].ends_with('M') {
        len = len.saturating_mul(1024 * 1024);
    }
    let option = |name: &str| {
        segments
            .iter()
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
    };
    let has_length = option("contentlength") != Some("no");
    let modified = option("modified_date") != Some("no");
    let last_modified = option("lastmodified")
        .filter(|v| *v != "no")
        .unwrap_or("Wed, 21 Oct 2015 07:28:00 GMT");
    let break_after = option("breakafter").and_then(|v| v.parse::<usize>().ok());
    let is_head = request.starts_with("HEAD ");
    let range_start = request.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .strip_prefix("range: bytes=")
            .and_then(|v| v.split('-').next())
            .and_then(|v| v.trim().parse::<usize>().ok())
    });
    let length_header = if has_length {
        format!("Content-Length: {len}\r\n")
    } else {
        String::new()
    };
    let modified_header = if modified {
        format!("Last-Modified: {last_modified}\r\n")
    } else {
        String::new()
    };
    if is_head {
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\n{length_header}Accept-Ranges: bytes\r\n{modified_header}Connection: close\r\n\r\n"
        );
        return;
    }
    let Some(start) = range_start else {
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\n{length_header}Connection: close\r\n\r\n"
        );
        let payload = Payload::Seeded { len, seed };
        let _ = payload.write_range(stream, 0, len);
        return;
    };
    if start >= len {
        let _ = write!(
            stream,
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{len}\r\nContent-Length: 0\r\n\r\n"
        );
        return;
    }
    let end = request
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("range: bytes=")
                .and_then(|v| v.split('-').nth(1))
                .and_then(|v| v.trim().parse::<usize>().ok())
        })
        .unwrap_or(len - 1)
        .min(len - 1);
    let count = end - start + 1;
    let key = route.clone();
    let should_break = break_after.is_some_and(|_| broken_routes.lock().unwrap().insert(key));
    let _ = write!(
        stream,
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {count}\r\nContent-Range: bytes {start}-{end}/{len}\r\n{modified_header}Connection: close\r\n\r\n"
    );
    let payload = Payload::Seeded { len, seed };
    let send = if should_break {
        break_after.unwrap().min(count)
    } else {
        count
    };
    let _ = payload.write_range(stream, start, send);
}

fn handle_request(
    stream: &mut TcpStream,
    body: &Payload,
    etag: &str,
    mode: ServerMode,
    disconnected: &AtomicBool,
    max_active_responses: &AtomicUsize,
    active_responses: &AtomicUsize,
) {
    let mut request = Vec::new();
    let mut buf = [0; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut buf).unwrap_or(0);
        if count == 0 {
            return;
        }
        request.extend_from_slice(&buf[..count]);
    }
    let request = String::from_utf8_lossy(&request);
    let is_head = request.starts_with("HEAD ");
    let range_start = request.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .strip_prefix("range: bytes=")
            .and_then(|value| value.split('-').next())
            .and_then(|value| value.trim().parse::<usize>().ok())
    });
    let effective_etag =
        if matches!(mode, ServerMode::ChangeValidator) && disconnected.load(Ordering::Relaxed) {
            "\"v2\""
        } else {
            etag
        };
    let etag_header = if matches!(mode, ServerMode::LastModified | ServerMode::NoValidator) {
        String::new()
    } else {
        format!("ETag: {effective_etag}\r\n")
    };
    let last_modified_header = if matches!(mode, ServerMode::LastModified) {
        "Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n"
    } else {
        ""
    };
    let supplied_etag = request
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("if-match:"));
    let supplied_modified = request.lines().find(|line| {
        line.to_ascii_lowercase()
            .starts_with("if-unmodified-since:")
    });
    if range_start.is_some()
        && !matches!(mode, ServerMode::IgnoreRanges)
        && (if matches!(mode, ServerMode::LastModified) {
            !supplied_modified.is_some_and(|line| line.ends_with("Wed, 21 Oct 2015 07:28:00 GMT"))
        } else if matches!(mode, ServerMode::NoValidator) {
            true
        } else {
            !supplied_etag.is_some_and(|line| line.ends_with(effective_etag))
        })
    {
        write!(
            stream,
            "HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\n\r\n"
        )
        .unwrap();
        return;
    }

    if is_head {
        let accept_ranges = if matches!(mode, ServerMode::IgnoreRanges) {
            ""
        } else {
            "Accept-Ranges: bytes\r\n"
        };
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{}{}{}\r\n",
            body.len(),
            etag_header,
            last_modified_header,
            accept_ranges
        )
        .unwrap();
        return;
    }

    if matches!(mode, ServerMode::StallOnce)
        && range_start.is_some()
        && !disconnected.swap(true, Ordering::Relaxed)
    {
        // The client test config uses a one-second worker timeout. The late
        // response arrives after that worker has already retried its range.
        thread::sleep(Duration::from_millis(1_250));
    }

    if matches!(mode, ServerMode::IgnoreRanges) || range_start.is_none() {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{}{}\r\n",
            body.len(),
            etag_header,
            last_modified_header
        )
        .unwrap();
        let _ = write_payload(
            stream,
            body,
            0,
            body.len(),
            max_active_responses,
            active_responses,
        );
        return;
    }

    let start = range_start.unwrap();
    if start >= body.len() {
        write!(
            stream,
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\n\r\n",
            body.len()
        )
        .unwrap();
        return;
    }
    let remaining_len = body.len() - start;
    write!(
        stream,
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\n{}{}\r\n",
        remaining_len,
        start,
        body.len() - 1,
        body.len(),
        etag_header,
        last_modified_header
    )
    .unwrap();
    if matches!(
        mode,
        ServerMode::DisconnectOnce | ServerMode::ChangeValidator
    ) && !disconnected.swap(true, Ordering::Relaxed)
        && remaining_len > 1
    {
        let _ = write_payload(
            stream,
            body,
            start,
            remaining_len / 2,
            max_active_responses,
            active_responses,
        );
    } else {
        let _ = write_payload(
            stream,
            body,
            start,
            remaining_len,
            max_active_responses,
            active_responses,
        );
    }
}

fn write_payload(
    stream: &mut TcpStream,
    payload: &Payload,
    start: usize,
    len: usize,
    max_active_responses: &AtomicUsize,
    active_responses: &AtomicUsize,
) -> std::io::Result<()> {
    let active = active_responses.fetch_add(1, Ordering::Relaxed) + 1;
    max_active_responses.fetch_max(active, Ordering::Relaxed);
    let result = payload.write_range(stream, start, len);
    active_responses.fetch_sub(1, Ordering::Relaxed);
    result
}

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("pwget-test-{name}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pwget(url: &str, output: &Path, extra: &[&str]) -> Output {
    pwget_with_workers(url, output, 1, 1, extra)
}

fn pwget_with_workers(
    url: &str,
    output: &Path,
    workers: usize,
    hard_limit: usize,
    extra: &[&str],
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pwget"));
    command
        .arg(url)
        .arg("--noui")
        .arg("--yes")
        .arg("-n")
        .arg(workers.to_string())
        .arg("-N")
        .arg(hard_limit.to_string())
        .arg("--timeout")
        .arg("2")
        .arg("--ttimeout")
        .arg("1")
        .arg("--tbackoff")
        .arg("0")
        .arg("-o")
        .arg(output)
        .args(extra)
        .output()
        .unwrap()
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "pwget failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_seeded_file(path: &Path, len: usize, seed: u64) {
    let mut file = fs::File::open(path).unwrap();
    let mut offset = 0usize;
    let mut actual = vec![0; 64 * 1024];
    while offset < len {
        let wanted = (len - offset).min(actual.len());
        let read = file.read(&mut actual[..wanted]).unwrap();
        assert_eq!(read, wanted, "short output at byte {offset}");
        for (index, byte) in actual[..read].iter().enumerate() {
            assert_eq!(
                *byte,
                seeded_byte(seed, offset + index),
                "payload mismatch at byte {}",
                offset + index
            );
        }
        offset += read;
    }
    let mut extra = [0u8; 1];
    assert_eq!(
        file.read(&mut extra).unwrap(),
        0,
        "output longer than expected"
    );
}

fn write_meta(
    path: &Path,
    output: &Path,
    url: &str,
    len: usize,
    etag: &str,
    body: &[u8],
    done: Vec<bool>,
) {
    let bytes_per_block = len.div_ceil(done.len());
    let checksums: Vec<Option<u32>> = done
        .iter()
        .enumerate()
        .map(|(idx, is_done)| {
            let start = idx * bytes_per_block;
            let end = (start + bytes_per_block).min(body.len());
            (*is_done && start < end).then(|| crc32fast::hash(&body[start..end]))
        })
        .collect();
    let value = serde_json::json!({
        "version": 3,
        "output": output.display().to_string(),
        "url": url,
        "etag": etag,
        "last_modified": null,
        "total_len": len,
        "blocks_used": done.len(),
        "done": done,
        "block_checksums": checksums,
    });
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

#[test]
fn parallel_download_and_nested_output() {
    let body = b"small, deterministic mock payload".to_vec();
    let server = MockServer::start(body.clone(), "\"v1\"", ServerMode::Ranges);
    let dir = TestDir::new("parallel");
    let output = dir.0.join("nested/path/file.bin");
    let result = pwget(&server.url(), &output, &[]);
    assert!(
        result.status.success(),
        "pwget failed:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(output).unwrap(),
        body,
        "pwget output:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn downloads_seeded_generated_payload_with_multiple_workers() {
    let len = 8 * 1024 * 1024;
    let seed = 0x5eed_cafe_1234_9876;
    let server = MockServer::start_seeded(len, seed, "\"seeded-v1\"", ServerMode::Ranges);
    let dir = TestDir::new("seeded-parallel");
    let output = dir.0.join("large.bin");
    let result = pwget_with_workers(&server.url(), &output, 4, 4, &[]);
    assert_success(result);
    assert_seeded_file(&output, len, seed);
    assert!(server.requests.load(Ordering::Relaxed) > 2);
    assert!(
        server.max_active_responses.load(Ordering::Relaxed) >= 2,
        "mock server observed no overlapping range responses"
    );
}

#[test]
fn url_scenario_matrix_covers_stream_ranges_and_one_time_disconnect() {
    let seed = 0x9a71_5eed_d15c_a11e;
    let server = ScenarioServer::start(seed);
    let dir = TestDir::new("scenario-matrix");
    let cases = [
        (
            "/12M/contentlength=no/modified_date=no/download",
            12 * 1024 * 1024,
        ),
        (
            "/17M/contentlength=yes/lastmodified=Wed%2C%2021%20Oct%202015%2007%3A28%3A00%20GMT/download",
            17 * 1024 * 1024,
        ),
        (
            "/1233242/contentlength=yes/breakafter=1232/download",
            1_233_242,
        ),
    ];
    for (index, (route, len)) in cases.into_iter().enumerate() {
        let output = dir.0.join(format!("case-{index}.bin"));
        let result = pwget_with_workers(&server.url(route), &output, 4, 4, &[]);
        assert_success(result);
        assert_seeded_file(&output, len, seed);
    }
    let broken = server.broken_routes.lock().unwrap();
    assert!(broken.contains("/1233242/contentlength=yes/breakafter=1232/download"));
}

#[test]
fn reconnects_after_truncated_response() {
    let body = vec![b'x'; 16 * 1024];
    let server = MockServer::start(body.clone(), "\"v1\"", ServerMode::DisconnectOnce);
    let dir = TestDir::new("disconnect");
    let output = dir.0.join("file.bin");
    assert_success(pwget(&server.url(), &output, &[]));
    assert_eq!(fs::read(output).unwrap(), body);
    assert!(server.requests.load(Ordering::Relaxed) >= 3);
}

#[test]
fn retries_a_worker_that_makes_no_progress_before_timeout() {
    let body = vec![b's'; 16 * 1024];
    let server = MockServer::start(body.clone(), "\"v1\"", ServerMode::StallOnce);
    let dir = TestDir::new("stalled-worker");
    let output = dir.0.join("file.bin");
    assert_success(pwget(&server.url(), &output, &[]));
    assert_eq!(fs::read(output).unwrap(), body);
    assert!(server.requests.load(Ordering::Relaxed) >= 3);
}

#[test]
fn rejects_a_resource_that_changes_during_retry() {
    let body = vec![b'x'; 16 * 1024];
    let server = MockServer::start(body, "\"v1\"", ServerMode::ChangeValidator);
    let dir = TestDir::new("change-during-retry");
    let result = pwget(&server.url(), &dir.0.join("file.bin"), &[]);
    assert!(
        !result.status.success(),
        "unexpected success: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("412 Precondition Failed"),
        "unexpected error: {stderr}"
    );
}

#[test]
fn debug_output_redacts_header_values() {
    let body = b"secret test".to_vec();
    let server = MockServer::start(body, "\"v1\"", ServerMode::Ranges);
    let dir = TestDir::new("redaction");
    let result = pwget(
        &server.url(),
        &dir.0.join("file.bin"),
        &["--debug", "--header", "Authorization=top-secret"],
    );
    assert!(
        result.status.success(),
        "pwget failed:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(!stderr.contains("top-secret"));
    assert!(stderr.contains("authorization=<redacted>"));
}

#[test]
fn resumes_only_missing_blocks() {
    let body = b"0123456789abcdef".to_vec();
    let server = MockServer::start(body.clone(), "\"v1\"", ServerMode::Ranges);
    let dir = TestDir::new("resume");
    let output = dir.0.join("file.bin");
    let meta = dir.0.join("resume.meta");
    let mut partial = vec![0; body.len()];
    partial[..8].copy_from_slice(&body[..8]);
    fs::write(&output, partial).unwrap();
    write_meta(
        &meta,
        &output,
        &server.url(),
        body.len(),
        "\"v1\"",
        &body,
        [vec![true; 8], vec![false; 8]].concat(),
    );
    assert_success(pwget(
        &server.url(),
        &output,
        &["--meta", meta.to_str().unwrap()],
    ));
    assert_eq!(fs::read(output).unwrap(), body);
    assert!(!meta.exists());
}

#[test]
fn rechecks_same_length_corrupted_resume_blocks() {
    let body = b"resume integrity checks catch same-size edits".to_vec();
    let server = MockServer::start(body.clone(), "\"v1\"", ServerMode::Ranges);
    let dir = TestDir::new("corrupt-resume");
    let output = dir.0.join("file.bin");
    let meta = dir.0.join("resume.meta");
    let mut corrupted = body.clone();
    corrupted[0] ^= 0xff;
    fs::write(&output, corrupted).unwrap();
    let mut done = vec![false; body.len()];
    done[0] = true;
    write_meta(
        &meta,
        &output,
        &server.url(),
        body.len(),
        "\"v1\"",
        &body,
        done,
    );
    assert_success(pwget(
        &server.url(),
        &output,
        &["--meta", meta.to_str().unwrap()],
    ));
    assert_eq!(fs::read(output).unwrap(), body);
}

#[test]
fn rejects_changed_etag_size_and_missing_resume_output() {
    let body = b"0123456789abcdef".to_vec();
    let server = MockServer::start(body.clone(), "\"new\"", ServerMode::Ranges);
    let dir = TestDir::new("resume-reject");

    for (name, meta_len, create_output, expected) in [
        ("etag", body.len(), true, "ETag mismatch"),
        ("size", body.len() + 1, true, "total size mismatch"),
        ("missing", body.len(), false, "output file is missing"),
    ] {
        let output = dir.0.join(format!("{name}.bin"));
        let meta = dir.0.join(format!("{name}.meta"));
        if create_output {
            fs::write(&output, vec![0; body.len()]).unwrap();
        }
        write_meta(
            &meta,
            &output,
            &server.url(),
            meta_len,
            if name == "etag" { "\"old\"" } else { "\"new\"" },
            &body,
            vec![false; body.len()],
        );
        let result = pwget(&server.url(), &output, &["--meta", meta.to_str().unwrap()]);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(expected));
    }
}

#[test]
fn falls_back_to_stream_and_handles_empty_files() {
    let body = b"server ignores Range".to_vec();
    let stream_server = MockServer::start(body.clone(), "\"v1\"", ServerMode::IgnoreRanges);
    let dir = TestDir::new("stream-empty");
    let stream_output = dir.0.join("stream.bin");
    assert_success(pwget(&stream_server.url(), &stream_output, &[]));
    assert_eq!(fs::read(stream_output).unwrap(), body);

    let no_validator_body = b"stream without validator".to_vec();
    let no_validator_server =
        MockServer::start(no_validator_body.clone(), "", ServerMode::NoValidator);
    let no_validator_output = dir.0.join("no-validator.bin");
    let result = pwget(&no_validator_server.url(), &no_validator_output, &[]);
    assert_success(result);
    assert_eq!(fs::read(no_validator_output).unwrap(), no_validator_body);

    let empty_server = MockServer::start(Vec::new(), "\"empty\"", ServerMode::Ranges);
    let empty_output = dir.0.join("empty.bin");
    let result = pwget(&empty_server.url(), &empty_output, &[]);
    assert_success(Output {
        status: result.status,
        stdout: result.stdout.clone(),
        stderr: result.stderr.clone(),
    });
    assert!(String::from_utf8_lossy(&result.stderr).contains("total_bytes: 0B"));
    assert_eq!(fs::metadata(empty_output).unwrap().len(), 0);
}

#[test]
fn uses_last_modified_precondition_when_etag_is_absent() {
    let body = b"last modified fallback".to_vec();
    let server = MockServer::start(body.clone(), "", ServerMode::LastModified);
    let dir = TestDir::new("last-modified");
    let output = dir.0.join("file.bin");
    assert_success(pwget(&server.url(), &output, &[]));
    assert_eq!(fs::read(output).unwrap(), body);
}

#[test]
fn batch_output_collision_is_a_failure() {
    let body = b"batch body".to_vec();
    let server = MockServer::start(body, "\"v1\"", ServerMode::Ranges);
    let dir = TestDir::new("collision");
    let list = dir.0.join("list.json");
    let entries = serde_json::json!([
        {"url": server.url(), "save_as": "same.bin"},
        {"url": server.url(), "save_as": "./same.bin"}
    ]);
    fs::write(&list, serde_json::to_vec(&entries).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_pwget"))
        .arg("-T")
        .arg(&list)
        .arg("-d")
        .arg(dir.0.join("downloads"))
        .arg("--list-format")
        .arg("json")
        .arg("--yes")
        .arg("-n")
        .arg("1")
        .arg("-N")
        .arg("1")
        .output()
        .unwrap();
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("batch output collision"));
    assert!(stderr.contains("1 batch download(s) failed"));
}

#[test]
fn ordinary_batch_download_failures_return_nonzero() {
    let dir = TestDir::new("batch-failure");
    let list = dir.0.join("list.json");
    fs::write(
        &list,
        br#"[{"url":"not a valid absolute URL","save_as":"bad.bin"}]"#,
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_pwget"))
        .arg("-T")
        .arg(&list)
        .arg("-d")
        .arg(dir.0.join("downloads"))
        .arg("--list-format")
        .arg("json")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("1 batch download(s) failed"));
}
