use std::{
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

const RELEASE_API: &str = "https://api.github.com/repos/wushilin/pwget_rs/releases/latest";
const MAX_BINARY_SIZE: u64 = 128 * 1024 * 1024;

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    size: u64,
    digest: Option<String>,
}

struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub(crate) async fn self_update() -> Result<()> {
    let target = release_target(std::env::consts::OS, std::env::consts::ARCH)?;
    let executable = std::env::current_exe()
        .context("failed to locate current executable")?
        .canonicalize()
        .context("failed to resolve current executable path")?;

    let client = reqwest::Client::builder()
        .user_agent(concat!("pwget/", env!("CARGO_PKG_VERSION")))
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .context("failed to create update HTTP client")?;
    let release: Release = client
        .get(RELEASE_API)
        .header(ACCEPT, "application/vnd.github+json")
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .context("failed to query latest GitHub release")?
        .error_for_status()
        .context("GitHub latest-release request failed")?
        .json()
        .await
        .context("failed to parse GitHub release metadata")?;

    let asset_name = format!("pwget-{target}");
    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == asset_name)
        .with_context(|| {
            format!(
                "latest release {} has no {asset_name} asset",
                release.tag_name
            )
        })?;
    ensure!(asset.size > 0, "release asset {asset_name} is empty");
    ensure!(
        asset.size <= MAX_BINARY_SIZE,
        "release asset {asset_name} is unexpectedly large"
    );
    let expected_sha256 = asset
        .digest
        .as_deref()
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .with_context(|| format!("GitHub did not provide a SHA-256 digest for {asset_name}"))?;
    ensure!(
        expected_sha256.len() == 64 && expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "GitHub returned an invalid SHA-256 digest for {asset_name}"
    );

    let current_version = env!("CARGO_PKG_VERSION");
    let release_version = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name);
    if release_version == current_version {
        println!("pwget {current_version} is already the latest release");
        return Ok(());
    }

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    let temp_path =
        executable.with_file_name(format!(".pwget-update-{}-{nonce}.tmp", std::process::id()));
    let mut temp_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await
        .with_context(|| {
            format!(
                "failed to create temporary update file {}",
                temp_path.display()
            )
        })?;
    let _cleanup = TempFile(temp_path.clone());

    let response = client
        .get(&asset.browser_download_url)
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .context("failed to download release binary")?
        .error_for_status()
        .context("release binary download failed")?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    let mut downloaded = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error while receiving release binary")?;
        downloaded = downloaded
            .checked_add(chunk.len() as u64)
            .context("release binary size overflow")?;
        ensure!(
            downloaded <= MAX_BINARY_SIZE,
            "release binary exceeds size limit"
        );
        hasher.update(&chunk);
        temp_file
            .write_all(&chunk)
            .await
            .context("failed writing temporary update file")?;
    }
    temp_file
        .flush()
        .await
        .context("failed flushing temporary update file")?;
    temp_file
        .sync_all()
        .await
        .context("failed syncing temporary update file")?;
    drop(temp_file);

    ensure!(
        downloaded == asset.size,
        "downloaded {downloaded} bytes, expected {}",
        asset.size
    );
    let actual_sha256 = digest_hex(&hasher.finalize());
    ensure!(
        actual_sha256.eq_ignore_ascii_case(expected_sha256),
        "SHA-256 mismatch for {asset_name}"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&executable)
            .context("failed to inspect current executable permissions")?
            .permissions()
            .mode()
            & 0o777;
        std::fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(mode))
            .context("failed to set updated executable permissions")?;
    }

    let output = Command::new(&temp_path)
        .arg("--version")
        .output()
        .with_context(|| format!("downloaded {asset_name} is not executable on this system"))?;
    let version_line = String::from_utf8_lossy(&output.stdout);
    ensure!(
        output.status.success() && version_line.trim() == format!("pwget {release_version}"),
        "downloaded binary reports an unexpected version: {}",
        version_line.trim()
    );

    tokio::fs::rename(&temp_path, &executable)
        .await
        .with_context(|| format!("failed to replace executable {}", executable.display()))?;
    println!("updated pwget from {current_version} to {release_version} ({target})");
    Ok(())
}

fn release_target(os: &str, arch: &str) -> Result<&'static str> {
    match (os, arch) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("freebsd", "x86_64") => Ok("x86_64-unknown-freebsd"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-musl"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-musl"),
        _ => bail!("self-update is not available for {os}/{arch}"),
    }
}

fn digest_hex(digest: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{digest_hex, release_target};
    use sha2::{Digest, Sha256};

    #[test]
    fn selects_release_asset_for_supported_platforms() {
        assert_eq!(
            release_target("macos", "aarch64").unwrap(),
            "aarch64-apple-darwin"
        );
        assert_eq!(
            release_target("macos", "x86_64").unwrap(),
            "x86_64-apple-darwin"
        );
        assert_eq!(
            release_target("freebsd", "x86_64").unwrap(),
            "x86_64-unknown-freebsd"
        );
        assert_eq!(
            release_target("linux", "aarch64").unwrap(),
            "aarch64-unknown-linux-musl"
        );
        assert_eq!(
            release_target("linux", "x86_64").unwrap(),
            "x86_64-unknown-linux-musl"
        );
        assert!(release_target("windows", "x86_64").is_err());
    }

    #[test]
    fn formats_sha256_as_lowercase_hex() {
        assert_eq!(
            digest_hex(&Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
