use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use url::Url;

pub(crate) fn resolve_save_as(download_dir: &Path, save_as: &str) -> Result<PathBuf> {
    let path = PathBuf::from(save_as.trim());
    if path.as_os_str().is_empty() {
        bail!("save_as is empty");
    }
    if path.is_absolute() {
        bail!(
            "save_as must be relative, got absolute path: {}",
            path.display()
        );
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => bail!("save_as must not contain '..': {}", path.display()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => {
                bail!("save_as must be a relative path: {}", path.display());
            }
        }
    }
    Ok(download_dir.join(normalized))
}

pub(crate) fn sanitize_output_filename(path: &Path, user_specified: bool) -> PathBuf {
    if user_specified {
        return path.to_path_buf();
    }
    if let Some(name) = path.file_name().and_then(|name| name.to_str())
        && !name.is_empty()
        && name != "."
        && name != ".."
    {
        return PathBuf::from(name);
    }
    PathBuf::from("download.bin")
}

pub(crate) fn filename_from_content_disposition(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    if let Some(index) = lower.find("filename*=") {
        let encoded = value[(index + "filename*=".len())..]
            .trim()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .trim_matches('"');
        let encoded = encoded
            .find("''")
            .map_or(encoded, |position| &encoded[(position + 2)..]);
        let decoded = percent_decode_str(encoded).decode_utf8().ok()?;
        let decoded = decoded.trim();
        if !decoded.is_empty() {
            return Some(decoded.to_owned());
        }
    }
    if let Some(index) = lower.find("filename=") {
        let filename = value[(index + "filename=".len())..]
            .trim()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim();
        if !filename.is_empty() {
            return Some(filename.to_owned());
        }
    }
    None
}

pub(crate) fn default_output_path(url: &str) -> PathBuf {
    if let Ok(url) = Url::parse(url)
        && let Some(segment) = url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
        && !segment.is_empty()
        && segment != "/"
    {
        return PathBuf::from(segment);
    }
    PathBuf::from("download.bin")
}

pub(crate) async fn ensure_output_parent(output_path: &Path) -> Result<()> {
    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_safe_batch_paths() {
        assert_eq!(
            resolve_save_as(Path::new("downloads"), "./nested/file").unwrap(),
            Path::new("downloads/nested/file")
        );
        assert!(resolve_save_as(Path::new("downloads"), "../file").is_err());
    }

    #[test]
    fn parses_and_sanitizes_remote_names() {
        let name = filename_from_content_disposition("attachment; filename*=UTF-8''a%20b.bin");
        assert_eq!(name.as_deref(), Some("a b.bin"));
        assert_eq!(
            sanitize_output_filename(Path::new("../../evil"), false),
            Path::new("evil")
        );
    }
}
