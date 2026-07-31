use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaV2 {
    pub version: u32,
    pub output: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>,
    pub total_len: u64,
    pub blocks_used: usize,
    pub done: Vec<bool>,
}

impl MetaV2 {
    pub fn meta_path_for_output(output: &Path) -> PathBuf {
        PathBuf::from(format!("{}.meta", output.display()))
    }
}

pub async fn load_meta_if_present(path: &Path) -> Result<Option<MetaV2>> {
    match tokio::fs::read_to_string(path).await {
        Ok(s) => {
            let m: MetaV2 = serde_json::from_str(&s).with_context(|| {
                format!("failed parsing meta file {} (expected v2)", path.display())
            })?;
            Ok(Some(m))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow!(e).context(format!("failed reading meta file {}", path.display()))),
    }
}

pub async fn write_meta_atomic(path: &Path, meta: &MetaV2) -> Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let data = serde_json::to_vec(meta).context("failed serializing meta")?;
    tokio::fs::write(&tmp, data)
        .await
        .with_context(|| format!("failed writing tmp meta {}", tmp.display()))?;
    // Best-effort atomic replace.
    tokio::fs::rename(&tmp, path).await.with_context(|| {
        format!(
            "failed renaming meta {} -> {}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}
