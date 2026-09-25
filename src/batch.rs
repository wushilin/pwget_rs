use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use crate::cli::ListFormat;

#[derive(Debug, Clone)]
pub(crate) struct BatchItem {
    pub(crate) url: String,
    pub(crate) save_as: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonEntry {
    url: String,
    #[serde(default)]
    save_as: Option<String>,
}

pub(crate) fn parse_url_list(text: &str, format: ListFormat) -> Result<Vec<BatchItem>> {
    match format {
        ListFormat::Plain => Ok(text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|url| BatchItem {
                url: url.to_owned(),
                save_as: None,
            })
            .collect()),
        ListFormat::Csv => parse_csv(text),
        ListFormat::Json => {
            let entries: Vec<JsonEntry> =
                serde_json::from_str(text).context("failed to parse json url list")?;
            entries
                .into_iter()
                .enumerate()
                .map(|(index, entry)| {
                    if entry.url.trim().is_empty() {
                        bail!("json entry {}: url is empty", index + 1);
                    }
                    Ok(BatchItem {
                        url: entry.url,
                        save_as: entry.save_as,
                    })
                })
                .collect()
        }
    }
}

fn parse_csv(text: &str) -> Result<Vec<BatchItem>> {
    let mut items = Vec::new();
    for (index, original) in text.lines().enumerate() {
        let line = original.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (save_as, rest) = parse_field(line)
            .with_context(|| format!("csv line {}: failed parsing save_as", index + 1))?;
        let rest = rest
            .trim_start()
            .strip_prefix(',')
            .ok_or_else(|| anyhow!("csv line {}: expected ',' separator", index + 1))?;
        let (url, trailing) = parse_field(rest)
            .with_context(|| format!("csv line {}: failed parsing url", index + 1))?;
        if !trailing.trim().is_empty() {
            bail!("csv line {}: unexpected trailing data", index + 1);
        }
        if url.trim().is_empty() {
            bail!("csv line {}: url is empty", index + 1);
        }
        items.push(BatchItem {
            url: url.trim().to_owned(),
            save_as: (!save_as.trim().is_empty()).then(|| save_as.trim().to_owned()),
        });
    }
    Ok(items)
}

fn parse_field(mut input: &str) -> Result<(String, &str)> {
    input = input.trim_start();
    let Some(mut remaining) = input.strip_prefix('"') else {
        let end = input.find(',').unwrap_or(input.len());
        return Ok((input[..end].trim().to_owned(), &input[end..]));
    };
    let mut output = String::new();
    loop {
        let Some(position) = remaining.find('"') else {
            bail!("unterminated quote");
        };
        output.push_str(&remaining[..position]);
        remaining = &remaining[(position + 1)..];
        if let Some(after_escape) = remaining.strip_prefix('"') {
            output.push('"');
            remaining = after_escape;
        } else {
            return Ok((output, remaining));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_csv_and_rejects_extra_columns() {
        let items = parse_url_list("\"a,b\", \"https://example.test/a\"", ListFormat::Csv).unwrap();
        assert_eq!(items[0].save_as.as_deref(), Some("a,b"));
        assert!(parse_url_list("a,url,extra", ListFormat::Csv).is_err());
    }

    #[test]
    fn rejects_empty_json_urls() {
        assert!(parse_url_list(r#"[{"url":""}]"#, ListFormat::Json).is_err());
    }
}
