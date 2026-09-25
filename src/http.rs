use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG,
    HeaderMap, HeaderName, HeaderValue, IF_MATCH, IF_UNMODIFIED_SINCE, LAST_MODIFIED, RANGE,
};

use crate::debug_println;

#[derive(Debug, Clone)]
pub(crate) struct ProbeInfo {
    pub(crate) len: Option<u64>,
    pub(crate) ranges_ok: bool,
    pub(crate) content_disposition: Option<String>,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamFallbackReason {
    UnknownLength,
    RangesUnavailable,
    MissingValidator,
}

impl StreamFallbackReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::UnknownLength => "unknown content length",
            Self::RangesUnavailable => "byte ranges unavailable",
            Self::MissingValidator => "no stable resource validator",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RangePrecondition {
    pub(crate) name: HeaderName,
    pub(crate) value: HeaderValue,
}

pub(crate) fn range_precondition(probe: &ProbeInfo) -> Option<RangePrecondition> {
    if let Some(etag) = probe
        .etag
        .as_deref()
        .filter(|value| !value.starts_with("W/"))
        && let Ok(value) = HeaderValue::from_str(etag)
    {
        return Some(RangePrecondition {
            name: IF_MATCH,
            value,
        });
    }
    probe
        .last_modified
        .as_deref()
        .and_then(|value| HeaderValue::from_str(value).ok())
        .map(|value| RangePrecondition {
            name: IF_UNMODIFIED_SINCE,
            value,
        })
}

pub(crate) async fn probe_len_and_ranges(
    client: &reqwest::Client,
    url: &str,
    debug: bool,
) -> Result<ProbeInfo> {
    let mut len = None;
    let mut ranges_ok = false;
    let mut content_disposition = None;
    let mut etag = None;
    let mut last_modified = None;

    if let Ok(response) = client.head(url).send().await {
        debug_println(debug, format!("probe: HEAD status={}", response.status()));
        if response.status().is_success() {
            len = parse_len(response.headers().get(CONTENT_LENGTH));
            ranges_ok = parse_ranges(response.headers().get(ACCEPT_RANGES));
            content_disposition = header_string(response.headers(), CONTENT_DISPOSITION);
            etag = header_string(response.headers(), ETAG);
            last_modified = header_string(response.headers(), LAST_MODIFIED);
        }
    }

    if len.is_none() || !ranges_ok {
        let response = client
            .get(url)
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .context("failed to GET for probing")?;
        debug_println(
            debug,
            format!("probe: GET bytes=0-0 status={}", response.status()),
        );
        if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            ranges_ok = true;
            len = len.or_else(|| {
                parse_total_len_from_content_range(response.headers().get(CONTENT_RANGE))
            });
        } else if response.status().is_success() {
            ranges_ok |= parse_ranges(response.headers().get(ACCEPT_RANGES));
            len = len.or_else(|| parse_len(response.headers().get(CONTENT_LENGTH)));
        } else {
            bail!(
                "range probe failed: server returned {} for GET bytes=0-0",
                response.status()
            );
        }
        content_disposition =
            content_disposition.or_else(|| header_string(response.headers(), CONTENT_DISPOSITION));
        etag = etag.or_else(|| header_string(response.headers(), ETAG));
        last_modified = last_modified.or_else(|| header_string(response.headers(), LAST_MODIFIED));
    }

    Ok(ProbeInfo {
        len,
        ranges_ok,
        content_disposition,
        etag,
        last_modified,
    })
}

fn header_string(headers: &HeaderMap, name: HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn parse_len(value: Option<&HeaderValue>) -> Option<u64> {
    value
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn parse_ranges(value: Option<&HeaderValue>) -> bool {
    value
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("bytes"))
}

fn parse_total_len_from_content_range(value: Option<&HeaderValue>) -> Option<u64> {
    let value = value?.to_str().ok()?;
    let total = value[(value.rfind('/')? + 1)..].trim();
    (total != "*").then(|| total.parse().ok()).flatten()
}

#[derive(Debug)]
struct ParsedContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(value: Option<&HeaderValue>) -> Option<ParsedContentRange> {
    let value = value?.to_str().ok()?.trim();
    let rest = value.strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some(ParsedContentRange {
        start: start.trim().parse().ok()?,
        end: end.trim().parse().ok()?,
        total: match total.trim() {
            "*" => None,
            number => Some(number.parse().ok()?),
        },
    })
}

pub(crate) fn validate_content_range(
    value: Option<&HeaderValue>,
    requested_start: u64,
    expected_total: u64,
) -> Result<()> {
    let range =
        parse_content_range(value).ok_or_else(|| anyhow!("missing/invalid Content-Range"))?;
    if range.start != requested_start {
        bail!(
            "Content-Range start mismatch: requested {}, got {}",
            requested_start,
            range.start
        );
    }
    if range.end < range.start {
        bail!(
            "Content-Range end is before start: {}-{}",
            range.start,
            range.end
        );
    }
    if range.end >= expected_total {
        bail!(
            "Content-Range end {} is outside expected total {}",
            range.end,
            expected_total
        );
    }
    if range.total.is_some_and(|total| total != expected_total) {
        bail!(
            "Content-Range total mismatch: expected {}, got {}",
            expected_total,
            range.total.unwrap()
        );
    }
    Ok(())
}

pub(crate) fn validate_identity_encoding(headers: &HeaderMap) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chooses_only_safe_preconditions() {
        let mut probe = ProbeInfo {
            len: Some(1),
            ranges_ok: true,
            content_disposition: None,
            etag: Some("W/\"weak\"".into()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
        };
        assert_eq!(
            range_precondition(&probe).unwrap().name,
            IF_UNMODIFIED_SINCE
        );
        probe.etag = Some("\"strong\"".into());
        assert_eq!(range_precondition(&probe).unwrap().name, IF_MATCH);
        probe.etag = Some("W/\"weak\"".into());
        probe.last_modified = None;
        assert!(range_precondition(&probe).is_none());
    }

    #[test]
    fn validates_content_ranges() {
        let valid = HeaderValue::from_static("bytes 5-9/10");
        validate_content_range(Some(&valid), 5, 10).unwrap();
        let wrong = HeaderValue::from_static("bytes 4-9/10");
        assert!(validate_content_range(Some(&wrong), 5, 10).is_err());
    }
}
