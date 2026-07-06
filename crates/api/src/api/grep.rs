//! Grep API request/response types and linear-time regex search engine.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GrepRequest {
    pub pattern: String,
    #[serde(default)]
    pub filters: GrepFilters,
    #[serde(default)]
    pub normalization: NormalizationMode,
    #[serde(default)]
    pub order: GrepOrder,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GrepFilters {
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub containers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationMode {
    #[default]
    Nfkc,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrepOrder {
    #[default]
    DateDesc,
    DateAsc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrepRecord {
    pub record_id: String,
    pub source_type: String,
    pub anchor_url: String,
    pub source_title: String,
    #[serde(default)]
    pub source_location: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub text: String,
    pub normalized_text: String,
    #[serde(default)]
    pub thread_ts: Option<String>,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrepMatch {
    pub record_id: String,
    pub source_type: String,
    pub anchor_url: String,
    pub source_title: String,
    #[serde(default)]
    pub source_location: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub snippet: String,
    pub matched_ranges: Vec<MatchedRange>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatchedRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrepResponse {
    pub matches: Vec<GrepMatch>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    pub complete: bool,
    pub projection_watermark: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDetailResponse {
    pub record: GrepRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadResponse {
    pub thread_ts: String,
    pub records: Vec<GrepRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structure: Option<ThreadStructure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadStructure {
    pub thread_key: String,
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_session: Option<ThreadSession>,
    #[serde(default)]
    pub sidechains: Vec<ThreadSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadSession {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub is_sidechain: bool,
    pub record_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveLinkRequest {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveLinkResponse {
    pub record_id: String,
    pub source_type: String,
    pub anchor_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorQaSearchRequest {
    pub query: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorQaSearchResponse<T> {
    pub matches: Vec<T>,
    pub is_primary_source: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum GrepError {
    #[error("invalid regex pattern: {0}")]
    InvalidPattern(String),
    #[error("cursor must be an encoded keyset cursor")]
    InvalidCursor,
    #[error("limit must be between 1 and {0}")]
    InvalidLimit(usize),
    #[error("regex execution exceeded {0}ms")]
    TimedOut(u64),
}

pub struct GrepEngine {
    max_limit: usize,
    timeout: Duration,
    use_trigram_index: bool,
}

impl GrepEngine {
    pub fn new(max_limit: usize) -> Self {
        Self {
            max_limit,
            timeout: Duration::from_millis(500),
            use_trigram_index: true,
        }
    }

    pub fn without_trigram_index(mut self) -> Self {
        self.use_trigram_index = false;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn search(
        &self,
        records: &[GrepRecord],
        request: &GrepRequest,
        projection_watermark: String,
    ) -> Result<GrepResponse, GrepError> {
        let limit = request.limit.unwrap_or(100);
        if limit == 0 || limit > self.max_limit {
            return Err(GrepError::InvalidLimit(self.max_limit));
        }
        let cursor = request.cursor.as_deref().map(decode_cursor).transpose()?;
        let pattern = match request.normalization {
            NormalizationMode::Nfkc => normalize(&request.pattern),
            NormalizationMode::None => request.pattern.clone(),
        };
        let regex =
            Regex::new(&pattern).map_err(|err| GrepError::InvalidPattern(err.to_string()))?;
        let filtered = records
            .iter()
            .filter(|record| filters_match(record, &request.filters))
            .collect::<Vec<_>>();
        let index = self
            .use_trigram_index
            .then(|| TrigramIndex::build_refs(&filtered));
        let candidate_indices = index
            .as_ref()
            .and_then(|index| index.candidate_indices(&pattern))
            .unwrap_or_else(|| (0..filtered.len()).collect());

        let mut candidates = candidate_indices
            .into_iter()
            .map(|idx| filtered[idx])
            .collect::<Vec<_>>();
        match request.order {
            GrepOrder::DateDesc => candidates.sort_by(|left, right| {
                right
                    .timestamp
                    .cmp(&left.timestamp)
                    .then_with(|| left.record_id.cmp(&right.record_id))
            }),
            GrepOrder::DateAsc => candidates.sort_by(|left, right| {
                left.timestamp
                    .cmp(&right.timestamp)
                    .then_with(|| left.record_id.cmp(&right.record_id))
            }),
        }

        let start = Instant::now();
        let mut matches = Vec::new();
        for record in candidates {
            if start.elapsed() > self.timeout {
                return Err(GrepError::TimedOut(self.timeout.as_millis() as u64));
            }
            if cursor
                .as_ref()
                .is_some_and(|cursor| !is_after_cursor(record, cursor, request.order))
            {
                continue;
            }
            if let Some(matched) = match_record(record, &regex, request.normalization) {
                matches.push(matched);
                if matches.len() > limit {
                    break;
                }
            }
        }
        let complete = matches.len() <= limit;
        if !complete {
            matches.truncate(limit);
        }
        let next_cursor = if complete {
            None
        } else {
            matches.last().map(encode_cursor).transpose()?
        };
        Ok(GrepResponse {
            matches,
            next_cursor,
            complete,
            projection_watermark,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrepCursor {
    timestamp: DateTime<Utc>,
    record_id: String,
}

fn encode_cursor(record: &GrepMatch) -> Result<String, GrepError> {
    let cursor = GrepCursor {
        timestamp: record.timestamp,
        record_id: record.record_id.clone(),
    };
    let bytes = serde_json::to_vec(&cursor).map_err(|_| GrepError::InvalidCursor)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(cursor: &str) -> Result<GrepCursor, GrepError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| GrepError::InvalidCursor)?;
    serde_json::from_slice(&bytes).map_err(|_| GrepError::InvalidCursor)
}

fn is_after_cursor(record: &GrepRecord, cursor: &GrepCursor, order: GrepOrder) -> bool {
    match order {
        GrepOrder::DateDesc => {
            record.timestamp < cursor.timestamp
                || (record.timestamp == cursor.timestamp
                    && record.record_id.as_str() > cursor.record_id.as_str())
        }
        GrepOrder::DateAsc => {
            record.timestamp > cursor.timestamp
                || (record.timestamp == cursor.timestamp
                    && record.record_id.as_str() > cursor.record_id.as_str())
        }
    }
}

#[derive(Debug)]
struct TrigramIndex {
    postings: HashMap<String, Vec<usize>>,
}

impl TrigramIndex {
    fn build_refs(records: &[&GrepRecord]) -> Self {
        let mut postings: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, record) in records.iter().enumerate() {
            let mut seen = HashSet::new();
            for trigram in trigrams(&record.normalized_text) {
                if seen.insert(trigram.clone()) {
                    postings.entry(trigram).or_default().push(idx);
                }
            }
        }
        Self { postings }
    }

    fn candidate_indices(&self, normalized_pattern: &str) -> Option<Vec<usize>> {
        let required = plain_literal_trigrams(normalized_pattern)?;
        let mut iter = required.iter();
        let first = self
            .postings
            .get(iter.next()?)?
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let intersection = iter.fold(first, |acc, trigram| {
            let Some(posting) = self.postings.get(trigram) else {
                return HashSet::new();
            };
            let posting = posting.iter().copied().collect::<HashSet<_>>();
            acc.intersection(&posting).copied().collect()
        });
        let mut result = intersection.into_iter().collect::<Vec<_>>();
        result.sort_unstable();
        Some(result)
    }
}

fn plain_literal_trigrams(pattern: &str) -> Option<Vec<String>> {
    if pattern.chars().any(|ch| {
        matches!(
            ch,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        )
    }) {
        return None;
    }
    let trigrams = trigrams(pattern);
    (!trigrams.is_empty()).then_some(trigrams)
}

fn trigrams(value: &str) -> Vec<String> {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() < 3 {
        return Vec::new();
    }
    chars
        .windows(3)
        .map(|window| window.iter().collect::<String>())
        .collect()
}

pub fn normalize(value: &str) -> String {
    value.nfkc().collect()
}

fn filters_match(record: &GrepRecord, filters: &GrepFilters) -> bool {
    if !filters.types.is_empty()
        && !filters
            .types
            .iter()
            .any(|value| value == &record.source_type)
    {
        return false;
    }
    if filters.from.is_some_and(|from| record.timestamp < from) {
        return false;
    }
    if filters.to.is_some_and(|to| record.timestamp > to) {
        return false;
    }
    if !filters.channels.is_empty()
        && !record
            .container
            .as_ref()
            .is_some_and(|container| filters.channels.iter().any(|value| value == container))
    {
        return false;
    }
    if !filters.containers.is_empty()
        && !record
            .container
            .as_ref()
            .is_some_and(|container| filters.containers.iter().any(|value| value == container))
    {
        return false;
    }
    true
}

fn match_record(
    record: &GrepRecord,
    regex: &Regex,
    normalization: NormalizationMode,
) -> Option<GrepMatch> {
    let haystack = match normalization {
        NormalizationMode::Nfkc => record.normalized_text.as_str(),
        NormalizationMode::None => record.text.as_str(),
    };
    let ranges = regex
        .find_iter(haystack)
        .map(|matched| MatchedRange {
            start: matched.start(),
            end: matched.end(),
        })
        .collect::<Vec<_>>();
    if ranges.is_empty() {
        return None;
    }
    Some(GrepMatch {
        record_id: record.record_id.clone(),
        source_type: record.source_type.clone(),
        anchor_url: record.anchor_url.clone(),
        source_title: record.source_title.clone(),
        source_location: record.source_location.clone(),
        timestamp: record.timestamp,
        snippet: snippet(&record.text),
        matched_ranges: ranges,
        metadata: record.metadata.clone(),
    })
}

fn snippet(text: &str) -> String {
    const MAX_CHARS: usize = 240;
    let mut chars = text.chars();
    let snippet = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{snippet}...")
    } else {
        snippet
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, text: &str) -> GrepRecord {
        record_at(id, text, "2026-01-01T00:00:00Z")
    }

    fn record_at(id: &str, text: &str, timestamp: &str) -> GrepRecord {
        GrepRecord {
            record_id: id.into(),
            source_type: "docs".into(),
            anchor_url: "https://example.test".into(),
            source_title: "Doc".into(),
            source_location: None,
            timestamp: DateTime::parse_from_rfc3339(timestamp)
                .unwrap()
                .with_timezone(&Utc),
            text: text.into(),
            normalized_text: normalize(text),
            thread_ts: None,
            container: None,
            metadata: serde_json::json!({}),
        }
    }

    fn record_with_type(id: &str, source_type: &str, text: &str) -> GrepRecord {
        GrepRecord {
            source_type: source_type.into(),
            ..record(id, text)
        }
    }

    #[test]
    fn nfkc_search_matches_fullwidth_digits() {
        let engine = GrepEngine::new(100);
        let response = engine
            .search(
                &[record("r1", "部屋１２３")],
                &GrepRequest {
                    pattern: "123".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();
        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].snippet, "部屋１２３");
    }

    #[test]
    fn backreference_is_rejected_by_regex_crate() {
        let engine = GrepEngine::new(100);
        let result = engine.search(
            &[record("r1", "aa")],
            &GrepRequest {
                pattern: r"(a)\1".into(),
                ..GrepRequest::default()
            },
            "wm".into(),
        );
        assert!(matches!(result, Err(GrepError::InvalidPattern(_))));
    }

    #[test]
    fn trigram_index_and_full_scan_return_same_matches() {
        let records = vec![
            record("r1", "忘れ物は受付"),
            record("r2", "別の文章"),
            record("r3", "受付に忘れ物"),
        ];
        let request = GrepRequest {
            pattern: "忘れ物".into(),
            ..GrepRequest::default()
        };
        let indexed = GrepEngine::new(100)
            .search(&records, &request, "wm".into())
            .unwrap();
        let full_scan = GrepEngine::new(100)
            .without_trigram_index()
            .search(&records, &request, "wm".into())
            .unwrap();
        assert_eq!(indexed.matches, full_scan.matches);
    }

    #[test]
    fn type_filter_is_applied_with_trigram_index() {
        let records = vec![
            record_with_type("r1", "claude-ai", "needle from claude"),
            record_with_type("r2", "github-commit", "needle from github"),
            record_with_type("r3", "codex", "needle from codex"),
        ];
        let request = GrepRequest {
            pattern: "needle".into(),
            filters: GrepFilters {
                types: vec!["github-commit".into()],
                ..GrepFilters::default()
            },
            ..GrepRequest::default()
        };
        let indexed = GrepEngine::new(100)
            .search(&records, &request, "wm".into())
            .unwrap();
        let full_scan = GrepEngine::new(100)
            .without_trigram_index()
            .search(&records, &request, "wm".into())
            .unwrap();

        assert_eq!(indexed.matches, full_scan.matches);
        assert_eq!(indexed.matches.len(), 1);
        assert_eq!(indexed.matches[0].source_type, "github-commit");
    }

    #[test]
    fn keyset_cursor_does_not_duplicate_when_newer_record_is_inserted() {
        let engine = GrepEngine::new(100);
        let first_records = vec![
            record_at("r3", "needle newest", "2026-01-03T00:00:00Z"),
            record_at("r2", "needle middle", "2026-01-02T00:00:00Z"),
            record_at("r1", "needle oldest", "2026-01-01T00:00:00Z"),
        ];
        let request = GrepRequest {
            pattern: "needle".into(),
            limit: Some(1),
            ..GrepRequest::default()
        };

        let first_page = engine
            .search(&first_records, &request, "wm".into())
            .unwrap();
        assert_eq!(first_page.matches[0].record_id, "r3");

        let second_records = vec![
            record_at("r4", "needle inserted", "2026-01-04T00:00:00Z"),
            record_at("r3", "needle newest", "2026-01-03T00:00:00Z"),
            record_at("r2", "needle middle", "2026-01-02T00:00:00Z"),
            record_at("r1", "needle oldest", "2026-01-01T00:00:00Z"),
        ];
        let second_page = engine
            .search(
                &second_records,
                &GrepRequest {
                    cursor: first_page.next_cursor,
                    ..request
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(second_page.matches[0].record_id, "r2");
    }

    #[test]
    fn integer_offset_cursor_is_rejected() {
        let engine = GrepEngine::new(100);
        let result = engine.search(
            &[record("r1", "needle")],
            &GrepRequest {
                pattern: "needle".into(),
                cursor: Some("1".into()),
                ..GrepRequest::default()
            },
            "wm".into(),
        );

        assert!(matches!(result, Err(GrepError::InvalidCursor)));
    }
}
