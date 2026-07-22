//! Grep API request/response types and linear-time regex search engine.

use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const SNIPPET_MAX_CHARS: usize = 240;
pub const MATCHED_RANGES_LIMIT: usize = 20;
const DEFAULT_LIMIT: usize = 100;
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_key: Option<String>,
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
    pub complete: bool,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactSearchField {
    RecordId,
    SourceObjectId,
    ThreadKey,
    SessionId,
    SourceType,
    Container,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactSearchRequest {
    pub field: ExactSearchField,
    pub value: String,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactSearchResponse {
    pub matches: Vec<GrepRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
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

/// Decoded keyset boundary carried by an opaque grep cursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrepCursorBoundary {
    timestamp: DateTime<Utc>,
    record_id: String,
}

impl GrepCursorBoundary {
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn record_id(&self) -> &str {
        &self.record_id
    }
}

/// Validated grep request reusable by persistent and in-memory search backends.
#[derive(Debug)]
pub struct PreparedGrepQuery {
    filters: GrepFilters,
    normalization: NormalizationMode,
    order: GrepOrder,
    limit: usize,
    cursor: Option<GrepCursorBoundary>,
    matcher: QueryMatcher,
    required_literal_ngrams: Vec<String>,
    required_literal_ngram_groups: Vec<Vec<String>>,
    timeout: Duration,
}

impl PreparedGrepQuery {
    pub fn compile(request: &GrepRequest, max_limit: usize) -> Result<Self, GrepError> {
        let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 || limit > max_limit {
            return Err(GrepError::InvalidLimit(max_limit));
        }

        let cursor = request.cursor.as_deref().map(decode_cursor).transpose()?;
        let pattern = match request.normalization {
            NormalizationMode::Nfkc => normalize(&request.pattern),
            NormalizationMode::None => request.pattern.clone(),
        };
        let matcher = QueryMatcher::compile(pattern)?;
        let required_literal_ngrams = if request.normalization == NormalizationMode::Nfkc {
            matcher.required_literal_ngrams()
        } else {
            Vec::new()
        };
        let required_literal_ngram_groups = if request.normalization == NormalizationMode::Nfkc {
            matcher.required_literal_ngram_groups()
        } else {
            Vec::new()
        };

        Ok(Self {
            filters: request.filters.clone(),
            normalization: request.normalization,
            order: request.order,
            limit,
            cursor,
            matcher,
            required_literal_ngrams,
            required_literal_ngram_groups,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn filters(&self) -> &GrepFilters {
        &self.filters
    }

    pub fn normalization(&self) -> NormalizationMode {
        self.normalization
    }

    pub fn order(&self) -> GrepOrder {
        self.order
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Number of verified matches needed to decide whether another page exists.
    pub fn limit_with_sentinel(&self) -> usize {
        self.limit.saturating_add(1)
    }

    pub fn cursor_boundary(&self) -> Option<&GrepCursorBoundary> {
        self.cursor.as_ref()
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Safe NFKC literal n-grams that may be ANDed to narrow index candidates.
    ///
    /// Each literal contributes every safe window at width
    /// `min(3, char_count)`, so one- and two-character terms remain available
    /// to the candidate planner. A persistent backend may select the rarest
    /// required window because every exact match contains every returned
    /// window. Regex terms and normalization-none queries contribute no
    /// candidate constraint.
    pub fn required_literal_ngrams(&self) -> &[String] {
        &self.required_literal_ngrams
    }

    /// True when the request cannot be narrowed by the persistent literal
    /// index and therefore belongs to the asynchronous search-job class.
    pub fn requires_async_search_job(&self) -> bool {
        self.required_literal_ngrams.is_empty()
    }

    /// Returns safe n-gram candidates grouped by independently required query
    /// term. A persistent backend may select one rarest n-gram from each
    /// non-empty group and intersect those terms before loading stored fields.
    pub fn required_literal_ngram_groups(&self) -> &[Vec<String>] {
        &self.required_literal_ngram_groups
    }

    pub fn is_after_cursor(&self, record: &GrepRecord) -> bool {
        self.cursor
            .as_ref()
            .is_none_or(|cursor| is_after_cursor(record, cursor, self.order))
    }

    pub fn matches_record(&self, record: &GrepRecord) -> Option<GrepMatch> {
        filters_match(record, &self.filters).then_some(())?;
        match_record(record, &self.matcher, self.normalization)
    }

    pub fn finish(
        &self,
        mut verified_matches: Vec<GrepMatch>,
        projection_watermark: String,
    ) -> Result<GrepResponse, GrepError> {
        let complete = verified_matches.len() <= self.limit;
        if !complete {
            verified_matches.truncate(self.limit);
        }
        let next_cursor = if complete {
            None
        } else {
            verified_matches.last().map(encode_cursor).transpose()?
        };
        Ok(GrepResponse {
            matches: verified_matches,
            next_cursor,
            complete,
            projection_watermark,
        })
    }
}

pub struct GrepEngine {
    max_limit: usize,
    timeout: Duration,
}

impl GrepEngine {
    pub fn new(max_limit: usize) -> Self {
        Self {
            max_limit,
            timeout: DEFAULT_TIMEOUT,
        }
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
        let prepared =
            PreparedGrepQuery::compile(request, self.max_limit)?.with_timeout(self.timeout);
        let mut candidates = records
            .iter()
            .filter(|record| filters_match(record, prepared.filters()))
            .collect::<Vec<_>>();
        match prepared.order() {
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
            if start.elapsed() > prepared.timeout() {
                return Err(GrepError::TimedOut(prepared.timeout().as_millis() as u64));
            }
            if !prepared.is_after_cursor(record) {
                continue;
            }
            if let Some(matched) = prepared.matches_record(record) {
                matches.push(matched);
                if matches.len() >= prepared.limit_with_sentinel() {
                    break;
                }
            }
        }
        prepared.finish(matches, projection_watermark)
    }
}

fn encode_cursor(record: &GrepMatch) -> Result<String, GrepError> {
    let cursor = GrepCursorBoundary {
        timestamp: record.timestamp,
        record_id: record.record_id.clone(),
    };
    let bytes = serde_json::to_vec(&cursor).map_err(|_| GrepError::InvalidCursor)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(cursor: &str) -> Result<GrepCursorBoundary, GrepError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| GrepError::InvalidCursor)?;
    serde_json::from_slice(&bytes).map_err(|_| GrepError::InvalidCursor)
}

fn is_after_cursor(record: &GrepRecord, cursor: &GrepCursorBoundary, order: GrepOrder) -> bool {
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

fn plain_literal_ngrams(pattern: &str) -> Option<Vec<String>> {
    if pattern.chars().any(|ch| {
        matches!(
            ch,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        )
    }) {
        return None;
    }
    let ngrams = literal_ngrams(pattern);
    (!ngrams.is_empty()).then_some(ngrams)
}

fn literal_ngrams(value: &str) -> Vec<String> {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return Vec::new();
    }
    let width = chars.len().min(3);
    chars
        .windows(width)
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

#[derive(Debug)]
enum QueryMatcher {
    Single(TermMatcher),
    Multi(Vec<TermMatcher>),
}

impl QueryMatcher {
    fn compile(pattern: String) -> Result<Self, GrepError> {
        let terms = split_query_terms(&pattern);
        if terms.len() > 1 {
            Ok(Self::Multi(
                terms
                    .into_iter()
                    .map(|term| TermMatcher::compile_lossy(&term))
                    .collect(),
            ))
        } else {
            let regex =
                Regex::new(&pattern).map_err(|err| GrepError::InvalidPattern(err.to_string()))?;
            Ok(Self::Single(TermMatcher::Regex { pattern, regex }))
        }
    }

    fn required_literal_ngrams(&self) -> Vec<String> {
        self.required_literal_ngram_groups()
            .into_iter()
            .flatten()
            .fold(Vec::new(), |mut required, ngram| {
                if !required.contains(&ngram) {
                    required.push(ngram);
                }
                required
            })
    }

    fn required_literal_ngram_groups(&self) -> Vec<Vec<String>> {
        let terms = match self {
            Self::Single(term) => std::slice::from_ref(term),
            Self::Multi(terms) => terms.as_slice(),
        };
        terms
            .iter()
            .filter_map(|term| plain_literal_ngrams(term.candidate_text()))
            .collect()
    }

    fn find_ranges(&self, haystack: &str) -> Option<Vec<MatchedRange>> {
        match self {
            Self::Single(term) => term.find_ranges(haystack),
            Self::Multi(terms) => {
                let mut ranges = Vec::new();
                for term in terms {
                    let term_ranges = term.find_ranges(haystack)?;
                    ranges.extend(term_ranges);
                }
                ranges.sort_by(|left, right| {
                    left.start
                        .cmp(&right.start)
                        .then_with(|| left.end.cmp(&right.end))
                });
                ranges.dedup();
                Some(ranges)
            }
        }
    }
}

#[derive(Debug)]
enum TermMatcher {
    Regex { pattern: String, regex: Regex },
    Literal(String),
}

impl TermMatcher {
    fn compile_lossy(pattern: &str) -> Self {
        Regex::new(pattern)
            .map(|regex| Self::Regex {
                pattern: pattern.to_owned(),
                regex,
            })
            .unwrap_or_else(|_| Self::Literal(pattern.to_owned()))
    }

    fn candidate_text(&self) -> &str {
        match self {
            Self::Regex { pattern, .. } => pattern,
            Self::Literal(pattern) => pattern,
        }
    }

    fn find_ranges(&self, haystack: &str) -> Option<Vec<MatchedRange>> {
        let ranges = match self {
            Self::Regex { regex, .. } => regex
                .find_iter(haystack)
                .map(|matched| MatchedRange {
                    start: matched.start(),
                    end: matched.end(),
                })
                .collect::<Vec<_>>(),
            Self::Literal(pattern) => haystack
                .match_indices(pattern)
                .map(|(start, matched)| MatchedRange {
                    start,
                    end: start + matched.len(),
                })
                .collect::<Vec<_>>(),
        };
        (!ranges.is_empty()).then_some(ranges)
    }
}

fn split_query_terms(query: &str) -> Vec<String> {
    query
        .split([' ', '\t', '\u{3000}'])
        .filter(|term| !term.is_empty())
        .map(str::to_owned)
        .collect()
}

fn match_record(
    record: &GrepRecord,
    matcher: &QueryMatcher,
    normalization: NormalizationMode,
) -> Option<GrepMatch> {
    let haystack = match normalization {
        NormalizationMode::Nfkc => record.normalized_text.as_str(),
        NormalizationMode::None => record.text.as_str(),
    };
    let mut ranges = matcher.find_ranges(haystack)?;
    let first_match_char = byte_to_char_index(haystack, ranges[0].start);
    ranges.truncate(MATCHED_RANGES_LIMIT);
    Some(GrepMatch {
        record_id: record.record_id.clone(),
        source_type: record.source_type.clone(),
        anchor_url: record.anchor_url.clone(),
        source_title: record.source_title.clone(),
        source_location: record.source_location.clone(),
        timestamp: record.timestamp,
        thread_key: thread_key(record),
        snippet: snippet(&record.text, first_match_char),
        matched_ranges: ranges,
        metadata: record.metadata.clone(),
    })
}

fn thread_key(record: &GrepRecord) -> Option<String> {
    record
        .metadata
        .get("thread_key")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| record.thread_ts.clone())
}

fn snippet(text: &str, hit_char_index: usize) -> String {
    let total_chars = text.chars().count();
    if total_chars <= SNIPPET_MAX_CHARS {
        return text.to_owned();
    }

    let hit_char_index = hit_char_index.min(total_chars.saturating_sub(1));
    let mut has_prefix = hit_char_index > SNIPPET_MAX_CHARS / 2;
    let mut has_suffix = total_chars.saturating_sub(hit_char_index) > SNIPPET_MAX_CHARS / 2;
    let (start, end) = loop {
        let marker_chars = usize::from(has_prefix) * 3 + usize::from(has_suffix) * 3;
        let body_chars = SNIPPET_MAX_CHARS.saturating_sub(marker_chars).max(1);
        let mut start = hit_char_index.saturating_sub(body_chars / 2);
        let mut end = start + body_chars;
        if end > total_chars {
            end = total_chars;
            start = total_chars - body_chars;
        }
        let actual_prefix = start > 0;
        let actual_suffix = end < total_chars;
        if actual_prefix == has_prefix && actual_suffix == has_suffix {
            break (start, end);
        }
        has_prefix = actual_prefix;
        has_suffix = actual_suffix;
    };

    let body = text
        .chars()
        .skip(start)
        .take(end - start)
        .collect::<String>();
    match (start > 0, end < total_chars) {
        (true, true) => format!("...{body}..."),
        (true, false) => format!("...{body}"),
        (false, true) => format!("{body}..."),
        (false, false) => body,
    }
}

fn byte_to_char_index(value: &str, byte_index: usize) -> usize {
    value[..byte_index].chars().count()
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
    fn prepared_query_and_reference_engine_return_same_matches() {
        let records = vec![
            record("r1", "忘れ物は受付"),
            record("r2", "別の文章"),
            record("r3", "受付に忘れ物"),
        ];
        let request = GrepRequest {
            pattern: "忘れ物".into(),
            ..GrepRequest::default()
        };
        let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();
        assert_eq!(prepared.required_literal_ngrams(), ["忘れ物"]);

        let manually_verified = records
            .iter()
            .filter_map(|record| prepared.matches_record(record))
            .collect::<Vec<_>>();
        let prepared_response = prepared.finish(manually_verified, "wm".into()).unwrap();
        let reference_response = GrepEngine::new(100)
            .search(&records, &request, "wm".into())
            .unwrap();

        assert_eq!(prepared_response.matches, reference_response.matches);
    }

    #[test]
    fn prepared_query_only_exposes_safe_nfkc_literal_ngrams() {
        let request = GrepRequest {
            pattern: "ａｌｐｈａ be.* xy gamma".into(),
            ..GrepRequest::default()
        };
        let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();
        assert_eq!(
            prepared.required_literal_ngrams(),
            ["alp", "lph", "pha", "xy", "gam", "amm", "mma"]
        );

        let without_normalization = PreparedGrepQuery::compile(
            &GrepRequest {
                normalization: NormalizationMode::None,
                ..request
            },
            100,
        )
        .unwrap();
        assert!(without_normalization.required_literal_ngrams().is_empty());
    }

    #[test]
    fn prepared_query_indexes_one_and_two_character_literals() {
        let prepared = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: "納期 x".into(),
                ..GrepRequest::default()
            },
            100,
        )
        .unwrap();

        assert_eq!(prepared.required_literal_ngrams(), ["納期", "x"]);
    }

    #[test]
    fn prepared_query_exposes_boundaries_and_finishes_limit_plus_one() {
        let newest = record_at("r3", "needle newest", "2026-01-03T00:00:00Z");
        let older = record_at("r2", "needle older", "2026-01-02T00:00:00Z");
        let prepared = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: "needle".into(),
                limit: Some(1),
                ..GrepRequest::default()
            },
            100,
        )
        .unwrap();

        assert_eq!(prepared.order(), GrepOrder::DateDesc);
        assert_eq!(prepared.limit(), 1);
        assert_eq!(prepared.limit_with_sentinel(), 2);
        assert_eq!(prepared.timeout(), Duration::from_millis(500));
        assert!(prepared.cursor_boundary().is_none());

        let response = prepared
            .finish(
                vec![
                    prepared.matches_record(&newest).unwrap(),
                    prepared.matches_record(&older).unwrap(),
                ],
                "wm-prepared".into(),
            )
            .unwrap();
        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].record_id, "r3");
        assert!(!response.complete);
        assert_eq!(response.projection_watermark, "wm-prepared");

        let next = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: "needle".into(),
                limit: Some(1),
                cursor: response.next_cursor,
                ..GrepRequest::default()
            },
            100,
        )
        .unwrap()
        .with_timeout(Duration::from_secs(2));
        let boundary = next.cursor_boundary().unwrap();
        assert_eq!(boundary.record_id(), "r3");
        assert_eq!(boundary.timestamp(), newest.timestamp);
        assert_eq!(next.timeout(), Duration::from_secs(2));
        assert!(!next.is_after_cursor(&newest));
        assert!(next.is_after_cursor(&older));
    }

    #[test]
    fn prepared_query_compile_validates_limit_cursor_and_pattern() {
        let invalid_limit = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: "needle".into(),
                limit: Some(0),
                ..GrepRequest::default()
            },
            100,
        );
        assert!(matches!(invalid_limit, Err(GrepError::InvalidLimit(100))));

        let invalid_cursor = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: "needle".into(),
                cursor: Some("1".into()),
                ..GrepRequest::default()
            },
            100,
        );
        assert!(matches!(invalid_cursor, Err(GrepError::InvalidCursor)));

        let invalid_pattern = PreparedGrepQuery::compile(
            &GrepRequest {
                pattern: r"(a)\1".into(),
                ..GrepRequest::default()
            },
            100,
        );
        assert!(matches!(invalid_pattern, Err(GrepError::InvalidPattern(_))));
    }

    #[test]
    fn compound_query_requires_all_terms_and_returns_all_ranges() {
        let engine = GrepEngine::new(100);
        let records = vec![
            record("r1", "Nanihold OS notes. ロードマップ is stable."),
            record("r2", "Nanihold OS notes only."),
            record("r3", "ロードマップ only."),
        ];
        let response = engine
            .search(
                &records,
                &GrepRequest {
                    pattern: "Nanihold OS ロードマップ".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].record_id, "r1");
        let matched_text = response.matches[0]
            .matched_ranges
            .iter()
            .map(|range| &records[0].normalized_text[range.start..range.end])
            .collect::<Vec<_>>();
        assert_eq!(matched_text, vec!["Nanihold", "OS", "ロードマップ"]);
    }

    #[test]
    fn compound_query_splits_on_fullwidth_space() {
        let engine = GrepEngine::new(100);
        let response = engine
            .search(
                &[record("r1", "Nanihold planning includes ロードマップ")],
                &GrepRequest {
                    pattern: "Nanihold　ロードマップ".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(response.matches.len(), 1);
    }

    #[test]
    fn compound_query_uses_literal_term_when_term_regex_is_invalid() {
        let engine = GrepEngine::new(100);
        let response = engine
            .search(
                &[record("r1", "Nanihold literal [ロードマップ")],
                &GrepRequest {
                    pattern: "Nanihold [ロードマップ".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].matched_ranges.len(), 2);
    }

    #[test]
    fn snippet_is_centered_on_first_hit() {
        let engine = GrepEngine::new(100);
        let text = format!("{} needle {}", "a".repeat(300), "b".repeat(300));
        let response = engine
            .search(
                &[record("r1", &text)],
                &GrepRequest {
                    pattern: "needle".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        let snippet = &response.matches[0].snippet;
        assert!(snippet.starts_with("..."));
        assert!(snippet.contains("needle"));
        assert!(snippet.ends_with("..."));
        assert!(snippet.chars().count() <= SNIPPET_MAX_CHARS);
    }

    #[test]
    fn matched_ranges_are_capped_per_record() {
        let engine = GrepEngine::new(100);
        let text = "needle ".repeat(MATCHED_RANGES_LIMIT + 15);
        let response = engine
            .search(
                &[record("r1", &text)],
                &GrepRequest {
                    pattern: "needle".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(
            response.matches[0].matched_ranges.len(),
            MATCHED_RANGES_LIMIT
        );
    }

    #[test]
    fn match_promotes_thread_key_from_metadata() {
        let engine = GrepEngine::new(100);
        let mut record = record("r1", "needle in thread");
        record.thread_ts = Some("thread-ts".into());
        record.metadata = serde_json::json!({"thread_key": "codex:session:main"});

        let response = engine
            .search(
                &[record],
                &GrepRequest {
                    pattern: "needle".into(),
                    ..GrepRequest::default()
                },
                "wm".into(),
            )
            .unwrap();

        assert_eq!(
            response.matches[0].thread_key.as_deref(),
            Some("codex:session:main")
        );
    }

    #[test]
    fn type_filter_is_applied_by_prepared_and_reference_search() {
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
        let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();
        let prepared_matches = records
            .iter()
            .filter_map(|record| prepared.matches_record(record))
            .collect::<Vec<_>>();
        let prepared_response = prepared.finish(prepared_matches, "wm".into()).unwrap();
        let reference_response = GrepEngine::new(100)
            .search(&records, &request, "wm".into())
            .unwrap();

        assert_eq!(prepared_response.matches, reference_response.matches);
        assert_eq!(reference_response.matches.len(), 1);
        assert_eq!(reference_response.matches[0].source_type, "github-commit");
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
