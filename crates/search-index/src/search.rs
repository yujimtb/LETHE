use std::collections::HashSet;
use std::ops::Bound;
use std::time::Duration;
use std::time::Instant;

use lethe_api::api::grep::{GrepError, GrepOrder, GrepRequest, GrepResponse, PreparedGrepQuery};
use tantivy::collector::TopDocs;
use tantivy::collector::sort_key::{SortByStaticFastValue, SortByString};
use tantivy::query::{AllQuery, BooleanQuery, Query, RangeQuery, TermQuery, TermSetQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Order, Term};

use crate::index::{IndexError, PersistentCorpusIndex};
use crate::schema::{IndexCommitMetadata, IndexSchema, asc_sort_key, desc_sort_key};

const CANDIDATE_PAGE_SIZE: usize = 128;

impl PersistentCorpusIndex {
    /// Searches the published Corpus materialization without loading the full corpus.
    pub fn search(
        &self,
        request: &GrepRequest,
        max_limit: usize,
    ) -> Result<GrepResponse, IndexError> {
        Ok(self.search_with_metadata(request, max_limit)?.0)
    }

    pub fn search_with_metadata(
        &self,
        request: &GrepRequest,
        max_limit: usize,
    ) -> Result<(GrepResponse, IndexCommitMetadata), IndexError> {
        let prepared = PreparedGrepQuery::compile(request, max_limit)?;
        self.search_with_candidate_page_size(&prepared, CANDIDATE_PAGE_SIZE)
    }

    /// Executes the isolated asynchronous-search cost class with a job-sized
    /// timeout.  The index and catch-up state machine are unchanged; callers
    /// must invoke this only from the search-job worker.
    pub fn search_with_metadata_async(
        &self,
        request: &GrepRequest,
        max_limit: usize,
    ) -> Result<(GrepResponse, IndexCommitMetadata), IndexError> {
        let prepared =
            PreparedGrepQuery::compile(request, max_limit)?.with_timeout(Duration::from_secs(30));
        self.search_with_candidate_page_size(&prepared, CANDIDATE_PAGE_SIZE)
    }

    fn search_with_candidate_page_size(
        &self,
        prepared: &PreparedGrepQuery,
        candidate_page_size: usize,
    ) -> Result<(GrepResponse, IndexCommitMetadata), IndexError> {
        debug_assert!(candidate_page_size > 0);

        let (searcher, metadata) = self.search_snapshot()?;
        let fields = self.fields();
        let (sort_field, sort_field_name) = match prepared.order() {
            GrepOrder::DateAsc => (fields.sort_asc, "sort_asc"),
            GrepOrder::DateDesc => (fields.sort_desc, "sort_desc"),
        };
        let mut after_key = prepared
            .cursor_boundary()
            .map(|cursor| {
                let timestamp_nanos = cursor
                    .timestamp()
                    .timestamp_nanos_opt()
                    .ok_or(GrepError::InvalidCursor)?;
                Ok::<_, IndexError>(match prepared.order() {
                    GrepOrder::DateAsc => asc_sort_key(timestamp_nanos, cursor.record_id()),
                    GrepOrder::DateDesc => desc_sort_key(timestamp_nanos, cursor.record_id()),
                })
            })
            .transpose()?;
        let start = Instant::now();
        let required_literal_ngrams = rarest_required_literal_ngrams(&searcher, prepared, fields)?;
        let candidate_page_size = if required_literal_ngrams.is_empty() {
            CANDIDATE_PAGE_SIZE
        } else {
            prepared.limit_with_sentinel().min(CANDIDATE_PAGE_SIZE)
        };
        let mut verified_matches = Vec::with_capacity(prepared.limit_with_sentinel());

        loop {
            ensure_within_timeout(start, prepared)?;
            let query = candidate_query(
                prepared,
                fields,
                sort_field,
                after_key.as_deref(),
                &required_literal_ngrams,
            );
            let timestamp_order = match prepared.order() {
                GrepOrder::DateAsc => Order::Asc,
                GrepOrder::DateDesc => Order::Desc,
            };
            let collector = TopDocs::with_limit(candidate_page_size).order_by((
                (
                    SortByStaticFastValue::<i64>::for_field("timestamp_nanos"),
                    timestamp_order,
                ),
                (SortByString::for_field("record_id"), Order::Asc),
            ));
            let page = searcher.search(query.as_ref(), &collector)?;
            ensure_within_timeout(start, prepared)?;
            if page.is_empty() {
                break;
            }

            let mut last_page_key = None;
            for (_, address) in &page {
                ensure_within_timeout(start, prepared)?;
                let record = self.load_record(&searcher, *address)?;
                let sort_key = record_sort_key(&record, prepared.order())?;
                if after_key
                    .as_deref()
                    .is_some_and(|after| sort_key.as_str() <= after)
                    || last_page_key
                        .as_deref()
                        .is_some_and(|previous| sort_key.as_str() <= previous)
                {
                    return Err(IndexError::InvalidDocument(format!(
                        "{sort_field_name} is not strictly ordered"
                    )));
                }
                last_page_key = Some(sort_key);
                if !prepared.is_after_cursor(&record) {
                    return Err(IndexError::InvalidDocument(format!(
                        "{sort_field_name} disagrees with record cursor order"
                    )));
                }
                if let Some(matched) = prepared.matches_record(&record) {
                    verified_matches.push(matched);
                    if verified_matches.len() >= prepared.limit_with_sentinel() {
                        let response = prepared
                            .finish(verified_matches, metadata.projection_watermark.clone())?;
                        return Ok((response, metadata));
                    }
                }
            }

            after_key = last_page_key;
            if page.len() < candidate_page_size {
                break;
            }
        }

        let response = prepared.finish(verified_matches, metadata.projection_watermark.clone())?;
        Ok((response, metadata))
    }
}

fn record_sort_key(
    record: &lethe_api::api::grep::GrepRecord,
    order: GrepOrder,
) -> Result<String, IndexError> {
    let timestamp_nanos = record.timestamp.timestamp_nanos_opt().ok_or_else(|| {
        IndexError::InvalidDocument("timestamp is outside nanosecond range".to_owned())
    })?;
    Ok(match order {
        GrepOrder::DateAsc => asc_sort_key(timestamp_nanos, &record.record_id),
        GrepOrder::DateDesc => desc_sort_key(timestamp_nanos, &record.record_id),
    })
}

fn rarest_required_literal_ngrams(
    searcher: &tantivy::Searcher,
    prepared: &PreparedGrepQuery,
    fields: &IndexSchema,
) -> Result<Vec<String>, IndexError> {
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    for group in prepared.required_literal_ngram_groups() {
        let mut rarest: Option<(u64, &str)> = None;
        for ngram in group {
            let term = Term::from_field_text(fields.normalized_text, ngram);
            let document_frequency = searcher.doc_freq(&term)?;
            if rarest.is_none_or(|best| (document_frequency, ngram.as_str()) < best) {
                rarest = Some((document_frequency, ngram));
            }
        }
        if let Some((_, ngram)) = rarest
            && seen.insert(ngram)
        {
            selected.push(ngram.to_owned());
        }
    }
    Ok(selected)
}

fn candidate_query(
    prepared: &PreparedGrepQuery,
    fields: &IndexSchema,
    sort_field: tantivy::schema::Field,
    after_key: Option<&str>,
    required_literal_ngrams: &[String],
) -> Box<dyn Query> {
    let mut clauses = required_literal_ngrams
        .iter()
        .map(|ngram| {
            Box::new(TermQuery::new(
                Term::from_field_text(fields.normalized_text, ngram),
                IndexRecordOption::Basic,
            )) as Box<dyn Query>
        })
        .collect::<Vec<_>>();
    if clauses.is_empty() {
        clauses.push(Box::new(AllQuery));
    }
    let filters = prepared.filters();
    if !filters.types.is_empty() {
        clauses.push(Box::new(TermSetQuery::new(
            filters
                .types
                .iter()
                .map(|source_type| Term::from_field_text(fields.source_type, source_type))
                .collect::<Vec<_>>(),
        )));
    }
    let lower_timestamp = filters
        .from
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(|timestamp| Bound::Included(Term::from_field_i64(fields.timestamp_nanos, timestamp)))
        .unwrap_or(Bound::Unbounded);
    let upper_timestamp = filters
        .to
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(|timestamp| Bound::Included(Term::from_field_i64(fields.timestamp_nanos, timestamp)))
        .unwrap_or(Bound::Unbounded);
    if !matches!(lower_timestamp, Bound::Unbounded) || !matches!(upper_timestamp, Bound::Unbounded)
    {
        clauses.push(Box::new(RangeQuery::new(lower_timestamp, upper_timestamp)));
    }
    for containers in [&filters.channels, &filters.containers] {
        if !containers.is_empty() {
            clauses.push(Box::new(TermSetQuery::new(
                containers
                    .iter()
                    .map(|container| Term::from_field_text(fields.container, container))
                    .collect::<Vec<_>>(),
            )));
        }
    }
    if let Some(after_key) = after_key {
        clauses.push(Box::new(RangeQuery::new(
            Bound::Excluded(Term::from_field_text(sort_field, after_key)),
            Bound::Unbounded,
        )));
    }
    if clauses.len() == 1 {
        clauses.pop().expect("one candidate clause")
    } else {
        Box::new(BooleanQuery::intersection(clauses))
    }
}

fn ensure_within_timeout(start: Instant, prepared: &PreparedGrepQuery) -> Result<(), GrepError> {
    if start.elapsed() > prepared.timeout() {
        return Err(GrepError::TimedOut(prepared.timeout().as_millis() as u64));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;

    use chrono::{DateTime, Utc};
    use lethe_api::api::grep::{GrepEngine, GrepFilters, GrepRecord, NormalizationMode};
    use lethe_projection_corpus::{CorpusRecord, normalized_text};
    use pretty_assertions::assert_eq;
    use tantivy::collector::Count;

    use super::*;
    use crate::index::MIN_WRITER_HEAP_BYTES;

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("lethe-search-query-test-{}", uuid::Uuid::now_v7()))
    }

    fn timestamp(day: u32, hour: u32) -> DateTime<Utc> {
        format!("2026-01-{day:02}T{hour:02}:00:00Z")
            .parse()
            .unwrap()
    }

    fn record(
        record_id: &str,
        source_type: &str,
        container: Option<&str>,
        timestamp: DateTime<Utc>,
        text: impl Into<String>,
    ) -> CorpusRecord {
        let text = text.into();
        CorpusRecord {
            record_id: record_id.to_owned(),
            source_type: source_type.to_owned(),
            anchor_url: format!("https://example.test/{record_id}"),
            source_title: format!("title-{record_id}"),
            source_location: container.map(|value| format!("#{value}")),
            timestamp,
            normalized_text: normalized_text(&text),
            text,
            thread_ts: Some(format!("thread-{record_id}")),
            container: container.map(str::to_owned),
            metadata: serde_json::json!({
                "record": record_id,
                "thread_key": format!("key-{record_id}"),
            }),
        }
    }

    fn as_grep_record(record: &CorpusRecord) -> GrepRecord {
        GrepRecord {
            record_id: record.record_id.clone(),
            source_type: record.source_type.clone(),
            anchor_url: record.anchor_url.clone(),
            source_title: record.source_title.clone(),
            source_location: record.source_location.clone(),
            timestamp: record.timestamp,
            text: record.text.clone(),
            normalized_text: record.normalized_text.clone(),
            thread_ts: record.thread_ts.clone(),
            container: record.container.clone(),
            metadata: record.metadata.clone(),
        }
    }

    fn create_index(records: &[CorpusRecord]) -> (PathBuf, PersistentCorpusIndex) {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".to_owned()).unwrap();
        index
            .upsert_records(
                records,
                17,
                records.len() as u64,
                "proj:corpus:17".to_owned(),
            )
            .unwrap();
        (path, index)
    }

    fn assert_matches_reference(
        index: &PersistentCorpusIndex,
        records: &[GrepRecord],
        request: &GrepRequest,
        candidate_page_size: usize,
    ) -> GrepResponse {
        let expected = GrepEngine::new(100)
            .search(records, request, "proj:corpus:17".to_owned())
            .unwrap();
        let prepared = PreparedGrepQuery::compile(request, 100).unwrap();
        let actual = index
            .search_with_candidate_page_size(&prepared, candidate_page_size)
            .unwrap()
            .0;
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        actual
    }

    #[test]
    fn persistent_search_matches_full_width_and_filters_and_snippet_contract() {
        let long_text = format!("{}alpha gap omega{}", "前".repeat(190), "後".repeat(190));
        let records = vec![
            record("r1", "slack", Some("eng"), timestamp(2, 3), long_text),
            record(
                "r2",
                "slack",
                Some("eng"),
                timestamp(2, 2),
                "omega lives far before alpha",
            ),
            record("r3", "slack", Some("eng"), timestamp(2, 1), "alpha only"),
            record(
                "r4",
                "drive",
                Some("eng"),
                timestamp(2, 0),
                "alpha omega wrong type",
            ),
            record(
                "r5",
                "slack",
                Some("sales"),
                timestamp(1, 23),
                "alpha omega wrong container",
            ),
            record(
                "r6",
                "slack",
                Some("eng"),
                timestamp(1, 0),
                "alpha omega lower boundary",
            ),
            record(
                "r7",
                "slack",
                Some("eng"),
                timestamp(3, 0),
                "alpha omega upper boundary",
            ),
            record(
                "r8",
                "slack",
                Some("eng"),
                timestamp(4, 0),
                "alpha omega outside range",
            ),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let request = GrepRequest {
            pattern: "alpha\u{3000}omega".to_owned(),
            filters: GrepFilters {
                types: vec!["slack".to_owned()],
                from: Some(timestamp(1, 0)),
                to: Some(timestamp(3, 0)),
                channels: vec!["eng".to_owned()],
                containers: vec!["eng".to_owned()],
            },
            normalization: NormalizationMode::Nfkc,
            order: GrepOrder::DateDesc,
            limit: Some(20),
            cursor: None,
        };

        let response = assert_matches_reference(&index, &grep_records, &request, 2);
        assert_eq!(
            response
                .matches
                .iter()
                .map(|matched| matched.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["r7", "r1", "r2", "r6"]
        );
        assert!(response.matches[1].snippet.starts_with("..."));
        assert!(response.matches[1].snippet.ends_with("..."));
        assert!(response.matches[1].snippet.contains("alpha gap omega"));
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn regex_and_normalization_none_use_all_candidates_and_match_reference() {
        let records = vec![
            record(
                "r1",
                "slack",
                Some("eng"),
                timestamp(2, 1),
                "prefix alpha middle z suffix",
            ),
            record(
                "r2",
                "slack",
                Some("eng"),
                timestamp(2, 2),
                "ＡＬＰＨＡ only full width",
            ),
            record(
                "r3",
                "slack",
                Some("eng"),
                timestamp(2, 3),
                "does not match",
            ),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let requests = [
            GrepRequest {
                pattern: "alpha.+z".to_owned(),
                limit: Some(20),
                ..GrepRequest::default()
            },
            GrepRequest {
                pattern: "ＡＬＰＨＡ".to_owned(),
                normalization: NormalizationMode::None,
                limit: Some(20),
                ..GrepRequest::default()
            },
        ];
        for request in &requests {
            assert_matches_reference(&index, &grep_records, request, 1);
        }
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn one_and_two_character_literals_use_selective_index_candidates() {
        let records = vec![
            record(
                "deadline",
                "slack",
                Some("eng"),
                timestamp(2, 1),
                "納期を確認する",
            ),
            record(
                "single",
                "slack",
                Some("eng"),
                timestamp(2, 2),
                "marker x only",
            ),
            record(
                "noise",
                "slack",
                Some("eng"),
                timestamp(2, 3),
                "unrelated body",
            ),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let (searcher, _) = index.search_snapshot().unwrap();

        for (pattern, expected_id) in [("納期", "deadline"), ("x", "single")] {
            let request = GrepRequest {
                pattern: pattern.to_owned(),
                limit: Some(20),
                ..GrepRequest::default()
            };
            let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();
            assert_eq!(prepared.required_literal_ngrams(), [pattern]);
            let required_literal_ngrams =
                rarest_required_literal_ngrams(&searcher, &prepared, index.fields()).unwrap();
            assert_eq!(required_literal_ngrams, vec![pattern.to_owned()]);
            let query = candidate_query(
                &prepared,
                index.fields(),
                index.fields().sort_desc,
                None,
                &required_literal_ngrams,
            );
            assert_eq!(searcher.search(query.as_ref(), &Count).unwrap(), 1);
            let response = assert_matches_reference(&index, &grep_records, &request, 1);
            assert_eq!(response.matches[0].record_id, expected_id);
        }

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rarest_required_literal_ngram_is_selected_per_literal_term() {
        let records = vec![
            record(
                "target",
                "slack",
                Some("eng"),
                timestamp(2, 1),
                "common zebra yak",
            ),
            record(
                "noise-a",
                "slack",
                Some("eng"),
                timestamp(2, 2),
                "common alpha",
            ),
            record(
                "noise-b",
                "slack",
                Some("eng"),
                timestamp(2, 3),
                "common beta",
            ),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let (searcher, _) = index.search_snapshot().unwrap();
        let request = GrepRequest {
            pattern: "common zebra yak".to_owned(),
            limit: Some(20),
            ..GrepRequest::default()
        };
        let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();

        let selected =
            rarest_required_literal_ngrams(&searcher, &prepared, index.fields()).unwrap();
        assert_eq!(selected, vec!["com", "bra", "yak"]);
        let query = candidate_query(
            &prepared,
            index.fields(),
            index.fields().sort_desc,
            None,
            &selected,
        );
        assert_eq!(searcher.search(query.as_ref(), &Count).unwrap(), 1);
        let response = assert_matches_reference(&index, &grep_records, &request, 1);
        assert_eq!(response.matches[0].record_id, "target");

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn both_orders_and_cursors_match_reference_across_candidate_pages() {
        let records = vec![
            record("same-a", "slack", Some("eng"), timestamp(2, 0), "needle"),
            record("same-b", "slack", Some("eng"), timestamp(2, 0), "needle"),
            record("same-c", "slack", Some("eng"), timestamp(2, 0), "needle"),
            record("old-a", "slack", Some("eng"), timestamp(1, 0), "needle"),
            record("old-b", "slack", Some("eng"), timestamp(1, 0), "needle"),
            record("new-a", "slack", Some("eng"), timestamp(3, 0), "needle"),
            record("new-b", "slack", Some("eng"), timestamp(3, 0), "needle"),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);

        for order in [GrepOrder::DateAsc, GrepOrder::DateDesc] {
            let mut request = GrepRequest {
                pattern: "needle".to_owned(),
                order,
                limit: Some(2),
                ..GrepRequest::default()
            };
            let mut seen = Vec::new();
            loop {
                let response = assert_matches_reference(&index, &grep_records, &request, 2);
                seen.extend(
                    response
                        .matches
                        .iter()
                        .map(|matched| matched.record_id.clone()),
                );
                let Some(cursor) = response.next_cursor else {
                    assert!(response.complete);
                    break;
                };
                assert!(!response.complete);
                request.cursor = Some(cursor);
            }
            assert_eq!(seen.len(), records.len());
            assert_eq!(seen.iter().collect::<HashSet<_>>().len(), records.len());
        }

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn submicro_timestamps_preserve_reference_order_response_and_cursor() {
        let earlier = "2026-01-02T00:00:00.000000100Z".parse().unwrap();
        let later = "2026-01-02T00:00:00.000000200Z".parse().unwrap();
        let records = vec![
            record("z-earlier", "slack", Some("eng"), earlier, "needle"),
            record("a-later", "slack", Some("eng"), later, "needle"),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let mut request = GrepRequest {
            pattern: "needle".to_owned(),
            order: GrepOrder::DateAsc,
            limit: Some(1),
            ..GrepRequest::default()
        };

        let first = assert_matches_reference(&index, &grep_records, &request, 1);
        assert_eq!(first.matches[0].record_id, "z-earlier");
        assert_eq!(first.matches[0].timestamp, earlier);
        request.cursor = first.next_cursor;
        let second = assert_matches_reference(&index, &grep_records, &request, 1);
        assert_eq!(second.matches[0].record_id, "a-later");
        assert_eq!(second.matches[0].timestamp, later);
        assert!(second.complete);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn candidate_filters_preserve_or_within_and_across_dimensions_and_inclusive_bounds() {
        let boundary = timestamp(2, 0);
        let records = vec![
            record("slack-eng", "slack", Some("eng"), boundary, "needle"),
            record("drive-eng", "drive", Some("eng"), boundary, "needle"),
            record("other-eng", "mail", Some("eng"), boundary, "needle"),
            record("slack-sales", "slack", Some("sales"), boundary, "needle"),
            record(
                "slack-before",
                "slack",
                Some("eng"),
                timestamp(1, 23),
                "needle",
            ),
            record(
                "slack-after",
                "slack",
                Some("eng"),
                timestamp(2, 1),
                "needle",
            ),
        ];
        let grep_records = records.iter().map(as_grep_record).collect::<Vec<_>>();
        let (path, index) = create_index(&records);
        let request = GrepRequest {
            pattern: "needle".to_owned(),
            filters: GrepFilters {
                types: vec!["slack".to_owned(), "drive".to_owned()],
                from: Some(boundary),
                to: Some(boundary),
                channels: vec!["eng".to_owned(), "sales".to_owned()],
                containers: vec!["eng".to_owned()],
            },
            limit: Some(20),
            ..GrepRequest::default()
        };

        let prepared = PreparedGrepQuery::compile(&request, 100).unwrap();
        let (searcher, _) = index.search_snapshot().unwrap();
        let selected =
            rarest_required_literal_ngrams(&searcher, &prepared, index.fields()).unwrap();
        let query = candidate_query(
            &prepared,
            index.fields(),
            index.fields().sort_desc,
            None,
            &selected,
        );
        assert_eq!(searcher.search(query.as_ref(), &Count).unwrap(), 2);

        let response = assert_matches_reference(&index, &grep_records, &request, 1);
        assert_eq!(
            response
                .matches
                .iter()
                .map(|matched| matched.record_id.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["slack-eng", "drive-eng"])
        );

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_with_metadata_returns_the_same_commit_snapshot() {
        let records = vec![
            record("r1", "slack", Some("eng"), timestamp(1, 0), "needle"),
            record("r2", "drive", Some("eng"), timestamp(2, 0), "needle"),
        ];
        let (path, index) = create_index(&records);
        let (response, metadata) = index
            .search_with_metadata(
                &GrepRequest {
                    pattern: "needle".to_owned(),
                    limit: Some(20),
                    ..GrepRequest::default()
                },
                100,
            )
            .unwrap();

        assert_eq!(response.projection_watermark, metadata.projection_watermark);
        assert_eq!(metadata.observation_count, 2);
        assert_eq!(metadata.record_count, 2);
        assert!(metadata.committed_at <= Utc::now());

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn request_errors_do_not_require_rebuild() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".to_owned()).unwrap();
        let error = index
            .search(
                &GrepRequest {
                    pattern: "[".to_owned(),
                    ..GrepRequest::default()
                },
                100,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            IndexError::Grep(GrepError::InvalidPattern(_))
        ));
        assert!(!error.requires_rebuild());
        assert!(IndexError::MissingCommitMetadata.requires_rebuild());
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }
}
