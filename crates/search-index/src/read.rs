use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use lethe_api::api::grep::{ExactSearchField, GrepOrder, GrepRecord};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Query, RangeQuery, TermQuery, TermSetQuery};
use tantivy::schema::{Field, IndexRecordOption, TantivyDocument, Value};
use tantivy::{DocAddress, Order, Searcher, Term};

use crate::index::{IndexError, PersistentCorpusIndex};

const READ_CANDIDATE_PAGE_SIZE: usize = 128;
type ExactRecordMatcher = Box<dyn Fn(&GrepRecord) -> bool>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingSessionEdge {
    pub source_type: String,
    pub session_id: String,
    pub parent_session_id: Option<String>,
}

impl PersistentCorpusIndex {
    pub fn record_by_thread_ts(&self, thread_ts: &str) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![exact_term_query(fields.thread_ts, thread_ts)],
                    fields.sort_desc,
                    after,
                )
            },
            |record| record.thread_ts.as_deref() == Some(thread_ts),
        )?;
        Ok(records.pop())
    }

    pub fn record_by_thread_key(&self, thread_key: &str) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![exact_term_query(fields.thread_key, thread_key)],
                    fields.sort_desc,
                    after,
                )
            },
            |record| metadata_str(record, "thread_key") == Some(thread_key),
        )?;
        Ok(records.pop())
    }

    pub fn record_by_session_id(&self, session_id: &str) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![exact_term_query(fields.session_id, session_id)],
                    fields.sort_desc,
                    after,
                )
            },
            |record| metadata_str(record, "session_id") == Some(session_id),
        )?;
        Ok(records.pop())
    }

    pub fn coding_record_by_thread_key(
        &self,
        thread_key: &str,
    ) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let source_terms = ["claude-code", "codex"]
            .into_iter()
            .map(|source_type| Term::from_field_text(fields.source_type, source_type))
            .collect::<Vec<_>>();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![
                        exact_term_query(fields.thread_key, thread_key),
                        Box::new(TermSetQuery::new(source_terms.clone())),
                    ],
                    fields.sort_desc,
                    after,
                )
            },
            |record| {
                is_coding_source(&record.source_type)
                    && metadata_str(record, "thread_key") == Some(thread_key)
            },
        )?;
        Ok(records.pop())
    }

    pub fn coding_record_by_session_id(
        &self,
        session_id: &str,
    ) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let source_terms = ["claude-code", "codex"]
            .into_iter()
            .map(|source_type| Term::from_field_text(fields.source_type, source_type))
            .collect::<Vec<_>>();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![
                        exact_term_query(fields.session_id, session_id),
                        Box::new(TermSetQuery::new(source_terms.clone())),
                    ],
                    fields.sort_desc,
                    after,
                )
            },
            |record| {
                is_coding_source(&record.source_type)
                    && metadata_str(record, "session_id") == Some(session_id)
            },
        )?;
        Ok(records.pop())
    }

    pub fn record_by_thread_ref(&self, thread_ref: &str) -> Result<Option<GrepRecord>, IndexError> {
        let fields = self.fields();
        let (mut records, _) = self.record_query_page(
            0,
            1,
            "sort_desc",
            |after| {
                query_with_after(
                    vec![Box::new(BooleanQuery::union(vec![
                        exact_term_query(fields.thread_ts, thread_ref),
                        exact_term_query(fields.thread_key, thread_ref),
                    ]))],
                    fields.sort_desc,
                    after,
                )
            },
            |record| {
                record.thread_ts.as_deref() == Some(thread_ref)
                    || metadata_str(record, "thread_key") == Some(thread_ref)
            },
        )?;
        Ok(records.pop())
    }

    pub fn records_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, u64), IndexError> {
        let fields = self.fields();
        let (searcher, metadata) = self.search_snapshot()?;
        let (addresses, total) =
            query_addresses_page(&searcher, offset, limit, "sort_desc", |after| {
                query_with_after(Vec::new(), fields.sort_desc, after)
            })?;
        if total != metadata.record_count {
            return Err(IndexError::IncompatibleMetadata(format!(
                "AllQuery count is {total}, metadata requires {}",
                metadata.record_count
            )));
        }
        Ok((self.load_records(&searcher, addresses)?, total))
    }

    pub fn records_keyset_page(
        &self,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, Option<String>), IndexError> {
        validate_limit(limit)?;
        let fields = self.fields();
        let (searcher, _) = self.search_snapshot()?;
        let (addresses, next_sort_key) =
            query_addresses_keyset(&searcher, after_sort_key, limit, "sort_desc", |after| {
                query_with_after(Vec::new(), fields.sort_desc, after)
            })?;
        Ok((self.load_records(&searcher, addresses)?, next_sort_key))
    }

    pub fn exact_records_page(
        &self,
        field: ExactSearchField,
        value: &str,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, Option<String>), IndexError> {
        validate_limit(limit)?;
        if value.trim().is_empty() {
            return Err(IndexError::InvalidReadRequest(
                "exact search value must not be blank".to_owned(),
            ));
        }
        let fields = self.fields();
        let expected_value = value.to_owned();
        let (field, matches): (Field, ExactRecordMatcher) = match field {
            ExactSearchField::RecordId => (
                fields.record_id,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| record.record_id == expected_value
                }),
            ),
            ExactSearchField::SourceObjectId => (
                fields.source_object_id,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| {
                        metadata_str(record, "source_object_id") == Some(expected_value.as_str())
                    }
                }),
            ),
            ExactSearchField::ThreadKey => (
                fields.thread_key,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| {
                        metadata_str(record, "thread_key") == Some(expected_value.as_str())
                    }
                }),
            ),
            ExactSearchField::SessionId => (
                fields.session_id,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| {
                        metadata_str(record, "session_id") == Some(expected_value.as_str())
                    }
                }),
            ),
            ExactSearchField::SourceType => (
                fields.source_type,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| record.source_type == expected_value
                }),
            ),
            ExactSearchField::Container => (
                fields.container,
                Box::new({
                    let expected_value = expected_value.clone();
                    move |record| record.container.as_deref() == Some(expected_value.as_str())
                }),
            ),
        };
        let (searcher, _) = self.search_snapshot()?;
        let query_value = expected_value;
        let query = move |after: Option<&str>| -> Box<dyn Query> {
            query_with_after(
                vec![exact_term_query(field, &query_value)],
                fields.sort_desc,
                after,
            )
        };
        let (addresses, candidate_next) =
            query_addresses_keyset(&searcher, after_sort_key, limit, "sort_desc", query)?;
        let records = self
            .load_records(&searcher, addresses)?
            .into_iter()
            .filter(|record| matches(record))
            .collect::<Vec<_>>();
        Ok((records, candidate_next))
    }

    pub fn source_type_counts(&self) -> Result<BTreeMap<String, u64>, IndexError> {
        Ok(self.metadata()?.source_type_counts)
    }

    pub fn thread_records_page(
        &self,
        source_type: &str,
        thread_ref: &str,
        order: GrepOrder,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, u64), IndexError> {
        let fields = self.fields();
        let (sort_field, sort_field_name) = match order {
            GrepOrder::DateAsc => (fields.sort_asc, "sort_asc"),
            GrepOrder::DateDesc => (fields.sort_desc, "sort_desc"),
        };
        self.record_query_page(
            offset,
            limit,
            sort_field_name,
            |after| {
                let thread_query = BooleanQuery::union(vec![
                    exact_term_query(fields.thread_ts, thread_ref),
                    exact_term_query(fields.thread_key, thread_ref),
                ]);
                query_with_after(
                    vec![
                        exact_term_query(fields.source_type, source_type),
                        Box::new(thread_query),
                    ],
                    sort_field,
                    after,
                )
            },
            |record| {
                record.source_type == source_type
                    && (record.thread_ts.as_deref() == Some(thread_ref)
                        || metadata_str(record, "thread_key") == Some(thread_ref))
            },
        )
    }

    pub fn thread_records_all(
        &self,
        source_type: &str,
        thread_ref: &str,
        order: GrepOrder,
    ) -> Result<Vec<GrepRecord>, IndexError> {
        let fields = self.fields();
        let (sort_field, sort_field_name) = match order {
            GrepOrder::DateAsc => (fields.sort_asc, "sort_asc"),
            GrepOrder::DateDesc => (fields.sort_desc, "sort_desc"),
        };
        self.record_query_all(
            sort_field_name,
            |after| {
                let thread_query = BooleanQuery::union(vec![
                    exact_term_query(fields.thread_ts, thread_ref),
                    exact_term_query(fields.thread_key, thread_ref),
                ]);
                query_with_after(
                    vec![
                        exact_term_query(fields.source_type, source_type),
                        Box::new(thread_query),
                    ],
                    sort_field,
                    after,
                )
            },
            |record| {
                record.source_type == source_type
                    && (record.thread_ts.as_deref() == Some(thread_ref)
                        || metadata_str(record, "thread_key") == Some(thread_ref))
            },
        )
    }

    pub fn thread_key_records_page(
        &self,
        thread_key: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, u64), IndexError> {
        let fields = self.fields();
        self.record_query_page(
            offset,
            limit,
            "sort_asc",
            |after| {
                query_with_after(
                    vec![exact_term_query(fields.thread_key, thread_key)],
                    fields.sort_asc,
                    after,
                )
            },
            |record| metadata_str(record, "thread_key") == Some(thread_key),
        )
    }

    pub fn coding_session_edges_by_session_id(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<CodingSessionEdge>, u64), IndexError> {
        let fields = self.fields();
        self.session_edge_query_page(offset, limit, "sort_asc", |after| {
            query_with_after(
                vec![exact_term_query(fields.session_id, session_id)],
                fields.sort_asc,
                after,
            )
        })
    }

    pub fn coding_source_session_edges(
        &self,
        source_type: &str,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<CodingSessionEdge>, u64), IndexError> {
        let fields = self.fields();
        self.session_edge_query_page(offset, limit, "sort_asc", |after| {
            query_with_after(
                vec![
                    exact_term_query(fields.source_type, source_type),
                    exact_term_query(fields.session_id, session_id),
                ],
                fields.sort_asc,
                after,
            )
        })
    }

    pub fn coding_source_session_edges_all(
        &self,
        source_type: &str,
        session_id: &str,
    ) -> Result<Vec<CodingSessionEdge>, IndexError> {
        let fields = self.fields();
        self.session_edge_query_all("sort_asc", |after| {
            query_with_after(
                vec![
                    exact_term_query(fields.source_type, source_type),
                    exact_term_query(fields.session_id, session_id),
                ],
                fields.sort_asc,
                after,
            )
        })
    }

    pub fn coding_child_session_edges(
        &self,
        source_type: &str,
        parent_session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<CodingSessionEdge>, u64), IndexError> {
        let fields = self.fields();
        self.session_edge_query_page(offset, limit, "sort_asc", |after| {
            query_with_after(
                vec![
                    exact_term_query(fields.source_type, source_type),
                    exact_term_query(fields.parent_session_id, parent_session_id),
                ],
                fields.sort_asc,
                after,
            )
        })
    }

    pub fn coding_child_session_edges_all(
        &self,
        source_type: &str,
        parent_session_id: &str,
    ) -> Result<Vec<CodingSessionEdge>, IndexError> {
        let fields = self.fields();
        self.session_edge_query_all("sort_asc", |after| {
            query_with_after(
                vec![
                    exact_term_query(fields.source_type, source_type),
                    exact_term_query(fields.parent_session_id, parent_session_id),
                ],
                fields.sort_asc,
                after,
            )
        })
    }

    pub fn coding_records_page(
        &self,
        source_type: &str,
        session_ids: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<GrepRecord>, u64), IndexError> {
        if session_ids.is_empty() {
            validate_limit(limit)?;
            return Ok((Vec::new(), 0));
        }
        let fields = self.fields();
        let session_terms = session_ids
            .iter()
            .map(|session_id| Term::from_field_text(fields.session_id, session_id))
            .collect::<Vec<_>>();
        self.record_query_page(
            offset,
            limit,
            "sort_asc",
            |after| {
                query_with_after(
                    vec![
                        exact_term_query(fields.source_type, source_type),
                        Box::new(TermSetQuery::new(session_terms.clone())),
                    ],
                    fields.sort_asc,
                    after,
                )
            },
            |record| {
                record.source_type == source_type
                    && metadata_str(record, "session_id")
                        .is_some_and(|session_id| session_ids.iter().any(|id| id == session_id))
            },
        )
    }

    pub fn coding_records_all(
        &self,
        source_type: &str,
        session_ids: &[String],
    ) -> Result<Vec<GrepRecord>, IndexError> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let fields = self.fields();
        let session_terms = session_ids
            .iter()
            .map(|session_id| Term::from_field_text(fields.session_id, session_id))
            .collect::<Vec<_>>();
        self.record_query_all(
            "sort_asc",
            |after| {
                query_with_after(
                    vec![
                        exact_term_query(fields.source_type, source_type),
                        Box::new(TermSetQuery::new(session_terms.clone())),
                    ],
                    fields.sort_asc,
                    after,
                )
            },
            |record| {
                record.source_type == source_type
                    && metadata_str(record, "session_id")
                        .is_some_and(|session_id| session_ids.iter().any(|id| id == session_id))
            },
        )
    }

    pub fn resolve_link(&self, request_url: &str) -> Result<Option<GrepRecord>, IndexError> {
        let prefix_terms = request_url
            .char_indices()
            .map(|(start, ch)| &request_url[..start + ch.len_utf8()])
            .map(|prefix| Term::from_field_text(self.fields().anchor_url, prefix))
            .collect::<Vec<_>>();
        if prefix_terms.is_empty() {
            return Ok(None);
        }
        let fields = self.fields();
        let (searcher, _) = self.search_snapshot()?;
        let (addresses, _) = query_addresses_page(&searcher, 0, 1, "sort_desc", |after| {
            query_with_after(
                vec![Box::new(TermSetQuery::new(prefix_terms.clone()))],
                fields.sort_desc,
                after,
            )
        })?;
        let Some(address) = addresses.first().copied() else {
            return Ok(None);
        };
        let record = self.load_record(&searcher, address)?;
        if record.anchor_url != request_url && !request_url.starts_with(&record.anchor_url) {
            return Err(IndexError::InvalidDocument(format!(
                "anchor_url index disagrees with record {}",
                record.record_id
            )));
        }
        Ok(Some(record))
    }

    pub(crate) fn visit_source_object_records(
        &self,
        source_object_ids: &HashSet<String>,
        mut visit: impl FnMut(GrepRecord) -> Result<(), IndexError>,
    ) -> Result<(), IndexError> {
        if source_object_ids.is_empty() {
            return Ok(());
        }
        let fields = self.fields();
        let terms = source_object_ids
            .iter()
            .map(|source_object_id| {
                Term::from_field_text(fields.source_object_id, source_object_id)
            })
            .collect::<Vec<_>>();
        let (searcher, _) = self.search_snapshot()?;
        visit_query_addresses(
            &searcher,
            "sort_asc",
            |after| {
                query_with_after(
                    vec![Box::new(TermSetQuery::new(terms.clone()))],
                    fields.sort_asc,
                    after,
                )
            },
            |address| visit(self.load_record(&searcher, address)?),
        )
    }

    pub(crate) fn records_by_record_ids(
        &self,
        record_ids: &HashSet<&str>,
    ) -> Result<Vec<GrepRecord>, IndexError> {
        if record_ids.is_empty() {
            return Ok(Vec::new());
        }
        let fields = self.fields();
        let terms = record_ids
            .iter()
            .map(|record_id| Term::from_field_text(fields.record_id, record_id))
            .collect::<Vec<_>>();
        let (searcher, _) = self.search_snapshot()?;
        let mut records = Vec::with_capacity(record_ids.len());
        let mut seen = HashSet::with_capacity(record_ids.len());
        visit_query_addresses(
            &searcher,
            "sort_asc",
            |after| {
                query_with_after(
                    vec![Box::new(TermSetQuery::new(terms.clone()))],
                    fields.sort_asc,
                    after,
                )
            },
            |address| {
                let record = self.load_record(&searcher, address)?;
                if !record_ids.contains(record.record_id.as_str()) {
                    return Err(IndexError::InvalidDocument(format!(
                        "record_id index disagrees with record {}",
                        record.record_id
                    )));
                }
                if !seen.insert(record.record_id.clone()) {
                    return Err(IndexError::DuplicateRecord(record.record_id));
                }
                records.push(record);
                Ok(())
            },
        )?;
        Ok(records)
    }

    fn record_query_page(
        &self,
        offset: usize,
        limit: usize,
        sort_field_name: &'static str,
        query: impl Fn(Option<&str>) -> Box<dyn Query>,
        verify: impl Fn(&GrepRecord) -> bool,
    ) -> Result<(Vec<GrepRecord>, u64), IndexError> {
        let (searcher, _) = self.search_snapshot()?;
        let (addresses, total) =
            query_addresses_page(&searcher, offset, limit, sort_field_name, query)?;
        let records = self.load_records(&searcher, addresses)?;
        if let Some(record) = records.iter().find(|record| !verify(record)) {
            return Err(IndexError::InvalidDocument(format!(
                "exact field index disagrees with record {}",
                record.record_id
            )));
        }
        Ok((records, total))
    }

    fn session_edge_query_page(
        &self,
        offset: usize,
        limit: usize,
        sort_field_name: &'static str,
        query: impl Fn(Option<&str>) -> Box<dyn Query>,
    ) -> Result<(Vec<CodingSessionEdge>, u64), IndexError> {
        let (searcher, _) = self.search_snapshot()?;
        let (addresses, total) =
            query_addresses_page(&searcher, offset, limit, sort_field_name, query)?;
        let edges = addresses
            .into_iter()
            .map(|address| self.load_session_edge(&searcher, address))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((edges, total))
    }

    fn record_query_all(
        &self,
        sort_field_name: &'static str,
        query: impl Fn(Option<&str>) -> Box<dyn Query>,
        verify: impl Fn(&GrepRecord) -> bool,
    ) -> Result<Vec<GrepRecord>, IndexError> {
        let (searcher, _) = self.search_snapshot()?;
        let mut records = Vec::new();
        visit_query_addresses(&searcher, sort_field_name, query, |address| {
            let record = self.load_record(&searcher, address)?;
            if !verify(&record) {
                return Err(IndexError::InvalidDocument(format!(
                    "exact field index disagrees with record {}",
                    record.record_id
                )));
            }
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    fn session_edge_query_all(
        &self,
        sort_field_name: &'static str,
        query: impl Fn(Option<&str>) -> Box<dyn Query>,
    ) -> Result<Vec<CodingSessionEdge>, IndexError> {
        let (searcher, _) = self.search_snapshot()?;
        let mut edges = Vec::new();
        visit_query_addresses(&searcher, sort_field_name, query, |address| {
            edges.push(self.load_session_edge(&searcher, address)?);
            Ok(())
        })?;
        Ok(edges)
    }

    fn load_records(
        &self,
        searcher: &Searcher,
        addresses: Vec<DocAddress>,
    ) -> Result<Vec<GrepRecord>, IndexError> {
        addresses
            .into_iter()
            .map(|address| self.load_record(searcher, address))
            .collect()
    }

    fn load_session_edge(
        &self,
        searcher: &Searcher,
        address: DocAddress,
    ) -> Result<CodingSessionEdge, IndexError> {
        let document = searcher.doc::<TantivyDocument>(address)?;
        let required = |field, name: &str| {
            document
                .get_first(field)
                .and_then(|value| value.as_str())
                .map(str::to_owned)
                .ok_or_else(|| IndexError::InvalidDocument(format!("missing {name}")))
        };
        Ok(CodingSessionEdge {
            source_type: required(self.fields().source_type, "source_type")?,
            session_id: required(self.fields().session_id, "session_id")?,
            parent_session_id: document
                .get_first(self.fields().parent_session_id)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
        })
    }
}

fn query_addresses_page(
    searcher: &Searcher,
    offset: usize,
    limit: usize,
    sort_field_name: &'static str,
    query: impl Fn(Option<&str>) -> Box<dyn Query>,
) -> Result<(Vec<DocAddress>, u64), IndexError> {
    validate_limit(limit)?;
    let total = searcher.search(query(None).as_ref(), &Count)? as u64;
    if (offset as u64) >= total {
        return Ok((Vec::new(), total));
    }

    let mut after_key = None;
    let mut skipped = 0_usize;
    let mut addresses = Vec::with_capacity(limit);
    loop {
        let collector = TopDocs::with_limit(READ_CANDIDATE_PAGE_SIZE)
            .order_by_string_fast_field(sort_field_name, Order::Asc);
        let page = searcher.search(query(after_key.as_deref()).as_ref(), &collector)?;
        if page.is_empty() {
            break;
        }
        let mut last_page_key = None;
        for (sort_key, address) in &page {
            let sort_key = validate_sort_key(
                sort_key.as_deref(),
                after_key.as_deref(),
                last_page_key.as_deref(),
                sort_field_name,
            )?;
            last_page_key = Some(sort_key.to_owned());
            if skipped < offset {
                skipped += 1;
                continue;
            }
            addresses.push(*address);
            if addresses.len() == limit {
                return Ok((addresses, total));
            }
        }
        after_key = last_page_key;
        if page.len() < READ_CANDIDATE_PAGE_SIZE {
            break;
        }
    }
    Ok((addresses, total))
}

fn query_addresses_keyset(
    searcher: &Searcher,
    after: Option<&str>,
    limit: usize,
    sort_field_name: &'static str,
    query: impl for<'a> Fn(Option<&'a str>) -> Box<dyn Query>,
) -> Result<(Vec<DocAddress>, Option<String>), IndexError> {
    validate_limit(limit)?;
    let collector = TopDocs::with_limit(limit.saturating_add(1))
        .order_by_string_fast_field(sort_field_name, Order::Asc);
    let page = searcher.search(query(after).as_ref(), &collector)?;
    let mut previous = None;
    let mut addresses = Vec::with_capacity(page.len().min(limit));
    let mut sort_keys = Vec::with_capacity(page.len());
    for (sort_key, address) in &page {
        let sort_key = validate_sort_key(
            sort_key.as_deref(),
            after,
            previous.as_deref(),
            sort_field_name,
        )?;
        previous = Some(sort_key.to_owned());
        sort_keys.push(sort_key.to_owned());
        if addresses.len() < limit {
            addresses.push(*address);
        }
    }
    let next_cursor = (page.len() > limit).then(|| sort_keys[limit - 1].clone());
    Ok((addresses, next_cursor))
}

fn visit_query_addresses(
    searcher: &Searcher,
    sort_field_name: &'static str,
    query: impl Fn(Option<&str>) -> Box<dyn Query>,
    mut visit: impl FnMut(DocAddress) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    let mut after_key = None;
    loop {
        let collector = TopDocs::with_limit(READ_CANDIDATE_PAGE_SIZE)
            .order_by_string_fast_field(sort_field_name, Order::Asc);
        let page = searcher.search(query(after_key.as_deref()).as_ref(), &collector)?;
        if page.is_empty() {
            return Ok(());
        }
        let mut last_page_key = None;
        for (sort_key, address) in &page {
            let sort_key = validate_sort_key(
                sort_key.as_deref(),
                after_key.as_deref(),
                last_page_key.as_deref(),
                sort_field_name,
            )?;
            last_page_key = Some(sort_key.to_owned());
            visit(*address)?;
        }
        after_key = last_page_key;
        if page.len() < READ_CANDIDATE_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn query_with_after(
    mut clauses: Vec<Box<dyn Query>>,
    sort_field: Field,
    after: Option<&str>,
) -> Box<dyn Query + 'static> {
    if let Some(after) = after {
        clauses.push(Box::new(RangeQuery::new(
            Bound::Excluded(Term::from_field_text(sort_field, after)),
            Bound::Unbounded,
        )));
    }
    match clauses.len() {
        0 => Box::new(tantivy::query::AllQuery),
        1 => clauses.pop().expect("one query clause"),
        _ => Box::new(BooleanQuery::intersection(clauses)),
    }
}

fn exact_term_query(field: Field, value: &str) -> Box<dyn Query + 'static> {
    Box::new(TermQuery::new(
        Term::from_field_text(field, value),
        IndexRecordOption::Basic,
    ))
}

fn validate_limit(limit: usize) -> Result<(), IndexError> {
    if limit == 0 {
        return Err(IndexError::InvalidReadRequest(
            "limit must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_sort_key<'a>(
    sort_key: Option<&'a str>,
    after: Option<&str>,
    previous: Option<&str>,
    sort_field_name: &str,
) -> Result<&'a str, IndexError> {
    let sort_key = sort_key
        .ok_or_else(|| IndexError::InvalidDocument(format!("missing {sort_field_name}")))?;
    if after.is_some_and(|after| sort_key <= after)
        || previous.is_some_and(|previous| sort_key <= previous)
    {
        return Err(IndexError::InvalidDocument(format!(
            "{sort_field_name} is not strictly ordered"
        )));
    }
    Ok(sort_key)
}

fn metadata_str<'a>(record: &'a GrepRecord, key: &str) -> Option<&'a str> {
    record.metadata.get(key).and_then(serde_json::Value::as_str)
}

fn is_coding_source(source_type: &str) -> bool {
    matches!(source_type, "claude-code" | "codex")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use lethe_projection_corpus::{CorpusRecord, normalized_text};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::MIN_WRITER_HEAP_BYTES;

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("lethe-search-read-test-{}", uuid::Uuid::now_v7()))
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        record_id: &str,
        day: u32,
        source_type: &str,
        thread_ts: Option<&str>,
        thread_key: Option<&str>,
        session_id: Option<&str>,
        parent_session_id: Option<&str>,
        anchor_url: &str,
    ) -> CorpusRecord {
        let text = format!("body {record_id}");
        CorpusRecord {
            record_id: record_id.to_owned(),
            source_type: source_type.to_owned(),
            anchor_url: anchor_url.to_owned(),
            source_title: format!("title {record_id}"),
            source_location: None,
            timestamp: format!("2026-01-{day:02}T00:00:00Z").parse().unwrap(),
            normalized_text: normalized_text(&text),
            text,
            thread_ts: thread_ts.map(str::to_owned),
            container: None,
            metadata: serde_json::json!({
                "thread_key": thread_key,
                "session_id": session_id,
                "parent_session_id": parent_session_id,
            }),
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
                records.len() as u64,
                records.len() as u64,
                format!("proj:corpus:{}", records.len()),
            )
            .unwrap();
        (path, index)
    }

    #[test]
    fn records_page_is_bounded_and_crosses_internal_keyset_pages() {
        let mut records = (0..300)
            .map(|index| {
                record(
                    &format!("r{index:03}"),
                    if index < 160 { 2 } else { 1 },
                    if index % 2 == 0 { "slack" } else { "drive" },
                    None,
                    None,
                    None,
                    None,
                    &format!("https://example.test/{index}"),
                )
            })
            .collect::<Vec<_>>();
        records.reverse();
        let (path, index) = create_index(&records);

        let (page, total) = index.records_page(150, 3).unwrap();
        assert_eq!(total, 300);
        assert_eq!(
            page.iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["r150", "r151", "r152"]
        );
        let (timestamp_boundary, _) = index.records_page(158, 5).unwrap();
        assert_eq!(
            timestamp_boundary
                .iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["r158", "r159", "r160", "r161", "r162"]
        );
        assert!(timestamp_boundary[1].timestamp > timestamp_boundary[2].timestamp);
        assert_eq!(
            index.source_type_counts().unwrap(),
            BTreeMap::from([("drive".to_owned(), 150), ("slack".to_owned(), 150)])
        );
        assert!(matches!(
            index.records_page(0, 0),
            Err(IndexError::InvalidReadRequest(_))
        ));

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn records_keyset_page_crosses_same_sort_timestamp_without_duplicates() {
        let records = (0..5)
            .map(|index| {
                record(
                    &format!("same-{index}"),
                    2,
                    "slack",
                    None,
                    None,
                    None,
                    None,
                    &format!("https://example.test/{index}"),
                )
            })
            .collect::<Vec<_>>();
        let (path, index) = create_index(&records);

        let (first, first_cursor) = index.records_keyset_page(None, 2).unwrap();
        let first_cursor = first_cursor.expect("first page must expose a cursor");
        let (second, second_cursor) = index.records_keyset_page(Some(&first_cursor), 2).unwrap();
        let second_cursor = second_cursor.expect("second page must expose a cursor");
        let (third, no_cursor) = index.records_keyset_page(Some(&second_cursor), 2).unwrap();

        let ids = first
            .into_iter()
            .chain(second)
            .chain(third)
            .map(|record| record.record_id)
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 5);
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            5
        );
        assert!(no_cursor.is_none());

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn exact_source_object_id_reads_the_dedicated_index_path() {
        let mut indexed = record(
            "indexed",
            1,
            "slack",
            None,
            None,
            None,
            None,
            "https://example.test/indexed",
        );
        indexed.metadata = serde_json::json!({"source_object_id": "object-42"});
        let mut other = record(
            "other",
            2,
            "slack",
            None,
            None,
            None,
            None,
            "https://example.test/other",
        );
        other.metadata = serde_json::json!({"source_object_id": "object-99"});
        let (path, index) = create_index(&[indexed, other]);

        let (matches, next_cursor) = index
            .exact_records_page(ExactSearchField::SourceObjectId, "object-42", None, 10)
            .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record_id, "indexed");
        assert!(next_cursor.is_none());

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn exact_thread_session_and_unicode_link_reads_use_indexed_fields() {
        let records = vec![
            record(
                "earlier",
                1,
                "slack",
                Some("thread-shared"),
                Some("key-shared"),
                Some("root"),
                None,
                "https://例.example/",
            ),
            record(
                "other-source",
                2,
                "drive",
                Some("thread-shared"),
                Some("key-other"),
                Some("drive-session"),
                None,
                "https://elsewhere.example/",
            ),
            record(
                "later",
                3,
                "slack",
                Some("thread-shared"),
                Some("key-shared"),
                Some("child"),
                Some("root"),
                "https://例.example/資料",
            ),
        ];
        let (path, index) = create_index(&records);

        assert_eq!(
            index
                .record_by_thread_ts("thread-shared")
                .unwrap()
                .unwrap()
                .record_id,
            "later"
        );
        assert_eq!(
            index
                .record_by_thread_key("key-shared")
                .unwrap()
                .unwrap()
                .record_id,
            "later"
        );
        let (thread, total) = index
            .thread_records_page("slack", "thread-shared", GrepOrder::DateAsc, 0, 10)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(
            thread
                .iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["earlier", "later"]
        );
        let (thread_desc, total) = index
            .thread_records_page("slack", "thread-shared", GrepOrder::DateDesc, 0, 10)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(
            thread_desc
                .iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["later", "earlier"]
        );
        let (by_key, total) = index.thread_key_records_page("key-shared", 1, 1).unwrap();
        assert_eq!(total, 2);
        assert_eq!(by_key[0].record_id, "later");

        let (edges, total) = index
            .coding_child_session_edges("slack", "root", 0, 10)
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(
            edges,
            vec![CodingSessionEdge {
                source_type: "slack".to_owned(),
                session_id: "child".to_owned(),
                parent_session_id: Some("root".to_owned()),
            }]
        );
        assert_eq!(
            index
                .coding_session_edges_by_session_id("root", 0, 10)
                .unwrap()
                .1,
            1
        );
        assert_eq!(
            index
                .coding_source_session_edges("slack", "child", 0, 10)
                .unwrap()
                .1,
            1
        );
        let (coding_records, total) = index
            .coding_records_page("slack", &["root".to_owned(), "child".to_owned()], 0, 10)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(
            coding_records
                .iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["earlier", "later"]
        );

        let resolved = index
            .resolve_link("https://例.example/資料/詳細?x=1")
            .unwrap()
            .unwrap();
        assert_eq!(resolved.record_id, "later");
        assert!(
            index
                .resolve_link("https://missing.example/")
                .unwrap()
                .is_none()
        );

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn generic_threads_are_ascending_and_coding_seed_fields_are_exact() {
        let records = vec![
            record(
                "generic-new",
                4,
                "drive",
                Some("generic-thread"),
                Some("generic-key"),
                None,
                None,
                "https://example.test/generic-new",
            ),
            record(
                "generic-old",
                1,
                "drive",
                Some("generic-thread"),
                Some("generic-key"),
                None,
                None,
                "https://example.test/generic-old",
            ),
            record(
                "non-coding-newest",
                9,
                "slack",
                None,
                Some("coding-key"),
                Some("coding-session"),
                None,
                "https://example.test/non-coding",
            ),
            record(
                "coding-by-thread",
                5,
                "claude-code",
                None,
                Some("coding-key"),
                Some("other-session"),
                None,
                "https://example.test/coding-thread",
            ),
            record(
                "coding-by-session",
                6,
                "codex",
                None,
                Some("other-key"),
                Some("coding-session"),
                None,
                "https://example.test/coding-session",
            ),
        ];
        let (path, index) = create_index(&records);

        let (generic, total) = index
            .thread_records_page("drive", "generic-thread", GrepOrder::DateAsc, 0, 10)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(
            generic
                .iter()
                .map(|record| record.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["generic-old", "generic-new"]
        );
        assert_eq!(
            index
                .coding_record_by_thread_key("coding-key")
                .unwrap()
                .unwrap()
                .record_id,
            "coding-by-thread"
        );
        assert_eq!(
            index
                .coding_record_by_session_id("coding-session")
                .unwrap()
                .unwrap()
                .record_id,
            "coding-by-session"
        );

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn unpaged_thread_and_coding_reads_cross_internal_keyset_pages_once() {
        let records = (0..300)
            .map(|index| {
                record(
                    &format!("r{index:03}"),
                    1,
                    "codex",
                    Some("thread-all"),
                    Some("key-all"),
                    Some("child"),
                    Some("root"),
                    &format!("https://example.test/{index}"),
                )
            })
            .collect::<Vec<_>>();
        let (path, index) = create_index(&records);

        let thread = index
            .thread_records_all("codex", "thread-all", GrepOrder::DateAsc)
            .unwrap();
        assert_eq!(thread.len(), 300);
        assert_eq!(thread.first().unwrap().record_id, "r000");
        assert_eq!(thread.last().unwrap().record_id, "r299");

        let session_edges = index
            .coding_source_session_edges_all("codex", "child")
            .unwrap();
        assert_eq!(session_edges.len(), 300);
        assert!(session_edges.iter().all(|edge| {
            edge.session_id == "child" && edge.parent_session_id.as_deref() == Some("root")
        }));
        let child_edges = index
            .coding_child_session_edges_all("codex", "root")
            .unwrap();
        assert_eq!(child_edges, session_edges);

        let coding_records = index
            .coding_records_all("codex", &["child".to_owned()])
            .unwrap();
        assert_eq!(coding_records.len(), 300);
        assert_eq!(coding_records.first().unwrap().record_id, "r000");
        assert_eq!(coding_records.last().unwrap().record_id, "r299");

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn resolve_link_selects_latest_record_across_multiple_matching_prefixes() {
        let records = vec![
            record(
                "specific-old",
                1,
                "drive",
                None,
                None,
                None,
                None,
                "https://example.test/docs/item",
            ),
            record(
                "broad-latest",
                3,
                "drive",
                None,
                None,
                None,
                None,
                "https://example.test/",
            ),
            record(
                "medium-middle",
                2,
                "drive",
                None,
                None,
                None,
                None,
                "https://example.test/docs/",
            ),
        ];
        let (path, index) = create_index(&records);

        let resolved = index
            .resolve_link("https://example.test/docs/item/details")
            .unwrap()
            .unwrap();
        assert_eq!(resolved.record_id, "broad-latest");

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_metadata_and_record_count_are_from_one_commit() {
        let records = vec![
            record(
                "older",
                1,
                "drive",
                None,
                None,
                None,
                None,
                "https://example.test/older",
            ),
            record(
                "newer",
                2,
                "slack",
                None,
                None,
                None,
                None,
                "https://example.test/newer",
            ),
        ];
        let (path, index) = create_index(&records);

        let ((page, total), metadata) = index
            .read_with_metadata(|snapshot| snapshot.records_page(0, 10))
            .unwrap();
        assert_eq!(page.len() as u64, total);
        assert_eq!(total, metadata.record_count);
        assert_eq!(
            metadata.source_type_counts.values().sum::<u64>(),
            metadata.record_count
        );

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }
}
