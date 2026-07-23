use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use lethe_core::domain::ConsentDecision;
use serde::{Deserialize, Serialize};
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};

pub const INDEX_FORMAT_VERSION: u32 = 3;
pub const NGRAM_TOKENIZER: &str = "lethe_ngram_1_3_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexCommitMetadata {
    pub index_format_version: u32,
    pub schema_fingerprint: String,
    pub corpus_config_fingerprint: String,
    pub last_append_seq: u64,
    pub observation_count: u64,
    pub projection_watermark: String,
    pub committed_at: DateTime<Utc>,
    pub record_count: u64,
    pub source_type_counts: BTreeMap<String, u64>,
    pub linked_form_sheet_ids: Vec<String>,
    pub consent_by_subject: BTreeMap<String, ConsentDecision>,
    pub consent_by_identifier: BTreeMap<String, ConsentDecision>,
    pub retracted_observation_ids: Vec<String>,
    pub retracted_source_object_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub schema: Schema,
    pub record_id: Field,
    pub source_type: Field,
    pub anchor_url: Field,
    pub source_title: Field,
    pub source_location: Field,
    pub timestamp_nanos: Field,
    pub text: Field,
    pub normalized_text: Field,
    pub thread_ts: Field,
    pub thread_key: Field,
    pub session_id: Field,
    pub parent_session_id: Field,
    pub container: Field,
    pub metadata_json: Field,
    pub source_object_id: Field,
    pub linked_sheet_id: Field,
    pub sort_asc: Field,
    pub sort_desc: Field,
}

impl IndexSchema {
    pub fn build() -> Self {
        let mut builder = Schema::builder();
        let record_id = builder.add_text_field("record_id", STRING | STORED | FAST);
        let source_type = builder.add_text_field("source_type", STRING | STORED | FAST);
        let anchor_url = builder.add_text_field("anchor_url", STRING | STORED);
        let source_title = builder.add_text_field("source_title", STORED);
        let source_location = builder.add_text_field("source_location", STORED);
        let timestamp_nanos = builder.add_i64_field("timestamp_nanos", INDEXED | STORED | FAST);
        let text = builder.add_text_field("text", STORED);
        let ngram_indexing = TextFieldIndexing::default()
            .set_tokenizer(NGRAM_TOKENIZER)
            .set_index_option(IndexRecordOption::Basic)
            .set_fieldnorms(false);
        let normalized_text_options = TextOptions::default().set_indexing_options(ngram_indexing);
        let normalized_text = builder.add_text_field("normalized_text", normalized_text_options);
        let thread_ts = builder.add_text_field("thread_ts", STRING | STORED);
        let thread_key = builder.add_text_field("thread_key", STRING | STORED);
        let session_id = builder.add_text_field("session_id", STRING | STORED);
        let parent_session_id = builder.add_text_field("parent_session_id", STRING | STORED);
        let container = builder.add_text_field("container", STRING | STORED);
        let metadata_json = builder.add_text_field("metadata_json", STORED);
        let source_object_id = builder.add_text_field("source_object_id", STRING | STORED);
        let linked_sheet_id = builder.add_text_field("linked_sheet_id", STRING | STORED);
        let sort_asc = builder.add_text_field("sort_asc", STRING | FAST);
        let sort_desc = builder.add_text_field("sort_desc", STRING | FAST);
        Self {
            schema: builder.build(),
            record_id,
            source_type,
            anchor_url,
            source_title,
            source_location,
            timestamp_nanos,
            text,
            normalized_text,
            thread_ts,
            thread_key,
            session_id,
            parent_session_id,
            container,
            metadata_json,
            source_object_id,
            linked_sheet_id,
            sort_asc,
            sort_desc,
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};

        let encoded = serde_json::to_vec(&self.schema).expect("Tantivy schema serializes");
        format!("{:x}", Sha256::digest(encoded))
    }
}

pub fn asc_sort_key(timestamp_nanos: i64, record_id: &str) -> String {
    format!("{:016x}:{record_id}", biased_timestamp(timestamp_nanos))
}

pub fn desc_sort_key(timestamp_nanos: i64, record_id: &str) -> String {
    format!("{:016x}:{record_id}", !biased_timestamp(timestamp_nanos))
}

fn biased_timestamp(timestamp_nanos: i64) -> u64 {
    (timestamp_nanos as u64) ^ (1_u64 << 63)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_keys_preserve_contract_order() {
        let old = -1;
        let new = 1;

        assert!(asc_sort_key(old, "b") < asc_sort_key(new, "a"));
        assert!(desc_sort_key(new, "a") < desc_sort_key(old, "b"));
        assert!(asc_sort_key(old, "a") < asc_sort_key(old, "b"));
        assert!(desc_sort_key(old, "a") < desc_sort_key(old, "b"));
    }

    #[test]
    fn schema_and_commit_metadata_are_versioned() {
        let schema = IndexSchema::build();
        assert!(!schema.fingerprint().is_empty());
        assert_eq!(schema.schema.get_field_name(schema.record_id), "record_id");
        assert_eq!(
            schema.schema.get_field_name(schema.timestamp_nanos),
            "timestamp_nanos"
        );

        let metadata = IndexCommitMetadata {
            index_format_version: INDEX_FORMAT_VERSION,
            schema_fingerprint: schema.fingerprint(),
            corpus_config_fingerprint: "policy-v1".to_owned(),
            last_append_seq: 12,
            observation_count: 10,
            projection_watermark: "proj:corpus:12".to_owned(),
            committed_at: "2026-01-02T03:04:05Z".parse().unwrap(),
            record_count: 7,
            source_type_counts: BTreeMap::from([("slack".to_owned(), 7)]),
            linked_form_sheet_ids: vec!["sheet-1".to_owned()],
            consent_by_subject: BTreeMap::new(),
            consent_by_identifier: BTreeMap::new(),
            retracted_observation_ids: Vec::new(),
            retracted_source_object_ids: Vec::new(),
        };
        let round_trip: IndexCommitMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(round_trip, metadata);
    }
}
