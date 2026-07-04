//! Answer Log projection for prior QA scaffolding.

use chrono::{DateTime, Utc};
use lethe_core::domain::{IdempotencyKey, Observation};
use lethe_engine::projection::runner::Projector;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const ANSWER_LOG_SCHEMA: &str = "schema:bot-answer-log";
pub const ANSWER_LOG_PROJECTION_ID: &str = "proj:answer-log";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Citation {
    pub url: String,
    #[serde(default)]
    pub record_id: Option<String>,
    #[serde(default)]
    pub source_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnswerLogRecord {
    pub record_id: String,
    pub question: String,
    pub answer: String,
    #[serde(default)]
    pub citations: Vec<Citation>,
    #[serde(default)]
    pub used_queries: Vec<String>,
    #[serde(default)]
    pub asker: Option<String>,
    pub ts: DateTime<Utc>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: serde_json::Value,
    #[serde(default)]
    pub confidence: Option<String>,
    #[serde(default)]
    pub unknowns: Vec<String>,
    pub normalized_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriorQaResult {
    pub record_id: String,
    pub question: String,
    pub answer: String,
    pub citations: Vec<Citation>,
    pub ts: DateTime<Utc>,
    pub is_primary_source: bool,
}

#[derive(Debug, Default, Clone)]
pub struct AnswerLogProjector;

impl AnswerLogProjector {
    pub fn project_observations(&self, observations: &[Observation]) -> Vec<AnswerLogRecord> {
        let mut records = observations
            .iter()
            .filter(|observation| observation.schema.as_str() == ANSWER_LOG_SCHEMA)
            .filter_map(answer_record)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .ts
                .cmp(&left.ts)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        records
    }

    pub fn search(
        &self,
        records: &[AnswerLogRecord],
        query: &str,
        limit: usize,
    ) -> Vec<PriorQaResult> {
        let normalized_query = normalize(query);
        records
            .iter()
            .filter(|record| record.normalized_text.contains(&normalized_query))
            .take(limit)
            .map(|record| PriorQaResult {
                record_id: record.record_id.clone(),
                question: record.question.clone(),
                answer: record.answer.clone(),
                citations: record.citations.clone(),
                ts: record.ts,
                is_primary_source: false,
            })
            .collect()
    }
}

impl Projector for AnswerLogProjector {
    type Input = Observation;
    type Output = AnswerLogRecord;

    fn project(&self, inputs: &[Observation]) -> Vec<AnswerLogRecord> {
        self.project_observations(inputs)
    }
}

pub fn answer_log_identity_key(question: &str, answer: &str, ts: DateTime<Utc>) -> IdempotencyKey {
    let canonical = serde_json::json!({
        "question": question,
        "answer": answer,
        "ts": ts.to_rfc3339(),
    })
    .to_string();
    let hash = sha256_hex(&canonical);
    IdempotencyKey::new(format!("bot-answer-log:{hash}"))
}

fn answer_record(observation: &Observation) -> Option<AnswerLogRecord> {
    let question = string_at(&observation.payload, &["question"])?.to_owned();
    let answer = string_at(&observation.payload, &["answer"])?.to_owned();
    let citations = observation
        .payload
        .get("citations")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    let used_queries: Vec<String> = observation
        .payload
        .get("used_queries")
        .and_then(serde_json::Value::as_array)
        .map(|queries| {
            queries
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let unknowns: Vec<String> = observation
        .payload
        .get("unknowns")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let ts = string_at(&observation.payload, &["ts"])
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.to_utc())
        .unwrap_or(observation.published);
    let normalized_text = normalize(&format!(
        "{}\n{}\n{}",
        question,
        answer,
        used_queries.join("\n")
    ));
    Some(AnswerLogRecord {
        record_id: format!("answer-log:{}", observation.id),
        question,
        answer,
        citations,
        used_queries,
        asker: string_at(&observation.payload, &["asker"]).map(str::to_owned),
        ts,
        model: string_at(&observation.payload, &["model"]).map(str::to_owned),
        usage: observation
            .payload
            .get("usage")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        confidence: string_at(&observation.payload, &["confidence"]).map(str::to_owned),
        unknowns,
        normalized_text,
    })
}

fn normalize(value: &str) -> String {
    value.nfkc().collect::<String>().to_lowercase()
}

fn string_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_str()
}

fn sha256_hex(value: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_core::domain::*;

    fn observation(question: &str, answer: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(ANSWER_LOG_SCHEMA),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:search-bot"),
            source_system: Some(SourceSystemRef::new("sys:lethe-internal")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("answer-log:test"),
            target: None,
            payload: serde_json::json!({
                "question": question,
                "answer": answer,
                "citations": [{"url": "https://example.test", "record_id": "r1", "source_type": "docs"}],
                "used_queries": ["忘れ物"],
                "ts": Utc::now().to_rfc3339(),
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("answer-log:test"),
            meta: serde_json::json!({"canonical_json": "{}"}),
        }
    }

    #[test]
    fn search_marks_results_as_not_primary_source() {
        let projector = AnswerLogProjector;
        let records = projector.project_observations(&[observation("Q", "忘れ物は受付です")]);
        let results = projector.search(&records, "忘れ物", 10);
        assert_eq!(results.len(), 1);
        assert!(!results[0].is_primary_source);
    }
}
