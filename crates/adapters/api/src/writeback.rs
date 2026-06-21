//! SaaS Write-back Adapter trait — the generic protocol for pushing
//! projection outputs to external SaaS services.

use serde::{Deserialize, Serialize};

use crate::error::AdapterError;

/// A record to be written to an external SaaS destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRecord {
    /// Unique identifier for this record in the LETHE domain (e.g. person_id).
    pub entity_id: String,
    /// Display name / title for the record.
    pub title: String,
    /// The structured payload to write.
    pub payload: serde_json::Value,
    /// Optional: the external page/record ID if this is an update.
    pub external_id: Option<String>,
}

/// Result of a write operation to an external SaaS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    /// The external ID assigned by the SaaS (e.g. Notion page ID).
    pub external_id: String,
    /// Whether the record was newly created or updated.
    pub action: WriteAction,
    /// Optional URL to the record in the SaaS.
    pub url: Option<String>,
}

/// Whether a write was a creation or an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAction {
    Created,
    Updated,
}

/// Batch write result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteResult {
    pub succeeded: Vec<WriteResult>,
    pub failed: Vec<WriteFailure>,
}

/// A single write failure in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFailure {
    pub entity_id: String,
    pub error: String,
}

/// The core write-back adapter protocol.
///
/// Implementors push projection output records to an external SaaS.
/// Each adapter handles its own authentication, rate limiting, and
/// content rendering.
pub trait SaaSWriteAdapter {
    /// Write (create or update) a single record.
    fn write_record(&self, record: &WriteRecord) -> Result<WriteResult, AdapterError>;

    /// Write a batch of records. Default implementation calls `write_record`
    /// sequentially. Implementors may override for bulk API support.
    fn write_batch(&self, records: &[WriteRecord]) -> BatchWriteResult {
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        for record in records {
            match self.write_record(record) {
                Ok(result) => succeeded.push(result),
                Err(err) => failed.push(WriteFailure {
                    entity_id: record.entity_id.clone(),
                    error: err.to_string(),
                }),
            }
        }
        BatchWriteResult { succeeded, failed }
    }

    /// Check if a record already exists in the external SaaS by entity_id.
    fn find_existing(&self, entity_id: &str) -> Result<Option<String>, AdapterError>;

    /// Delete a record from the external SaaS.
    fn delete_record(&self, external_id: &str) -> Result<(), AdapterError>;

    /// Return the adapter's display name (e.g. "notion", "airtable").
    fn adapter_name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture write adapter for testing.
    struct FixtureWriteAdapter {
        fail_on: Option<String>,
    }

    impl SaaSWriteAdapter for FixtureWriteAdapter {
        fn write_record(&self, record: &WriteRecord) -> Result<WriteResult, AdapterError> {
            if self.fail_on.as_deref() == Some(&record.entity_id) {
                return Err(AdapterError::Other("fixture failure".into()));
            }
            Ok(WriteResult {
                external_id: format!("ext-{}", record.entity_id),
                action: if record.external_id.is_some() {
                    WriteAction::Updated
                } else {
                    WriteAction::Created
                },
                url: Some(format!("https://example.com/{}", record.entity_id)),
            })
        }

        fn find_existing(&self, entity_id: &str) -> Result<Option<String>, AdapterError> {
            if entity_id == "existing" {
                Ok(Some("ext-existing".into()))
            } else {
                Ok(None)
            }
        }

        fn delete_record(&self, _external_id: &str) -> Result<(), AdapterError> {
            Ok(())
        }

        fn adapter_name(&self) -> &str {
            "fixture"
        }
    }

    #[test]
    fn write_record_creates_new() {
        let adapter = FixtureWriteAdapter { fail_on: None };
        let record = WriteRecord {
            entity_id: "person:alice".into(),
            title: "Alice".into(),
            payload: serde_json::json!({"name": "Alice"}),
            external_id: None,
        };
        let result = adapter.write_record(&record).unwrap();
        assert_eq!(result.action, WriteAction::Created);
        assert_eq!(result.external_id, "ext-person:alice");
    }

    #[test]
    fn write_record_updates_existing() {
        let adapter = FixtureWriteAdapter { fail_on: None };
        let record = WriteRecord {
            entity_id: "person:alice".into(),
            title: "Alice".into(),
            payload: serde_json::json!({"name": "Alice"}),
            external_id: Some("ext-123".into()),
        };
        let result = adapter.write_record(&record).unwrap();
        assert_eq!(result.action, WriteAction::Updated);
    }

    #[test]
    fn batch_write_partial_failure() {
        let adapter = FixtureWriteAdapter {
            fail_on: Some("person:bob".into()),
        };
        let records = vec![
            WriteRecord {
                entity_id: "person:alice".into(),
                title: "Alice".into(),
                payload: serde_json::json!({}),
                external_id: None,
            },
            WriteRecord {
                entity_id: "person:bob".into(),
                title: "Bob".into(),
                payload: serde_json::json!({}),
                external_id: None,
            },
        ];
        let result = adapter.write_batch(&records);
        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].entity_id, "person:bob");
    }

    #[test]
    fn find_existing_returns_id() {
        let adapter = FixtureWriteAdapter { fail_on: None };
        assert!(adapter.find_existing("existing").unwrap().is_some());
        assert!(adapter.find_existing("nonexistent").unwrap().is_none());
    }
}
