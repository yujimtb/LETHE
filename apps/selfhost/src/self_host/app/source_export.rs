use serde::{Deserialize, Serialize};

use super::{AppService, SelfHostError};
use lethe_core::domain::Observation;
use lethe_storage_api::StoragePorts;

const ASK_BOT_SOURCE_SCHEMA: &str = "schema:askbot-source-observation";
const ASK_BOT_SOURCE_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObservationExportQuery {
    pub after_append_seq: u64,
    pub limit: usize,
    pub watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObservationExportItem {
    pub append_seq: u64,
    pub observation: Observation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObservationExportPage {
    pub watermark: u64,
    pub next_after_append_seq: u64,
    pub complete: bool,
    pub items: Vec<SourceObservationExportItem>,
}

impl AppService {
    pub fn export_source_observations(
        &self,
        query: &SourceObservationExportQuery,
    ) -> Result<SourceObservationExportPage, SelfHostError> {
        let maximum_page_size = self.config.resource_limits.max_page_size;
        let scan_bound = self.config.resource_limits.max_source_export_scan_records;
        let storage = self.persistence_read_lock()?;
        export_source_observation_page(storage.as_ref(), query, maximum_page_size, scan_bound)
    }
}

pub fn export_source_observation_page(
    storage: &dyn StoragePorts,
    query: &SourceObservationExportQuery,
    maximum_page_size: usize,
    scan_bound: usize,
) -> Result<SourceObservationExportPage, SelfHostError> {
    if query.limit == 0 || query.limit > maximum_page_size {
        return Err(SelfHostError::SourceExportValidation {
            code: "source_export_limit_invalid",
            details: serde_json::json!({
                "actual": query.limit,
                "minimum": 1,
                "maximum": maximum_page_size,
            }),
        });
    }

    let stats = storage.observation_stats()?;
    let watermark = query.watermark.unwrap_or(stats.max_append_seq);
    if watermark > stats.max_append_seq {
        return Err(SelfHostError::SourceExportValidation {
            code: "source_export_watermark_ahead",
            details: serde_json::json!({
                "watermark": watermark,
                "durable_max_append_seq": stats.max_append_seq,
            }),
        });
    }
    if query.after_append_seq > watermark {
        return Err(SelfHostError::SourceExportValidation {
            code: "source_export_continuation_invalid",
            details: serde_json::json!({
                "after_append_seq": query.after_append_seq,
                "watermark": watermark,
            }),
        });
    }

    let mut cursor = query.after_append_seq;
    let mut scanned = 0usize;
    let mut complete = cursor >= watermark;
    let mut items = Vec::with_capacity(query.limit);

    while !complete && items.len() < query.limit {
        if scanned == scan_bound {
            return Err(SelfHostError::SourceExportUnavailable {
                code: "source_export_scan_bound_exhausted",
                details: serde_json::json!({
                    "after_append_seq": query.after_append_seq,
                    "last_scanned_append_seq": cursor,
                    "watermark": watermark,
                    "maximum_scanned_records": scan_bound,
                }),
            });
        }

        let fetch_limit = maximum_page_size.min(scan_bound - scanned);
        let page = storage.observation_page(cursor, fetch_limit)?;
        if page.is_empty() {
            cursor = watermark;
            complete = true;
            break;
        }

        for stored in page {
            if stored.append_seq <= cursor {
                return Err(SelfHostError::SourceExportUnavailable {
                    code: "source_export_storage_order_invalid",
                    details: serde_json::json!({
                        "after_append_seq": cursor,
                        "received_append_seq": stored.append_seq,
                    }),
                });
            }
            if stored.append_seq > watermark {
                cursor = watermark;
                complete = true;
                break;
            }

            cursor = stored.append_seq;
            scanned += 1;
            if stored.observation.schema.as_str() == ASK_BOT_SOURCE_SCHEMA
                && stored.observation.schema_version.as_str() == ASK_BOT_SOURCE_SCHEMA_VERSION
            {
                items.push(SourceObservationExportItem {
                    append_seq: stored.append_seq,
                    observation: stored.observation,
                });
                if items.len() == query.limit {
                    break;
                }
            }

            if scanned == scan_bound && cursor < watermark {
                break;
            }
        }

        complete |= cursor >= watermark;
    }

    if !complete && items.len() < query.limit && scanned == scan_bound {
        return Err(SelfHostError::SourceExportUnavailable {
            code: "source_export_scan_bound_exhausted",
            details: serde_json::json!({
                "after_append_seq": query.after_append_seq,
                "last_scanned_append_seq": cursor,
                "watermark": watermark,
                "maximum_scanned_records": scan_bound,
            }),
        });
    }

    Ok(SourceObservationExportPage {
        watermark,
        next_after_append_seq: cursor,
        complete,
        items,
    })
}
