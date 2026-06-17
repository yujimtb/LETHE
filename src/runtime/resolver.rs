//! Runtime resolver for logical lake reads over physical leaves.

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};

use chrono::{DateTime, Datelike, Utc};

use crate::domain::Observation;
use crate::runtime::partition::{PartitionError, PartitionTree};

#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error("published window start must be <= end")]
    InvalidPublishedWindow,
    #[error("container filter requires a source filter")]
    ContainerRequiresSource,
    #[error("partition error: {0}")]
    Partition(#[from] PartitionError),
    #[error("leaf stream is not sorted by (published, recorded_at, id)")]
    UnsortedLeafStream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeafFilter {
    pub published: Option<PublishedWindow>,
    pub source: Option<String>,
    pub container: Option<String>,
}

pub fn candidate_leaves(
    tree: &PartitionTree,
    filter: &LeafFilter,
) -> Result<Vec<String>, ResolverError> {
    if filter.container.is_some() && filter.source.is_none() {
        return Err(ResolverError::ContainerRequiresSource);
    }

    let bucket_axes = if let Some(window) = &filter.published {
        month_year_buckets(window)?
            .into_iter()
            .map(|(month, year)| vec![format!("{month:02}"), format!("{year:04}")])
            .collect::<Vec<_>>()
    } else {
        vec![Vec::new()]
    };

    let mut leaves = BTreeSet::new();
    for mut axes in bucket_axes {
        if let Some(source) = &filter.source {
            axes.push(source.clone());
        }
        if let Some(container) = &filter.container {
            axes.push(container.clone());
        }
        for leaf_id in tree.candidate_leaf_ids_for_prefix_axes(&axes)? {
            leaves.insert(leaf_id);
        }
    }

    Ok(leaves.into_iter().collect())
}

pub fn streaming_k_way_merge(
    leaf_streams: Vec<Vec<Observation>>,
) -> Result<Vec<Observation>, ResolverError> {
    for stream in &leaf_streams {
        ensure_sorted(stream)?;
    }

    let mut streams = leaf_streams.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
    let mut heap = BinaryHeap::new();
    for (leaf_index, stream) in streams.iter_mut().enumerate() {
        if let Some(observation) = stream.next() {
            heap.push(HeapItem {
                leaf_index,
                observation,
            });
        }
    }

    let mut merged = Vec::new();
    while let Some(item) = heap.pop() {
        let leaf_index = item.leaf_index;
        merged.push(item.observation);
        if let Some(observation) = streams[leaf_index].next() {
            heap.push(HeapItem {
                leaf_index,
                observation,
            });
        }
    }

    Ok(merged)
}

fn month_year_buckets(window: &PublishedWindow) -> Result<Vec<(u32, i32)>, ResolverError> {
    if window.start > window.end {
        return Err(ResolverError::InvalidPublishedWindow);
    }

    let mut year = window.start.year();
    let mut month = window.start.month();
    let end_year = window.end.year();
    let end_month = window.end.month();
    let mut buckets = Vec::new();

    loop {
        buckets.push((month, year));
        if year == end_year && month == end_month {
            break;
        }
        if month == 12 {
            year += 1;
            month = 1;
        } else {
            month += 1;
        }
    }

    Ok(buckets)
}

fn ensure_sorted(stream: &[Observation]) -> Result<(), ResolverError> {
    for pair in stream.windows(2) {
        if observation_order(&pair[0], &pair[1]) == Ordering::Greater {
            return Err(ResolverError::UnsortedLeafStream);
        }
    }
    Ok(())
}

fn observation_order(left: &Observation, right: &Observation) -> Ordering {
    (
        left.published,
        left.recorded_at,
        left.id.as_str().to_owned(),
    )
        .cmp(&(
            right.published,
            right.recorded_at,
            right.id.as_str().to_owned(),
        ))
}

#[derive(Debug)]
struct HeapItem {
    leaf_index: usize,
    observation: Observation,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        observation_order(&self.observation, &other.observation) == Ordering::Equal
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        observation_order(&other.observation, &self.observation)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use crate::domain::{
        AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObservationId,
        ObserverRef, SchemaRef, SemVer, SourceSystemRef,
    };
    use crate::runtime::partition::{
        initialize_event_json, parse_partition_event, split_commit_event_json, PartitionTree,
        PARTITION_EVENT_INITIALIZE, PARTITION_EVENT_SPLIT_COMMIT,
    };

    fn leaf_id() -> String {
        format!("lake:{}", uuid::Uuid::now_v7())
    }

    fn observation(id: &str, published: &str, recorded_at: &str) -> Observation {
        Observation {
            id: ObservationId::new(id),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("entity:{id}")),
            target: None,
            payload: serde_json::json!({}),
            attachments: vec![],
            published: chrono::DateTime::parse_from_rfc3339(published)
                .unwrap()
                .to_utc(),
            recorded_at: chrono::DateTime::parse_from_rfc3339(recorded_at)
                .unwrap()
                .to_utc(),
            consent: None,
            idempotency_key: IdempotencyKey::new(format!("key:{id}")),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({ "id": id }).to_string(),
                "source_container": "channel:C01",
            }),
        }
    }

    fn split_tree() -> (PartitionTree, String, String) {
        let root = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let initialize = parse_partition_event(
            PARTITION_EVENT_INITIALIZE,
            &initialize_event_json(&root).unwrap(),
        )
        .unwrap();
        let commit = parse_partition_event(
            PARTITION_EVENT_SPLIT_COMMIT,
            &split_commit_event_json(&root, &left, &right, 15).unwrap(),
        )
        .unwrap();
        let tree = PartitionTree::from_events(&[initialize, commit]).unwrap();
        (tree, left, right)
    }

    #[test]
    fn candidate_leaves_expands_published_window_to_month_year_buckets() {
        let (tree, left, right) = split_tree();
        let filter = LeafFilter {
            published: Some(PublishedWindow {
                start: Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2026, 5, 31, 23, 59, 59).unwrap(),
            }),
            source: Some("sys:slack".to_owned()),
            container: Some("channel:C01".to_owned()),
        };

        let leaves = candidate_leaves(&tree, &filter).unwrap();

        assert_eq!(leaves, vec![left, right]);
    }

    #[test]
    fn streaming_merge_orders_by_published_recorded_at_id() {
        let first = observation("obs:1", "2026-04-01T00:00:00Z", "2026-04-01T00:01:00Z");
        let second = observation("obs:2", "2026-04-01T00:00:00Z", "2026-04-01T00:02:00Z");
        let third = observation("obs:3", "2026-04-02T00:00:00Z", "2026-04-02T00:01:00Z");

        let merged =
            streaming_k_way_merge(vec![vec![first.clone(), third.clone()], vec![second.clone()]])
                .unwrap();

        assert_eq!(
            merged
                .into_iter()
                .map(|observation| observation.id)
                .collect::<Vec<_>>(),
            vec![first.id, second.id, third.id]
        );
    }

    #[test]
    fn streaming_merge_rejects_unsorted_leaf_stream() {
        let first = observation("obs:1", "2026-04-01T00:00:00Z", "2026-04-01T00:01:00Z");
        let second = observation("obs:2", "2026-04-01T00:00:00Z", "2026-04-01T00:02:00Z");

        let err = streaming_k_way_merge(vec![vec![second, first]]).unwrap_err();

        assert!(matches!(err, ResolverError::UnsortedLeafStream));
    }
}
