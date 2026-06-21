//! M06: Watermark State — tracks incremental propagation position.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::projection::BuildStatus;
use lethe_core::domain::ProjectionRef;

/// Watermark state for a single projection (M06 §3.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    pub projection_id: ProjectionRef,
    pub last_processed_position: usize,
    pub last_build_at: DateTime<Utc>,
    pub last_build_status: BuildStatus,
    pub pending_count: Option<usize>,
}

impl WatermarkState {
    pub fn new(projection_id: ProjectionRef) -> Self {
        Self {
            projection_id,
            last_processed_position: 0,
            last_build_at: Utc::now(),
            last_build_status: BuildStatus::Success,
            pending_count: None,
        }
    }
}

/// Per-(projection, leaf) cursor using leaf-local append_seq.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafWatermarkState {
    pub projection_id: ProjectionRef,
    pub leaf_id: String,
    pub last_processed_append_seq: usize,
    pub last_build_at: DateTime<Utc>,
    pub last_build_status: BuildStatus,
    pub pending_count: Option<usize>,
}

impl LeafWatermarkState {
    pub fn new(projection_id: ProjectionRef, leaf_id: String) -> Self {
        Self {
            projection_id,
            leaf_id,
            last_processed_append_seq: 0,
            last_build_at: Utc::now(),
            last_build_status: BuildStatus::Success,
            pending_count: None,
        }
    }
}

/// In-memory watermark store (M06 §3.3).
#[derive(Debug, Default)]
pub struct WatermarkStore {
    states: HashMap<String, WatermarkState>,
    leaf_states: HashMap<(String, String), LeafWatermarkState>,
}

impl WatermarkStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, projection_id: &ProjectionRef) -> Option<&WatermarkState> {
        self.states.get(projection_id.as_str())
    }

    pub fn get_or_init(&mut self, projection_id: &ProjectionRef) -> &WatermarkState {
        self.states
            .entry(projection_id.as_str().to_string())
            .or_insert_with(|| WatermarkState::new(projection_id.clone()))
    }

    pub fn update(&mut self, projection_id: &ProjectionRef, position: usize, status: BuildStatus) {
        let state = self
            .states
            .entry(projection_id.as_str().to_string())
            .or_insert_with(|| WatermarkState::new(projection_id.clone()));

        // Invariant 1: watermark is monotonically increasing.
        assert!(
            position >= state.last_processed_position,
            "Watermark must not decrease: {} -> {}",
            state.last_processed_position,
            position,
        );

        state.last_processed_position = position;
        state.last_build_at = Utc::now();
        state.last_build_status = status;
        state.pending_count = None;
    }

    /// Record a failed build — watermark stays unchanged (M06 invariant 4).
    pub fn record_failure(&mut self, projection_id: &ProjectionRef) {
        if let Some(state) = self.states.get_mut(projection_id.as_str()) {
            state.last_build_status = BuildStatus::Failed;
            state.last_build_at = Utc::now();
        }
    }

    /// Update pending count based on lake position.
    pub fn update_pending(&mut self, projection_id: &ProjectionRef, lake_position: usize) {
        if let Some(state) = self.states.get_mut(projection_id.as_str()) {
            let pending = lake_position.saturating_sub(state.last_processed_position);
            state.pending_count = Some(pending);
        }
    }

    pub fn all(&self) -> impl Iterator<Item = &WatermarkState> {
        self.states.values()
    }

    pub fn get_leaf(
        &self,
        projection_id: &ProjectionRef,
        leaf_id: &str,
    ) -> Option<&LeafWatermarkState> {
        self.leaf_states
            .get(&(projection_id.as_str().to_owned(), leaf_id.to_owned()))
    }

    pub fn get_or_init_leaf(
        &mut self,
        projection_id: &ProjectionRef,
        leaf_id: &str,
    ) -> &LeafWatermarkState {
        self.leaf_states
            .entry((projection_id.as_str().to_owned(), leaf_id.to_owned()))
            .or_insert_with(|| LeafWatermarkState::new(projection_id.clone(), leaf_id.to_owned()))
    }

    pub fn update_leaf(
        &mut self,
        projection_id: &ProjectionRef,
        leaf_id: &str,
        append_seq: usize,
        status: BuildStatus,
    ) {
        let state = self
            .leaf_states
            .entry((projection_id.as_str().to_owned(), leaf_id.to_owned()))
            .or_insert_with(|| LeafWatermarkState::new(projection_id.clone(), leaf_id.to_owned()));

        assert!(
            append_seq >= state.last_processed_append_seq,
            "Leaf watermark must not decrease: {} -> {}",
            state.last_processed_append_seq,
            append_seq,
        );

        state.last_processed_append_seq = append_seq;
        state.last_build_at = Utc::now();
        state.last_build_status = status;
        state.pending_count = None;
    }

    pub fn record_leaf_failure(&mut self, projection_id: &ProjectionRef, leaf_id: &str) {
        if let Some(state) = self
            .leaf_states
            .get_mut(&(projection_id.as_str().to_owned(), leaf_id.to_owned()))
        {
            state.last_build_status = BuildStatus::Failed;
            state.last_build_at = Utc::now();
        }
    }

    pub fn update_leaf_pending(
        &mut self,
        projection_id: &ProjectionRef,
        leaf_id: &str,
        leaf_append_seq: usize,
    ) {
        if let Some(state) = self
            .leaf_states
            .get_mut(&(projection_id.as_str().to_owned(), leaf_id.to_owned()))
        {
            state.pending_count =
                Some(leaf_append_seq.saturating_sub(state.last_processed_append_seq));
        }
    }

    pub fn leaf_states_for_projection(
        &self,
        projection_id: &ProjectionRef,
    ) -> Vec<&LeafWatermarkState> {
        let mut states = self
            .leaf_states
            .iter()
            .filter_map(|((projection, _), state)| {
                if projection == projection_id.as_str() {
                    Some(state)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        states.sort_by(|left, right| left.leaf_id.cmp(&right.leaf_id));
        states
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_init_at_zero() {
        let state = WatermarkState::new(ProjectionRef::new("proj:test"));
        assert_eq!(state.last_processed_position, 0);
        assert_eq!(state.last_build_status, BuildStatus::Success);
    }

    #[test]
    fn watermark_update_advances() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.get_or_init(&id);
        store.update(&id, 5, BuildStatus::Success);
        assert_eq!(store.get(&id).unwrap().last_processed_position, 5);
    }

    #[test]
    #[should_panic(expected = "must not decrease")]
    fn watermark_cannot_decrease() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.update(&id, 10, BuildStatus::Success);
        store.update(&id, 5, BuildStatus::Success);
    }

    #[test]
    fn failure_does_not_change_watermark() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.update(&id, 10, BuildStatus::Success);
        store.record_failure(&id);
        let state = store.get(&id).unwrap();
        assert_eq!(state.last_processed_position, 10);
        assert_eq!(state.last_build_status, BuildStatus::Failed);
    }

    #[test]
    fn pending_count_calculated() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.update(&id, 5, BuildStatus::Success);
        store.update_pending(&id, 12);
        assert_eq!(store.get(&id).unwrap().pending_count, Some(7));
    }

    #[test]
    fn leaf_watermark_tracks_append_seq_per_projection_leaf() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");

        store.get_or_init_leaf(&id, "lake:one");
        store.update_leaf(&id, "lake:one", 7, BuildStatus::Success);
        store.update_leaf_pending(&id, "lake:one", 10);

        let state = store.get_leaf(&id, "lake:one").unwrap();
        assert_eq!(state.last_processed_append_seq, 7);
        assert_eq!(state.pending_count, Some(3));
    }

    #[test]
    #[should_panic(expected = "Leaf watermark must not decrease")]
    fn leaf_watermark_cannot_decrease() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.update_leaf(&id, "lake:one", 10, BuildStatus::Success);
        store.update_leaf(&id, "lake:one", 9, BuildStatus::Success);
    }

    #[test]
    fn split_baseline_replay_starts_new_child_leaf_at_zero() {
        let mut store = WatermarkStore::new();
        let id = ProjectionRef::new("proj:test");
        store.update_leaf(&id, "lake:parent", 100, BuildStatus::Success);

        store.get_or_init_leaf(&id, "lake:child-left");
        store.get_or_init_leaf(&id, "lake:child-right");

        assert_eq!(
            store
                .get_leaf(&id, "lake:child-left")
                .unwrap()
                .last_processed_append_seq,
            0
        );
        assert_eq!(
            store
                .get_leaf(&id, "lake:child-right")
                .unwrap()
                .last_processed_append_seq,
            0
        );
    }
}
