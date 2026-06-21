//! M06: Propagation Scheduler — poll-based incremental propagation.

use crate::lake::LakeStore;
use crate::projection::catalog::ProjectionCatalog;
use crate::projection::runner::BuildStatus;
use lethe_core::domain::{ProjectionHealth, ProjectionRef};

use super::idempotent::CommutativeIdempotentObservationFold;
use super::watermark::WatermarkStore;
use lethe_storage_api::{ProjectionLeafWatermark, StoragePorts};

/// Result of a single propagation cycle for one projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropagationResult {
    /// No new data — skipped.
    NoOp,
    /// Incremental apply executed.
    Applied {
        new_position: usize,
        new_records: usize,
    },
    /// Build failed — watermark unchanged.
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafTail {
    pub leaf_id: String,
    pub from_append_seq_exclusive: usize,
    pub to_append_seq_inclusive: usize,
    pub new_records: usize,
}

/// Poll-based propagation scheduler (M06 §4.1 MVP).
pub struct PropagationScheduler;

impl PropagationScheduler {
    pub fn propagate_persistent(
        projection_id: &ProjectionRef,
        storage: &dyn StoragePorts,
        batch_size: usize,
        fold: &mut dyn CommutativeIdempotentObservationFold,
    ) -> Result<Vec<LeafTail>, String> {
        if batch_size == 0 {
            return Err("persistent propagation batch_size must be positive".to_owned());
        }
        let mut applied = Vec::new();
        for position in storage
            .leaf_positions()
            .map_err(|error| error.to_string())?
        {
            let watermark = storage
                .projection_leaf_watermark(projection_id, &position.leaf_id)
                .map_err(|error| error.to_string())?;
            if watermark.append_seq >= position.append_seq {
                continue;
            }
            let mut cursor = watermark.append_seq;
            let start = cursor;
            let mut count = 0usize;
            loop {
                let page = storage
                    .observations_for_leaf_after(&position.leaf_id, cursor, batch_size)
                    .map_err(|error| error.to_string())?;
                if page.is_empty() {
                    break;
                }
                for stored in &page {
                    fold.apply(&stored.observation)?;
                    cursor = stored.append_seq;
                    count += 1;
                }
                if page.len() < batch_size {
                    break;
                }
            }
            storage
                .commit_projection_leaf_watermark(&ProjectionLeafWatermark {
                    projection_id: projection_id.clone(),
                    leaf_id: position.leaf_id.clone(),
                    append_seq: cursor,
                    status: "success".to_owned(),
                })
                .map_err(|error| error.to_string())?;
            applied.push(LeafTail {
                leaf_id: position.leaf_id,
                from_append_seq_exclusive: start as usize,
                to_append_seq_inclusive: cursor as usize,
                new_records: count,
            });
        }
        Ok(applied)
    }

    /// Run a single poll cycle for one projection.
    ///
    /// Returns the incremental observations and the new position.
    /// The caller is responsible for running the actual projector.
    pub fn check_and_prepare(
        projection_id: &ProjectionRef,
        lake: &LakeStore,
        watermarks: &mut WatermarkStore,
    ) -> (usize, usize) {
        let wm = watermarks.get_or_init(projection_id);
        let current_pos = wm.last_processed_position;
        let lake_pos = lake.watermark().map(|w| w.position).unwrap_or(0);

        watermarks.update_pending(projection_id, lake_pos);
        (current_pos, lake_pos)
    }

    /// Commit a successful incremental apply.
    pub fn commit_success(
        projection_id: &ProjectionRef,
        new_position: usize,
        watermarks: &mut WatermarkStore,
        catalog: &mut ProjectionCatalog,
    ) {
        watermarks.update(projection_id, new_position, BuildStatus::Success);
        catalog.set_health(projection_id, ProjectionHealth::Healthy);
    }

    /// Record a failed build — watermark unchanged (M06 invariant 4).
    pub fn commit_failure(
        projection_id: &ProjectionRef,
        watermarks: &mut WatermarkStore,
        catalog: &mut ProjectionCatalog,
    ) {
        watermarks.record_failure(projection_id);
        catalog.set_health(projection_id, ProjectionHealth::Broken);
    }

    pub fn changed_leaf_tails(
        projection_id: &ProjectionRef,
        leaf_positions: &[(String, usize)],
        watermarks: &mut WatermarkStore,
    ) -> Vec<LeafTail> {
        let mut changed = Vec::new();
        for (leaf_id, leaf_append_seq) in leaf_positions {
            let current = watermarks
                .get_or_init_leaf(projection_id, leaf_id)
                .last_processed_append_seq;
            watermarks.update_leaf_pending(projection_id, leaf_id, *leaf_append_seq);
            if current < *leaf_append_seq {
                changed.push(LeafTail {
                    leaf_id: leaf_id.clone(),
                    from_append_seq_exclusive: current,
                    to_append_seq_inclusive: *leaf_append_seq,
                    new_records: leaf_append_seq - current,
                });
            }
        }
        changed
    }

    pub fn commit_leaf_success(
        projection_id: &ProjectionRef,
        leaf_id: &str,
        append_seq: usize,
        watermarks: &mut WatermarkStore,
        catalog: &mut ProjectionCatalog,
    ) {
        watermarks.update_leaf(projection_id, leaf_id, append_seq, BuildStatus::Success);
        catalog.set_health(projection_id, ProjectionHealth::Healthy);
    }

    pub fn commit_leaf_failure(
        projection_id: &ProjectionRef,
        leaf_id: &str,
        watermarks: &mut WatermarkStore,
        catalog: &mut ProjectionCatalog,
    ) {
        watermarks.record_leaf_failure(projection_id, leaf_id);
        catalog.set_health(projection_id, ProjectionHealth::Broken);
    }

    /// Run propagation in topological order for all projections.
    /// Returns ids of projections that had new data applied.
    pub fn propagate_all(
        lake: &LakeStore,
        watermarks: &mut WatermarkStore,
        catalog: &mut ProjectionCatalog,
    ) -> Result<Vec<(ProjectionRef, PropagationResult)>, crate::projection::catalog::CatalogError>
    {
        let order = match catalog.topological_order() {
            Ok(o) => o,
            Err(err) => {
                for proj_id in catalog.list_ids() {
                    catalog.set_health(&proj_id, ProjectionHealth::Broken);
                }
                return Err(err);
            }
        };

        let mut results = Vec::new();

        for proj_id in &order {
            let (current, lake_pos) = Self::check_and_prepare(proj_id, lake, watermarks);
            if current >= lake_pos {
                results.push((proj_id.clone(), PropagationResult::NoOp));
                continue;
            }

            let new_records = lake_pos - current;

            // In a real system the projector would be called here.
            // For the MVP framework, we just advance the watermark.
            Self::commit_success(proj_id, lake_pos, watermarks, catalog);
            results.push((
                proj_id.clone(),
                PropagationResult::Applied {
                    new_position: lake_pos,
                    new_records,
                },
            ));
        }

        Ok(results)
    }

    /// Mark downstream projections as degraded when an upstream fails.
    pub fn propagate_upstream_failure(failed_id: &ProjectionRef, catalog: &mut ProjectionCatalog) {
        let dependents = catalog.dependents(failed_id);
        for dep_id in &dependents {
            catalog.set_health(dep_id, ProjectionHealth::Degraded);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lake::LakeStore;
    use crate::projection::catalog::ProjectionCatalog;
    use crate::projection::spec::*;
    use lethe_core::domain::*;

    fn sample_obs(key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: None,
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("msg:1"),
            target: None,
            payload: serde_json::json!({}),
            attachments: vec![],
            published: chrono::Utc::now(),
            recorded_at: chrono::Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({}),
        }
    }

    fn lake_spec(id: &str) -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new(id),
            name: id.into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::PureProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            }],
            read_modes: vec![ReadModePolicy {
                mode: ReadMode::OperationalLatest,
                source_policy: "lake-latest".into(),
            }],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "p".into(),
            },
            outputs: vec![OutputSpec {
                format: "sql".into(),
                tables: vec!["t".into()],
            }],
            reconciliation: None,
            deterministic_in: vec![],
            gap_action: None,
            tags: vec![],
            description: None,
            created_by: "test".into(),
        }
    }

    #[test]
    fn no_new_data_is_noop() {
        let lake = LakeStore::new();
        let mut watermarks = WatermarkStore::new();
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();

        let results =
            PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, PropagationResult::NoOp);
    }

    #[test]
    fn new_observations_trigger_apply() {
        let mut lake = LakeStore::new();
        lake.append(sample_obs("k1")).unwrap();
        lake.append(sample_obs("k2")).unwrap();

        let mut watermarks = WatermarkStore::new();
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();

        let results =
            PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].1,
            PropagationResult::Applied {
                new_position: 2,
                new_records: 2
            }
        ));

        // Second run should be no-op.
        let results2 =
            PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap();
        assert_eq!(results2[0].1, PropagationResult::NoOp);
    }

    #[test]
    fn incremental_after_initial() {
        let mut lake = LakeStore::new();
        lake.append(sample_obs("k1")).unwrap();

        let mut watermarks = WatermarkStore::new();
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();

        PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap();

        // Add more data.
        lake.append(sample_obs("k2")).unwrap();
        lake.append(sample_obs("k3")).unwrap();

        let results =
            PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap();
        assert!(matches!(
            results[0].1,
            PropagationResult::Applied {
                new_position: 3,
                new_records: 2
            }
        ));
    }

    #[test]
    fn cycle_error_is_returned_and_health_is_broken() {
        let lake = LakeStore::new();
        let mut watermarks = WatermarkStore::new();
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();

        let mut dep = lake_spec("proj:b");
        dep.sources.insert(
            0,
            SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new("proj:a"),
                    version: ">=1.0.0".into(),
                },
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
        );
        dep.reconciliation = Some(ReconciliationPolicy::LakeFirst);
        catalog.register(dep).unwrap();

        let entry = catalog.get_mut(&ProjectionRef::new("proj:a")).unwrap();
        entry.spec.sources.insert(
            0,
            SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new("proj:b"),
                    version: ">=1.0.0".into(),
                },
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
        );
        entry.spec.reconciliation = Some(ReconciliationPolicy::LakeFirst);

        let err =
            PropagationScheduler::propagate_all(&lake, &mut watermarks, &mut catalog).unwrap_err();
        assert_eq!(
            err,
            crate::projection::catalog::CatalogError::CyclicDependency
        );
        assert_eq!(
            catalog.get(&ProjectionRef::new("proj:a")).unwrap().health,
            ProjectionHealth::Broken
        );
        assert_eq!(
            catalog.get(&ProjectionRef::new("proj:b")).unwrap().health,
            ProjectionHealth::Broken
        );
    }

    #[test]
    fn upstream_failure_degrades_downstream() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();

        let mut dep = lake_spec("proj:b");
        dep.sources.insert(
            0,
            SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new("proj:a"),
                    version: ">=1.0.0".into(),
                },
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
        );
        dep.reconciliation = Some(ReconciliationPolicy::LakeFirst);
        catalog.register(dep).unwrap();

        PropagationScheduler::propagate_upstream_failure(
            &ProjectionRef::new("proj:a"),
            &mut catalog,
        );

        let entry = catalog.get(&ProjectionRef::new("proj:b")).unwrap();
        assert_eq!(entry.health, ProjectionHealth::Degraded);
    }

    #[test]
    fn changed_leaf_tails_reads_per_leaf_append_seq() {
        let mut watermarks = WatermarkStore::new();
        let projection_id = ProjectionRef::new("proj:a");
        watermarks.update_leaf(&projection_id, "lake:one", 3, BuildStatus::Success);

        let tails = PropagationScheduler::changed_leaf_tails(
            &projection_id,
            &[("lake:one".to_owned(), 5), ("lake:two".to_owned(), 2)],
            &mut watermarks,
        );

        assert_eq!(
            tails,
            vec![
                LeafTail {
                    leaf_id: "lake:one".to_owned(),
                    from_append_seq_exclusive: 3,
                    to_append_seq_inclusive: 5,
                    new_records: 2,
                },
                LeafTail {
                    leaf_id: "lake:two".to_owned(),
                    from_append_seq_exclusive: 0,
                    to_append_seq_inclusive: 2,
                    new_records: 2,
                },
            ]
        );
    }

    #[test]
    fn commit_leaf_success_advances_only_that_leaf() {
        let mut watermarks = WatermarkStore::new();
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        let projection_id = ProjectionRef::new("proj:a");
        watermarks.update_leaf(&projection_id, "lake:one", 3, BuildStatus::Success);

        PropagationScheduler::commit_leaf_success(
            &projection_id,
            "lake:two",
            9,
            &mut watermarks,
            &mut catalog,
        );

        assert_eq!(
            watermarks
                .get_leaf(&projection_id, "lake:one")
                .unwrap()
                .last_processed_append_seq,
            3
        );
        assert_eq!(
            watermarks
                .get_leaf(&projection_id, "lake:two")
                .unwrap()
                .last_processed_append_seq,
            9
        );
    }
}
