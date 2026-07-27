use std::collections::BTreeSet;

use lethe_core::domain::{BlobRef, ProjectionRef, SupplementalId, SupplementalRecord};
use lethe_storage_api::{
    AuditEventRecord, ProjectionGenerationCleanup, ProjectionItem, ProjectionItemCommit,
    ProjectionMaterializer, StorageError, StorageResult, SupplementalProjectionCommitter,
    SupplementalStore,
};
use postgres::Transaction;

use super::PostgresPersistence;
use super::general_s3::lock_blob_admission;

const KEYSPEC_VERSION: &str = "default";

impl SupplementalStore for PostgresPersistence {
    fn put_supplemental(&self, record: &SupplementalRecord) -> StorageResult<()> {
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&supplemental_blob_refs(record))?;
        let json = serialize(record)?;
        transaction
            .execute(
                "INSERT INTO supplementals (
                    supplemental_id, created_at, supplemental_json
                 ) VALUES ($1, $2, $3::text::jsonb)
                 ON CONFLICT (supplemental_id) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    supplemental_json = EXCLUDED.supplemental_json",
                &[&record.id.as_str(), &record.created_at.to_rfc3339(), &json],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn load_supplementals(&self) -> StorageResult<Vec<SupplementalRecord>> {
        let mut reader = self.reader()?;
        reader
            .query(
                "SELECT supplemental_json::text FROM supplementals
                 ORDER BY created_at, supplemental_id",
                &[],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| deserialize(row.get(0)))
            .collect()
    }

    fn supplemental_by_id(&self, id: &SupplementalId) -> StorageResult<Option<SupplementalRecord>> {
        let mut reader = self.reader()?;
        reader
            .query_opt(
                "SELECT supplemental_json::text FROM supplementals
                 WHERE supplemental_id = $1",
                &[&id.as_str()],
            )
            .map_err(backend)?
            .map(|row| deserialize(row.get(0)))
            .transpose()
    }

    fn supplemental_page(
        &self,
        after_created_at: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<SupplementalRecord>> {
        positive("supplemental page limit", limit)?;
        if after_created_at.is_some_and(|value| value.trim().is_empty()) {
            return Err(StorageError::Invariant(
                "supplemental page cursor must not be blank".to_owned(),
            ));
        }
        let limit = to_i64("supplemental page limit", limit)?;
        let mut reader = self.reader()?;
        reader
            .query(
                "SELECT supplemental_json::text FROM supplementals
                 WHERE ($1::text IS NULL OR created_at > $1)
                 ORDER BY created_at, supplemental_id
                 LIMIT $2",
                &[&after_created_at, &limit],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| deserialize(row.get(0)))
            .collect()
    }
}

impl ProjectionMaterializer for PostgresPersistence {
    fn materialize_projection(
        &self,
        projection: &ProjectionRef,
        records: &serde_json::Value,
    ) -> StorageResult<()> {
        validate_projection(projection)?;
        validate_manifest(records)?;
        reject_manifest_blob_refs(records)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        advisory_projection_lock(&mut transaction, projection)?;
        let generation = create_empty_generation(&mut transaction, projection, records)?;
        activate_generation(&mut transaction, projection, generation)?;
        transaction.commit().map_err(backend)
    }

    fn projection_records(
        &self,
        projection: &ProjectionRef,
    ) -> StorageResult<Option<serde_json::Value>> {
        validate_projection(projection)?;
        let mut reader = self.reader()?;
        reader
            .query_opt(
                "SELECT materializations.manifest_json::text
                 FROM projection_materialization_heads heads
                 JOIN projection_materializations materializations
                   ON materializations.projection_id = heads.projection_id
                  AND materializations.keyspec_version = heads.keyspec_version
                  AND materializations.generation = heads.generation
                 WHERE heads.projection_id = $1
                   AND heads.keyspec_version = $2",
                &[&projection.as_str(), &KEYSPEC_VERSION],
            )
            .map_err(backend)?
            .map(|row| deserialize(row.get(0)))
            .transpose()
    }

    fn commit_projection_items(
        &self,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> StorageResult<()> {
        validate_projection(projection)?;
        validate_manifest(manifest)?;
        reject_manifest_blob_refs(manifest)?;
        commit.validate()?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&commit_blob_refs(commit))?;
        advisory_projection_lock(&mut transaction, projection)?;
        let generation =
            create_generation_for_commit(&mut transaction, projection, manifest, commit)?;
        activate_generation(&mut transaction, projection, generation)?;
        transaction.commit().map_err(backend)
    }

    fn publish_projection_items_from_staging(
        &self,
        target: &ProjectionRef,
        staging: &ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> StorageResult<()> {
        validate_projection(target)?;
        validate_projection(staging)?;
        validate_manifest(manifest)?;
        reject_manifest_blob_refs(manifest)?;
        if target == staging {
            return Err(StorageError::Invariant(
                "projection staging and target must differ".to_owned(),
            ));
        }
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&active_projection_blob_refs(self, staging)?)?;
        let mut lock_ids = [target, staging];
        lock_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        for projection in lock_ids {
            advisory_projection_lock(&mut transaction, projection)?;
        }
        let staging_generation =
            active_generation(&mut transaction, staging)?.ok_or_else(|| {
                StorageError::Invariant(format!(
                    "staging projection {} does not exist",
                    staging.as_str()
                ))
            })?;
        let actual_count = generation_item_count(&mut transaction, staging, staging_generation)?;
        if actual_count != expected_item_count {
            return Err(StorageError::Invariant(format!(
                "staging projection {} contains {actual_count} items, expected {expected_item_count}",
                staging.as_str()
            )));
        }
        let target_generation = next_generation(&mut transaction, target)?;
        insert_generation(&mut transaction, target, target_generation, manifest)?;
        copy_generation_items(
            &mut transaction,
            staging,
            staging_generation,
            target,
            target_generation,
        )?;
        activate_generation(&mut transaction, target, target_generation)?;
        retire_and_remove_head(&mut transaction, staging, staging_generation)?;
        transaction.commit().map_err(backend)
    }

    fn cleanup_retired_projection_generation(
        &self,
        limit: usize,
    ) -> StorageResult<ProjectionGenerationCleanup> {
        positive("projection generation cleanup limit", limit)?;
        let limit = to_i64("projection generation cleanup limit", limit)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let retired = transaction
            .query_opt(
                "SELECT projection_id, keyspec_version, generation
                 FROM retired_projection_materializations
                 ORDER BY retired_at, projection_id, keyspec_version, generation
                 LIMIT 1 FOR UPDATE",
                &[],
            )
            .map_err(backend)?;
        let Some(retired) = retired else {
            transaction.commit().map_err(backend)?;
            return Ok(ProjectionGenerationCleanup {
                storage_projection_id: None,
                deleted_items: 0,
                deleted_visible_blob_refs: 0,
                completed_generation: false,
                has_more: false,
            });
        };
        let projection_id: String = retired.get(0);
        let keyspec_version: String = retired.get(1);
        let generation: i64 = retired.get(2);
        let deleted_visible_blob_refs = transaction
            .execute(
                "DELETE FROM projection_visible_blob_refs
                 WHERE ctid IN (
                    SELECT ctid FROM projection_visible_blob_refs
                    WHERE projection_id = $1 AND keyspec_version = $2
                      AND generation = $3
                    LIMIT $4
                 )",
                &[&projection_id, &keyspec_version, &generation, &limit],
            )
            .map_err(backend)?;
        let deleted_items = transaction
            .execute(
                "DELETE FROM projection_materialization_items
                 WHERE ctid IN (
                    SELECT ctid FROM projection_materialization_items
                    WHERE projection_id = $1 AND keyspec_version = $2
                      AND generation = $3
                    LIMIT $4
                 )",
                &[&projection_id, &keyspec_version, &generation, &limit],
            )
            .map_err(backend)?;
        let generation_has_rows: bool = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM projection_materialization_items
                    WHERE projection_id = $1 AND keyspec_version = $2
                      AND generation = $3
                 ) OR EXISTS (
                    SELECT 1 FROM projection_visible_blob_refs
                    WHERE projection_id = $1 AND keyspec_version = $2
                      AND generation = $3
                 )",
                &[&projection_id, &keyspec_version, &generation],
            )
            .map_err(backend)?
            .get(0);
        let completed_generation = !generation_has_rows;
        if completed_generation {
            transaction
                .execute(
                    "DELETE FROM projection_materializations
                     WHERE projection_id = $1 AND keyspec_version = $2
                       AND generation = $3",
                    &[&projection_id, &keyspec_version, &generation],
                )
                .map_err(backend)?;
            transaction
                .execute(
                    "DELETE FROM retired_projection_materializations
                     WHERE projection_id = $1 AND keyspec_version = $2
                       AND generation = $3",
                    &[&projection_id, &keyspec_version, &generation],
                )
                .map_err(backend)?;
        }
        let has_more: bool = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM retired_projection_materializations
                 )",
                &[],
            )
            .map_err(backend)?
            .get(0);
        transaction.commit().map_err(backend)?;
        Ok(ProjectionGenerationCleanup {
            storage_projection_id: Some(storage_projection_id(
                &projection_id,
                &keyspec_version,
                generation,
            )),
            deleted_items,
            deleted_visible_blob_refs,
            completed_generation,
            has_more,
        })
    }

    fn projection_item_by_key(
        &self,
        projection: &ProjectionRef,
        item_key: &str,
    ) -> StorageResult<Option<ProjectionItem>> {
        validate_projection(projection)?;
        non_blank("projection item_key", item_key)?;
        let mut reader = self.reader()?;
        reader
            .query_opt(
                "SELECT items.item_key, items.owner_key, items.sort_key,
                        items.item_json::text
                 FROM projection_materialization_heads heads
                 JOIN projection_materialization_items items
                   ON items.projection_id = heads.projection_id
                  AND items.keyspec_version = heads.keyspec_version
                  AND items.generation = heads.generation
                 WHERE heads.projection_id = $1
                   AND heads.keyspec_version = $2
                   AND items.item_key = $3",
                &[&projection.as_str(), &KEYSPEC_VERSION, &item_key],
            )
            .map_err(backend)?
            .map(projection_item_row)
            .transpose()
    }

    fn projection_items_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<Vec<ProjectionItem>> {
        validate_projection(projection)?;
        non_blank("projection owner_key", owner_key)?;
        let mut reader = self.reader()?;
        projection_item_rows(
            reader
                .query(
                    "SELECT items.item_key, items.owner_key, items.sort_key,
                            items.item_json::text
                     FROM projection_materialization_heads heads
                     JOIN projection_materialization_items items
                       ON items.projection_id = heads.projection_id
                      AND items.keyspec_version = heads.keyspec_version
                      AND items.generation = heads.generation
                     WHERE heads.projection_id = $1
                       AND heads.keyspec_version = $2
                       AND items.owner_key = $3
                     ORDER BY items.sort_key, items.item_key",
                    &[&projection.as_str(), &KEYSPEC_VERSION, &owner_key],
                )
                .map_err(backend)?,
        )
    }

    fn projection_items_page(
        &self,
        projection: &ProjectionRef,
        owner_keys: &[String],
        item_key_prefix: Option<&str>,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<ProjectionItem>> {
        validate_projection(projection)?;
        positive("projection item page limit", limit)?;
        if owner_keys.is_empty() {
            return Err(StorageError::Invariant(
                "projection item page requires at least one owner".to_owned(),
            ));
        }
        for owner in owner_keys {
            non_blank("projection owner_key", owner)?;
        }
        if item_key_prefix.is_some_and(|value| value.trim().is_empty()) {
            return Err(StorageError::Invariant(
                "projection item_key_prefix must not be blank".to_owned(),
            ));
        }
        let (after_sort, after_item) = parse_sort_cursor(after_sort_key)?;
        let limit = to_i64("projection item page limit", limit)?;
        let mut reader = self.reader()?;
        projection_item_rows(
            reader
                .query(
                    "SELECT items.item_key, items.owner_key, items.sort_key,
                            items.item_json::text
                     FROM projection_materialization_heads heads
                     JOIN projection_materialization_items items
                       ON items.projection_id = heads.projection_id
                      AND items.keyspec_version = heads.keyspec_version
                      AND items.generation = heads.generation
                     WHERE heads.projection_id = $1
                       AND heads.keyspec_version = $2
                       AND items.owner_key = ANY($3)
                       AND ($4::text IS NULL OR items.item_key LIKE ($4 || '%'))
                       AND (
                            $5::text IS NULL
                            OR items.sort_key > $5
                            OR (
                                items.sort_key = $5
                                AND ($6::text IS NULL OR items.item_key > $6)
                            )
                       )
                     ORDER BY items.sort_key, items.item_key
                     LIMIT $7",
                    &[
                        &projection.as_str(),
                        &KEYSPEC_VERSION,
                        &owner_keys,
                        &item_key_prefix,
                        &after_sort,
                        &after_item,
                        &limit,
                    ],
                )
                .map_err(backend)?,
        )
    }

    fn projection_blob_ref_visible(
        &self,
        projection: &ProjectionRef,
        blob_ref: &BlobRef,
    ) -> StorageResult<bool> {
        validate_projection(projection)?;
        let mut reader = self.reader()?;
        Ok(reader
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM projection_materialization_heads heads
                    JOIN projection_visible_blob_refs visible
                      ON visible.projection_id = heads.projection_id
                     AND visible.keyspec_version = heads.keyspec_version
                     AND visible.generation = heads.generation
                    WHERE heads.projection_id = $1
                      AND heads.keyspec_version = $2
                      AND visible.blob_ref = $3
                 )",
                &[&projection.as_str(), &KEYSPEC_VERSION, &blob_ref.as_str()],
            )
            .map_err(backend)?
            .get(0))
    }

    fn projection_item_count_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<u64> {
        validate_projection(projection)?;
        non_blank("projection owner_key", owner_key)?;
        projection_count(self, projection, Some(owner_key))
    }

    fn projection_item_count(&self, projection: &ProjectionRef) -> StorageResult<u64> {
        validate_projection(projection)?;
        projection_count(self, projection, None)
    }
}

impl SupplementalProjectionCommitter for PostgresPersistence {
    fn commit_supplemental_and_projection(
        &self,
        record: &SupplementalRecord,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
    ) -> StorageResult<()> {
        commit_supplemental_projection(self, record, projection, manifest, item_delta, None)
    }

    fn commit_supplemental_and_projection_with_audit(
        &self,
        record: &SupplementalRecord,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
        audit_event: &AuditEventRecord,
    ) -> StorageResult<()> {
        commit_supplemental_projection(
            self,
            record,
            projection,
            manifest,
            item_delta,
            Some(audit_event),
        )
    }
}

fn commit_supplemental_projection(
    store: &PostgresPersistence,
    record: &SupplementalRecord,
    projection: &ProjectionRef,
    manifest: &serde_json::Value,
    item_delta: &ProjectionItemCommit,
    audit_event: Option<&AuditEventRecord>,
) -> StorageResult<()> {
    validate_projection(projection)?;
    validate_manifest(manifest)?;
    reject_manifest_blob_refs(manifest)?;
    let ProjectionItemCommit::Delta { .. } = item_delta else {
        return Err(StorageError::Invariant(
            "supplemental projection commit requires a delta".to_owned(),
        ));
    };
    item_delta.validate()?;
    let mut blob_refs = supplemental_blob_refs(record);
    blob_refs.extend(commit_blob_refs(item_delta));
    if let Some(audit) = audit_event {
        validate_audit(audit)?;
    }
    let mut writer = store.writer()?;
    let mut transaction = writer.transaction().map_err(backend)?;
    lock_blob_admission(&mut transaction)?;
    store.verify_blob_references_admitted(&blob_refs)?;
    advisory_projection_lock(&mut transaction, projection)?;
    insert_new_supplemental(&mut transaction, record)?;
    let generation =
        create_generation_for_commit(&mut transaction, projection, manifest, item_delta)?;
    if let Some(audit) = audit_event {
        insert_audit(&mut transaction, audit)?;
    }
    activate_generation(&mut transaction, projection, generation)?;
    transaction.commit().map_err(backend)
}

fn create_generation_for_commit(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    manifest: &serde_json::Value,
    commit: &ProjectionItemCommit,
) -> StorageResult<i64> {
    let generation = next_generation(transaction, projection)?;
    insert_generation(transaction, projection, generation, manifest)?;
    match commit {
        ProjectionItemCommit::Replace { items } => {
            for item in items {
                insert_item(transaction, projection, generation, item)?;
            }
        }
        ProjectionItemCommit::Delta {
            inserts,
            updates,
            deletes,
        } => {
            if let Some(previous) = active_generation(transaction, projection)? {
                copy_generation_items(transaction, projection, previous, projection, generation)?;
            }
            apply_delta(
                transaction,
                projection,
                generation,
                inserts,
                updates,
                deletes,
            )?;
        }
    }
    Ok(generation)
}

fn create_empty_generation(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    manifest: &serde_json::Value,
) -> StorageResult<i64> {
    let generation = next_generation(transaction, projection)?;
    insert_generation(transaction, projection, generation, manifest)?;
    Ok(generation)
}

fn insert_generation(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
    manifest: &serde_json::Value,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO projection_materializations (
                projection_id, keyspec_version, generation, manifest_json
             ) VALUES ($1, $2, $3, $4::text::jsonb)",
            &[
                &projection.as_str(),
                &KEYSPEC_VERSION,
                &generation,
                &serialize(manifest)?,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn next_generation(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
) -> StorageResult<i64> {
    let current: i64 = transaction
        .query_one(
            "SELECT COALESCE(MAX(generation), 0)
             FROM projection_materializations
             WHERE projection_id = $1 AND keyspec_version = $2",
            &[&projection.as_str(), &KEYSPEC_VERSION],
        )
        .map_err(backend)?
        .get(0);
    current.checked_add(1).ok_or_else(|| {
        StorageError::Invariant("projection generation overflowed BIGINT".to_owned())
    })
}

fn active_generation(
    client: &mut impl postgres::GenericClient,
    projection: &ProjectionRef,
) -> StorageResult<Option<i64>> {
    Ok(client
        .query_opt(
            "SELECT generation FROM projection_materialization_heads
             WHERE projection_id = $1 AND keyspec_version = $2",
            &[&projection.as_str(), &KEYSPEC_VERSION],
        )
        .map_err(backend)?
        .map(|row| row.get(0)))
}

fn activate_generation(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
) -> StorageResult<()> {
    let previous = active_generation(transaction, projection)?;
    transaction
        .execute(
            "INSERT INTO projection_materialization_heads (
                projection_id, keyspec_version, generation
             ) VALUES ($1, $2, $3)
             ON CONFLICT (projection_id, keyspec_version) DO UPDATE SET
                generation = EXCLUDED.generation",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?;
    if let Some(previous) = previous.filter(|previous| *previous != generation) {
        retire_generation(transaction, projection, previous)?;
    }
    Ok(())
}

fn retire_generation(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO retired_projection_materializations (
                projection_id, keyspec_version, generation
             ) VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?;
    Ok(())
}

fn retire_and_remove_head(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
) -> StorageResult<()> {
    let deleted = transaction
        .execute(
            "DELETE FROM projection_materialization_heads
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?;
    if deleted != 1 {
        return Err(StorageError::Invariant(format!(
            "staging head {} disappeared during publish",
            projection.as_str()
        )));
    }
    retire_generation(transaction, projection, generation)
}

fn copy_generation_items(
    transaction: &mut Transaction<'_>,
    source: &ProjectionRef,
    source_generation: i64,
    target: &ProjectionRef,
    target_generation: i64,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO projection_materialization_items (
                projection_id, keyspec_version, generation,
                item_key, owner_key, sort_key, item_json
             )
             SELECT $4, $2, $5, item_key, owner_key, sort_key, item_json
             FROM projection_materialization_items
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[
                &source.as_str(),
                &KEYSPEC_VERSION,
                &source_generation,
                &target.as_str(),
                &target_generation,
            ],
        )
        .map_err(backend)?;
    transaction
        .execute(
            "INSERT INTO projection_visible_blob_refs (
                projection_id, keyspec_version, generation, blob_ref
             )
             SELECT $4, $2, $5, blob_ref
             FROM projection_visible_blob_refs
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[
                &source.as_str(),
                &KEYSPEC_VERSION,
                &source_generation,
                &target.as_str(),
                &target_generation,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn apply_delta(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
    inserts: &[ProjectionItem],
    updates: &[ProjectionItem],
    deletes: &[String],
) -> StorageResult<()> {
    for item_key in deletes {
        let deleted = transaction
            .execute(
                "DELETE FROM projection_materialization_items
                 WHERE projection_id = $1 AND keyspec_version = $2
                   AND generation = $3 AND item_key = $4",
                &[
                    &projection.as_str(),
                    &KEYSPEC_VERSION,
                    &generation,
                    &item_key,
                ],
            )
            .map_err(backend)?;
        if deleted != 1 {
            return Err(StorageError::Invariant(format!(
                "projection delta delete requires existing item_key {item_key}"
            )));
        }
    }
    for item in updates {
        let updated = transaction
            .execute(
                "UPDATE projection_materialization_items
                 SET owner_key = $5, sort_key = $6, item_json = $7::text::jsonb
                 WHERE projection_id = $1 AND keyspec_version = $2
                   AND generation = $3 AND item_key = $4",
                &[
                    &projection.as_str(),
                    &KEYSPEC_VERSION,
                    &generation,
                    &item.item_key,
                    &item.owner_key,
                    &item.sort_key,
                    &serialize(&item.value)?,
                ],
            )
            .map_err(backend)?;
        if updated != 1 {
            return Err(StorageError::Invariant(format!(
                "projection delta update requires existing item_key {}",
                item.item_key
            )));
        }
        replace_visible_refs(transaction, projection, generation, item)?;
    }
    for item in inserts {
        insert_item(transaction, projection, generation, item)?;
    }
    rebuild_visible_refs(transaction, projection, generation)
}

fn insert_item(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
    item: &ProjectionItem,
) -> StorageResult<()> {
    item.validate()?;
    transaction
        .execute(
            "INSERT INTO projection_materialization_items (
                projection_id, keyspec_version, generation,
                item_key, owner_key, sort_key, item_json
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb)",
            &[
                &projection.as_str(),
                &KEYSPEC_VERSION,
                &generation,
                &item.item_key,
                &item.owner_key,
                &item.sort_key,
                &serialize(&item.value)?,
            ],
        )
        .map_err(|error| {
            if error.as_db_error().is_some_and(|database| {
                database.code() == &postgres::error::SqlState::UNIQUE_VIOLATION
            }) {
                StorageError::Invariant(format!(
                    "projection insert requires absent item_key {}",
                    item.item_key
                ))
            } else {
                backend(error)
            }
        })?;
    replace_visible_refs(transaction, projection, generation, item)
}

fn replace_visible_refs(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
    item: &ProjectionItem,
) -> StorageResult<()> {
    let mut refs = BTreeSet::new();
    collect_blob_refs(&item.value, &mut refs);
    for blob_ref in refs {
        transaction
            .execute(
                "INSERT INTO projection_visible_blob_refs (
                    projection_id, keyspec_version, generation, blob_ref
                 ) VALUES ($1, $2, $3, $4)
                 ON CONFLICT DO NOTHING",
                &[
                    &projection.as_str(),
                    &KEYSPEC_VERSION,
                    &generation,
                    &blob_ref,
                ],
            )
            .map_err(backend)?;
    }
    Ok(())
}

fn rebuild_visible_refs(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
    generation: i64,
) -> StorageResult<()> {
    transaction
        .execute(
            "DELETE FROM projection_visible_blob_refs
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?;
    let rows = transaction
        .query(
            "SELECT item_key, owner_key, sort_key, item_json::text
             FROM projection_materialization_items
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?;
    for row in rows {
        replace_visible_refs(
            transaction,
            projection,
            generation,
            &projection_item_row(row)?,
        )?;
    }
    Ok(())
}

fn collect_blob_refs(value: &serde_json::Value, refs: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(value) if value.starts_with("blob:sha256:") => {
            refs.insert(value.clone());
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_blob_refs(value, refs);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_blob_refs(value, refs);
            }
        }
        _ => {}
    }
}

fn supplemental_blob_refs(record: &SupplementalRecord) -> Vec<BlobRef> {
    let mut refs = record.derived_from.blobs.clone();
    refs.extend(json_blob_refs(&record.payload));
    refs
}

fn commit_blob_refs(commit: &ProjectionItemCommit) -> Vec<BlobRef> {
    let mut refs = Vec::new();
    match commit {
        ProjectionItemCommit::Replace { items } => {
            for item in items {
                refs.extend(json_blob_refs(&item.value));
            }
        }
        ProjectionItemCommit::Delta {
            inserts, updates, ..
        } => {
            for item in inserts.iter().chain(updates) {
                refs.extend(json_blob_refs(&item.value));
            }
        }
    }
    refs
}

fn json_blob_refs(value: &serde_json::Value) -> Vec<BlobRef> {
    let mut refs = BTreeSet::new();
    collect_blob_refs(value, &mut refs);
    refs.into_iter().map(BlobRef::new).collect()
}

fn reject_manifest_blob_refs(value: &serde_json::Value) -> StorageResult<()> {
    if json_blob_refs(value).is_empty() {
        Ok(())
    } else {
        Err(StorageError::Invariant(
            "projection manifests must not contain blob references; store them in projection items"
                .to_owned(),
        ))
    }
}

fn active_projection_blob_refs(
    store: &PostgresPersistence,
    projection: &ProjectionRef,
) -> StorageResult<Vec<BlobRef>> {
    let mut reader = store.reader()?;
    Ok(reader
        .query(
            "SELECT visible.blob_ref
             FROM projection_materialization_heads heads
             JOIN projection_visible_blob_refs visible
               ON visible.projection_id = heads.projection_id
              AND visible.keyspec_version = heads.keyspec_version
              AND visible.generation = heads.generation
             WHERE heads.projection_id = $1
               AND heads.keyspec_version = $2
             ORDER BY visible.blob_ref",
            &[&projection.as_str(), &KEYSPEC_VERSION],
        )
        .map_err(backend)?
        .into_iter()
        .map(|row| BlobRef::new(row.get::<_, String>(0)))
        .collect())
}

fn insert_new_supplemental(
    transaction: &mut Transaction<'_>,
    record: &SupplementalRecord,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO supplementals (
                supplemental_id, created_at, supplemental_json
             ) VALUES ($1, $2, $3::text::jsonb)",
            &[
                &record.id.as_str(),
                &record.created_at.to_rfc3339(),
                &serialize(record)?,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn insert_audit(transaction: &mut Transaction<'_>, audit: &AuditEventRecord) -> StorageResult<()> {
    super::general_runtime::insert_audit_record(transaction, audit)
}

fn validate_audit(audit: &AuditEventRecord) -> StorageResult<()> {
    super::general_runtime::validate_audit_record(audit)?;
    Ok(())
}

fn generation_item_count(
    client: &mut impl postgres::GenericClient,
    projection: &ProjectionRef,
    generation: i64,
) -> StorageResult<u64> {
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM projection_materialization_items
             WHERE projection_id = $1 AND keyspec_version = $2
               AND generation = $3",
            &[&projection.as_str(), &KEYSPEC_VERSION, &generation],
        )
        .map_err(backend)?
        .get(0);
    u64::try_from(count)
        .map_err(|_| StorageError::Invariant("projection item count is negative".to_owned()))
}

fn projection_count(
    store: &PostgresPersistence,
    projection: &ProjectionRef,
    owner: Option<&str>,
) -> StorageResult<u64> {
    let mut reader = store.reader()?;
    let count: i64 = reader
        .query_one(
            "SELECT COUNT(items.item_key)
             FROM projection_materialization_heads heads
             LEFT JOIN projection_materialization_items items
               ON items.projection_id = heads.projection_id
              AND items.keyspec_version = heads.keyspec_version
              AND items.generation = heads.generation
              AND ($3::text IS NULL OR items.owner_key = $3)
             WHERE heads.projection_id = $1
               AND heads.keyspec_version = $2",
            &[&projection.as_str(), &KEYSPEC_VERSION, &owner],
        )
        .map_err(backend)?
        .get(0);
    u64::try_from(count)
        .map_err(|_| StorageError::Invariant("projection item count is negative".to_owned()))
}

fn advisory_projection_lock(
    transaction: &mut Transaction<'_>,
    projection: &ProjectionRef,
) -> StorageResult<()> {
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&format!("lethe:projection:{}", projection.as_str())],
        )
        .map_err(backend)?;
    Ok(())
}

fn projection_item_rows(rows: Vec<postgres::Row>) -> StorageResult<Vec<ProjectionItem>> {
    rows.into_iter().map(projection_item_row).collect()
}

fn projection_item_row(row: postgres::Row) -> StorageResult<ProjectionItem> {
    Ok(ProjectionItem {
        item_key: row.get(0),
        owner_key: row.get(1),
        sort_key: row.get(2),
        value: deserialize(row.get(3))?,
    })
}

fn parse_sort_cursor(value: Option<&str>) -> StorageResult<(Option<&str>, Option<&str>)> {
    let Some(value) = value else {
        return Ok((None, None));
    };
    non_blank("projection item cursor", value)?;
    if let Some((sort_key, item_key)) = value.rsplit_once('\u{001f}') {
        non_blank("projection cursor sort_key", sort_key)?;
        non_blank("projection cursor item_key", item_key)?;
        Ok((Some(sort_key), Some(item_key)))
    } else {
        Ok((Some(value), None))
    }
}

fn validate_projection(projection: &ProjectionRef) -> StorageResult<()> {
    non_blank("projection id", projection.as_str())
}

fn validate_manifest(manifest: &serde_json::Value) -> StorageResult<()> {
    if manifest.is_object() {
        Ok(())
    } else {
        Err(StorageError::Invariant(
            "projection manifest must be a JSON object".to_owned(),
        ))
    }
}

fn positive(field: &str, value: usize) -> StorageResult<()> {
    if value == 0 {
        Err(StorageError::Invariant(format!(
            "{field} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

fn non_blank(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!(
            "{field} must not be blank"
        )))
    } else {
        Ok(())
    }
}

fn storage_projection_id(projection: &str, keyspec: &str, generation: i64) -> String {
    format!("{projection}\u{001f}{keyspec}\u{001f}{generation}")
}

fn to_i64(field: &str, value: usize) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn serialize(value: &impl serde::Serialize) -> StorageResult<String> {
    serde_json::to_string(value).map_err(|error| StorageError::Backend(error.to_string()))
}

fn deserialize<T: serde::de::DeserializeOwned>(value: String) -> StorageResult<T> {
    serde_json::from_str(&value).map_err(|error| StorageError::Backend(error.to_string()))
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}
