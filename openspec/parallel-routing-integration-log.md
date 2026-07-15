# Parallel Routing Integration Log

## Fixed Inputs

- Worktree: `D:\userdata\docs\projects\_mission_20260712_nanihold\wt-lethe-routing`
- Integration branch: `fix/lethe-linearization`
- Base: `a00e14a32dc031acf20213aa0291d6ab94c854c5`
- Routing plan tip / initial HEAD: `e2bb62e1354c53a5ecfa8363ca64941e0bb81881`
- Lane 1: `79398f0a6d68ee3309d5560a3f9b5649f524b8a0`
- Lane 3: `ddd462ad61b7dbd64a6880b5996d09278a99843c`
- Lane 2: `44e22bed346c0466dce41c528a9f02ad0d7081a4`
- Lane a: `0801f4060d38e144cf334bab61f9cc8d101bc953`
- Lane b C-stage: `e23e8845753f483f01a2ab10ea5c32f6d0c5d0a1`
- Lane b B-stage: not integrated by instruction; only C-stage hash was used.

## Execution Note

`git merge --no-ff --no-commit` failed before writing the first merge because Git could not lock the worktree `ORIG_HEAD` pseudoref under `D:/userdata/docs/projects/skcollege_database/.git/worktrees/wt-lethe-routing`. The same permission profile allowed object writes and `refs/heads/fix/lethe-linearization` updates, so the integration used the equivalent low-level sequence per lane:

- `git merge-tree --write-tree HEAD <fixed-lane-hash>`
- resolve conflict-marker trees function-by-function where needed
- run targeted tests and `cargo test --workspace`
- create true two-parent merge commits with `git commit-tree`
- advance only `refs/heads/fix/lethe-linearization` with `git update-ref`

No `git worktree` management command was used. No other branch ref was updated.

## Merge Records

| Step | Input hash | Merge commit | Conflicts | Required tests before commit |
|---|---|---|---|---|
| lane 1 | `79398f0a6d68ee3309d5560a3f9b5649f524b8a0` | `ec29b02f61d99ea7e0144cd3c1c60a87ef1bb997` | none | `cargo test -p lethe-projection-claim-queue --lib`; `cargo test -p lethe-projection-cognition --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test --workspace` |
| lane 3 | `ddd462ad61b7dbd64a6880b5996d09278a99843c` | `ebef01bf792b7bbd3049ce4f53a39242318658d5` | `apps/selfhost/src/self_host/app/mod.rs`; `crates/projections/cognition/src/lib.rs` | `cargo test -p lethe-projection-cognition --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test --workspace` |
| lane 2 | `44e22bed346c0466dce41c528a9f02ad0d7081a4` | `dfa0180e1664875817d66ffdd2c6ac3d77a00d58` | none | `cargo test -p lethe-storage-api --lib`; `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test --workspace` |
| lane a | `0801f4060d38e144cf334bab61f9cc8d101bc953` | `c04761c01fb1370f6a222c24678a63e31dab5a23` | none | `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test -p lethe-e2e --test self_host_api`; `cargo test --workspace` |
| lane b C | `e23e8845753f483f01a2ab10ea5c32f6d0c5d0a1` | `d589bf069fc1cdfb3004b47cf3c933c86c815fd1` | `apps/selfhost/src/self_host/app/mod.rs`; `apps/selfhost/src/self_host/app/tests.rs`; `docs/development/personal-lake-ingestion.md` | `cargo test -p lethe-engine --lib`; `cargo test -p lethe-projection-person --lib`; `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test --workspace` |

All listed commands exited successfully. The first lane b selfhost attempt exposed two integration compile errors; both were fixed before the recorded green `cargo test -p lethe-selfhost --lib` and before the lane b merge commit.

## Conflict Adjudication

### Lane 3

- `apps/selfhost/src/self_host/app/mod.rs`
  - Kept lane 1 `SupplementalProjectionCache`, single supplemental delta dispatcher, ClaimQueue/CardQueue/cognition state.
  - Removed the duplicate `AppCore.reply_slo_join_index`; `SupplementalProjectionCache.reply_slo` is the only ReplySLO supplemental join index.
  - Preserved atomic supplemental delta publication: cognition, CardQueue, ClaimQueue, ReplySLO items, and manifest are derived as one plan.
- `crates/projections/cognition/src/lib.rs`
  - Kept lane 3 `ReplySloProjector::project_observations(observations, &ReplySloJoinIndex)` API.
  - Merged lane 3 index API into lane 1 cache lifecycle via `ReplySloJoinIndex::from_records`, `upsert_record`, and `remove_record`.
  - Removed the old duplicate full-scan/update-object path; the same index reducer is used by full replay and incremental updates.

### Lane b C

- `apps/selfhost/src/self_host/app/mod.rs`
  - Kept lane 1/3 supplemental and ReplySLO cache, lane 2 Slack catalog storage ports, and lane a bulk lifecycle.
  - Integrated lane b identity/person component-local reprojection into the existing outer coordinator instead of replacing the coordinator wholesale.
  - Passed `core.supplemental_projection_cache.records` into component reprojection; no stale `supplemental_records` path remains.
  - Kept `NON_CORPUS_MATERIALIZATION_VERSION` as the single version source and preserved strict version mismatch rejection.
- `apps/selfhost/src/self_host/app/tests.rs`
  - Preserved lane 3 ReplySLO count/owner assertions in the normalized full-rebuild oracle tests.
  - Updated lane b tests to current `compact_incremental_delta` semantics and `ProjectionItemCommit` resident-item model.
- `docs/development/personal-lake-ingestion.md`
  - Combined explicit bulk-session Deferred/Ready lifecycle with component-local identity/person reprojection notes.

No file was resolved by whole-file ours/theirs selection.

## Cross-Lane Regression Coverage

| Combination | Coverage |
|---|---|
| 1 x 3 | `supplemental_delta_matches_full_build_and_updates_one_reply_row`, `supplemental_projection_cache_and_fingerprint_match_full_replay_after_each_delta`, and cognition crate ReplySLO index tests verify reply-draft/send updates, ClaimQueue sharing, ReplySLO row updates, and full replay equality. |
| 1 x a | `active_bulk_session_rejects_supplemental_write_and_source_sync_without_advancing_state` verifies supplemental write fail-fast during Deferred bulk; `bulk_import_session_defers_non_corpus_keeps_corpus_ready_and_rebuilds_once` verifies stale non-corpus reads and finalize equality. |
| 3 x a | `bulk_import_session_defers_non_corpus_keeps_corpus_ready_and_rebuilds_once` imports multiple Slack batches, keeps non-corpus stale during session, rebuilds once at finalize, and compares manifest/items including `__reply_slo__` owner to the sequential reference. |
| 2 x a | `active_bulk_session_rejects_supplemental_write_and_source_sync_without_advancing_state` verifies `sync_all` fails fast during active bulk and leaves canonical stats, Slack catalog discovery high-water, and supplemental state unchanged. |
| 2 x b | `thread_catalog_sync_matches_full_rediscovery_without_repolling_idle_threads`, `slack_late_bridge_reprojects_only_affected_components_and_matches_full_rebuild`, and `component_reprojection_is_invariant_to_slack_batch_partition` cover catalog correctness, root/reply ingestion, and unaffected identity components. |
| 3 x b | `slack_late_bridge_reprojects_only_affected_components_and_matches_full_rebuild`, `materialized_person_message_manifest_rejects_resident_rows_and_count_drift`, and large Slack import tests verify ReplySLO owner/key/count isolation from person owner changes. |
| a x b | `bulk_import_session_defers_non_corpus_keeps_corpus_ready_and_rebuilds_once` compares bulk finalize output against sequential per-observation reference; `component_reprojection_is_invariant_to_slack_batch_partition` verifies late-bridge partition invariance and no global renumber drift within C-stage identity semantics. |
| all | `person_message_item_sql_failure_does_not_install_manifest_in_core`, `paged_materialization_matches_full_build_and_publishes_atomically`, storage projection item SQL failure tests, and bootstrap recovery tests verify atomic publish/restart behavior and fail-fast stale materialization. |

Additional regression added during integration:

- `active_bulk_session_rejects_supplemental_write_and_source_sync_without_advancing_state`
  - rejects `write_supplemental` with `bulk_import_session_active`
  - rejects `sync_all` with `bulk_import_session_active`
  - asserts canonical observation stats are unchanged
  - asserts Slack thread discovery high-water is unchanged
  - asserts the rejected supplemental ID is not persisted

## Static Audit

- `refresh_materialized_snapshot` / `rebuild_materialized_snapshot_paged`
  - Remaining call sites are bootstrap/current snapshot selection, explicit refresh, bulk finalize, source sync catch-up, and tests/oracles.
  - No normal observation write or supplemental write falls back to full rebuild.
- `supplemental.list()` / `load_supplementals` / `project_records`
  - Full replay, cache construction, tests, and crate-local projector oracles remain.
  - Normal supplemental write uses `SupplementalProjectionCache` and per-record reducers.
- `known_thread_roots` / `observation_page`
  - No `known_thread_roots_from_observations` fallback remains.
  - Observation paging remains in explicit rebuild/oracle/catalog backfill contexts.
- `FullRebuildRequired` / `fallback` / `unwrap_or_default` / `unwrap_or_else`
  - No `FullRebuildRequired` type remains.
  - Remaining `fallback` strings are Slack profile/media/projection spec terminology or tests, not silent durable-state fallback.
  - `unwrap_or_default` / `unwrap_or_else` occurrences are optional JSON/metadata extraction, mutex poison recovery, deterministic display defaults, or test helpers; none convert missing durable state, format mismatch, or active session conflict into a successful path.

## Final Verification Before Documentation Commit

- `cargo fmt --all -- --check`: passed
- `cargo clippy --workspace --all-targets -- -D warnings`: passed
- `cargo test --workspace`: passed
- `cargo test -p lethe-selfhost active_bulk_session_rejects_supplemental_write_and_source_sync_without_advancing_state --lib`: passed
- `cargo test -p lethe-selfhost --lib`: passed, 66 tests

The code merge tip before this log/test follow-up was `d589bf069fc1cdfb3004b47cf3c933c86c815fd1`.

## Format / Migration

- Final non-corpus materialization format is `NON_CORPUS_MATERIALIZATION_VERSION = 5`.
- Intermediate lane format shapes are not accepted through compatibility layers.
- Version/stats/fingerprint mismatch invalidates derived materialization and requires reconstruction from canonical observations/supplementals through the explicit rebuild/bootstrap path.
- Lane b B-stage stable DSU/storage migration remains outside this integration pass.

## Completion Status

- Fixed hashes were integrated in the required order: `1 -> 3 -> 2 -> a -> b(C)`.
- No conflict markers remain.
- No old thread discovery full-history fallback, old person ID alias, or normal-write full rebuild fallback was found.
- All required targeted tests and workspace tests passed at each merge stage.
- Final fmt, clippy, workspace tests, and static audit passed before this documentation/test follow-up.
