## 1. Contracts and migrations

- [x] 1.1 Add backend-neutral referenced-blob/deep-health conformance helpers. [Owner: Implementer] [Spec: M03 observation-lake, s3-blob-storage] Acceptance: SQLite unit tests and storage-api tests compile and helpers reject missing/digest-mismatched blobs.
- [x] 1.2 Define strict PostgreSQL/S3 configuration value types and validation. [Owner: Implementer] [Spec: M15 runtime, M16 platform-generalization] Acceptance: valid tagged variants parse; missing, unknown, mixed, insecure, and legacy untagged configs fail before I/O.
- [x] 1.3 Add embedded migration ledger and initial general-Lake PostgreSQL schema. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: empty schema migrates once; repeat is idempotent; changed checksum and newer DB version fail.
- [x] 1.4 Add data-space, role, schema, writer, and read-pool connection admission. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: correct pins connect and every mismatch fails before a port is returned.

## 2. PostgreSQL StoragePorts

- [x] 2.1 Implement Observation append, batch/audit append, duplicate/collision, page, ID, privacy, leaf, split, and rehome operations. [Owner: Implementer] [Spec: M03 observation-lake, postgres-observation-leaf] Acceptance: ObservationStore conformance and concurrent idempotency tests pass.
- [x] 2.2 Implement cutover admission, state, bridge, readiness, activation, rollback, inventory, and health operations. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: existing cutover conformance fixtures produce normalized SQLite/PostgreSQL-equivalent outcomes.
- [x] 2.3 Implement Supplemental store and atomic Supplemental-plus-Projection commits. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: Supplemental conformance and injected rollback tests pass.
- [x] 2.4 Implement Projection manifests/items, delta/replace, staging publish, visibility, counts, and generation cleanup. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: ProjectionMaterializer conformance and failed-publish atomicity tests pass.
- [x] 2.5 Implement runtime state, audit, sync metrics/state, dead letters, retention, and deep check. [Owner: Implementer] [Spec: M15 runtime, postgres-observation-leaf] Acceptance: RuntimeStateStore conformance, keyset audit paging, retention, and corruption probes pass.
- [x] 2.6 Implement Slack thread catalog and Projection leaf watermarks. [Owner: Implementer] [Spec: postgres-observation-leaf] Acceptance: thread-generation and watermark conformance fixtures match SQLite results.

## 3. S3/MinIO BlobStore

- [x] 3.1 Implement strict S3 endpoint/bucket/key encoding and AWS SigV4 request signing. [Owner: Implementer] [Spec: s3-blob-storage] Acceptance: AWS signature fixtures and unsafe endpoint/key negative tests pass.
- [x] 3.2 Implement idempotent PUT/batch PUT, HEAD verification, GET digest verification, and bounded I/O. [Owner: Implementer] [Spec: s3-blob-storage] Acceptance: MinIO integration verifies deduplication, mismatch rejection, missing reads, timeout, and size limits.
- [x] 3.3 Enforce referenced-blob admission in PostgreSQL Observation and Supplemental transactions. [Owner: Implementer] [Spec: M03 observation-lake, s3-blob-storage] Acceptance: missing or corrupt references roll back the whole transaction.
- [x] 3.4 Implement two-scan minimum-age orphan marking, audit commit, deletion, and metrics. [Owner: Implementer] [Spec: s3-blob-storage] Acceptance: young/once-seen/referenced/audit-failed objects remain and only fully eligible objects delete.

## 4. Runtime, validation, and documentation

- [x] 4.1 Wire tagged general storage variants and PostgreSQL read pools into selfhost without fallback. [Owner: Implementer] [Spec: M15 runtime, M16 platform-generalization] Acceptance: SQLite and PostgreSQL fixture selfhosts boot separately; selected-backend failure leaves readiness false.
- [x] 4.2 Add disposable Docker PostgreSQL/MinIO integration topology and test command. [Owner: Implementer] [Spec: M15 runtime] Acceptance: one command creates test-only services, runs migrations/conformance/failure injection, and removes only test-scoped state.
- [x] 4.3 Run SQLite/PostgreSQL normalized parity, restart, concurrency, and Replay Law suites. [Owner: Reviewer] [Spec: M03 observation-lake, M16 platform-generalization] Acceptance: all declared parity dimensions pass with no ignored required test.
- [x] 4.4 Update storage, configuration, migration, MinIO, readiness, restore, and incident documentation. [Owner: Implementer] [Spec: M03, M15, M16] Acceptance: documented commands match the tested topology and contain no legacy untagged config or fallback instructions.
- [x] 4.5 Run format, clippy, workspace tests, dependency-layer checks, strict OpenSpec validation, and final review. [Owner: Reviewer] [Spec: all change specs] Acceptance: every command is green and every task has linked evidence.
