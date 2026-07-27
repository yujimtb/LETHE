## ADDED Requirements

### Requirement: PostgreSQL general Observation Leaf

`PostgresPersistence` SHALL implement every `StoragePorts` effect with the same domain outcomes, ordering, atomicity, and failure taxonomy as the reference SQLite implementation.

**Parent module:** M03 Observation Lake

**Dependencies:** M01 Domain Kernel, M08 Governance, M16 Platform Generalization
**Invariants:** Append-Only Law, Replay Law, Effect Isolation Law, No Direct Mutation Law

#### Scenario: Common conformance

- **WHEN** the storage-api conformance suite runs against an empty PostgreSQL schema
- **THEN** every Observation, cutover, Supplemental, Projection, runtime-state, Slack-thread, watermark, and blob contract passes without SQLite access

#### Scenario: Canonical duplicate and collision

- **WHEN** the same idempotency identity and canonical JSON are appended twice and then conflicting canonical JSON is appended at the same identity
- **THEN** PostgreSQL returns `Appended`, `Duplicate`, and `CanonicalCollision` respectively and never mutates the first Observation

### Requirement: Versioned PostgreSQL schema

The adapter SHALL apply ordered embedded SQL migrations and SHALL persist each exact migration version and SHA-256 before becoming ready.

#### Scenario: Migration checksum differs

- **WHEN** an applied migration version has a different stored checksum
- **THEN** startup fails with `StorageError::Invariant` and executes no later migration

#### Scenario: Unknown newer schema

- **WHEN** the database ledger contains a migration version unknown to the binary
- **THEN** startup fails without downgrade, repair, or table mutation

### Requirement: Data-space and role pinning

Each PostgreSQL schema SHALL be pinned to one data-space and one expected database role.

#### Scenario: Pin mismatch

- **WHEN** configured data-space or current role differs from the persisted pin
- **THEN** connection fails before any read or write port is exposed

### Requirement: Atomic Projection and supplemental commits

Projection replace/delta/publish and Supplemental-plus-Projection operations SHALL use database transactions and SHALL preserve No Direct Mutation Law.

#### Scenario: Failure during publish

- **WHEN** a failure occurs after staging validation but before target activation commit
- **THEN** the previous target manifest/items remain visible and no partial generation is exposed
