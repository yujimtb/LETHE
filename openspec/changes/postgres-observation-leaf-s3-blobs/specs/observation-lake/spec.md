## ADDED Requirements

### Requirement: Backend-independent blob transaction boundary

The ingestion pipeline SHALL store and verify content-addressed blobs before committing an Observation that references them, regardless of the selected storage backend.

**Parent module:** M03 Observation Lake

**Dependencies:** M01 Domain Kernel, M08 Governance, M16 Platform Generalization
**Invariants:** Append-Only Law, Replay Law, Explicit Authority Law

#### Scenario: Blob succeeds and Observation fails

- **WHEN** blob storage succeeds but the Observation transaction fails
- **THEN** no Observation references partial content and the blob remains an auditable orphan candidate

#### Scenario: Blob verification fails

- **WHEN** a referenced blob cannot be read or its SHA-256 differs
- **THEN** ingestion fails visibly and does not append an Observation without that attachment

### Requirement: Backend-independent replay ordering

All general Lake backends SHALL return page, leaf, privacy-key, and watermark reads in the exact ordering defined by their `StoragePorts` contract.

#### Scenario: Equivalent backend fixtures

- **WHEN** the same pinned Observation fixture is appended to SQLite and PostgreSQL
- **THEN** normalized outcomes, append order, leaf positions, and replay input are identical
