## ADDED Requirements

### Requirement: S3 content-addressed BlobStore

The S3 adapter SHALL map `blob:sha256:{lowercase-hex}` to exactly one configured bucket object `sha256/{lowercase-hex}` and SHALL implement `BlobStore` without exposing endpoint, bucket, or object path in `BlobRef`.

**Parent module:** M03 Observation Lake

**Dependencies:** M01 Domain Kernel, M15 Runtime, M16 Platform Generalization
**Invariants:** Replay Law, Effect Isolation Law

#### Scenario: Idempotent put

- **WHEN** identical bytes are put repeatedly or in a repeated batch
- **THEN** one content object exists and every returned `BlobRef` is identical

#### Scenario: Existing object differs

- **WHEN** an object already exists at the digest key but its bytes, length, or checksum differs
- **THEN** put fails as an invariant violation and does not overwrite the object

### Requirement: Blob reference admission

Observation and Supplemental transactions SHALL verify every referenced blob exists and matches its digest before commit.

#### Scenario: Missing attachment

- **WHEN** an append references a blob absent from the configured bucket
- **THEN** the whole database transaction fails and no canonical or derived record is committed

### Requirement: Audited conservative orphan collection

Orphan collection SHALL delete only objects older than the configured minimum age, unreferenced in two consecutive scans, and covered by a committed audit event.

#### Scenario: Recently uploaded orphan

- **WHEN** an unreferenced object is younger than the minimum age or appears in only one scan
- **THEN** it is retained

#### Scenario: Audit write fails

- **WHEN** an eligible orphan cannot be recorded in the database audit ledger
- **THEN** the object is not deleted

### Requirement: Strict S3 boundary

Endpoint, region, bucket, path-style mode, credential references, timeout, maximum object bytes, and TLS policy SHALL be required configuration.

#### Scenario: Insecure endpoint outside test mode

- **WHEN** an HTTP S3 endpoint or disabled certificate validation is configured outside explicit test mode
- **THEN** selfhost startup fails before making a request
