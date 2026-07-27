## ADDED Requirements

### Requirement: General storage deep readiness

Selfhost readiness SHALL verify selected backend migration state, data-space/role pin, write/read connection pools, BlobStore put-head-get-delete probe, and referenced-blob consistency.

**Parent module:** M15 Runtime

**Dependencies:** M03 Observation Lake, M16 Platform Generalization
**Invariants:** Effect Isolation Law, Filtering-before-Exposure Law

#### Scenario: Database is healthy but S3 is unavailable

- **WHEN** PostgreSQL probes succeed and the configured S3 probe fails
- **THEN** readiness is false and ingestion/data tools are not published as ready

### Requirement: Pre-NAS integration topology

The repository SHALL provide an isolated Docker test topology for PostgreSQL and MinIO that accepts only generated fixture credentials and non-production data.

#### Scenario: Integration suite starts

- **WHEN** the pre-NAS integration command runs on a Docker host
- **THEN** disposable PostgreSQL and MinIO services start, migrations and conformance run, and all volumes are test-scoped

#### Scenario: Production deployment mode

- **WHEN** the Docker test topology is invoked with production deployment mode
- **THEN** startup fails and no public listener or external source credential is used
