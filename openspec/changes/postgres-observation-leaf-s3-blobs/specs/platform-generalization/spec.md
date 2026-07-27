## ADDED Requirements

### Requirement: Explicit general storage backend selection

General Lake persistence SHALL be selected by one required tagged configuration variant, `sqlite` or `postgres`, and both variants SHALL satisfy the same `StoragePorts` conformance suite.

**Parent module:** M16 Platform Generalization

**Dependencies:** M01 Domain Kernel, M03 Observation Lake, M15 Runtime
**Invariants:** Effect Isolation Law, Replay Law

#### Scenario: Backend is omitted or unknown

- **WHEN** selfhost config omits `storage.backend` or supplies an unknown value
- **THEN** strict deserialization fails before a database or blob connection is opened

#### Scenario: PostgreSQL is unavailable

- **WHEN** the selected PostgreSQL backend cannot connect or pass deep health
- **THEN** selfhost is unready and does not open SQLite, local blobs, or any fallback backend

### Requirement: Backend-specific configuration isolation

Each storage variant SHALL reject fields belonging to another backend and SHALL NOT interpret legacy untagged storage fields.

#### Scenario: Mixed backend fields

- **WHEN** a PostgreSQL variant contains `database_path` or a SQLite variant contains `dsn_env`
- **THEN** strict config parsing fails with the offending field
