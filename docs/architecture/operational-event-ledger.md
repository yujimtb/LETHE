# Operational Event Ledger

## Purpose

LETHE is the canonical Event Ledger for Nanihold operational state. A Nanihold
event is a first-class `Observation`; a conversation body, long log, or raw
artifact is stored once as a content-addressed blob and referenced by that
Observation. A Projection may be rebuilt from the ordered events and blobs, but
it is never an alternative source of truth.

This contract is independent of Nanihold's UI, Pilot implementation, and model
provider. It deliberately has no `Run` concept.

## DataSpace isolation

One operational backend is pinned to exactly one `DataSpaceId`.

- SQLite uses a dedicated database file and blob directory for each DataSpace.
  Opening a file with a different DataSpace fails.
- PostgreSQL uses a pre-created dedicated schema and role for each DataSpace.
  Startup checks the connected role and schema before creating tables, then
  pins the schema to the configured DataSpace.
- Personal and company data are not co-located merely for query convenience.
  Cross-DataSpace access is represented by a time-, purpose-, and
  subject-bounded `ReferenceGrant` event in Nanihold, not by copying raw
  conversations.

The selfhost requires an explicit `[operational_ledger]` configuration. It does
not infer a backend from the ordinary Observation store. There is no
dual-write, backend fallback, or runtime backend switch.

```toml
[operational_ledger]
backend = "sqlite"
data_space_id = "space:personal"
database_path = "./data/personal-operational.sqlite3"
blob_dir = "./data/personal-operational-blobs"
encryption_key_env = "LETHE_OPERATIONAL_STORAGE_ENCRYPTION_KEY"
```

PostgreSQL instead requires `backend = "postgres"`, `data_space_id`,
`dsn_env`, `schema`, and `role`. The current adapter deliberately supports only
an explicit no-TLS database connection; deployments must use a local socket or
a separately managed private/TLS transport. It never downgrades a TLS request
or falls back to SQLite.

## Append contract

`OperationalEventStore` provides:

- optimistic concurrency through `expected_stream_version`;
- unique event IDs and idempotency keys with collision detection;
- an append-only event and Observation write in one database transaction;
- a monotonically increasing DataSpace cursor;
- cursor pages, stream-version pages, event lookup, and stream version lookup;
- content-addressed blob put/get.

An event's declared `stream_version` must equal
`expected_stream_version + 1`. A stale expectation returns
`version_conflict`; it is never reinterpreted as success. Reusing an
idempotency key or event ID with different content is an invariant violation.
Update and delete triggers reject mutation of operational events.

## HTTP surface

The selfhost exposes the same contract:

| Method and path | Scope | Meaning |
|---|---|---|
| `POST /api/operational-events` | `write:operational` | append event requests in one transaction |
| `GET /api/operational-events?after_cursor=&limit=` | `read:operational` | cursor page |
| `GET /api/operational-events/stats` | `read:operational` | count and high-water cursor |
| `GET /api/operational-events/{event_id}` | `read:operational` | event lookup |
| `GET /api/operational-streams/{stream_id}?after_stream_version=&limit=` | `read:operational` | ordered stream page |
| `POST /api/operational-blobs` | `write:operational` | put raw bytes by digest |
| `GET /api/operational-blobs/{sha256}` | `read:operational` | get raw bytes by digest |

Both operational scopes must be assigned at startup. Missing cursor/limit
parameters, invalid page limits, invalid events, and DataSpace mismatches fail
explicitly.

## Signed backend cutover

`export_operational_archive` creates a canonical archive containing ordered
events and a blob digest manifest. `sign_operational_archive` authenticates its
canonical bytes with a caller-supplied signing key. Migration follows this
sequence:

1. stop writers for the source DataSpace;
2. export and sign the canonical archive;
3. copy the separately enumerated blobs;
4. verify the signature and every blob digest in a different Lake;
5. replay with optimistic versions and verify event/Projection counts;
6. change the explicit backend configuration and restart.

The implementation has no live dual-write path and no reader for an old
backend after cutover.

## Conformance

The SQLite and PostgreSQL adapters run the shared storage contract for
idempotency, conflicts, cursor/stream reads, event lookup, and blob storage.
SQLite additionally tests file pinning and export/replay. PostgreSQL
conformance runs against a disposable real PostgreSQL instance and verifies
the required schema/role boundary.
