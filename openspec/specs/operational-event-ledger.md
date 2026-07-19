# M18: Operational Event Ledger

**Module:** operational-event-ledger
**Scope:** Nanihold operational events / DataSpace isolation / backend cutover
**Dependencies:** M01 Domain Kernel, M03 Observation Lake, M08 Governance, M14 API Serving
**Parent docs:** [Operational Event Ledger](../../docs/architecture/operational-event-ledger.md)

## OEL-01 Canonical operation

Every accepted Nanihold event SHALL be stored as a first-class immutable
Observation with `lake_authoritative` authority and `event` capture model.
Long conversation text, raw logs, and artifacts SHALL be stored as
content-addressed blobs referenced by the Observation.

## OEL-02 DataSpace boundary

An operational backend SHALL be pinned to exactly one explicit `DataSpaceId`.
SQLite SHALL use a separate database file and blob directory per DataSpace.
PostgreSQL SHALL use a pre-created dedicated schema and exact role per
DataSpace. A missing backend, location, DataSpace, schema, role, or secret SHALL
fail startup.

The runtime SHALL NOT dual-write, infer a backend, switch backend during
execution, or fall back to another backend.

## OEL-03 Append semantics

Append SHALL atomically persist the operational event and its Observation.
Each stream SHALL use an optimistic expected version. Stale versions SHALL
return a conflict and SHALL NOT be treated as success. Event IDs and
idempotency keys SHALL reject reuse with different canonical content.

The store SHALL expose a monotonically increasing DataSpace cursor, cursor page,
per-stream version page, exact event lookup, and content-addressed blob
put/get.

## OEL-04 Capability-scoped API

Operational read endpoints SHALL require `read:operational`. Operational append
and blob writes SHALL require `write:operational`. The selfhost SHALL reject a
configuration in which either capability is unavailable.

## OEL-05 Backend cutover

Backend cutover SHALL use a signed canonical event export and blob digest
manifest. The target Lake SHALL verify the signature, blob digests, replay
results, and Projection equivalence before configuration changes. Runtime
readers for the old backend and live dual-write SHALL NOT be provided.

## OEL-06 Conformance

Every storage adapter SHALL pass the same deterministic contract tests for
append, optimistic conflict, idempotency, cursor/stream reads, event lookup,
DataSpace isolation, and blob storage. PostgreSQL conformance SHALL run against
a real disposable PostgreSQL server rather than a mock.
