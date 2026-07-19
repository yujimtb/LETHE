# M19 Personal History Ingestion

**Status:** Normative

## HIS-01 Personal boundary

History import SHALL target one explicit DataSpace. Every source SHALL have an
explicit Personal owner or unresolved ownership. Any unresolved source SHALL
block import. Raw personal conversation SHALL be stored as a content-addressed
blob and SHALL NOT be copied to a company DataSpace.

## HIS-02 Source identity

A record identity SHALL be the tuple of source instance, source session, and
source-native immutable occurrence ID. A coding-agent occurrence ID SHALL
include its native message ID and immutable transcript locator; the unmodified
native message ID SHALL remain metadata. Deduplication SHALL require both that
identity and the raw SHA-256 to match. Equal message text SHALL NOT be a
deduplication key. Reuse of one occurrence identity with different raw bytes
SHALL fail.

## HIS-03 Inventory and receipt

Dry-run SHALL produce a canonical manifest containing source counts, raw byte
counts, cutover cursors, ownership, record digests, and one manifest digest.
It SHALL NOT print message bodies or raw records. Import SHALL require the
expected manifest digest, rebuild the inventory, fail on mismatch, store the
raw blobs, atomically append each bounded message batch, and append a
`HistoryImportReceipt` only after every message batch succeeds. A missing
receipt SHALL mean the import is incomplete and SHALL block activation.
Idempotent retry with the same manifest SHALL complete an interrupted import.
The receipt SHALL preserve all source cursors.

## HIS-04 Native source intake

The native CLI SHALL read Claude Code project JSONL and Codex session JSONL
without requiring an archive copy. A Codex native root SHALL include both
`sessions` and `archived_sessions` when present. Execute mode SHALL require an
explicit SQLite or PostgreSQL backend and all corresponding locations,
credentials, DataSpace, size limit, and expected manifest digest. It SHALL NOT
fallback to another backend.

Native and generic JSONL intake SHALL stream bounded records into an explicit
new spool database. File and tree digests SHALL be incremental. Execute SHALL
hold no more event requests than the explicit resident batch limit. The CLI
SHALL also accept repeatable
`--history-jsonl=<source-kind>:<source-instance-id>:<path>` producers whose
lines are exact `HistoryRawRecord` values. Unknown source kinds, malformed
records, and identity collisions SHALL fail.

Every record that exposes upstream provenance SHALL provide the complete tuple
of source kind, source instance, source session, and native message ID.
Inventory SHALL count identities present in more than one physical source,
include that count in the manifest and activation handoff, and set readiness
false when the count is non-zero. Import SHALL reject any such overlap. Message
text or text digest SHALL NOT resolve an overlap.

`HistoryRecordKind` SHALL represent legacy Node memory as a first-class
`node_memory` value with non-empty `memory_id` and `node_id`. Producers SHALL
NOT coerce Node memory into preferences or current state.

## HIS-05 Existing LETHE source

The existing Personal Lake adapter SHALL freeze one source `append_seq`
watermark, read observations through positive bounded pages, and include only
conversation observations from Claude, ChatGPT, Claude Code, Codex, Slack, and
Discord. It SHALL retain the immutable LETHE observation ID as source identity,
the serialized Observation as raw content, and the upstream native message ID
as metadata.

The adapter SHALL exclude `sys:lethe-history` and history schemas to prevent a
self-import loop. The current Personal Lake source backend SHALL be explicit
SQLite with an existing database, blob directory, key environment variable,
stable source instance, explicit routing key order, and positive page size.
Missing configuration SHALL fail before scanning; no backend or remote
endpoint fallback is permitted.

Every eligible upstream source system SHALL have an explicit stable source
instance mapping. Missing mappings SHALL fail. Empty-text observations SHALL
be explicitly ineligible conversation records.

## HIS-06 Activation handoff

Dry-run and execute SHALL produce the same versioned compact activation
handoff. It SHALL contain DataSpace, inventory and manifest identity, aggregate
record and byte counts, cross-source overlap count, per-source kind, instance,
record count, byte count, source digest and cutover cursor, plus a bounded
session index of references, source identities, time ranges and message
counts. It SHALL NOT contain message bodies or every message reference.

Each source SHALL use `source_id=<kind>:<instance>`, resolved ownership and
owner, and `digest_sha256`. LETHE SHALL generate and validate deterministic
`session_index_ref`, `open_commitments_ref`, and `current_state_ref` values in
the form `history-projection:<projection>:sha256:<digest>`. Nanihold SHALL
consume these references and SHALL NOT create a competing projection identity.

The caller SHALL provide a positive maximum session-entry count. Exceeding it
SHALL fail rather than truncate. The normative fixture is
`crates/history/tests/fixtures/history_activation_handoff.json`.

## HIS-07 Query port

Nanihold SHALL read history only through `POST /api/history/query` with
`data_space_id`, one closed operation, its typed argument, an optional opaque
page cursor, and positive `max_result_bytes`.

The closed operations SHALL be `list_sessions`, `read_timeline`, `read_raw`,
`search`, and `resolve_reference`. A response SHALL contain `result_json`,
`next_cursor`, and the operational Projection `source_cursor`.

The canonical serialization of `result_json` SHALL not exceed
`max_result_bytes`. The server SHALL fail if one result cannot fit and SHALL
never silently truncate it. A continuation cursor SHALL bind operation,
Projection watermark, and offset. A watermark change SHALL return the
machine-readable error `HistoryCursorStale`; the server SHALL NOT silently
restart from the first page. DataSpace mismatch and malformed arguments or
cursors SHALL fail.

## HIS-08 Determinism and conformance

The history Projection SHALL rebuild only from ordered operational events and
their referenced blobs. SQLite and PostgreSQL SHALL run the same history
ingestion/query conformance. Tests SHALL cover same-text distinct IDs,
source-ID collision, unresolved ownership, manifest mismatch, receipt
idempotency, raw digest verification, bounded pagination, stale cursors,
first-class Node memory, bounded existing-Lake paging, and self-import
exclusion. Streaming conformance SHALL also cover cross-source overlap
blocking and the activation handoff fixture.
