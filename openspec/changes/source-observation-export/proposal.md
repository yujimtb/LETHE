## Why

Projection builders need a reproducible page stream of canonical source Observations. LETHE
currently exposes strict atomic source writes but no bounded, watermark-pinned read contract, so
consumers must either violate the Lake boundary by reading storage directly or cannot build from
the canonical source at all.

## What Changes

- Add an authenticated v3 source-Observation export endpoint with a fixed append-sequence
  watermark, bounded pages, and strict request/response shapes.
- Export only canonical Observations using `schema:askbot-source-observation@1.0.0`; operational,
  supplemental, derived Projection, and unrelated schemas are excluded.
- Return the immutable outer Observation and append sequence without interpreting or normalizing
  its source-native payload.
- Require a dedicated read scope and reject invalid limits, cursors, or watermark changes without
  falling back to search, v1/v2 APIs, or direct-storage aliases.
- Add SQLite/PostgreSQL parity, restart, concurrent-append, authorization, and pagination tests.

Non-goals:

- General corpus search, public MCP exposure, arbitrary schema export, or provider-specific
  filtering.
- A storage-level compatibility view or direct PostgreSQL contract for downstream consumers.
- Projection semantics or activation logic.

## Capabilities

### New Capabilities

- `source-observation-export`: Watermark-pinned, bounded export of canonical ask_bot source
  Observations for trusted Projection and migration consumers.

### Modified Capabilities

None.

## Impact

M03 Observation Lake and M14 API Serving gain one read-only v3 contract. `AppService`, Axum
routing, storage parity tests, selfhost E2E tests, and ingestion/API documentation are affected.
Append-Only, Replay, Effect Isolation, Explicit Authority, and Filtering-before-Exposure laws are
preserved: the endpoint mutates nothing, fixes its source watermark, requires explicit scope, and
exports only the closed canonical source envelope.
