## Context

The v3 atomic write contract stores each ask_bot source record as the payload of a closed
`schema:askbot-source-observation@1.0.0` outer Observation. Projection and migration consumers need
those exact payloads, but current APIs expose search/projection views rather than a stable canonical
Observation stream. Both SQLite and PostgreSQL already implement append-sequence pages and stats.

## Goals / Non-Goals

**Goals:**

- Expose a bounded, authorization-gated source Observation page from one immutable high watermark.
- Preserve outer Observation and native payload bytes semantically without source normalization.
- Produce identical visible pages on SQLite and PostgreSQL while concurrent appends continue above
  the pinned watermark.
- Fail fast on malformed queries, unsupported schema/export attempts, or storage errors.

**Non-Goals:**

- Search, arbitrary predicate filters, public/user OAuth access, or derived Projection export.
- Cursor aliases, default page sizes, storage table contracts, or streaming an unbounded response.
- Changing Observation identity, retention, or source ingestion.

## Decisions

### Dedicated exact v3 endpoint

Use `GET /api/v3/export/source-observations` with required `limit` and `after_append_seq`, plus an
optional `watermark` only on the first request. A dedicated endpoint makes the trusted canonical
boundary explicit. Reusing corpus search was rejected because snippets, ranking, filtering, and
index freshness are not a replayable Lake stream.

### Append-sequence watermark

The first response pins the current storage `max_append_seq`. Every subsequent request sends that
exact watermark. Items with a higher append sequence are ignored, so concurrent writes do not
invalidate or extend the build. A watermark above current storage is rejected. The response
returns `next_after_append_seq` and `complete`; clients never infer completion from a short page.

### Closed schema filter in the service

The service exports only outer Observations whose schema and version are exactly
`schema:askbot-source-observation` and `1.0.0`. It does not accept a schema query parameter.
Operational, supplemental, legacy, and derived records therefore cannot be requested accidentally.
The complete immutable outer Observation is returned so downstream code can validate and decode
its closed source payload itself.

### Storage paging, not full-load

`AppService` reads `observation_stats` and bounded `observation_page` calls. It scans across
unrelated rows until the response limit is filled or the pinned watermark is reached. A configured
scan bound prevents one request from becoming an unbounded table walk; exceeding it is a typed
service-unavailable error rather than a partial-success receipt.

### Dedicated scope

The endpoint requires `read:source-observations`. It is intended for the internal Projection and
migration principals, not general MCP users. No other read scope implies it.

## Risks / Trade-offs

- [Many unrelated rows increase scan work] → Bound every request by configured page/scan limits and
  expose typed failure; ask_bot source records remain a closed high-volume stream.
- [A consumer loses its cursor] → It restarts from zero with a newly selected watermark; no server
  session or mutable export state exists.
- [Concurrent appends create sequence gaps] → Completion is based on reaching the pinned watermark,
  not item count or contiguity.
- [Raw source data is sensitive] → A dedicated internal scope and no public route are mandatory;
  response logging contains counts/cursors only.

## Migration Plan

1. Deploy the endpoint disabled to principals lacking the new scope.
2. Grant `read:source-observations` only to the Projection/migration token.
3. Run SQLite/PostgreSQL parity and concurrent-append replay tests.
4. Point ask_bot Projection at v3 and verify the complete watermark.
5. Rollback removes the principal scope and consumer traffic; the append-only Lake is unchanged.

## Open Questions

None.
