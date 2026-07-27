## 1. Registry and wire contracts

- [x] 1.1 Define strict v3 atomic-page request, success receipt, and typed rejection details. [Owner: Implementer] [Spec: M03 observation-lake, atomic-source-page-ingestion ASP-01..04] Acceptance: serde round-trip accepts only the declared fields and rejects empty pages, duplicate client refs, unknown fields, and invalid generations.
- [x] 1.2 Register the closed ask_bot source Observation envelope and seven source-specific observer contracts. [Owner: Implementer] [Spec: M02 registry, atomic-source-page-ingestion ASP-06] Acceptance: all seven valid fixtures prepare and every observer/source/schema mismatch fails before append.

## 2. Atomic backend port

- [x] 2.1 Add an atomic v2-identity page append operation to StoragePorts/CutoverStore. [Owner: Implementer] [Spec: M03 observation-lake, atomic-source-page-ingestion ASP-03..05] Acceptance: the port returns only ordered appended/duplicate outcomes and exposes collision as an error that requires rollback.
- [x] 2.2 Implement the atomic page transaction for SQLite. [Owner: Implementer] [Spec: M03 observation-lake, atomic-source-page-ingestion ASP-03..05] Acceptance: validation, missing blob, collision, stale generation, and injected audit/storage failures leave ledger/audit/metrics unchanged.
- [x] 2.3 Implement the atomic page transaction for PostgreSQL. [Owner: Implementer] [Spec: M03 observation-lake, atomic-source-page-ingestion ASP-03..05] Acceptance: the same failure fixtures roll back one transaction and concurrent exact retries converge to one append plus duplicates.
- [x] 2.4 Add normalized SQLite/PostgreSQL atomic-page parity conformance. [Owner: Reviewer] [Spec: M03 observation-lake, atomic-source-page-ingestion ASP-05] Acceptance: success, retry, collision, blob, generation, and fault reports are identical after normalizing generated IDs/timestamps.

## 3. Selfhost v3 APIs

- [x] 3.1 Implement AppService all-item preparation and atomic append orchestration. [Owner: Implementer] [Spec: M03 observation-lake, M09 adapter-policy, atomic-source-page-ingestion ASP-01..04] Acceptance: any terminal item prevents the storage append call; successful pages preserve input result order and trigger materialization only after commit.
- [x] 3.2 Implement `POST /api/v3/import/atomic-observation-pages` with authorization, body/count limits, required generation, and typed Problems. [Owner: Implementer] [Spec: M14 api-serving, atomic-source-page-ingestion ASP-01..04] Acceptance: HTTP tests prove zero ledger delta for every rejection and durable results for success/duplicate.
- [x] 3.3 Implement general-Lake `PUT /api/v3/import/source-blobs/{sha256}` with source/generation admission, size limit, digest verification, and strict receipt. [Owner: Implementer] [Spec: M03 observation-lake, source-blob-admission SBA-01..04] Acceptance: exact retry is idempotent; bad auth/path/digest/size store no object; atomic Observation append rechecks the reference.
- [x] 3.4 Implement `POST /api/v3/source-units/{source_instance_id}/bootstrap` for empty v3-only units. [Owner: Implementer] [Spec: atomic-source-page-ingestion ASP-07] Acceptance: SQLite/PostgreSQL atomically reject existing data/state, issue only v2 generation 1, allow exact pre-append retry, and refuse rollback to a protocol that never existed.

## 4. Validation and documentation

- [x] 4.1 Add selfhost E2E tests for valid ask_bot Slack/Google/note envelopes, mixed-page rejection, blob admission, and restart durability. [Owner: Reviewer] [Spec: M02/M03/M09/M14, all change specs] Acceptance: both configured backends pass without ignored required tests or legacy endpoint fallback.
- [x] 4.2 Update ingestion, Registry, blob, cutover, and client documentation with exact v3 request/response and failure semantics. [Owner: Implementer] [Spec: M03/M09/M14] Acceptance: documented commands use only v3 for atomic adapters and list every required header/limit.
- [x] 4.3 Run format, clippy, focused/workspace tests, dependency-layer checks, and strict OpenSpec validation. [Owner: Reviewer] [Spec: all] Acceptance: all commands are green and no v1/v2 alias or silent fallback was added.
