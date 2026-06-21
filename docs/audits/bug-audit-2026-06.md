# Bug audit report

This report lists **likely bug locations** found during a static audit of the repository.

Baseline at audit time:

- `cargo build` passed
- `cargo test` passed (`208` unit tests, `25` integration/doc tests)

That means the items below are mostly **latent bugs, behavior mismatches, silent failure paths, or test gaps** rather than currently failing assertions.

## Remediation status

All findings in this report have now been addressed in code.

- Items 1-3: fixed Slack channel mapping, cursor persistence, and real file-share blob ingestion.
- Items 4-7: fixed API `500` envelope shape, multi-source reconciliation validation, propagation error surfacing, and nested/array filtering.
- Items 8-13: fixed persistence/no-op writes, replay nondeterminism, slide-analysis persistence swallowing, and Notion lookup/header/delete error handling.
- Items 14-15: fixed panic-prone poisoned mutex handling and bootstrap-time duplicate observation suppression.
- Follow-up audit fixes: also fixed Google multi-revision snapshot miscapture, ingestion observer/source/schema contract enforcement, slide-analysis document-ID identity pollution, persisted slide-analysis bootstrap restore, analysis-refresh error swallowing, unsupported adapter incremental defaults, Slack thread-reply ingestion, and post-append durability rollback for SQLite failures.
- Verification after remediation: `cargo build --quiet && cargo test --quiet` passed.

## High-confidence findings

### 1. Slack message channel is lost in person-page output *(Fixed)*

- **Location:** `src\person_page\projector.rs:297-321`
- **Related producer:** `src\adapter\slack\mapper.rs:86-94`
- **Why it looks buggy:** The Slack adapter stores `channel_id` and `channel_name`, but the person-page projector reads `payload["channel"]`.
- **Likely effect:** Messages ingested through the real Slack adapter will show `channel = "unknown"` in person-page and timeline responses.
- **Why tests missed it:** `tests\lane_c_integration.rs:29-54` and `src\person_page\projector.rs:474-491` build handcrafted Slack observations with a `channel` field that the adapter never produces.
- **Confidence:** High

### 2. Slack cursor state stores an empty string and later reuses it as `oldest` *(Fixed)*

- **Location:** `src\self_host\app.rs:251-289`
- **Why it looks buggy:** When a sync finds no new messages, the code persists `""` with `set_state(..., latest_ts.as_deref().unwrap_or(""))`. On the next sync, that value is read back and passed to `conversations_history(..., oldest.as_deref(), ...)`.
- **Likely effect:** The next Slack poll may send `oldest=""` instead of omitting the parameter, which can produce malformed requests or undefined paging behavior.
- **Confidence:** High

### 3. Slack file-share ingestion is broken end-to-end *(Fixed)*

- **Locations:** `src\self_host\slack.rs:198-214`, `src\self_host\app.rs:257-270`, `src\adapter\slack\mapper.rs:120-124`
- **Why it looks buggy:** `file_download()` fetches `https://slack.com/api/files.info?file=...`, which returns metadata JSON, not the file bytes. On top of that, `sync_all()` never calls `file_download()` at all, and the mapper only creates attachments when `blob_ref` is already populated.
- **Likely effect:** Real Slack file shares will either lose attachments completely or, if `file_download()` ever gets wired in, persist JSON metadata bytes instead of the file body.
- **Why tests missed it:** `tests\adapter_integration.rs:355-382` injects a pre-made `blob_ref` directly into the fixture message.
- **Confidence:** High

### 4. Internal server errors are serialized as `bad_request` *(Fixed)*

- **Location:** `src\self_host\server.rs:113-140`
- **Why it looks buggy:** `ApiError::internal()` and the catch-all `From<SelfHostError>` branch both return HTTP `500`, but build the body with `ErrorResponse::bad_request(...)`.
- **Likely effect:** Clients receive `500` responses whose JSON body says `"error": "bad_request"`, making monitoring and client logic inconsistent.
- **Confidence:** High

### 5. Multi-source projection validation is narrower than the error name says *(Fixed)*

- **Location:** `src\projection\spec.rs:154-159`
- **Why it looks buggy:** The code only requires reconciliation when a spec mixes `Lake` and `SourceNative`, even though the validation error is named `MultiSourceWithoutReconciliation`.
- **Likely effect:** Specs that combine other source types (`Lake + Projection`, `Lake + Supplemental`, `Projection + Supplemental`) can pass without reconciliation even though the model suggests they should not.
- **Why tests missed it:** The current tests only exercise a subset of multi-source combinations.
- **Confidence:** High

### 6. Propagation graph errors are silently converted into a no-op *(Fixed)*

- **Location:** `src\propagation\scheduler.rs:70-72`
- **Why it looks buggy:** If `catalog.topological_order()` fails, `propagate_all()` returns `vec![]` instead of surfacing an error or marking anything unhealthy.
- **Likely effect:** A cyclic or otherwise invalid projection DAG can look like “nothing to do,” which hides real scheduler failures.
- **Confidence:** High

### 7. Filtering does not work for array roots, but responses are filtered generically *(Fixed)*

- **Locations:** `src\governance\filter.rs:67-112`, `src\self_host\app.rs:885-886`
- **Why it looks buggy:** `FilteringGate::apply_mask()` immediately returns `false` unless the current JSON node is an object. `apply_filter()` runs that logic on any serialized response, including arrays.
- **Likely effect:** Any future restricted fields that appear inside array responses or paginated array members will bypass masking.
- **Current impact:** `person_detail` is still protected because it filters a top-level object; list-style responses are the risky part.
- **Confidence:** High

### 8. Persistence uses `INSERT OR IGNORE`, so DB write failures can be silent *(Fixed)*

- **Locations:** `src\self_host\persistence.rs:54-65`, `src\self_host\app.rs:908-915`
- **Why it looks buggy:** `persist_observation()` always returns `Ok(())` even when SQLite ignored the insert because of a duplicate primary key or unique `idempotency_key`.
- **Likely effect:** The in-memory lake can accept an observation while the persisted store silently does not, creating divergence across restart boundaries.
- **Confidence:** High

### 9. Projection outputs embed wall-clock time, breaking replay determinism *(Fixed)*

- **Locations:** `src\identity\projector.rs:352-415`, `src\person_page\projector.rs:48-84`
- **Why it looks buggy:** Identity resolution sets `resolved_at = Utc::now()`, and person-page profiles set `profile_updated_at = Utc::now()`.
- **Likely effect:** Re-running the same projection inputs produces different outputs, conflicting with the repo's replay law.
- **Why tests missed it:** `tests\lane_c_integration.rs:259-281` and `src\person_page\projector.rs:633-645` only compare counts/basic names, not full output equality.
- **Confidence:** High

### 10. Slide-analysis persistence errors are ignored *(Fixed)*

- **Location:** `src\self_host\app.rs:581-589`
- **Why it looks buggy:** The results of both `core.add_supplemental(record)` and `core.ingest(draft)` are discarded with `let _ = ...`.
- **Likely effect:** Sync reports can count slide analyses as processed even when the supplemental record or analysis observation failed to store.
- **Confidence:** High

### 11. Notion lookup failures are ignored before write-back *(Fixed)*

- **Location:** `src\self_host\app.rs:635-641`
- **Why it looks buggy:** `find_existing()` errors are converted to `None` with `.ok().flatten()`.
- **Likely effect:** A transient lookup failure can turn an intended update into a create path, producing duplicate Notion pages.
- **Confidence:** High

### 12. Notion client silently substitutes invalid headers *(Fixed)*

- **Location:** `src\adapter\writeback\notion\client.rs:74-86`
- **Why it looks buggy:** Invalid bearer token or version header construction falls back to an empty auth header or a default version instead of returning an error.
- **Likely effect:** Misconfiguration becomes a confusing downstream API failure instead of an immediate, explicit startup/config error.
- **Confidence:** High

### 13. Notion block deletion failures are swallowed during stacking updates *(Fixed)*

- **Location:** `src\adapter\writeback\notion\client.rs:288-291`
- **Why it looks buggy:** The delete loop ignores every `delete_block()` error.
- **Likely effect:** Old bot-managed blocks can remain on the page while the write still reports success, causing duplicated or stale content.
- **Confidence:** High

## Medium-confidence findings

### 14. Production mutex poisoning can escalate into panics *(Fixed)*

- **Locations:** `src\governance\audit.rs:32-38`, `src\governance\audit.rs:42-52`, `src\governance\audit.rs:74-76`, `src\self_host\google.rs:242-245`, `src\self_host\google.rs:284-287`
- **Why it looks buggy:** Several non-test paths call `lock().unwrap()` directly.
- **Likely effect:** A single panic while holding these locks can permanently turn later audit emission or Google token reuse into panics rather than recoverable errors.
- **Confidence:** Medium

### 15. Bootstrap silently drops persisted duplicate observations *(Fixed)*

- **Location:** `src\self_host\app.rs:106-110`
- **Why it looks buggy:** Rehydration re-appends persisted observations with `let _ = lake.append(observation);`, discarding duplicate errors.
- **Likely effect:** If the SQLite store ever contains duplicate `idempotency_key` rows because of manual edits or previous bugs, bootstrap will silently lose data instead of surfacing corruption.
- **Confidence:** Medium

## Test gaps worth noting

These are not separate bugs, but they explain why the current suite stays green:

- `tests\adapter_integration.rs:355-382` only covers Slack file shares with a pre-injected `blob_ref`; it never exercises the real file download path.
- `tests\lane_c_integration.rs:259-281` and `src\person_page\projector.rs:633-645` treat replay as “same counts,” not “same full output.”
- `tests\self_host_api.rs:22-78` and `tests\lane_c_integration.rs:29-54` use handcrafted Slack payloads with a `channel` field that the real adapter does not emit.

## Suggested remediation order

1. Fix the Slack channel-field mismatch and the Slack cursor empty-string bug.
2. Repair Slack file-share ingestion so real files become real blobs.
3. Stop swallowing errors in propagation, slide analysis, and Notion write-back.
4. Remove wall-clock timestamps from pure projection outputs, or inject deterministic build time explicitly.
5. Tighten validation and filtering so multi-source specs and restricted arrays behave as intended.
