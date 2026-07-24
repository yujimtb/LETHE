# Production-scale memory gate (v15)

Docker-based, standalone tooling to catch the class of bug that shipped in
v15: an import-request memory leak that scales with corpus size (~1.4GiB
retained at 568k observations) and was invisible at small-corpus scale.
Run this **before** any deploy that touches the import/ingestion path.

Everything here is a self-contained Python script (`python3.14` stdlib +
`httpx`, `requests` also works). Nothing pushes to git, nothing talks to a
non-loopback host, and no `.env` value is ever printed to stdout/stderr or
written into a report.

```
pip install httpx
```

## Files

- `gate_common.py` — shared building blocks (not run directly): `.env`
  token loading, discord-message-shaped `ObservationDraft` generation for
  `POST /api/import/observation-drafts`, the import HTTP client + outcome
  tallying, and a `docker stats` RSS sampler.
- `seed_gate_corpus.py` — loads a synthetic corpus (default 568,000 items)
  into a running instance.
- `run_gate.py` — boots a memory-capped, loopback-only container from a
  *copy* of the seeded data directory, waits for it to settle, and runs
  five memory probes against it (three direct-import tests, two of them
  repeated inside a bulk-import session).

`scripts/import_memory_harness.py` is unrelated (drives a local CLI
command, not the HTTP import endpoint) and is not touched by this change.

## Usage

### 1. Seed once, keep the data directory

Point `--base-url` at a real (throwaway) self-host instance — e.g. one
started manually from `deploy/personal-lake/compose.yaml` with a scratch
`./data` volume, or any container you control on 127.0.0.1. **Never point
this at production.**

```
python seed_gate_corpus.py \
  --base-url http://127.0.0.1:8080 \
  --env-file /path/to/gate.env \
  --tag gate-2026-07-23 \
  --count 568000
```

- Progress prints once per 10,000 records.
- Fails immediately (exit 1) if any item comes back `rejected` or
  `quarantined` — seeding is expected to produce only `ingested` /
  `duplicate`.
- Idempotent: re-running with the same `--tag`/`--source-instance`/`--count`
  reproduces the same `idempotency_key`s, so a rerun lands as `duplicate`.
- At 568k items and the default 1000-item batch, expect on the order of an
  hour depending on the target instance's hardware — this is the same
  order of magnitude as the leak that motivated this gate, so budget time
  accordingly. Once seeded, **stop the instance and keep its data
  directory** (e.g. `deploy/personal-lake/data`) as the reusable
  `--data-dir` input to `run_gate.py`. `run_gate.py` always copies it into
  a scratch directory and discards the copy, so the same seed corpus can
  be reused across many gate runs.

### 2. Run the gate

```
python run_gate.py \
  --image lethe-selfhost:candidate \
  --data-dir /path/to/seeded/data \
  --config /path/to/config.toml \
  --jwks /path/to/mcp-jwks.json \
  --env-file /path/to/gate.env \
  --tag gate-2026-07-23 \
  --seed-count 568000 \
  --report gate-report.json
```

`--tag`, `--source-instance`, and `--seed-count` must match what
`seed_gate_corpus.py` was run with — the dup-only tests reconstruct the
exact same drafts by index, and the bulk-import tests need to know where
the seeded index range ends so their "new" indices don't collide with it.
`--seed-count` must be ≥ 2200: indices `[0, 800)` and `[800, 1600)` are the
test 1 / test 3 dup samples, `[1600, 2000)` is the test 1b dup sample,
`[2000, 2200)` is the warmup dup sample (see "Warmup" below), and
everything from `--seed-count` onward is used for fresh (never-seeded)
indices by tests 2, 2b, and the ACK-latency probe — each in its own
disjoint range, so a single `run_gate.py` invocation never collides with
itself and repeat runs against the same `--data-dir` are always safe (the
data dir is only ever copied, never mutated in place).

The container is published on `127.0.0.1:<port>` only (default 18098),
started with `--memory <mem-limit> --memory-swap <mem-limit>` (default
16g, swap disabled so the cap actually bounds RSS), and is always removed
in a `finally` block — including on any exception, OOM (`docker inspect`
exit code / `OOMKilled` are checked at every phase boundary), or lost
health.

Sequence: copy `--data-dir` to a scratch dir → `docker run -d --memory ...`
→ poll `/health` (up to 15 min) → poll single-item import ACK latency until
it drops below 2s, then a no-op bulk session (begin -> end) until it
succeeds (both gates share one `--ack-timeout-seconds` budget, up to 30
min total; see "Convergence is two gates" below) → warmup (100 x 2 dup
batches) → 60s baseline RSS sample → test 1 → test 1b → test 2 (batch=25
then batch=1000) → test 2b → test 3 → stop/remove container → delete
scratch data dir → write `--report` JSON → print a PASS/FAIL summary.

Any single test failing exits 1 with `"passed": false` in the report. Tests
1b and 2b are the exception: if the bulk-session API itself is unusable in
this environment (see "Bulk-session tests" below), they report
`"skipped": true, "passed": true` instead of failing the whole gate.

## What each test checks (and why)

| # | Test | What it sends | Failure signature it targets |
|---|---|---|---|
| 1 | dup-only replay | 8 × 100 already-seeded drafts (all must come back `duplicate`) | any residual growth from resending pure duplicates — should be ~flat |
| 1b | bulk session dup-only | 4 × (begin session → 100 already-seeded drafts → end session) | the pathology the sol audit traced this gate to: `.../bulk-sessions/{id}/end` runs a corpus/materialization rebuild (`bulk_import.rs::end_bulk_import_session`) that, pre-fix, scaled with corpus size even for a session that only ever imported duplicates — visible both as residual growth and as anomalously slow `end` calls (~26s/batch was the reported regression signature) |
| 2 | new bulk import | 1000 fresh drafts, once at batch=25 and once at batch=1000 (`ingested + duplicate` must total the batch — see "Retried-as-duplicate accounting" below) | a large single request/response transient spike, or steady per-request growth that isn't 100% freed after the batch |
| 2b | bulk session rebuild x2 consecutive rounds | two back-to-back rounds of 1000 fresh drafts (batch=25) each wrapped in its own begin/.../end bulk session | round 2's peak/residual are judged *against round 1's own settled plateau*, not the original baseline — a real per-invocation leak shows up as round 2 adding meaningfully more on top of round 1, not as round 1's one-time rebuild cost itself (see "Test 2b: two consecutive rebuilds, judged against each other" below) |
| 3 | slope detection | 8 more × 100 dup batches, sampling RSS after each one and regressing batch-number vs. RSS | the actual v15 bug class: growth *proportional to how many import requests have been made*, invisible in single-batch peak/residual checks but visible as a positive slope |

### Bulk-session tests (1b / 2b)

These drive `POST /api/import/bulk-sessions/begin` and
`POST /api/import/bulk-sessions/{session_id}/end`
(`apps/selfhost/src/self_host/server.rs`), both authorized under the same
`write:observations` scope as the import endpoint, with no request body.
`begin` returns a `BulkImportSessionReport` (`session_id`, `state`,
`base_append_seq`, `target_append_seq`, `target_observation_count`); the
returned `session_id` is then passed as `bulk_session_id` on
`POST /api/import/observation-drafts` for every import inside that session;
`end` takes no body and returns the same report shape with
`state = "ready"` once its (possibly expensive) rebuild has converged. Test
1b records each batch's `end` duration and treats the batch as a failure
signature if it exceeds `--bulk-session-end-latency-threshold-seconds`
(default 10s — the regression this gate was written for reproduced at
~26s/batch); those violations are listed explicitly in the report's
`end_latency_violations` field. This threshold is specifically for test
1b's dup-only sessions, which never trigger a non-corpus rebuild — it does
**not** apply to test 2b's `end`; see the next section.

If `begin` fails with an auth/scope/routing-class HTTP status (401, 403,
404, 405, 501 — see `gate_common.SESSION_API_UNAVAILABLE_STATUS_CODES`),
the affected test is marked `"skipped": true` with a `"skip_reason"` in the
report and a warning is printed to stdout — this does **not** fail the
gate.

Every `begin`/`end` call goes through the same transient-failure handling
as import sends (see "Network resilience" below): a timeout / connection
error / 5xx is retried up to `--max-consecutive-timeouts` times (observed
in practice — a `begin` call can itself time out while
`bulk_import_operation_lock` is briefly held by a concurrent end/import on
the same session). Separately, an HTTP 409 whose error code is a
known-transient conflict — `bulk_import_session_active` (most often an
earlier, client-timed-out `begin`/`end` that actually succeeded
server-side) or `bulk_import_non_bulk_projection_active` — is retried
after a fixed `--bulk-session-conflict-wait-seconds` (these codes carry no
server-provided `retry_after`, unlike HTTP 429) up to
`--max-consecutive-bulk-session-conflicts` times. Any *other* 409 (e.g.
`bulk_import_session_mismatch`, a real logic error) or other 4xx still
fails immediately, not retried. Each test's report entry includes
`session_call_transient_events` / `session_call_conflict_events` listing
every retried `begin`/`end` attempt. This is the retry policy for test
1b's `begin`/`end` and test 2b's `begin` — test 2b's `end` uses a
different, dedicated policy; see the next section.

#### Test 2b: two consecutive rebuilds, judged against each other

Test 2b's `end` is expected to legitimately trigger a corpus-scale
non-corpus rebuild (observed ~25-30min at 568k observations) —
fundamentally unlike test 1b's dup-only sessions, where a fast `end` is
itself the pass criterion. A single such rebuild costs real memory once:
observed **+3.9GiB** at 568k. Judging that one-time cost against a tight
threshold (the same one test 2's plain new-import path uses) would either
false-fail on legitimate rebuild cost or be too loose to catch a real leak
— neither is right, because a single rebuild's cost isn't the leak signal
at all.

The signal is what happens on a **second, immediately consecutive**
rebuild. Real-run evidence: round 1 raises settled RSS from baseline to
**9.386GiB**; round 2 — another full begin/1000-new-imports/end cycle
right after — settles at **9.386GiB again, bit-identical**. A single arena
holds and reuses its rebuild working set across invocations; it does not
grow it. So test 2b runs the rebuild twice back-to-back and judges round
2's peak and settled residual **against round 1's own settled plateau**,
not the original pre-test baseline:

- Round 1's own peak/residual (over the *original* baseline) are recorded
  as `first_rebuild_plateau_peak_over_baseline_mib` /
  `first_rebuild_plateau_residual_over_baseline_mib`, gated only by a
  loose runaway ceiling, `--bulk-session-first-rebuild-runaway-threshold-mib`
  (default 8192MiB / 8GiB) — large enough to never fire on a legitimate
  rebuild, only on something wildly larger.
- Round 2's peak/residual (over round 1's *own settled plateau*,
  `first_rebuild_plateau_after_mib`) are the actual leak check:
  `--bulk-session-round2-peak-threshold-mib` (default 1024MiB) and
  `--bulk-session-round2-residual-threshold-mib` (default 512MiB). The
  healthy expectation from the real-run evidence is ~0MiB; these
  thresholds bound how far round 2 may exceed round 1's plateau before
  it's read as a per-invocation leak rather than one-time arena reuse.

Each round's `end` independently goes through the same `projection_stale`
polling described below, each with its own fresh
`--bulk-end-rebuild-timeout-seconds` budget. The report's
`bulk_session_new_batch25` entry carries the round-vs-round summary at the
top level plus full per-round detail nested under `round0` / `round1`
(each including `end_rebuild_wait_seconds`, `end_rebuild_poll_count`,
`peak_over_reference_mib`, `residual_over_reference_mib`, and everything
else described in this README for a single round).

While `end` is polling through a rebuild, it can return **HTTP 503
`projection_stale`** — the server's own documented response
(`apps/selfhost/src/self_host/server.rs`'s mapping of
`SelfHostError::ProjectionStale`, via `ErrorResponse::projection_stale` in
`crates/api/src/api/envelope.rs`) for "the non-corpus rebuild this
triggered hasn't finished within its own internal wait window yet" — not
a failure. Treating that the same as a generic transient failure (capped
at `--max-consecutive-timeouts`, default 3) meant the gate gave up on a
legitimate ~30-minute rebuild after about 3 of the server's own ~60s
polling windows — roughly 3 minutes.

The fix: HTTP 503 with `error == "projection_stale"` is its own subtype
(`gate_common.ProjectionStaleError`), polled at the server's own
`retry_after` hint (always present for this code, default 30s) via a
**dedicated function** (`gate_common.wait_for_end_bulk_session_rebuild`)
with **no consecutive-failure cap** — only the dedicated
`--bulk-end-rebuild-timeout-seconds` budget (default 3600s = 1h, well
above the observed ~25-30min), separate from `--ack-timeout-seconds`,
bounds it. Any other transient failure (timeout/connection-error/other
5xx) during this same wait gets the same uncapped, budget-bounded
polling; a hard 4xx or an unrecognized 409/503 reason still fails
immediately, not retried. Each round's `end_rebuild_wait_seconds` /
`end_rebuild_poll_count` and every poll's detail
(`end_rebuild_projection_stale_events` / `end_rebuild_transient_events`)
are recorded per round. The pass criterion for each round's `end` is
simply `end_rebuild_wait_seconds <= --bulk-end-rebuild-timeout-seconds`
(which, by construction, can only be false if
`wait_for_end_bulk_session_rebuild` already raised instead of returning —
it's an explicit, self-documenting restatement of that budget in the
report, not a separate live check) — never a fixed short latency.

### Convergence is two gates, not one

`run_gate.py` waits for two conditions before any test starts, in
sequence, sharing one `--ack-timeout-seconds` budget (`wait_for_convergence()`
in `run_gate.py`):

1. Single-item import ACK latency drops below
   `--ack-latency-threshold-seconds` (backlog catch-up convergence — this
   is the original gate).
2. A no-op bulk session (`begin` immediately followed by `end`, no imports
   in between) succeeds.

(2) exists because on v15.2+, a background non-corpus rebuild can still be
running even once (1) is satisfied: a bounded-permit single import
returns in well under a second regardless of whether that rebuild has
finished, so fast ACK latency stopped implying "rebuild done". Without
(2), the gate would start test 1b — which itself drives bulk-sessions —
while the rebuild is still in progress, and every `begin`/`end` call would
get HTTP 409 (`bulk_import_non_bulk_projection_active` /
`bulk_import_session_active`) for as long as the rebuild continues, which
used to be misread as a test failure instead of "not converged yet".

While waiting on (2), a 409 with a known-transient conflict code is
retried after `--noop-session-retry-wait-seconds` (default 3s) —
deliberately **without** the `--max-consecutive-bulk-session-conflicts`
cap that applies elsewhere, since the entire point here is to wait out a
rebuild that can legitimately keep returning this 409 for a long time;
only the shared `--ack-timeout-seconds` budget (whatever (1) left of it)
bounds it. A timeout/connection-error/5xx during `begin`/`end` gets the
same "not converged yet, keep polling" treatment as the ACK probe, not a
hard failure — if `begin` itself timed out, the next attempt retries
`begin` (no session id was ever obtained); if `end` timed out, the next
attempt retries `end` on the same session id (which is idempotent for
this purpose). Once `begin`/`end` succeed, that session's id and final
`state` (expected `"ready"`) are logged, and `report.phases.convergence`
records `noop_session_wait_seconds`, `noop_session_id`,
`noop_session_state`, `noop_session_conflict_events`, and
`noop_session_transient_events`.

### Warmup (before baseline)

Mirrors `scripts/import_memory_harness.py`'s own "dup-only warmup twice,
then re-take baseline" design. The first import(s) into a freshly booted
container pay for one-time lazy initialization (search index / registry
residency, etc. — observed on the order of +1.5GiB) that has nothing to do
with any per-import leak. Without warming that up first, it lands inside
test 1's residual window and gets misread as a leak. `run_gate.py` now
sends 100 x 2 dup-only batches (indices `[2000, 2200)`) right after the
ACK-convergence wait and *before* sampling baseline RSS, recording
`rss_before_mib` / `rss_after_mib` / `one_time_init_delta_mib` in the
report's `phases.warmup` for visibility. This cannot mask a real
corpus-size-proportional leak (a one-time cost can't recur) — that class
of bug is what test 3's slope continues to catch.

### Network resilience (timeouts are not crashes)

Observed in practice: a freshly booted container doing backlog catch-up can
take 90s+ to ACK a single import. Every import send in this script — the
ACK-convergence probe and every test batch (1, 1b, 2, 2b, 3) — treats a
request timeout, connection error, or HTTP 5xx as *transient*, never as an
unhandled crash:

- In the ACK-convergence wait, a transient failure is logged
  (`gate_common.TransientImportError`, one line, no traceback) and treated
  as "not converged yet" — polling continues against the
  `--ack-timeout-seconds` budget. Only exhausting that budget is a failure.
- In test batches, a transient failure is retried on the *same* batch
  (its latency is recorded as the spike it was) up to
  `--max-consecutive-timeouts` times; if it never comes back clean, the
  gate aborts with an explicit "N consecutive transient import failures"
  reason. A clean non-2xx response other than 5xx (e.g. 400/401/403) is
  still an immediate hard failure, as is any wrong outcome (unexpected
  rejected/quarantined counts, wrong ingested/duplicate totals).

Every test's report entry includes a `transient_events` (or, for test 1b,
`import_transient_events`) list recording each transient attempt's elapsed
time, HTTP status (if any), and message, so a run that passed despite some
retries is still visible in `--report`.

#### Connection reuse is disabled (Windows Docker Desktop transport stalls)

Observed in practice: a dup-only batch whose server-side handler logged
349ms of actual work took the client 241 seconds to get a response for,
with zero server-side log activity in that ~4-minute gap — i.e. the stall
was in the transport layer, before the request ever reached the handler.
The suspected cause is Windows Docker Desktop's loopback port proxy
mishandling HTTP keep-alive connection reuse. Every request this script
makes therefore forces a fresh connection: `Connection: close` is sent on
every request (both the httpx and requests code paths), and when using
httpx the client is additionally constructed with
`httpx.Limits(max_keepalive_connections=0, max_connections=1)` so it never
tries to pool a connection for reuse in the first place. The report's
top-level `http_transport` field records that this workaround is active.
The performance cost of a fresh loopback connection per request is
negligible for this gate's purposes — correctness (not silently mistaking
a transport stall for a memory/latency finding) is what matters here.

#### The timeout → 429 orphan chain (and why raising `--probe-timeout-seconds` is correct, not lowering it)

Observed in a real 568k run: the ACK-convergence probe hit its client-side
timeout, but **the server kept processing that import after the client
gave up** — it doesn't cancel work just because the caller stopped
waiting. That orphaned request went on holding one of the server's bounded
import-permit slots (`config.limits.max_concurrent_imports`, commonly 2)
until it actually finished. The *next* probe then got HTTP 429
`import_concurrency_limit` because the permit pool was full — and, before
this fix, the gate treated any non-2xx/non-5xx status as a hard failure
and died immediately on that 429, even though nothing was actually broken.

The fix:

- HTTP 429 `import_concurrency_limit` is now its own transient subtype
  (`gate_common.ImportConcurrencyLimitError`), always retried honoring the
  server's `retry_after` hint (mirrors
  `apps/selfhost/src/self_host/import_client.rs`'s own 429 handling) —
  never a hard 4xx failure.
- It is counted **separately** from timeouts/connection-errors/5xx: a run
  of 429s does not consume the `--max-consecutive-timeouts` budget, and a
  timeout does not consume the 429 budget. In the ACK-convergence loop,
  429s are retried for as long as `--ack-timeout-seconds` allows (no
  separate cap); in test batches, `--max-consecutive-concurrency-retries`
  (default 60) caps a consecutive 429 run before the gate aborts.
- While the ACK-convergence loop is draining a 429 streak, it **does not**
  advance to a new probe (new index) — it re-polls the *same* logical
  probe at the `retry_after` cadence, so the gate itself never adds more
  orphans on top of the one it's waiting to clear.

**Because the server keeps working on an import regardless of whether the
client is still waiting, a shorter `--probe-timeout-seconds` does not make
convergence faster — it only orphans more in-flight requests, each of
which can starve the next probe with 429 until it finishes server-side.**
`--probe-timeout-seconds` defaults to 900 (15 min) for this reason; prefer
raising it over lowering it if you suspect probes are giving up too eagerly.

#### Retried-as-duplicate accounting (tests 2 / 2b)

The same orphan pattern above applies to test 2/2b's "new" batches, not
just the ACK probe: if a batch is retried by `send_drafts_with_retry`
after a transient failure, the *previous* attempt's request can have
actually completed server-side while the client gave up on it — the retry
then legitimately comes back `duplicate` for those items, not `ingested`.
Observed in a real run: `batch [568100,568125)` attempt 1 timed out after
180s; attempt 2 got `ingested=0/duplicates=25`, and the gate died with
"expected 25 ingested" even though nothing was actually wrong.

Fix: a batch of "new" (never-before-seeded) drafts is now accepted when
`ingested + duplicates == batch size` and there are no
quarantined/rejected items — `assert_new_batch_outcome()` in
`gate_common.py`. A `duplicate` outcome is only accepted when the batch
was actually retried (`attempts > 1`); a `duplicate` on the very first
attempt (no retry) is still a hard failure, since it means an index that
should never have existed already did — real seed/index pollution, not an
orphaned-retry artifact. Each test's report entry records the count of
such orphan-retry duplicates as `retried_as_duplicates`.

## Threshold arguments (all overridable)

| Argument | Default | Meaning |
|---|---|---|
| `--dup-residual-threshold-mib` | 512 | test 1: `(RSS 3min after 8×100 dup batches) − baseline` must be ≤ this |
| `--bulk-peak-threshold-mib` | 4096 (4 GiB) | test 2: `(peak RSS during the 1000-item send) − baseline` must be ≤ this, for both batch=25 and batch=1000 |
| `--bulk-residual-threshold-mib` | 768 | test 2: `(RSS 3min after the 1000-item send) − baseline` must be ≤ this, for both sub-tests |
| `--slope-threshold-mib-per-batch` | 8 | test 3: the linear-regression slope of (batch number → post-settle RSS) across 8 batches must be ≤ this MiB/batch |
| `--bulk-session-end-latency-threshold-seconds` | 10 | test 1b: max acceptable `.../bulk-sessions/{id}/end` duration per batch (dup-only sessions only — not test 2b, see below) |
| `--bulk-end-rebuild-timeout-seconds` | 3600 (1h) | test 2b (each of round 1/2 and round 2/2 independently): dedicated budget for `end` to complete while polling through HTTP 503 `projection_stale` (a legitimate corpus-scale rebuild, observed ~25-30min at 568k) — separate from `--ack-timeout-seconds`, no consecutive-retry cap |
| `--bulk-session-first-rebuild-runaway-threshold-mib` | 8192 (8 GiB) | test 2b round 1/2: loose runaway ceiling for peak/residual over the original baseline — a real rebuild legitimately costs several GiB once (observed ~3.9GiB at 568k); this is not the leak signal, round 2 below is |
| `--bulk-session-round2-peak-threshold-mib` | 1024 | test 2b round 2/2: max peak RSS over round 1's *own settled plateau* — healthy expectation is ~0MiB (observed bit-identical in one real run) |
| `--bulk-session-round2-residual-threshold-mib` | 512 | test 2b round 2/2: max settled residual over round 1's *own settled plateau* — same rationale as the peak threshold above |
| `--max-consecutive-timeouts` | 3 | tests 1/1b/2/2b/3: consecutive transient failures (timeout/connection-error/5xx, NOT counting HTTP 429) on the same batch before the gate aborts |
| `--max-consecutive-concurrency-retries` | 60 | tests 1/1b/2/2b/3: consecutive HTTP 429 `import_concurrency_limit` responses (retried honoring `retry_after`, counted separately from timeouts) on the same batch before the gate aborts. The ACK probe also retries 429s but is bounded only by `--ack-timeout-seconds`, not this count |
| `--max-consecutive-bulk-session-conflicts` | 10 | tests 1b/2b: consecutive HTTP 409 responses from bulk-session begin/end with a known-transient conflict code (counted separately from `--max-consecutive-timeouts`) before the gate aborts |
| `--bulk-session-conflict-wait-seconds` | 1.5 | fixed wait between retries of a known-transient bulk-session 409 conflict (no server-provided `retry_after` for these codes) |
| `--probe-timeout-seconds` | 900 | per-request HTTP timeout for the ACK-convergence probe (not the test batches — those use their own per-call timeouts, see source). Raising this reduces orphaned server-side work, not the other way around — see "The timeout → 429 orphan chain" above |
| `--noop-session-retry-wait-seconds` | 3.0 | wait between retries of the no-op bulk session (begin -> end) convergence gate while it gets a known-transient HTTP 409 — no consecutive-retry cap, only the shared `--ack-timeout-seconds` budget bounds it — see "Convergence is two gates" above |
| `--health-timeout-seconds` | 900 (15 min) | max wait for `/health` to return 200 |
| `--ack-timeout-seconds` | 1800 (30 min) | max wait for single-item import ACK latency to converge, **and** for the subsequent no-op bulk session to succeed — one shared budget for both gates |
| `--ack-latency-threshold-seconds` | 2.0 | the ACK latency convergence target |
| `--warmup-settle-seconds` | 10 | settle time before sampling pre-warmup RSS, ahead of the dup-only warmup that runs before baseline is sampled |
| `--baseline-settle-seconds` | 60 | idle time (and averaging window) before sampling baseline RSS, taken *after* warmup |
| `--post-batch-wait-seconds` | 180 (3 min) | settle time before residual RSS sampling in tests 1 and 2 |
| `--slope-settle-seconds` | 60 | settle time after each of test 3's 8 batches |
| `--rss-average-window-seconds` | 15 | averaging window used when reading a residual/slope RSS point, to smooth `docker stats` sampling noise |
| `--mem-limit` | 16g | `docker run --memory` / `--memory-swap` value |
| `--stats-poll-interval-seconds` | 1.0 | `docker stats` polling interval |

## Notes

- `--env-file` uses the same `KEY=value` shape as
  `deploy/personal-lake/.env.example`. Only the key name configured via
  `--token-env` (default `LETHE_API_WRITE_TOKEN`) is read; its value is
  held in memory only and is never printed, logged, or written to the
  report.
- The report JSON never contains token values, only threshold numbers,
  timings, memory readings, and pass/fail booleans.
- If a permission error occurs mounting the copied data directory into the
  container, it's a Docker Desktop / host filesystem permissions concern
  (bind mounts into a non-root container user) — not something this
  tooling controls.
