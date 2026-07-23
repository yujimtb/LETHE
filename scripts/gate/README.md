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
  three memory probes against it.

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

The container is published on `127.0.0.1:<port>` only (default 18098),
started with `--memory <mem-limit> --memory-swap <mem-limit>` (default
16g, swap disabled so the cap actually bounds RSS), and is always removed
in a `finally` block — including on any exception, OOM (`docker inspect`
exit code / `OOMKilled` are checked at every phase boundary), or lost
health.

Sequence: copy `--data-dir` to a scratch dir → `docker run -d --memory ...`
→ poll `/health` (up to 15 min) → poll single-item import ACK latency until
it drops below 2s (up to 30 min; this waits out migration/index-rebuild
convergence on container start) → 60s baseline RSS sample → test 1 → test
2 (batch=25 then batch=1000) → test 3 → stop/remove container → delete
scratch data dir → write `--report` JSON → print a PASS/FAIL summary.

Any single test failing exits 1 with `"passed": false` in the report.

## What each test checks (and why)

| # | Test | What it sends | Failure signature it targets |
|---|---|---|---|
| 1 | dup-only replay | 8 × 100 already-seeded drafts (all must come back `duplicate`) | any residual growth from resending pure duplicates — should be ~flat |
| 2 | new bulk import | 1000 fresh drafts, once at batch=25 and once at batch=1000 (all must come back `ingested`) | a large single request/response transient spike, or steady per-request growth that isn't 100% freed after the batch |
| 3 | slope detection | 8 more × 100 dup batches, sampling RSS after each one and regressing batch-number vs. RSS | the actual v15 bug class: growth *proportional to how many import requests have been made*, invisible in single-batch peak/residual checks but visible as a positive slope |

## Threshold arguments (all overridable)

| Argument | Default | Meaning |
|---|---|---|
| `--dup-residual-threshold-mib` | 512 | test 1: `(RSS 3min after 8×100 dup batches) − baseline` must be ≤ this |
| `--bulk-peak-threshold-mib` | 4096 (4 GiB) | test 2: `(peak RSS during the 1000-item send) − baseline` must be ≤ this, for both batch=25 and batch=1000 |
| `--bulk-residual-threshold-mib` | 768 | test 2: `(RSS 3min after the 1000-item send) − baseline` must be ≤ this, for both sub-tests |
| `--slope-threshold-mib-per-batch` | 8 | test 3: the linear-regression slope of (batch number → post-settle RSS) across 8 batches must be ≤ this MiB/batch |
| `--health-timeout-seconds` | 900 (15 min) | max wait for `/health` to return 200 |
| `--ack-timeout-seconds` | 1800 (30 min) | max wait for single-item import ACK latency to converge |
| `--ack-latency-threshold-seconds` | 2.0 | the ACK latency convergence target |
| `--baseline-settle-seconds` | 60 | idle time (and averaging window) before sampling baseline RSS |
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
