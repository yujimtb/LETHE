# Track I Handoff: Integration and E2E

Date: 2026-07-06
Status: Local integration complete; live claude.ai/Tailscale production exposure pending user-run credentials and browser flow.

## Implemented

- Added storage port methods to load all observations and supplementals for explicit projection refresh.
- Updated `POST /supplementals` so a successful write refreshes and materializes the current projection snapshot from durable storage.
- Added E2E coverage for claim write-to-read integration:
  - POST `claim@1`
  - verify it appears in `GET /projections/claim-queue?state=open`
  - POST `claim-transition@1`
  - verify the same claim appears in the changed state.
- Added E2E coverage for coding-agent decision integration:
  - import a minimal Codex JSONL through `CodexImporter`
  - find the persisted `sys:codex` observation
  - POST `decision@1` anchored to it
  - verify it is searchable via `GET /projections/decisions`.
- Generated `requirements-coverage.md` with SHALL judgement and evidence.
- Marked I1 and I2 complete in `tasks.md`; kept I3 open because live Funnel + claude.ai connector verification has not been executed.

## Changed Files

- `crates/storage/api/src/lib.rs`
- `crates/storage/sqlite/src/persistence/mod.rs`
- `apps/selfhost/src/self_host/app/service_support.rs`
- `apps/selfhost/src/self_host/app/supplemental_write.rs`
- `tests/e2e/Cargo.toml`
- `tests/e2e/tests/self_host_api.rs`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/requirements-coverage.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-i.md`

## Tests

- `cargo test -p lethe-e2e --test self_host_api -- --nocapture`: pass, 18 tests.
- `cargo test --workspace`: pass.
- `cargo fmt --all -- --check`: pass.
- `openspec validate supplemental-write-and-mcp-read --strict`: pass.
- `openspec validate --all`: pass, 13 passed / 0 failed.
- `python scripts/personal_lake_pipeline_smoke.py --work-dir <temp>`: pass, 9 synthetic observations, duplicate re-runs idempotent.
- Local selfhost W0 check against the smoke DB: pass, `/health/deep` ok and partition initialize invariants valid.

## Open Items For Production

- Create real `deploy/personal-lake/mcp-jwks.json` from the managed OAuth/OIDC provider.
- Set real MCP config values for resource URL, metadata URL, issuer, and audience.
- Run selfhost with internal API and MCP on separate ports.
- Expose only the MCP host port through Tailscale Funnel.
- Register claude.ai custom connector, complete OAuth, and call `search_lake`.

## Notes

- No live connector or Funnel check was claimed complete. The local server contract, OAuth token verification, projection-only tool reads, and write-to-read integration are covered by automated tests.
