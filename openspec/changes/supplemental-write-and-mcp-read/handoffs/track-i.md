# Track I Handoff: Integration and E2E

Date: 2026-07-06
Status: Integration and public MCP/Auth0/Funnel checks complete; claude.ai connector UI registration pending Claude login.

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
- Marked I1, I2, and I3 complete in `tasks.md`.
- Configured the production MCP public surface:
  - `https://yujiws.tail474356.ts.net/` Funnel -> `http://127.0.0.1:8090`
  - Auth0 issuer `https://lethe-mcp.jp.auth0.com/`
  - API/audience `https://yujiws.tail474356.ts.net/mcp`
  - scope `mcp:read`
  - DCR enabled
- Verified public metadata, tokenless 401 challenge, Auth0 DCR smoke, and Auth0-issued JWT `tools/list` returning the five MCP tools.

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
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-h.md`
- `README.md`
- `docs/development/personal-lake-ingestion.md`
- `deploy/personal-lake/config.toml`
- `deploy/personal-lake/config.host.toml`
- `.gitignore`

## Tests

- `cargo test -p lethe-e2e --test self_host_api -- --nocapture`: pass, 18 tests.
- `cargo test --workspace`: pass.
- `cargo fmt --all -- --check`: pass.
- `openspec validate supplemental-write-and-mcp-read --strict`: pass.
- `openspec validate --all`: pass, 13 passed / 0 failed.
- `python scripts/personal_lake_pipeline_smoke.py --work-dir <temp>`: pass, 9 synthetic observations, duplicate re-runs idempotent.
- Local selfhost W0 check against the smoke DB: pass, `/health/deep` ok and partition initialize invariants valid.
- `tailscale funnel status --json`: pass, public HTTPS 443 proxies only to `http://127.0.0.1:8090`.
- `GET https://yujiws.tail474356.ts.net/.well-known/oauth-protected-resource`: pass, returns Auth0 issuer and MCP resource.
- `POST https://yujiws.tail474356.ts.net/mcp` without token: pass, returns 401 + `WWW-Authenticate`.
- `POST https://yujiws.tail474356.ts.net/mcp` with Auth0-issued JWT: pass, `tools/list` returns `search_lake`, `get_record`, `get_thread`, `claim_queue`, and `search_decisions`.
- Auth0 DCR smoke: pass, temporary client registered through `/oidc/register` and was deleted; no smoke client/grant remained after cleanup.

## Open Items For Claude UI

- Register claude.ai custom connector, complete OAuth, and call `search_lake`.

## Notes

- The live Claude connector UI step was attempted via Playwright, but the browser session was not logged in to Claude and redirected to `https://claude.ai/login?from=logout`.
- Server contract, OAuth token verification, projection-only tool reads, write-to-read integration, Auth0 DCR, and Tailscale Funnel public exposure are verified.
