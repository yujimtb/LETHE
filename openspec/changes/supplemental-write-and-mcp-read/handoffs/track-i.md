# Track I Handoff: Integration and E2E

Date: 2026-07-06
Status: Complete. Integration, public MCP/Auth0/Funnel checks, and live claude.ai, ChatGPT, Claude Code, and Codex MCP access are verified.

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
- Verified live client MCP reads:
  - claude.ai custom connector `LETHE Personal Lake`
  - ChatGPT developer-mode custom app `LETHE Personal Lake`
  - Claude Code via the connected claude.ai-scoped MCP connector using `--model opus`
  - Codex CLI `lethe-personal-lake`
- All four clients returned `result_count=1` and `first_record_id=corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345` for `search_lake(query="aquisition", source_types=["github-commit"], limit=3)`.
- Added a bootstrap repair so selfhost rebuilds and materializes projection snapshots from persisted observations/supplementals on startup, and added a regression test for persisted observation loading.
- Fixed filtered corpus grep so source-type filters are applied before trigram indexing; this prevents broad personal-lake text from timing out before a narrow type filter can reduce the candidate set.
- Added MCP read-only tool annotations and E2E coverage for them.
- Added `scripts/start_personal_lake_services.ps1` / `.cmd` and installed a Windows Startup VBS to keep the Docker selfhost and Tailscale Funnel active after login.

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
- `apps/selfhost/src/self_host/app/mod.rs`
- `apps/selfhost/src/self_host/app/tests.rs`
- `apps/selfhost/src/self_host/mcp.rs`
- `crates/api/src/api/grep.rs`
- `tests/e2e/tests/mcp_read_port.rs`
- `scripts/new_personal_lake_env.ps1`
- `scripts/start_personal_lake_services.ps1`
- `scripts/start_personal_lake_services.cmd`

## Tests

- `cargo test -p lethe-e2e --test self_host_api -- --nocapture`: pass, 18 tests.
- `cargo test -p lethe-selfhost`: pass, 28 tests.
- `cargo test -p lethe-api type_filter_is_applied_with_trigram_index`: pass.
- `cargo test -p lethe-e2e --test mcp_read_port`: pass, 4 tests.
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
- claude.ai: pass, custom connector OAuth and `search_lake` returned live projected corpus data.
- ChatGPT: pass, custom app OAuth and tool call returned live projected corpus data.
- Claude Code: pass through the claude.ai-scoped MCP connector with `--model opus`; Fable was not used.
- Codex: pass, CLI MCP call returned live projected corpus data.

## Notes

- Server contract, OAuth token verification, projection-only tool reads, write-to-read integration, Auth0 DCR, Tailscale Funnel public exposure, and four-client MCP access are verified.
- The Claude Code user-scope MCP entry remains unauthenticated because the installed CLI has no `claude mcp login` command. The connected claude.ai-scoped connector is the verified Claude Code path.
- Auth0 still warns about tenant use of Auth0-provided Google development keys. Configure tenant-owned Google OAuth credentials before treating this as production identity infrastructure.
