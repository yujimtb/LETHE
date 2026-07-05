# Track H Handoff: MCP read port

Date: 2026-07-06
Status: Complete. Server/Auth0/Tailscale public surface and live claude.ai, ChatGPT, Claude Code, and Codex MCP access are verified.

## Implemented

- Added a dedicated MCP listener in the selfhost process. It binds `server.mcp_bind_addr` separately from the internal API listener and does not add `/mcp` to the internal API router.
- Added required MCP config: `server.mcp_bind_addr` plus `[mcp] resource_url`, `protected_resource_metadata_url`, `oauth_issuer`, `oauth_audience`, and `oauth_jwks_path`.
- Made config validation fail fast for missing/blank MCP config, shared internal/MCP ports, invalid metadata header value, missing JWKS, empty JWKS, duplicate `kid`, unsupported key type, and malformed EC/RSA JWK material.
- Implemented Streamable HTTP-style JSON-RPC over `POST /mcp` for `initialize`, `tools/list`, and `tools/call`; notifications return 202.
- Implemented OAuth resource-server behavior:
  - `GET /.well-known/oauth-protected-resource`
  - Bearer JWT signature validation against configured JWKS
  - issuer, `exp`, and audience validation
  - `401` with `WWW-Authenticate` for invalid tokens
  - no token issuance, no DCR, no consent UI, no fixed Bearer token, no API key auth
- Exposed exactly five read-only MCP tools:
  - `search_lake`
  - `get_record`
  - `get_thread`
  - `claim_queue`
  - `search_decisions`
- Routed all MCP tools through projection read surfaces:
  - corpus projection for `search_lake`, `get_record`, and `get_thread`
  - claim-queue projection for `claim_queue` and `search_decisions`
- Removed the earlier CLQ-06 contract stub path. `claim_queue` and `search_decisions` now call the real Track C projection APIs in-process.
- Preserved explicit errors. `RecordNotFound` and `ProjectionStale` are propagated as MCP JSON-RPC errors instead of being hidden as empty results.
- Updated deployment config and docs so Tailscale Funnel targets only the MCP host port and documents that the endpoint is reachable only while this PC, the selfhost process, and Funnel are running.
- Configured Auth0 tenant `lethe-mcp.jp.auth0.com`:
  - enabled tenant flag `flags.enable_dynamic_client_registration`
  - created API `LETHE MCP Read Port`
  - API identifier/audience `https://yujiws.tail474356.ts.net/mcp`
  - scope `mcp:read`
  - signing algorithm `RS256`
  - token lifetime `3600`
  - offline access enabled
- Downloaded the Auth0 public JWKS to local `deploy/personal-lake/mcp-jwks.json`; the file is gitignored because it is generated from the external provider and may rotate.
- Started Tailscale Funnel: `https://yujiws.tail474356.ts.net/` proxies to `http://127.0.0.1:8090`.
- Verified public metadata, public 401 challenge, DCR smoke registration, and Auth0-issued JWT `tools/list` against the public endpoint.
- Registered claude.ai custom connector `LETHE Personal Lake`, completed Auth0 OAuth, and verified `search_lake(query="aquisition", source_types=["github-commit"], limit=3)` with Claude Opus 4.8 Max. It returned `result_count=1` and `first_record_id=corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345`.
- Registered ChatGPT custom app `LETHE Personal Lake`, completed Auth0 OAuth, and verified the same `search_lake` call from ChatGPT with the same record id.
- Verified Codex CLI MCP access with the same `search_lake` call and record id.
- Verified Claude Code MCP access through the claude.ai-scoped connector with `--model opus`; Fable was not used. The user-scope Claude Code MCP entry still shows `Needs authentication` because the installed Claude Code CLI does not expose an `mcp login` command, but the active claude.ai-scoped connector is connected and usable.
- Added read-only MCP tool annotations for all five tools: `readOnlyHint=true`, `destructiveHint=false`, `idempotentHint=true`, and `openWorldHint=false`.

## Changed Files

- `Cargo.toml`
- `Cargo.lock`
- `apps/selfhost/Cargo.toml`
- `apps/selfhost/src/main.rs`
- `apps/selfhost/src/self_host/config.rs`
- `apps/selfhost/src/self_host/mod.rs`
- `apps/selfhost/src/self_host/mcp.rs`
- `apps/selfhost/src/self_host/mcp_contract.rs`
- `apps/selfhost/src/self_host/app/mod.rs`
- `apps/selfhost/src/self_host/app/projection_api.rs`
- `apps/selfhost/src/self_host/app/service_support.rs`
- `apps/selfhost/src/self_host/registry.rs`
- `tests/e2e/Cargo.toml`
- `tests/e2e/tests/mcp_read_port.rs`
- `config.example.toml`
- `.env.example`
- `deploy/personal-lake/.env.example`
- `deploy/personal-lake/compose.yaml`
- `deploy/personal-lake/config.toml`
- `deploy/personal-lake/config.host.toml`
- `deploy/personal-lake/mcp-jwks.example.json`
- `.gitignore`
- `scripts/start_personal_lake_services.ps1`
- `scripts/start_personal_lake_services.cmd`
- `scripts/personal_lake_pipeline_smoke.py`
- `README.md`
- `docs/development/personal-lake-ingestion.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-h.md`

## Tests

- `cargo fmt --all -- --check`: passed.
- `cargo test -p lethe-e2e --test mcp_read_port`: passed, 4 tests.
- `cargo test -p lethe-selfhost`: passed, 28 tests.
- `cargo test -p lethe-api type_filter_is_applied_with_trigram_index`: passed.
- `cargo test -p lethe-e2e --test self_host_api`: passed, 18 tests.
- `cargo test --workspace`: passed. One real-data Codex archive test remains ignored by design unless its environment points to a real archive.
- `python scripts\check_markdown_links.py`: failed on pre-existing archived sharding-refactor links under `openspec/changes/archive/2026-07-05-sharding-refactor`; no Track H markdown links were added that depend on those missing paths.

## Funnel, OAuth, And Mock-Key Notes

- Funnel target port: `LETHE_MCP_HOST_PORT`, default `8090`.
- Internal API port: `LETHE_HTTP_HOST_PORT`, default `8080`; this must not be exposed by Funnel.
- Docker config maps `127.0.0.1:${LETHE_MCP_HOST_PORT}:8090` and mounts `deploy/personal-lake/mcp-jwks.json` as `/etc/lethe/mcp-jwks.json`.
- Host config uses `server.mcp_bind_addr = "127.0.0.1:8090"`. Container config uses `server.mcp_bind_addr = "0.0.0.0:8090"`.
- Live resource URL: `https://yujiws.tail474356.ts.net/mcp`.
- Live protected resource metadata URL: `https://yujiws.tail474356.ts.net/.well-known/oauth-protected-resource`.
- Live OAuth issuer: `https://lethe-mcp.jp.auth0.com/`.
- Live OAuth audience: `https://yujiws.tail474356.ts.net/mcp`.
- OAuth config fields:
  - `mcp.resource_url`
  - `mcp.protected_resource_metadata_url`
  - `mcp.oauth_issuer`
  - `mcp.oauth_audience`
  - `mcp.oauth_jwks_path`
- `deploy/personal-lake/mcp-jwks.example.json` documents the required JWKS shape. The local `deploy/personal-lake/mcp-jwks.json` was generated from Auth0 and is intentionally gitignored; refresh it from `https://lethe-mcp.jp.auth0.com/.well-known/jwks.json` after Auth0 signing-key rotation.
- Mock-key test method: `tests/e2e/tests/mcp_read_port.rs` generates an ES256 P-256 key pair with `ring`, builds a matching public JWKS, signs JWTs with controlled `iss`, `aud`, and `exp`, then exercises expired, wrong-audience, and valid-token paths.

## Live Connectivity

All live client checks used the public MCP URL `https://yujiws.tail474356.ts.net/mcp` and Auth0 issuer `https://lethe-mcp.jp.auth0.com/`.

| Client | Status | Evidence |
| --- | --- | --- |
| claude.ai | Pass | Custom connector `LETHE Personal Lake` completed OAuth and returned `tool_ok=yes; result_count=1; first_record_id=corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345` for `search_lake("aquisition", ["github-commit"], 3)` using Claude Opus 4.8 Max. |
| ChatGPT | Pass | Developer-mode custom app `LETHE Personal Lake` completed OAuth and the tool call returned the same result count and record id. |
| Claude Code | Pass | `claude -p --model opus --permission-mode bypassPermissions ...` called the connected claude.ai-scoped MCP connector and returned the same result count and record id. |
| Codex | Pass | `codex exec --sandbox read-only ...` called `lethe-personal-lake` and returned the same result count and record id. |

## SHALL Evidence

| Requirement | SHALL | Evidence |
| --- | --- | --- |
| MCPR-01 | MCP must run in the same selfhost process on a dedicated listener and must not share the internal API port. | `apps/selfhost/src/main.rs` binds both listeners; `config.rs` rejects shared ports; `mcp_and_internal_api_routes_are_separate` verifies `/mcp` is absent from the internal router and internal routes are absent from MCP. |
| MCPR-01 | Funnel exposes only the MCP port. | `tailscale funnel status --json` showed `yujiws.tail474356.ts.net:443` proxying `/` only to `http://127.0.0.1:8090`; public `https://yujiws.tail474356.ts.net/health/deep` returned 404. `deploy/personal-lake/compose.yaml`, `README.md`, and `docs/development/personal-lake-ingestion.md` document `LETHE_MCP_HOST_PORT` as the only Funnel target and prohibit exposing internal/admin API. |
| MCPR-02 | MCP endpoint must use Streamable HTTP rather than SSE-only transport. | `apps/selfhost/src/self_host/mcp.rs` implements JSON-RPC over `POST /mcp` and supports `initialize`, `tools/list`, and `tools/call`; `five_mcp_tools_have_contracts_and_read_via_projection` exercises tool calls over HTTP. |
| MCPR-03 | Protected resource metadata must be public and include the managed issuer. | `protected_resource_metadata_contract_is_public` verifies `/.well-known/oauth-protected-resource`; live public metadata returned Auth0 issuer `https://lethe-mcp.jp.auth0.com/` and resource `https://yujiws.tail474356.ts.net/mcp`. |
| MCPR-03 | Bearer JWT signature, `exp`, and audience must be verified; invalid tokens return 401 with `WWW-Authenticate`. | `mcp_jwt_validation_rejects_expired_and_wrong_audience_and_accepts_valid` covers expired, wrong audience, and valid JWT paths. Live public `/mcp` without token returned 401 + `WWW-Authenticate`; Auth0-issued JWT for audience `https://yujiws.tail474356.ts.net/mcp` returned `tools/list`. |
| MCPR-03 | Token issuance, DCR, consent UI, and fixed API key auth must not be implemented. | MCP router has only metadata and `/mcp` routes; auth path accepts only Bearer JWTs validated against JWKS. |
| MCPR-04 | Exactly five read-only tools are exposed and no write tool is present. | `tools/list` contract in `five_mcp_tools_have_contracts_and_read_via_projection` asserts the five names, descriptions, and read-only, non-destructive, idempotent, closed-world annotations. |
| MCPR-04 | Tools read only projection outputs, not raw supplemental or raw observation stores. | MCP calls `AppService::corpus_*`, `claim_queue_response_filtered`, and `decision_search_response`; tests seed observations/supplementals then assert projected tool responses. No MCP tool directly accesses raw stores. |
| MCPR-04 | `claim_queue` supports state and `verification_mode` filtering in same-origin group shape. | `five_mcp_tools_have_contracts_and_read_via_projection` calls `claim_queue` with `verification_mode = "generate"` and asserts the projected group response. |
| MCPR-04 | `search_decisions` reads supersedes-resolved decision ledger projection. | `five_mcp_tools_have_contracts_and_read_via_projection` calls `search_decisions`; `claim_queue_api_filters_pages_and_searches_decisions` covers the underlying Track C projection behavior. |
| MCPR-04 Failure Modes | `RecordNotFound` and `ProjectionStale` are not hidden as empty results. | MCP error mapping returns explicit `RecordNotFound` and `ProjectionStale` JSON-RPC errors; `five_mcp_tools_have_contracts_and_read_via_projection` verifies a missing record is an error. |
| MCPR-05 | Personal lake corpus covers all text-bearing personal sources. | Track H consumes the Track G corpus projection; `personal_corpus_grep_hits_all_text_source_types` remains passing. |
| MCPR-06 | Docs state that MCP is reachable only while the PC is running. | `README.md` and `docs/development/personal-lake-ingestion.md` explicitly state the PC/selfhost/Funnel runtime constraint and no 24/7 SLA. |

## Open Items

- No Track H implementation items remain.
- No CLQ-06 MCP stubs remain. Integration should treat `claim_queue` and `search_decisions` as real Track C projection consumers.
- Operational follow-up: Auth0 still warns about using Auth0-provided Google development keys. Configure a tenant-owned Google OAuth client before treating this as a production identity setup.
