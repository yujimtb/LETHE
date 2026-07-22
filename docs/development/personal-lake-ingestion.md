# Personal Lake Ingestion

This document covers the `personal-lake-ingestion` OpenSpec change.

## Instance

The personal lake is a separate self-host instance. It uses local Docker bind
storage and does not share SQLite or blob state with the dormitory lake.

Config files:

- `deploy/personal-lake/compose.yaml`
- `deploy/personal-lake/config.toml` for Docker
- `deploy/personal-lake/config.host.toml` for offline maintenance and recovery
- `deploy/personal-lake/.env.example`

The personal config pins:

- `retention_days = 3650`
- empty `sources`
- scoped API tokens even on localhost
- routing order `year_month_source_container_published`
- `supplemental.reject_unregistered_kinds = true`
- `corpus.mode = "personal_all_text"` so every text-bearing personal
  observation is searchable through the corpus projection
- source freshness thresholds for `sys:claude-ai`, `sys:chatgpt`,
  `sys:claude-code`, and `sys:codex`
- `ops.backfill_nightly_budget_items` for the nightly backfill budget
- `LETHE_API_READ_TOKEN` includes `read:corpus`
- `LETHE_API_WRITE_TOKEN` includes `write:supplemental` and
  `write:observations`
- a separate MCP listener on port `8090`
- communication channels are declared explicitly under `[[channels]]`; the
  checked-in personal configs currently enable the Discord
  `chan:discord-primary:1507676023314059275` channel and keep Slack/Gmail absent
  until live ingress is configured
- communication freshness thresholds use channel ids such as
  `chan:slack-primary:C01234567`, not raw source-system ids

Generate environment values without writing secrets to the repository:

```powershell
./scripts/new_personal_lake_env.ps1
```

Store the generated encryption key in the password manager before using it in
`deploy/personal-lake/.env`. Losing the key makes encrypted local secrets
unrecoverable.

On Windows, generate a new storage key, store it in Windows Credential Manager,
and set the user/process environment without printing the key:

```powershell
./scripts/install_personal_lake_key_windows.ps1 `
  -CredentialTarget LETHE_PERSONAL_LAKE_STORAGE_ENCRYPTION_KEY `
  -CredentialUser LETHE_STORAGE_ENCRYPTION_KEY `
  -EnvironmentVariableName LETHE_STORAGE_ENCRYPTION_KEY
```

Start the instance:

```powershell
docker compose --env-file deploy/personal-lake/.env -f deploy/personal-lake/compose.yaml up --build
```

`LETHE_HTTP_HOST_PORT` and `LETHE_MCP_HOST_PORT` are required in
`deploy/personal-lake/.env`. Use different host ports when another local service
already owns `127.0.0.1:8080` or `127.0.0.1:8090`.
The 2026-07-05 compose verification used `LETHE_HTTP_HOST_PORT=18080` because
another WSL Docker container already owned `127.0.0.1:8080`.
The same stack was also verified through Docker Desktop 4.80.0 on Windows with
`LETHE_HTTP_HOST_PORT=18081`.

For the standing local service, use the checked-in launcher instead of starting
Compose by hand:

```powershell
./scripts/start_personal_lake_services.cmd
```

The launcher requires `deploy/personal-lake/.env`, reads the required
`LETHE_MCP_HOST_PORT` from that file, starts the Docker Compose service detached
with build, and then starts Tailscale Funnel against that port. On this host a
Windows Startup shortcut script is installed at
`C:\Users\mitob\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\LETHEPersonalLakeServices.vbs`;
it runs `scripts\start_personal_lake_services.cmd` hidden after user login.
Keep the `shell.Run` argument on one line; splitting the quoted command path
across lines makes Windows Script Host fail with `Unterminated string
constant`.

```vbscript
Set shell = CreateObject("WScript.Shell")
shell.Run """D:\userdata\docs\projects\skcollege_database\scripts\start_personal_lake_services.cmd""", 0, False
```

This keeps the personal lake and Funnel up across normal Windows logins, but it
still depends on Windows, Docker Desktop, and Tailscale being signed in and able
to start.

The Docker image includes the standing service plus the import CLIs for
Claude.ai, ChatGPT, Claude Code, Codex, and GitHub. Normal imports must keep the
selfhost running and send draft observations to the online API endpoint
`POST /api/import/observation-drafts` with a token that has
`write:observations`. The selfhost remains the only SQLite writer and performs
dedupe, audit, projection rebuild, and materialization inside the running
service. Do not write directly to `deploy/personal-lake/data/lethe.sqlite3` while
the container is running; direct SQLite access is reserved for explicit offline
maintenance or recovery.
Each import CLI supports `--help` / `-h`; use it before running a new source to
confirm the required arguments and the token environment variable expected by
`--api-token-env`.

### Ingestion API contract versions

The existing /api/import/observation-drafts endpoint is the frozen v1
contract. Existing clients, including nanihold_intercom and Nanihold_OS,
continue to use it without a client-side change; its response and
request-level failure semantics are not silently changed.

New clients should use
POST /api/v2/import/observation-drafts. v2 returns one results item for
each draft in input order and always uses HTTP 200 for partial success. Each
item has client_ref (or its input index), outcome, and the relevant
observation_id, existing_id, ticket, failure_class, error_code, and reason.
A durable ingested or duplicate result is an ACK for the canonical ledger;
projection and search-index catch-up health does not reverse that outcome.

For v2, clients must keep source_instance_id, meta.object_id,
meta.canonical_json, and the server-derived idempotency_key fixed across
retries. The server derives
source_instance_id:object_id:sha256(canonical_json) and rejects a mismatched
key with identity_mismatch. published is event metadata and must not be used
as a retry identity or routing uniqueness key. The request body limit is
128 MiB, the configured payload limit defaults to 1 MiB, and the configured
page limit defaults to 500 for the personal lake. Limit errors include both
the actual value and the applied maximum.

Quarantine error codes are classified from the typed quarantine cause, not
from the display text in `ticket.reason`: future timestamps use
`clock_skew_future`, policy denial or review uses `policy_quarantine`, and
other quarantine causes use `quarantine_required`. The ticket reason remains
human-readable and may change without changing the wire taxonomy.

The acceptance regression suite covers schema-v8 backfill of duplicate
identity rows across leaves (the smallest `append_seq` wins), v2 retry
deduplication after an event-time routing change across leaves, v2 canonical
collision responses (`quarantined` + ticket + `existing_id`), and HTTP-level
clock-skew/policy quarantine classification.

The 2026-07-06 production rebuild used:

```powershell
docker compose --env-file deploy/personal-lake/.env -f deploy/personal-lake/compose.yaml up -d --build
```

The rebuilt release image included `lethe-import-chatgpt`, recreated
`personal-lake-lethe-selfhost-1`, and passed `/health/deep` after startup.
After a freshness projection fix, the image was rebuilt and redeployed again.
Production no-op import reruns against the online API reported GitHub
`duplicates=160`, claude.ai `duplicates=365`, Claude Code `duplicates=639`,
and Codex `duplicates=11644`, all with `ingested=0` and `quarantined=0`.
ChatGPT was not run because the archive `chatgpt/` directory did not yet contain
a JSON export.

For host-run imports, load or set the required environment variables first and
fail immediately if they are missing:

```powershell
if ([string]::IsNullOrWhiteSpace($env:LETHE_HTTP_HOST_PORT)) { throw "LETHE_HTTP_HOST_PORT is required" }
if ([string]::IsNullOrWhiteSpace($env:LETHE_API_WRITE_TOKEN)) { throw "LETHE_API_WRITE_TOKEN is required" }
$baseUrl = "http://127.0.0.1:$($env:LETHE_HTTP_HOST_PORT)"
$apiTokenEnv = "LETHE_API_WRITE_TOKEN"
```

Deep health:

```powershell
Invoke-RestMethod -Headers @{ Authorization = "Bearer $env:LETHE_API_SYNC_TOKEN" } `
  http://127.0.0.1:8080/health/deep
```

Human-readable status summary:

```powershell
./scripts/lethe_status.ps1 `
  -BaseUrl http://127.0.0.1:8080 `
  -TokenEnv LETHE_API_SYNC_TOKEN `
  -ReadTokenEnv LETHE_API_READ_TOKEN
```

Gate W0 check:

```powershell
python ./scripts/personal_lake_w0_check.py `
  --config deploy/personal-lake/config.toml `
  --db deploy/personal-lake/data/lethe.sqlite3 `
  --base-url http://127.0.0.1:8080 `
  --api-token-env LETHE_API_SYNC_TOKEN
```

The script verifies `/health/deep`, the year-first routing keyspec recorded in
`partition_log`, the single-initialize index, and the append-only triggers.

## MCP Read Port

The MCP listener is configured by `server.mcp_bind_addr` and `[mcp]` in
`deploy/personal-lake/config.toml`. It is separate from the internal API
listener. Do not add `/mcp` to the internal API port.

OAuth is delegated to a managed ID provider. LETHE is only the resource server:
it publishes `/.well-known/oauth-protected-resource` and validates Bearer JWT
signature, `exp`, issuer, audience, and authorization grants against
`oauth_jwks_path`. Grants are read from the JWT `scope` claim and from Auth0's
RBAC/API `permissions` claim. It does not issue tokens, implement DCR, exchange
refresh tokens, show consent screens, or accept fixed API keys.

Expired or rejected access tokens return a `WWW-Authenticate` challenge with
the protected resource metadata URL and `scope="mcp:read write:supplemental"`.
MCP clients that already hold a refresh token should use the Auth0 token
endpoint to obtain a new access token. To make that possible, enable Allow
Offline Access on the Auth0 API, enable the Refresh Token grant and rotation for
the client/application, and ensure the client requests `offline_access` during
authorization. Do not add `offline_access` to LETHE's protected resource
`scopes_supported`; it is an authorization-server scope, not a LETHE resource
permission.

Create `deploy/personal-lake/mcp-jwks.json` from the provider JWKS before
starting the Docker stack. `deploy/personal-lake/mcp-jwks.example.json` shows
the expected file shape. `oauth_issuer` and `oauth_audience` must match the
managed provider configuration.

Current production values for this personal lake:

- MCP URL: `https://yujiws.tail474356.ts.net/mcp`
- protected resource metadata: `https://yujiws.tail474356.ts.net/.well-known/oauth-protected-resource`
- path-specific protected resource metadata: `https://yujiws.tail474356.ts.net/.well-known/oauth-protected-resource/mcp`
- Auth0 issuer: `https://lethe-mcp.jp.auth0.com/`
- Auth0 API identifier / LETHE `oauth_audience`: `https://yujiws.tail474356.ts.net/mcp`
- scopes: `mcp:read` for read tools; `write:supplemental` is additionally
  required for `write_supplemental`

`deploy/personal-lake/mcp-jwks.json` is generated local configuration and is
gitignored. Refresh it from
`https://lethe-mcp.jp.auth0.com/.well-known/jwks.json` whenever Auth0 rotates
signing keys, then restart selfhost so the in-process verifier reloads the
JWKS.

Tailscale Funnel must target only `LETHE_MCP_HOST_PORT`; do not funnel
`LETHE_HTTP_HOST_PORT` or any admin/internal API port. This endpoint is reachable
only while this PC is on, Tailscale Funnel is active, and the selfhost process is
running. It is not a 24/7 service.

The current Funnel command/state is:

```powershell
tailscale funnel --bg --yes 8090
tailscale funnel status --json
```

The expected public proxy is `https://yujiws.tail474356.ts.net/` to
`http://127.0.0.1:8090`. Public `/health/deep` should return 404 because the
internal API router is not exposed on the MCP listener.

Live client setup and verification:

- claude.ai: add custom connector `LETHE Personal Lake` with URL
  `https://yujiws.tail474356.ts.net/mcp`, leave OAuth Client ID/Secret blank so
  Claude uses Auth0 DCR, complete the Auth0 OAuth flow, then enable the
  connector in a conversation.
- ChatGPT: enable Developer mode, create custom app `LETHE Personal Lake` with
  MCP URL `https://yujiws.tail474356.ts.net/mcp`, select OAuth, acknowledge the
  custom-action risk prompt, and complete the Auth0 OAuth flow. After MCP tool
  descriptors or scopes change, open the app settings, select the draft app
  details, run `Refresh`, then `Reconnect`; the Auth0 consent page must include
  `mcp:read`, `write:supplemental`, and `offline_access`.
- Codex: configure MCP server `lethe-personal-lake` with URL
  `https://yujiws.tail474356.ts.net/mcp` and complete the OAuth flow.
- Claude Code: use the claude.ai-scoped connector `LETHE Personal Lake`.
  `claude mcp login "claude.ai LETHE Personal Lake"` can reauthorize it from the
  CLI; `claude mcp list` must show that connector as `Connected`.

The 2026-07-06 live verification query for all four clients was:

```text
search_lake(query="aquisition", source_types=["github-commit"], limit=3)
```

Each client returned `result_count=1` and
`first_record_id=corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345`.
Claude Code was tested with `--model opus`; no Interface Pilot was used.

The 2026-07-08 live reauthorization and verification query was:

```text
search_lake(query="aquisition", source_types=["github-commit"], limit=1)
```

Auth0 `Default Permissions for third-party applications` was updated to grant
both `mcp:read` and `write:supplemental`, so newly registered DCR clients request
both resource scopes before `offline_access`. Unused DCR clients created during
failed consent attempts were deleted, leaving the tenant at 9/10 applications.
Live evidence:

- claude.ai web connector returned
  `corpus:github-commit:019f35ff-3750-7721-8748-326adacde778`.
- ChatGPT.com custom app returned
  `corpus:github-commit:019f35ff-3750-7721-8748-326adacde778`.
- Claude Code `claude mcp list` showed
  `claude.ai LETHE Personal Lake ... Connected`, and
  `claude -p --model sonnet --allowedTools mcp__claude_ai_LETHE_Personal_Lake__search_lake`
  returned `corpus:github-commit:019f35ff-3750-7721-8748-326adacde778`.
- Codex CLI `codex exec` called `lethe-personal-lake/search_lake` and returned
  `corpus:github-commit:019f35ff-3750-7721-8748-326adacde778`.

The five read tools advertise read-only annotations:
`readOnlyHint=true`, `destructiveHint=false`, `idempotentHint=true`, and
`openWorldHint=false`. `write_supplemental` is the only write tool; it advertises
`readOnlyHint=false`, requires `write:supplemental`, and uses the same registry
schema and anchor validation path as `POST /supplementals`. The tool is only for
post-processing records already ingested into the lake. It must reject missing
or unresolved anchors for anchor-required kinds.
MCP read tools that accept `limit` cap it at 20 for response-size safety. When a
client requests a larger value, the tool result includes
`_meta["lethe/response_limit"]` with requested, effective, max, and clamped
fields. `search_lake` snippets are capped at 240 characters including ellipses,
and `matched_ranges` is capped at 20 ranges per record. `search_lake` also
accepts `from` / `to` ISO 8601 timestamps and `order =
"newest_first" | "oldest_first"`; invalid time ranges and unknown
`source_types` fail fast. The tool description lists valid source_types, and
successful search results include `_meta["lethe/available_source_types"]` with
live corpus counts. Search matches expose `thread_key` at top level and trim
internal plumbing fields from MCP search metadata; use `get_record` when full
record metadata is needed. `matched_ranges.start/end` are UTF-8 byte offsets.
MCP `get_thread` defaults to 20 records and returns `next_cursor` for
continuation.

Browser-use production verification on 2026-07-06 confirmed that the public
protected-resource metadata advertises both `mcp:read` and
`write:supplemental`, and that public `/health/deep` returns 404 because Funnel
targets only the MCP listener. Public read works from Claude and ChatGPT custom
clients. Claude returned `tool_ok="yes"`, `result_count=10`, and
`source_types_seen=["codex"]` for
`search_lake(query="。", source_types=["codex","claude-code","claude-ai"], limit=10)`.
ChatGPT returned `result_count=1` and
`first_record_id=corpus:github-commit:019f35ff-3750-7721-8748-326adacde778`
for the `aquisition` GitHub commit query.

Public write status:

- 2026-07-06: Claude exposed `write_supplemental`, but the approved call
  returned `{"error":"Error occurred during tool execution","request_id":"req_011CckfUfezTrCZsvuUWXyN5"}`.
  ChatGPT reported that `write_supplemental` was unavailable in the
  `LETHE_Personal_Lake` read-only tool. The same payload succeeded through the
  internal HTTP API, creating `sup:71591976-99db-4c29-bf71-c2c756d41c5f` and
  terminating it with `sup:cd488fa0-248e-4d0a-a4e3-b29c44853332`.
- 2026-07-07: the Auth0 tenant in use is `lethe-mcp.jp.auth0.com`. API
  `LETHE MCP Read Port` uses identifier
  `https://yujiws.tail474356.ts.net/mcp`, exposes `mcp:read` and
  `write:supplemental`, has Dynamic Client Registration enabled, allows
  offline access, and uses a domain-level `google-oauth2` connection for
  third-party Claude clients.
- 2026-07-07: Claude DCR created client `tpc_11NbEAfZ19vHyL5bGG1eL6`; Auth0
  API Access grant `cgr_qOVeYy4ndc50ZjnQ` gives that client 2/2 user-delegated
  LETHE MCP permissions. Auth0 consent showed `mcp:read` and
  `write:supplemental`.
- 2026-07-07: browser-use completed a live Claude connector smoke. Claude wrote
  claim `sup:86eea51a-03d4-4fa8-b241-3de111ed0ffb`, observed it in
  `claim_queue(state="open")`, wrote transition
  `sup:ad779751-43ec-4172-99b6-7b63040b4941`, and observed the claim in
  `claim_queue(state="terminated")`. Local verification found both records in
  SQLite, and `GET /projections/claim-queue?state=terminated&limit=20` returned
  the claim as `terminated`, transition
  `sup:ad779751-43ec-4172-99b6-7b63040b4941`, `stale=false`, and
  `built_at=2026-07-06T16:33:19.160389651Z`.
- 2026-07-07: rebuilt and restarted the Docker selfhost image after adding
  refresh-token support glue on the resource server side, then switched runtime
  config and JWKS back to `lethe-mcp.jp.auth0.com`. Local and public
  `/.well-known/oauth-protected-resource/mcp` return issuer
  `https://lethe-mcp.jp.auth0.com/`, resource
  `https://yujiws.tail474356.ts.net/mcp`, and scopes `mcp:read` /
  `write:supplemental`. Local and public tokenless `POST /mcp` return 401 with
  `WWW-Authenticate: Bearer ... scope="mcp:read write:supplemental"`. Auth0
  OIDC discovery advertises `offline_access` and `refresh_token`; the Auth0 API
  has `allow_offline_access=true` and scopes `mcp:read` / `write:supplemental`.
- 2026-07-08: Auth0 third-party default permissions were changed from
  `mcp:read` only to `mcp:read` plus `write:supplemental`; this prevents new
  Claude.ai / Codex DCR consent flows from silently dropping the write scope.
  Claude.ai, ChatGPT.com, Claude Code, and Codex CLI were rechecked against the
  public MCP endpoint and all returned live `search_lake` data.
- 2026-07-08: ChatGPT.com app settings `Refresh` returned six actions including
  `write_supplemental` as a write action with required OAuth scope
  `write:supplemental`. Because ChatGPT warned that enabled actions may require
  reconnecting before they are callable, `Reconnect` was completed and Auth0
  consent showed `mcp:read`, `write:supplemental`, and `offline_access`.
  A live ChatGPT smoke wrote decision
  `sup:beaf7489-61dd-48bb-8015-068390fb5cc5`, anchored to observation
  `019f35ff-3750-7721-8748-326adacde778`, with statement
  `ChatGPT write_supplemental smoke 2026-07-07T16:03:29Z`; the same ChatGPT
  conversation then found it through `search_decisions`, and Codex MCP
  verification also returned the persisted decision.

ChatGPT.com write is now verified. Do not weaken LETHE's
`write:supplemental` check to `mcp:read`; MCPW-03 requires read-only tokens to
be rejected for writes.

Scheduled reauthentication:

- `scripts/reauthorize_lethe_mcp.ps1` opens a reauthentication note, ChatGPT,
  Claude connector settings, and a visible PowerShell terminal that starts
  `codex mcp login lethe-personal-lake` and
  `claude mcp login "claude.ai LETHE Personal Lake"`.
- Windows Task Scheduler task `LETHE MCP Reauth Idle Precheck` is registered for
  2026-07-22 09:00 JST, one day before the expected ChatGPT/Codex idle
  refresh-token expiry on 2026-07-23 JST.
- Windows Task Scheduler task `LETHE MCP Reauth Absolute Renewal` is registered
  for 2026-08-06 09:00 JST, one day before the expected ChatGPT/Codex absolute
  refresh-token expiry on 2026-08-07 JST.
- The tasks run only in the interactive user session because Auth0 consent must
  be reviewed by the user. They do not attempt to click consent automatically.
- Re-register the same tasks with:

```powershell
.\scripts\register_lethe_mcp_reauth_tasks.ps1 `
  -IdlePrecheckAt '2026-07-22T09:00:00' `
  -AbsoluteRenewalAt '2026-08-06T09:00:00'
```

## Claude.ai

Request the export from claude.ai outside LETHE. After receiving the zip,
commit it into a private source archive repository:

```powershell
./scripts/archive_claude_export.ps1 `
  -ZipPath C:\path\to\claude-export.zip `
  -ArchiveRepo C:\path\to\private-claude-archive
```

The script expands the zip into conversation files, initializes the git
repository if needed, stages `conversations/`, and commits. The archive is
source input for ingest only; projections and APIs must not read from it.
The expander stores only conversation-shaped JSON entries. It explicitly skips
known claude.ai metadata entries such as `users.json` and `memories.json`, and
fails on unknown JSON entries instead of silently ignoring them.

Import the same zip into the lake:

```powershell
cargo run -p lethe-import-claude -- `
  --zip=C:\path\to\claude-export.zip `
  --source-instance=claude-personal `
  --base-url=$baseUrl `
  --api-token-env=$apiTokenEnv
```

Re-running the same command should report duplicates for unchanged messages.

To archive, import, re-import, and run the Claude count sanity check in one
operation after a real export arrives:

```powershell
./scripts/run_claude_personal_lake_import.ps1 `
  -ZipPath C:\path\to\claude-export.zip `
  -ArchiveRepo C:\path\to\private-claude-archive `
  -ConversationDir conversations `
  -CommitMessage "Archive claude.ai export" `
  -DatabasePath deploy/personal-lake/data/lethe.sqlite3 `
  -BaseUrl $baseUrl `
  -ApiTokenEnv $apiTokenEnv `
  -SourceInstance claude-personal
```

The script requires the environment variable named by `-ApiTokenEnv` to already
be set. It fails if the second import is not a complete no-op.

For the daily browser-assisted path, Claude sends a download link by email
instead of returning a zip directly from the export request. The job therefore
does all four steps: request the export in Claude, poll Gmail for the Anthropic
download email, download the zip, then call the same archive/import wrapper.
The browser profile must already be authenticated to both claude.ai and the
Gmail account that receives the export link.

The browser script requires Node and a `NODE_PATH` that resolves `playwright`.
On this host, the currently available Playwright package is the one bundled
under the global `@playwright/mcp` install:

```powershell
$playwrightNodeModules = Join-Path (npm root -g) "@playwright\mcp\node_modules"
```

Run the daily wrapper manually:

```powershell
./scripts/run_claude_personal_lake_daily_export.ps1 `
  -EnvFile deploy/personal-lake/.env `
  -ArchiveRepo D:\userdata\docs\private\claude-source-archive `
  -ConversationDir conversations `
  -DatabasePath deploy/personal-lake/data/lethe.sqlite3 `
  -BaseUrl $baseUrl `
  -ApiTokenEnv LETHE_API_WRITE_TOKEN `
  -SourceInstance claude-personal `
  -BrowserProfileDir C:\Users\mitob\AppData\Local\ms-playwright-mcp\mcp-chrome-a8ac35d `
  -DownloadDir .playwright-mcp `
  -ReportDir deploy/personal-lake/data/job-reports `
  -PlaywrightNodeModulesPath $playwrightNodeModules `
  -ExportPeriod "30 days" `
  -BrowserTimeoutMinutes 45 `
  -RequireFreshConversation
```

The `-RequireFreshConversation` check is for acceptance/manual runs where a
conversation from the last 24 hours is expected. Do not enable it on the daily
scheduled task unless daily Claude usage is guaranteed.

Register the daily task:

```powershell
./scripts/register_claude_personal_lake_daily_export.ps1 `
  -TaskName "LETHE Claude Personal Lake Daily Export" `
  -EnvFile deploy/personal-lake/.env `
  -ArchiveRepo D:\userdata\docs\private\claude-source-archive `
  -ConversationDir conversations `
  -DatabasePath deploy/personal-lake/data/lethe.sqlite3 `
  -BaseUrl $baseUrl `
  -ApiTokenEnv LETHE_API_WRITE_TOKEN `
  -SourceInstance claude-personal `
  -BrowserProfileDir C:\Users\mitob\AppData\Local\ms-playwright-mcp\mcp-chrome-a8ac35d `
  -DownloadDir .playwright-mcp `
  -ReportDir deploy/personal-lake/data/job-reports `
  -PlaywrightNodeModulesPath $playwrightNodeModules `
  -ExportPeriod "30 days" `
  -DailyAt "03:30" `
  -BrowserTimeoutMinutes 45
```

On 2026-07-07 this registered Windows Task Scheduler task
`LETHE Claude Personal Lake Daily Export`, state `Ready`, next run
`2026-07-07 03:30:00`. It uses the browser-use Chrome profile that was already
authenticated during implementation. If that MCP-managed profile is deleted or
locked by a simultaneous browser-use session, create a dedicated Chrome profile,
sign it in to Claude and Gmail once, then update the task's
`-BrowserProfileDir`.

Slack failure notification is implemented but not active in the registered
task because `LETHE_EXPORT_FAILURE_SLACK_WEBHOOK_URL` is not present in
`deploy/personal-lake/.env` or the process environment. On 2026-07-07, browser
setup created Slack app `LETHE Personal Lake Alerts` in the SHIMOKITA COLLEGE
workspace (app id `A0BFKEVERS8`) and submitted the incoming-webhook install
request for private channel `999_非公開緑地` (`C03L75JL6RM`). The workspace
requires admin approval, so no webhook URL has been issued. After approval,
store the incoming-webhook URL in `LETHE_EXPORT_FAILURE_SLACK_WEBHOOK_URL` and
re-register the task with
`-NotifyOnFailure -SlackWebhookEnvName LETHE_EXPORT_FAILURE_SLACK_WEBHOOK_URL`.
The notification script fails fast when the webhook variable is missing; it does
not silently continue without notification.

The 2026-07-05 real export run used:

- zip: `D:\mitob\Downloads\data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783183126-446768f9-batch-0000.zip`
- archive repo: `D:\userdata\docs\private\claude-source-archive`
- archive commit: `475089a Archive claude.ai export 2026-07-05`
- expanded conversations: 35
- first import: `ingested=365`, `duplicates=0`, `quarantined=0`
- second import: `ingested=0`, `duplicates=365`, `quarantined=0`
- Claude sanity: `expected=365`, `actual=365`

The same run fixed real export parser coverage for `chat_messages`,
design-chat `role`/nested `content`, and missing-parent message branches.

The 2026-07-07 browser-use production export used:

- request: Claude Settings -> Privacy -> Export data -> Export
- email: Anthropic `Your data is ready for download` received at
  2026-07-07 00:38 JST
- zip:
  `data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783352287-aced0e5a-batch-0000.zip`
- archive commit: `6eaae97 Archive claude.ai export 2026-07-07`
- expanded conversations: 41
- archive evidence: today's connector write test conversation
  `conversations/bc804247-0bf4-41c3-984b-0594e83016a2.json`
- first import: `ingested=106`, `duplicates=365`, `quarantined=0`
- second import: `ingested=0`, `duplicates=471`, `quarantined=0`
- Claude sanity: `expected=471`, `actual=471`
- freshness after import: `sys:claude-ai=fresh`,
  `latest_published=2026-07-06T15:34:39.944918Z`
- deep health after import: `status=ok`, all projections healthy

## Claude Code

Claude Code raw JSONL is preserved by the private source archive under
`claude-code/`. Import from the archive working copy, not directly from
`~/.claude/projects/`:

```powershell
cargo run -p lethe-import-claude-code -- `
  --archive-root=D:\userdata\docs\private\claude-source-archive `
  --source-instance=claude-code-personal `
  --base-url=$baseUrl `
  --api-token-env=$apiTokenEnv
```

The importer maps only the coding-agent conversation backbone: user
instructions, assistant text, and tool-call metadata. Tool outputs,
`toolUseResult`, command output, file contents, and raw tool argument bodies are
excluded before canonical JSON is created. Tool metadata keeps safe references
such as `file_path`, `path`, `pattern`, `glob`, `query`, and `url`.

Re-running the same archive snapshot should report duplicates for unchanged
Claude Code messages. The 2026-07-06 real archive E2E against a temporary lake
used 13 JSONL files and reported first import `ingested=639`, `duplicates=0`,
`quarantined=0`; second import `ingested=0`, `duplicates=639`, `quarantined=0`.

The 2026-07-08 immediate production import followed archive commit `48dcd66`
and reported `ingested=272`, `duplicates=639`, `quarantined=0`, `files=26`,
`lines=2951`, `skipped_malformed=0`, `skipped_unknown=0`,
`excluded_known=1160`, and `excluded_tool_results=552`. The production lake then
reported `sys:claude-code=911`, latest published
`2026-07-07T16:40:24.508Z`, and freshness `fresh`.

## Codex

Codex raw JSONL is preserved by the private source archive under
`codex/sessions/`. Import from the archive working copy, not directly from the
live Codex directory:

```powershell
cargo run -p lethe-import-codex -- `
  --archive=D:\userdata\docs\private\claude-source-archive `
  --source-instance=codex-personal `
  --base-url=$baseUrl `
  --api-token-env=$apiTokenEnv
```

The importer maps only the coding-agent conversation backbone: user text,
assistant text, and tool-call metadata. Tool outputs and raw tool argument
bodies are excluded before canonical JSON is created. Re-running the same
archive snapshot should report duplicates for unchanged Codex messages.

The 2026-07-06 production import used archive
`D:\userdata\docs\private\claude-source-archive` and source instance
`codex-personal`. The recovered DB already contained 10,212 Codex observations
from an interrupted run. After the bulk append optimization, the completion run
reported `ingested=1432`, `duplicates=10212`, `quarantined=0`, `files=210`,
`transcripts=210`, `skipped_malformed=0`, `skipped_unknown=2249`, and
`excluded_known=26423`. A second run was a full no-op:
`ingested=0`, `duplicates=11644`, `quarantined=0`.

After the online import endpoint was deployed on 2026-07-06, Codex was re-run
against the running service without stopping Docker. The API import was a no-op
with `ingested=0`, `duplicates=11644`, `quarantined=0`, and the subsequent deep
health and SQLite integrity checks passed.

The 2026-07-08 immediate production import followed archive commit `48dcd66`.
One Codex Desktop transcript used `source="vscode"` with `session_id` metadata
and no `thread_source`; the importer now treats only parentless `source="vscode"`
session metadata as the main user thread and still rejects missing
`thread_source` for sidechain or unknown session metadata. The import then
reported `ingested=4654`, `duplicates=11644`, `quarantined=0`, `files=235`,
`transcripts=235`, `skipped_malformed=0`, `skipped_unknown=2950`, and
`excluded_known=36320`. The production lake then reported `sys:codex=16298`,
latest published `2026-07-07T16:48:07.649Z`, and freshness `fresh`.

Import performance note: `AppService::ingest_observation_drafts` prepares the
batch once, appends observations through the storage bulk API inside one SQLite
transaction, and emits one summary audit event. For each non-empty batch, the
persistent corpus index consumes only the new canonical tail and upserts each
`record_id` once. Normal imports continue to fold non-corpus projections
incrementally after each request. Multi-request backfills must use the explicit bulk import
session described below so that the non-corpus full rebuild runs once at the
final high-water instead of once per request. A reference non-corpus rebuild
fixes one canonical high-water, performs two bounded page passes, writes message
and reply-SLO rows to a SQLite staging projection, and atomically publishes the
verified manifest and rows. Normal Slack deltas use compact identity Observation
references. Stable topology appends remain direct inserts; topology,
identifier-owner, and consent changes re-project only the affected old/new
component and commit strict owner-scoped message inserts/updates/deletes with
the manifest. Missing references or owner inconsistency fail explicitly. See
[Identity / person-page component-local re-projection](identity-person-page-local-reprojection.md).
These paths never load all observations or the full corpus into memory. Do not
reintroduce per-observation materialization, per-observation audit writes, a
normal Slack topology fallback to full non-corpus rebuild, or a full corpus-index
rebuild on normal import.

#### Import timing instrumentation

Every `POST /api/import/observation-drafts` request emits one structured
`tracing` event with `import_timing=true`. Successful requests are logged at
`info`; requests whose total duration exceeds the explicit
`OBSERVATION_IMPORT_SLOW_THRESHOLD_MS = 5000` constant are logged at `warn`.
The event contains these fields:

- `source_instance_id`, `schema_names`, `subject_kinds`
- `ledger_append_ms`, `non_corpus_materialize_ms`,
  `search_index_catch_up_ms`, `audit_ms`, `total_ms`
- `non_corpus_materialize_mode`, `non_corpus_classification`,
  `full_rebuild_reason`
- `slow_threshold_ms`, `bulk_session_requested`, `ingested`, `duplicates`,
  `quarantined`, and `result`

`source_instance_id`, `schema_names`, and `subject_kinds` are derived from the
request contract and subject kind prefix; the event never includes payload,
message text, or a subject identifier. A normal non-bulk request reports
`incremental` for freshness-only, Slack-message, and communication folds;
communication metadata is folded into the resident reply-SLO projection in
the same append path. An empty append is `not_applicable`/`no_op`. A declared
schema drift encountered after startup is `incremental`/`declared_schema_skip`
and emits a warning; it never triggers a full rebuild. A request bound to a
bulk session reports `deferred` because materialization is intentionally
performed at finalization.

The reply-SLO projection stores incoming communication facts keyed by
`(channel_id, thread_ref)` and evaluates `Pending` versus `Overdue` at read
time. Its freshness contract is: normally append-to-read propagation is
within 5 seconds; while a migration, recovery, or bootstrap rebuild runs in
the background, the last atomically published snapshot may be up to 60
seconds old. Reads never use an empty or partially rebuilt snapshot.

### Bulk import session

Use one explicit session around every multi-request archive load:

1. `POST /api/import/bulk-sessions/begin` with a `write:observations` bearer
   token. The response contains the generated `session_id`, state `deferred`,
   and the base canonical high-water.
2. Send each existing `POST /api/import/observation-drafts` request with the
   additional `bulk_session_id` field. `source_instance_id` and `drafts` keep
   their existing contract. A missing or mismatched session id fails with HTTP
   409 while a session is active.
3. `POST /api/import/bulk-sessions/{session_id}/end`. Finalization catches the
   corpus index up idempotently, fixes the last canonical append sequence, and
   runs the non-corpus snapshot rebuild once. Repeating a successful end call
   returns the completed session without another rebuild.

While the session is `deferred` or `catching_up`, every non-corpus projection
is marked stale and its HTTP read returns `503 projection_stale`. Corpus grep,
record, and thread reads remain available from the incrementally caught-up
persistent index. `/health` and `/health/deep` report a
`bulk_import_session` dependency with the session id, canonical high-water, and
lag; overall health is `degraded` until finalization succeeds. Supplemental
writes and source sync fail with a bulk-session conflict instead of publishing
a mixed projection generation.

Session state is stored durably in SQLite. If the process exits before end, the
next bootstrap detects the active state, rebuilds non-corpus projections to the
actual canonical high-water, and records the session as ready. Corrupt session
state fails startup explicitly; it is never ignored or replaced by a silent
fallback.

For `B` requests ending at cumulative sizes `N_i`, the old topology-changing
path could pay `sum(T_full(N_i))`, which is quadratic for fixed request sizes.
The session path pays incremental append and corpus-index work per request plus
one `T_full(N)` at end. With the current bounded-page projector and ordinary
identity bucket sizes, this removes the request-level O(N^2) term and makes the
bulk load O(N) in the total observation volume (subject to the projector's
documented single-rebuild internal costs).

Supplemental writes compute a strict non-corpus projection-item delta. SQLite
commits the supplemental append, item inserts/updates/deletes, and projection
manifest in one transaction; the in-memory snapshot is installed only after
that commit succeeds. A projection inconsistency fails the write and marks the
non-corpus materialization stale instead of serving a partially updated view.

## Communication Channels

Slack, Gmail, and Discord ingress is modeled as read-only observations. The
runtime supervisor owns long-lived subscriptions such as Slack socket mode and
Discord gateway connections, then sends observation drafts to
`POST /api/import/observation-drafts`. LETHE must not hold outbound send tokens
or call send APIs for these channels.

Each live communication channel must have one `[[channels]]` record in the
selfhost config. `config.example.toml` contains complete Slack channel, Slack
DM, Gmail, and Discord examples. When enabling channels for the personal lake,
replace `channels = []` in `deploy/personal-lake/config.toml` and
`deploy/personal-lake/config.host.toml` with the required `[[channels]]`
records. Do not keep both forms in the same TOML file.

Current personal live channel config enables Discord server `kana's server`,
channel `#general`, channel id `1507676023314059275`, as
`chan:discord-primary:1507676023314059275`. The `connection_ref` is
`discord-primary-tera`, reusing the existing `tera` Discord bot as the runtime
supervisor connection. LETHE stores no Discord bot token and does not open the
gateway itself. Until the runtime supervisor pushes observation drafts through
`POST /api/import/observation-drafts`, freshness reports this channel as
`unobserved`.

The lookup key is `(kind, source_instance_id, external_id)`. For Slack and
Discord, `external_id` is the channel id. For Gmail, `external_id` is the account
id used by the adapter. An incoming communication observation that does not
match an enabled channel is quarantined. The channel record supplies the
observation `consent_scope`, reply SLO seconds, freshness threshold seconds, and
break-glass declarations.

Slack source configs must declare both `channel_ids` and `mention_user_ids`.
DMs, configured mentions, and normal channel messages are classified before the
Slack adapter maps them to `schema:slack-message`.

Communication projection surfaces:

- `GET /projections/freshness` reports channel freshness using channel ids.
- `GET /projections/reply-slo` reads the resident communication projection,
  which incrementally folds incoming observations and the existing
  `reply-draft@1`/`send-record@1` join to show pending, overdue, and sent
  replies.
- `GET /projections/break-glass` exposes channel and sender allowlists for the
  runtime mode logic. LETHE exposes the declarations only; it does not decide or
  execute interruptions.

Cognition substrate projection surfaces:

- `GET /projections/freshness` also reports configured personal sources such as
  `sys:claude-ai`, `sys:chatgpt`, `sys:claude-code`, and `sys:codex`.
- `GET /projections/resume-snapshot` folds `session-summary@1`, `parking@1`,
  and open claims into project cards for resuming work.
- `GET /projections/plan-state` folds open claims, parking, and current
  decisions after supersedes resolution into project-level portfolio state.
- `GET /projections/card-queue` folds `reply-draft@1`, `reply-approval@1`, and
  `send-record@1`. It supports `state`, `channel`, `automatic`, `limit`, and
  `cursor` query parameters. Each card also exposes `agent_name`: an
  `agent:<name>` draft `created_by` is preferred, and a terminal
  `/agent/<name>` segment in `lineage` is used only when `created_by` is not an
  agent reference. Other ownership prefixes produce `null` when no lineage
  fallback applies.

The card queue derives `agent_name` during the supplemental fold, so it is not
stored independently. The materialized non-corpus projection format is bumped
when this serialized shape changes; an older snapshot is rebuilt from all
persisted supplementals on bootstrap and existing cards receive attribution
without a manual data migration.

## ChatGPT

ChatGPT export is source input under `chatgpt/` in the private source archive
working copy. The importer reads JSON files recursively below that directory,
maps conversation messages to `schema:chatgpt-message`, skips malformed message
records into the structured audit report, and keeps the import idempotent with
identity keys shaped as `chatgpt:{conversation_id}:{message_id}:H(canonical)`.

On 2026-07-06 the production archive contained only `chatgpt/README.md` and no
ChatGPT JSON export, so the real ChatGPT import was intentionally deferred. The
importer and Docker image are ready; run the command below once the export file
is committed into the private archive working copy.

Import the archive working copy:

```powershell
cargo run -p lethe-import-chatgpt -- `
  --archive-root=D:\userdata\docs\private\claude-source-archive `
  --source-instance=chatgpt-personal `
  --base-url=$baseUrl `
  --api-token-env=$apiTokenEnv `
  --backfill
```

Optional filters:

- `--from=2026-07-01T00:00:00Z`
- `--to=2026-07-06T00:00:00Z`
- repeat `--conversation-id=<id>` for a bounded replay
- `--json` for a structured report

Re-running the same archive snapshot should report duplicates for unchanged
messages. The `--backfill` flag sets `meta.backfill=true` on imported
observations so downstream projections and operations can separate archive
inventory from live ingress.

## GitHub

Use `gh api` to dump owned repositories into a gitignored scratch path:

```powershell
./scripts/dump_github_personal_lake.ps1 -OutputPath data/github-scratch/github-dump.json
```

The dump script fetches issues, issue comments, pull requests, reviews, review
comments, commits with file lists, and timeline events. It stores no diff or
patch content in the mapper output.

Import the dump:

```powershell
cargo run -p lethe-import-github -- `
  --dump=data/github-scratch/github-dump.json `
  --source-instance=github-personal `
  --base-url=$baseUrl `
  --api-token-env=$apiTokenEnv
```

Re-running against an unchanged dump should report duplicates.

## Sanity Checks

Before using real exports, run the synthetic pipeline smoke test through the
real import CLIs and a temporary selfhost instance:

```powershell
$smoke = Join-Path $env:TEMP ("lethe-pipeline-smoke-" + [guid]::NewGuid().ToString("N"))
python ./scripts/personal_lake_pipeline_smoke.py --work-dir $smoke
Remove-Item -LiteralPath $smoke -Recurse -Force
```

The smoke test creates one Claude conversation, one ChatGPT archive fixture, and
one GitHub dump. It imports each source twice through the online import API,
requires the second import to be entirely `Duplicate`, and expects 11 total
observations: 2 Claude, 2 ChatGPT, and 7 GitHub.

After imports, compare source-side counts with SQLite observations:

```powershell
python ./scripts/personal_lake_sanity.py `
  --db deploy/personal-lake/data/lethe.sqlite3 `
  --github-dump data/github-scratch/github-dump.json `
  --github-source-instance github-personal `
  --claude-conversations-dir C:\path\to\private-claude-archive\conversations `
  --claude-source-instance claude-personal
```

The script exits non-zero if expected source counts and imported Observation
counts diverge.

The 2026-07-05 full sanity check passed with GitHub `expected=160`,
`actual=160`, Claude `expected=365`, `actual=365`, and 525 total observations.

The 2026-07-06 production sanity check after the online API no-op imports passed
with 12,808 total observations: `claude-personal=365`,
`claude-code-personal=639`, `codex-personal=11644`, and `github-personal=160`.
The W0 check passed with `--timeout-seconds 60`.

## Personal Corpus

The personal lake uses the corpus projection as the read surface for MCP
`search_lake`, `get_record`, and `get_thread`. Unlike the dormitory lake
workspace-search corpus, the personal corpus does not apply consent-management
selection filters. It includes text-bearing observations from claude.ai,
ChatGPT, GitHub issues, pull requests, comments, commit messages, Claude Code
sessions, Codex sessions, and future text observations.

Current corpus source types for personal search are:

- `claude-ai`
- `chatgpt`
- `github-issue`
- `github-pr`
- `github-comment`
- `github-commit`
- `claude-code`
- `codex`

The 2026-07-06 production materialization had corpus record counts:
`codex=11644`, `claude-code=639`, `claude-ai=311`, `github-commit=99`,
`github-event=36`, `github-issue=16`, and `github-pr=9`.

`get_thread` should call
`GET /api/projections/proj:corpus/threads/{record_id}` for coding-agent
records. The response includes the flat `records` list plus `structure` with
`root_session` and `sidechains`, so callers must preserve the parent/child
relationship instead of flattening sidechains into a single anonymous thread.

Personal corpus search is backed by a persistent Tantivy index under
`/var/lib/lethe/corpus-index`. `CURRENT` names the published generation and the
generation files live under `generations/<UUIDv7>/`. Selfhost opens and validates
that generation at startup, then incrementally catches up only observations
after its stored append sequence. Online imports use the same tail catch-up and
idempotent `record_id` upserts, so new durable data becomes searchable without a
service restart or a full rebuild.

The index stores the corpus fields required by `search_lake`, `get_record`, and
`get_thread`; reads load only index data and the matching records. Selfhost does
not retain every Observation or Corpus record in memory, and it does not build a
trigram index for each request. Search uses the persistent 1〜3-gram Tantivy
index for candidate selection and applies the existing source/time/order/cursor
contract and final match verification to those candidates.

A missing first generation, an incompatible schema or corpus configuration, or
detected index corruption starts a background rebuild from fixed-size canonical
pages. While the index is opening, catching up, rebuilding, or failed, HTTP and
MCP corpus reads return an explicit unavailable error; they must never return an
empty result as a silent substitute. A rebuilt generation becomes visible only
after validation and atomic `CURRENT` publication, and the retired generation
is removed only after its in-flight readers release it.
