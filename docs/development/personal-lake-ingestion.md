# Personal Lake Ingestion

This document covers the `personal-lake-ingestion` OpenSpec change.

## Instance

The personal lake is a separate self-host instance. It uses local Docker bind
storage and does not share SQLite or blob state with the dormitory lake.

Config files:

- `deploy/personal-lake/compose.yaml`
- `deploy/personal-lake/config.toml` for Docker
- `deploy/personal-lake/config.host.toml` for host-run one-shot CLIs
- `deploy/personal-lake/.env.example`

The personal config pins:

- `retention_days = 3650`
- empty `sources`
- scoped API tokens even on localhost
- routing order `year_month_source_container_published`
- `supplemental.reject_unregistered_kinds = true`
- `corpus.mode = "personal_all_text"` so every text-bearing personal
  observation is searchable through the corpus projection
- `LETHE_API_READ_TOKEN` includes `read:corpus`
- `LETHE_API_WRITE_TOKEN` includes `write:supplemental`
- a separate MCP listener on port `8090`

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

Deep health:

```powershell
Invoke-RestMethod -Headers @{ Authorization = "Bearer $env:LETHE_API_SYNC_TOKEN" } `
  http://127.0.0.1:8080/health/deep
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
signature, `exp`, issuer, and audience against `oauth_jwks_path`. It does not
issue tokens, implement DCR, show consent screens, or accept fixed API keys.

Create `deploy/personal-lake/mcp-jwks.json` from the provider JWKS before
starting the Docker stack. `deploy/personal-lake/mcp-jwks.example.json` shows
the expected file shape. `oauth_issuer` and `oauth_audience` must match the
managed provider configuration.

Tailscale Funnel must target only `LETHE_MCP_HOST_PORT`; do not funnel
`LETHE_HTTP_HOST_PORT` or any admin/internal API port. This endpoint is reachable
only while this PC is on, Tailscale Funnel is active, and the selfhost process is
running. It is not a 24/7 service.

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
$env:LETHE_CONFIG_PATH = "D:\userdata\docs\projects\skcollege_database\deploy\personal-lake\config.host.toml"
cargo run -p lethe-import-claude -- `
  --zip=C:\path\to\claude-export.zip `
  --source-instance=claude-personal
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
  -ConfigPath deploy/personal-lake/config.host.toml `
  -DatabasePath deploy/personal-lake/data/lethe.sqlite3 `
  -SourceInstance claude-personal
```

The script requires `LETHE_STORAGE_ENCRYPTION_KEY`, `LETHE_API_READ_TOKEN`,
`LETHE_API_WRITE_TOKEN`, and `LETHE_API_SYNC_TOKEN` to already be set. It fails
if the second import is not a complete no-op.

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

## Claude Code

Claude Code raw JSONL is preserved by the private source archive under
`claude-code/`. Import from the archive working copy, not directly from
`~/.claude/projects/`:

```powershell
$env:LETHE_CONFIG_PATH = "D:\userdata\docs\projects\skcollege_database\deploy\personal-lake\config.host.toml"
cargo run -p lethe-import-claude-code -- `
  --archive-root=D:\userdata\docs\private\claude-source-archive `
  --source-instance=claude-code-personal
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

## Codex

Codex raw JSONL is preserved by the private source archive under
`codex/sessions/`. Import from the archive working copy, not directly from the
live Codex directory:

```powershell
$env:LETHE_CONFIG_PATH = "D:\userdata\docs\projects\skcollege_database\deploy\personal-lake\config.host.toml"
cargo run -p lethe-import-codex -- `
  --archive=D:\userdata\docs\private\claude-source-archive `
  --source-instance=codex-personal
```

The importer maps only the coding-agent conversation backbone: user text,
assistant text, and tool-call metadata. Tool outputs and raw tool argument
bodies are excluded before canonical JSON is created. Re-running the same
archive snapshot should report duplicates for unchanged Codex messages.

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
$env:LETHE_CONFIG_PATH = "D:\userdata\docs\projects\skcollege_database\deploy\personal-lake\config.host.toml"
cargo run -p lethe-import-github -- `
  --dump=data/github-scratch/github-dump.json `
  --source-instance=github-personal
```

Re-running against an unchanged dump should report duplicates.

## Sanity Checks

Before using real exports, run the synthetic pipeline smoke test through the
real import CLIs:

```powershell
$smoke = Join-Path $env:TEMP ("lethe-pipeline-smoke-" + [guid]::NewGuid().ToString("N"))
python ./scripts/personal_lake_pipeline_smoke.py --work-dir $smoke
Remove-Item -LiteralPath $smoke -Recurse -Force
```

The smoke test creates one Claude conversation and one GitHub dump, imports
each source twice, and requires the second import to be entirely `Duplicate`.

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

## Personal Corpus

The personal lake uses the corpus projection as the read surface for MCP
`search_lake`, `get_record`, and `get_thread`. Unlike the dormitory lake
workspace-search corpus, the personal corpus does not apply consent-management
selection filters. It includes text-bearing observations from claude.ai,
GitHub issues, pull requests, comments, commit messages, Claude Code sessions,
Codex sessions, and future text observations.

Current corpus source types for personal search are:

- `claude-ai`
- `github-issue`
- `github-pr`
- `github-comment`
- `github-commit`
- `claude-code`
- `codex`

`get_thread` should call
`GET /api/projections/proj:corpus/threads/{record_id}` for coding-agent
records. The response includes the flat `records` list plus `structure` with
`root_session` and `sidechains`, so callers must preserve the parent/child
relationship instead of flattening sidechains into a single anonymous thread.
