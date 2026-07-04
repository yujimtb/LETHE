# Closeout Notes: personal-lake-ingestion

**Date:** 2026-07-05
**Status:** Implemented and archived.

## Completed Evidence

- W0 runtime path is implemented and verified without Docker:
  - host selfhost boot smoke reached `/health/deep`
  - `partition_log` recorded year-first routing axes
  - keyspec mismatch fails fast in SQLite tests
- Storage encryption key is installed:
  - generated a 32-byte hex key without printing it
  - stored it in Windows Credential Manager target `LETHE_PERSONAL_LAKE_STORAGE_ENCRYPTION_KEY`
  - set user environment variable `LETHE_STORAGE_ENCRYPTION_KEY`
- GitHub lane is complete:
  - `gh` authenticated against `yujimtb/LETHE`
  - owned repository dump covered 13 repositories
  - expected GitHub observations: 160
  - first import: `ingested=160`, `duplicates=0`, `quarantined=0`
  - second import: `ingested=0`, `duplicates=160`, `quarantined=0`
  - sanity check: `expected=160`, `actual=160`
- Synthetic pipeline smoke is available:
  - generated Claude zip imports twice through `lethe-import-claude`
  - generated GitHub dump imports twice through `lethe-import-github`
  - second import for each source is all `Duplicate`
- Claude archive/import run script is available:
  - synthetic export archived into a git repo
  - first import: `ingested=2`, `duplicates=0`, `quarantined=0`
  - second import: `ingested=0`, `duplicates=2`, `quarantined=0`
  - Claude sanity check: `expected=2`, `actual=2`
- Real Claude lane is complete:
  - claude.ai export zip: `D:\mitob\Downloads\data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783183126-446768f9-batch-0000.zip`
  - source archive repo: `D:\userdata\docs\private\claude-source-archive`
  - archive commit: `475089a Archive claude.ai export 2026-07-05`
  - expanded conversations: 35
  - first full import: `ingested=365`, `duplicates=0`, `quarantined=0`
  - second full import: `ingested=0`, `duplicates=365`, `quarantined=0`
  - Claude sanity check: `expected=365`, `actual=365`
  - one-conversation e2e in an isolated temp DB: `ingested=2`, then `duplicates=2`, sanity `expected=2`, `actual=2`
- Full source sanity is complete:
  - GitHub: `expected=160`, `actual=160`
  - Claude: `expected=365`, `actual=365`
  - SQLite observations: 525 total

## Real Claude Export Search

- Checked `D:\mitob\Downloads` for Claude/Anthropic/export/conversation candidate zip files; no real claude.ai export shape was found.
- Checked Gmail for Claude/Anthropic data export or download notifications using narrow `in:anywhere` searches; no export email was found.
- The provided `personal-lake-ingestion.zip` contains the OpenSpec proposal/design/spec/tasks inputs only, not source conversation data.
- Later received the real claude.ai export zip from the user at `D:\mitob\Downloads\data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783183126-446768f9-batch-0000.zip`; this supersedes the earlier search result.

## Decisions Captured During Implementation

- `deploy/personal-lake/config.toml` remains Docker-facing and uses container paths.
- `deploy/personal-lake/config.host.toml` is the host CLI config and uses host-local `deploy/personal-lake/data`.
- GitHub empty repositories return `HTTP 409` for commits; the dump script treats that exact commits endpoint case as an empty commit list and still fails fast for unrelated API errors.
- GitHub `committed` timeline events can omit numeric `id`, `created_at`, and `actor`; the mapper uses `sha` as `event_key`, `author.date` as `published`, and raw author attribution in canonical content.
- GitHub dump remains scratch data and is not archived.
- Real Claude export processing should use `scripts/run_claude_personal_lake_import.ps1` so archive, import, no-op re-import, and count sanity stay one audited operation.
- Claude export archives include metadata JSON (`users.json`, `memories.json`) and design chat JSON entries. The importer and archive expander now process only conversation-shaped JSON entries, explicitly skip known metadata entries, and fail fast on unknown JSON entries.
- Claude messages can appear as `chat_messages` with `sender`/`text` or as design-chat `messages` with `role`/nested `content`; both shapes are parsed into the same canonical message mapping.
- Real data exposed a missing-parent branch in `parent_message_uuid`; missing UUID derivation now assigns deterministic `orphan:{n}` roots sorted by missing parent UUID and is covered by regression tests.

## Issue Updates

- Updated and closed #7: U3 / claude.ai importer property-test issue. Real export failure case is fixed and recorded in `openspec/changes/sharding-refactor/design.md`.
- Updated #13: AI conversation ingestion umbrella. Real one-shot export archive/import/no-op/sanity evidence is recorded; periodic export, Gmail Observer, and enrichment remain follow-up work.
- Updated #14: GitHub Observer / dashboard follow-up. Mapper reuse and real GitHub import evidence are recorded.

## New Follow-up Issues

- #15: Validate GitHub commit published timestamp policy.
- #16: Personal lake NAS migration and backup runbook.
- #17: Personal lake retrieval ports after ingestion.

## Residual Notes

- OpenSpec delta specs were synced to main specs at `openspec/specs/adapter-policy/spec.md` and `openspec/specs/runtime/spec.md`.
- The change was archived to `openspec/changes/archive/2026-07-05-personal-lake-ingestion`.
- Docker compose uses required `LETHE_HTTP_HOST_PORT` so local port conflicts are explicit. It can be run on a non-8080 host port when another local service already owns `127.0.0.1:8080`.
- Docker compose was verified on 2026-07-05 through WSL Docker with `LETHE_HTTP_HOST_PORT=18080`: image build succeeded, container started on `127.0.0.1:18080`, `/health/deep` returned `ok`, and `scripts/personal_lake_w0_check.py` passed against the bind-mounted SQLite database.
- Docker Desktop 4.80.0 was installed and verified on Windows. The same compose stack was built and started through the Windows Docker Desktop engine with `LETHE_HTTP_HOST_PORT=18081`, and `scripts/personal_lake_w0_check.py` passed against `http://127.0.0.1:18081`.
