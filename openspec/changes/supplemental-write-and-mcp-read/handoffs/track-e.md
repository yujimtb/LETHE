# Track E Handoff: Codex importer

**Change:** supplemental-write-and-mcp-read
**Tasks:** E1, E2, E3
**Status:** Complete
**Date:** 2026-07-06

## 実装した内容

- Codex の実セッション保存場所、JSONL 行 schema、subagent/sidechain 相当の存在を実測し、`coding-agent-adapters` spec に追記した。
- `crates/adapters/coding-agent` に共有背骨写像を追加した。
  - `response_item:message` の `role=user` / `role=assistant` の text を取り込む。
  - `response_item:function_call` の tool 名と allowlist 済み対象参照だけを取り込む。
  - `function_call_output`, `reasoning`, `event_msg`, `turn_context`, `developer` role は取り込まない。
  - Codex subagent の `session_id`, `transcript_id`, `parent_thread_id`, `thread_source` を payload/meta/canonical に保持する。
- `apps/tools/lethe-import-codex` CLI を追加した。
  - `--archive=<path>` は archive root(`codex/sessions`), codex root(`sessions`), sessions dir を受け付ける。
  - `--source-instance=<id>` を必須にし、既存 importer と同じ `AppService::ingest_observation_drafts` 経路に接続した。
- selfhost registry seed に `sys:codex`, `obs:codex-importer`, `schema:coding-agent-message` を追加した。
- `docs/development/personal-lake-ingestion.md` に Codex archive import 手順を追記した。

## 変更ファイル一覧

- `Cargo.toml`
- `crates/adapters/coding-agent/Cargo.toml`
- `crates/adapters/coding-agent/src/lib.rs`
- `crates/adapters/coding-agent/src/backbone.rs`
- `crates/adapters/coding-agent/src/codex.rs`
- `apps/tools/lethe-import-codex/Cargo.toml`
- `apps/tools/lethe-import-codex/src/main.rs`
- `apps/selfhost/src/self_host/registry.rs`
- `docs/development/personal-lake-ingestion.md`
- `openspec/changes/supplemental-write-and-mcp-read/specs/coding-agent-adapters/spec.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-e.md`

`Cargo.toml`, `apps/selfhost/src/self_host/registry.rs`, `crates/adapters/coding-agent/Cargo.toml`, `crates/adapters/coding-agent/src/lib.rs` には同時並行 Track の変更も混在している。こちらからは戻していない。

## 実行したテストと結果

- `cargo test -p lethe-adapter-coding-agent codex::tests`
  - Result: pass
  - Evidence: Codex fixture の negative test / idempotency test / subagent metadata test / malformed line audit test / legacy session_meta test / pre-session-id meta test が pass。ignored real archive smoke は通常実行では ignored。
- `rustfmt .\crates\adapters\coding-agent\src\lib.rs .\crates\adapters\coding-agent\src\backbone.rs .\crates\adapters\coding-agent\src\codex.rs .\apps\tools\lethe-import-codex\src\main.rs .\apps\selfhost\src\self_host\registry.rs`
  - Result: pass
- `cargo check -p lethe-adapter-coding-agent --lib`
  - Result: pass
  - Note: 同時並行 Track D の `claude_code.rs` に unused variable warning があるが、Codex 側の compile は通過。
- `cargo check -p lethe-import-codex`
  - Result: pass
- `$env:LETHE_CODEX_ARCHIVE_E2E_PATH='D:\userdata\docs\private\claude-source-archive'; cargo test -p lethe-adapter-coding-agent real_codex_archive_imports_when_env_points_to_archive -- --ignored`
  - Result: pass
  - Evidence: 実 archive 全体を adapter が parse し、draft を生成できることを確認。
- temp archive subset CLI E2E
  - Command: `cargo run -p lethe-import-codex -- --archive=<temp archive root> --source-instance=codex-e2e` を2回実行。
  - Source: `D:\userdata\docs\private\claude-source-archive` から legacy main、modern main、subagent の3 transcript を temp archive にコピー。
  - Result: first run `ingested=129, duplicates=0, quarantined=0, files=3, transcripts=3, skipped_malformed=0, skipped_unknown=6, excluded_known=447`; second run `ingested=0, duplicates=129, quarantined=0, files=3, transcripts=3, skipped_malformed=0, skipped_unknown=6, excluded_known=447`.

## 未完了または統合担当に引き継ぐ事項

- 全 archive の temp lake CLI 取り込みは一度試したが、5分 timeout した。adapter の全 archive parser smoke は pass しており、gate E2E は上記 subset で確認済み。production/personal lake への全件投入は、統合担当が長時間実行枠で行う。
- ignored real archive smoke は環境変数が必要。
  - PowerShell: `$env:LETHE_CODEX_ARCHIVE_E2E_PATH='D:\userdata\docs\private\claude-source-archive'; cargo test -p lethe-adapter-coding-agent real_codex_archive_imports_when_env_points_to_archive -- --ignored`
- Track I は Codex 観測を Projection 経由で読むこと。archive repo や raw JSONL を行動根拠にしない。

## 仕様 SHALL と evidence

| Requirement | Judgement | Evidence |
|---|---|---|
| CAGT-05: Codex 保存場所・行 schema・sidechain 相当を実測して spec に記録 | Pass | `specs/coding-agent-adapters/spec.md` の "Codex 実測記録(2026-07-06 JST)" |
| CAGT-02: 背骨のみ canonical に写像し、ツール結果・引数本体を除外 | Pass for Codex frontend | `codex_fixture_excludes_tool_results_and_argument_body_from_canonical` が pass。`function_call_output.output` と raw `command` を payload/canonical に含めない |
| CAGT-03: subagent 親子 metadata を保持 | Pass for Codex frontend | `codex_subagent_metadata_is_preserved` が pass。`session_id`, `transcript_id`, `parent_thread_id`, `thread_source` を保持 |
| CAGT-04: per-message 粒度、identity key `source:object_id:H(canonical)`, published=message timestamp | Pass | `codex_import_is_deterministic_and_uses_message_timestamps` と temp archive subset の2回実行 duplicate E2E |
| CAGT-01: lake 取り込みは archive working copy を入力にする | Pass | `--archive=<temp archive root>` で `codex/sessions` を読み、実データ subset を gate 経由で ingest |
