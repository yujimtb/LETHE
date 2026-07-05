# Track D Handoff: Claude Code importer

## 実装した内容

- `apps/tools/lethe-import-claude-code` を追加した。
  - 入力は archive ワーキングコピー root の `--archive-root=<path>`。
  - `archive-root/claude-code/**/*.jsonl` を再帰走査する。
  - 既存 importer と同じく `--source-instance=<id>` を受け、`AppService::ingest_observation_drafts` で gate を通す。
  - 実行結果は `ingested / duplicates / quarantined` と parse skip 数を報告する。
- Claude Code JSONL frontend を `crates/adapters/coding-agent/src/claude_code.rs` に追加した。
  - `user`、`assistant`、top-level `tool_use`、既知 metadata 行を型判別する。
  - 不正 JSON と未知 top-level `type` は skip し、audit line として保持する。
  - `tool_result` / `toolUseResult` は Observation 化しない。
- 共有背骨写像は既存 `crates/adapters/coding-agent/src/backbone.rs` を拡張して利用した。
  - Track E の Codex frontend も同じ `BackboneRecord` / `BackboneItem` / `to_observation_draft` を使う。
  - `parent_message_id` と `is_sidechain` を追加し、Claude Code の `parentUuid` / `isSidechain` を保持できるようにした。
- selfhost seed registry に `sys:claude-code` / `obs:claude-code-importer` を追加し、共通 schema `schema:coding-agent-message` を emit できるようにした。
- Dockerfile に `lethe-import-claude-code` の build/copy を追加した。
- `docs/development/personal-lake-ingestion.md` に Claude Code import 手順と E2E 観測数を追記した。
- `tasks.md` の D1-D4 を完了状態に更新した。

## 共有写像モジュール契約

- 場所: `crates/adapters/coding-agent/src/backbone.rs`
- 入力型:
  - `BackboneRecord`
    - `session_id`
    - `transcript_id`
    - `parent_message_id`
    - `is_sidechain`
    - `parent_thread_id`
    - `thread_source`
    - `object_id`
    - `published`
    - `item`
  - `BackboneItem`
    - `Message { role, text }`
    - `ToolCall { tool_name, references }`
  - `CodingAgentSourceConfig`
    - `source_key`
    - `observer_id`
    - `source_system_id`
- 出力型:
  - `ObservationDraft`
  - schema: `schema:coding-agent-message`
  - identity: `identity_key(source_key, object_id, canonical_json)`
  - canonical は `meta.canonical_json` に格納される。
- Claude Code frontend の identity 入力:
  - `source_key = "claude-code"`
  - `object_id = "{session_id}:{message_uuid}"`
  - 結果: `claude-code:{session_id}:{message_uuid}:H(canonical)`
- 制約:
  - source frontend は tool output / command output / file content / raw command body / write body を `BackboneItem` に渡してはならない。
  - Claude Code frontend は tool input から allowlist された参照キーだけを `references` に入れる。
  - Claude Code allowlist: `file_path`, `file_paths`, `path`, `paths`, `pattern`, `patterns`, `glob`, `query`, `url`, `urls`, `notebook_path`, `old_path`, `new_path`, `relative_path`。
  - `command`, `description`, `old_string`, `new_string`, `content`, `toolUseResult`, `output` は canonical に入れない。
  - `published` は必ず source row の `timestamp`。取り込み時刻は使わない。

## 変更ファイル一覧

- `Cargo.toml`
- `Cargo.lock`
- `apps/selfhost/Dockerfile`
- `apps/selfhost/src/self_host/registry.rs`
- `apps/tools/lethe-import-claude-code/Cargo.toml`
- `apps/tools/lethe-import-claude-code/src/main.rs`
- `crates/adapters/coding-agent/Cargo.toml`
- `crates/adapters/coding-agent/src/backbone.rs`
- `crates/adapters/coding-agent/src/claude_code.rs`
- `crates/adapters/coding-agent/src/codex.rs`
- `crates/adapters/coding-agent/src/lib.rs`
- `docs/development/personal-lake-ingestion.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-d.md`

## 実行したテストと結果

- `cargo test -p lethe-adapter-coding-agent`
  - 結果: pass。9 passed, 1 ignored。
  - covered:
    - 実形式 fixture parse
    - 壊れ JSON 行混入でも完走
    - unknown type skip + audit
    - `.env` tool result 内容の canonical 非混入
    - identity key 形式
    - published が source timestamp
    - sidechain metadata
    - IngestionGate 2 回目 duplicate
- `cargo check -p lethe-import-claude-code`
  - 結果: pass。
- 実 archive E2E
  - archive: `D:\userdata\docs\private\claude-source-archive`
  - DB: temporary lake `D:\tmp\lethe-claude-code-e2e-7c9d32238c7a4102b986463ceaf015bc\lethe.sqlite3`
  - first run:
    - `ingested=639`
    - `duplicates=0`
    - `quarantined=0`
    - `files=13`
    - `lines=1816`
    - `observed=639`
    - `skipped_malformed=0`
    - `skipped_unknown=0`
    - `excluded_known=599`
    - `excluded_tool_results=379`
  - second run:
    - `ingested=0`
    - `duplicates=639`
    - `quarantined=0`
    - same parse counts
- Formatting:
  - `cargo fmt` は Track H 側の未作成 `apps/selfhost/src/self_host/mcp.rs` 参照で失敗した。
  - Track D で触った Rust ファイルは `rustfmt` 直接実行済み。

## 未完了または統合担当に引き継ぐ事項

- production personal DB (`deploy/personal-lake/data/lethe.sqlite3`) への Claude Code 初回投入は未実施。
  - 理由: 実行時に別プロセス `lethe-import-codex` が同 DB を利用中で、最初の試行は timeout した。確認時点で `identity_key LIKE 'claude-code:%'` は 0 件だった。
  - 代替 evidence として、同じ実 archive を一時 lake DB に import し、初回 ingest と再実行 duplicate を確認済み。
- 統合時は personal DB が空いている状態で次を実行する:
  - `LETHE_CONFIG_PATH=deploy/personal-lake/config.host.toml`
  - `cargo run -p lethe-import-claude-code -- --archive-root=D:\userdata\docs\private\claude-source-archive --source-instance=claude-code-personal`
  - 再実行で全件 duplicate になることを確認する。
- スタブは追加していない。
- 読み取り消費者は Projection 経由に限定する方針を維持した。Track D は importer なので生 supplemental を読まない。

## SHALL と evidence

- CAGT-01 source archive 入力
  - Evidence: CLI `--archive-root` は archive root 直下の `claude-code/` を必須入力にする。実 archive E2E で `D:\userdata\docs\private\claude-source-archive` を使用。
- CAGT-02 背骨のみ canonical
  - Evidence: `claude_code.rs` は user text / assistant text / tool call metadata のみ `BackboneRecord` に変換。
  - Evidence: test `env_tool_result_content_never_enters_canonical`。
- CAGT-03 sidechain 取り込み
  - Evidence: `parentUuid` → `parent_message_id`, `isSidechain` → `is_sidechain`, `parentSessionId` → `parent_thread_id`。
  - Evidence: test `sidechain_parent_metadata_is_preserved`。
- CAGT-04 per-message identity / published / idempotency
  - Evidence: `object_id = "{session_id}:{message_uuid}"`, `source_key = "claude-code"`。
  - Evidence: test `identity_key_and_published_use_source_message_fields`。
  - Evidence: test `same_archive_snapshot_reingest_is_all_duplicate`。
  - Evidence: 実 archive E2E second run `duplicates=639`。
- Failure Modes
  - `MalformedTranscriptLine`: 不正 JSON は skip + audit。
  - `UnknownMessageType`: 未知 top-level `type` は skip + audit。
  - Evidence: test `parses_real_shape_fixture_and_skips_broken_lines`。
