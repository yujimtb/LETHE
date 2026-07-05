# Spec Delta: coding-agent-adapters

**Change:** supplemental-write-and-mcp-read
**Module:** (new) coding-agent-adapters(M09 Adapter Policy 準拠)+ source archive 運用規律
**Scope:** `lethe-import-claude-code` / `lethe-import-codex`(apps/tools)と生 JSONL の日次 archive 同期
**Dependencies:** M01 Domain Kernel, M03 Observation Lake, M09 Adapter Policy(既存 claude / github importer と同族)
**Agent:** Spec Designer(canonical 写像表)→ Implementer(importer×2+cron)→ Reviewer(idempotency・背骨規則検証)

---

## ADDED Requirements

### Requirement: CAGT-01 source archive への日次同期(Day 0 先行)

コーディングエージェントの生トランスクリプト JSONL は、日次 cron で既存の private source archive リポジトリへ同期 SHALL する。ディレクトリ構成: `claude-code/`(`~/.claude/projects/` 以下をミラー)、`codex/`(Codex セッションディレクトリをミラー)、`chatgpt/`(change ② 用に予約、本 change では空ディレクトリと README のみ)。同期は追記的ミラー(archive 側の削除はしない — 一次ストア側の自動削除を archive に伝播させないことが本規律の目的そのもの)。lake の取り込みは archive のワーキングコピーを入力とする。

**背景(規範根拠):** Claude Code のトランスクリプトは既定 30 日で起動時に自動削除され、削除無効化設定は現行バージョンで弾かれ、mtime 基準削除により保持期間内でも消える不具合が報告されている。保全をベンダーの掃除ロジックに賭けない。archive が生(ツール結果込み全文)、lake が正規化後(背骨のみ)という役割分担は claude.ai 系と同型。

#### Scenario: 一次ストア側の削除に対する耐性
- **WHEN** Claude Code の起動時掃除がローカルの古いセッション JSONL を削除する
- **THEN** archive リポジトリには当該セッションの全文が残存し、lake への(再)取り込みが可能である

### Requirement: CAGT-02 背骨のみの canonical 写像

importer は各セッション JSONL から「会話の背骨」のみを Observation に写像 SHALL する。含む: 本人の指示メッセージ本文、エージェントの応答メッセージ本文、ツール呼び出しのメタデータ(ツール名+対象の参照 — ファイルパス・パターン等の識別子)。含まない: ツール実行結果の中身(ファイル内容・コマンド出力)、ツール呼び出し引数の本体(書き込み内容等)。

**根拠:** (1) 境界原理 — 成果物の内容は git が一次ストアで lake への複製は情報を増やさない。(2) 容量 — トランスクリプトの大半はツール結果。(3) 安全性 — ツール結果には .env の値・コマンド出力経由の認証情報が混入しうる(公式文書明記)。公開 MCP から全文検索可能な corpus に流し込むことは mcp-read-port の公開構成と両立しない。

#### Scenario: ツール結果の非取り込み
- **WHEN** セッション中にエージェントが .env を読み、その内容がトランスクリプトのツール結果に記録されている
- **THEN** 生成される Observation 群のいかなる canonical content にも当該ファイル内容は含まれない

#### Scenario: ツール呼び出しメタデータの保持
- **WHEN** エージェントが `str_replace` を `src/main.rs` に対して実行した
- **THEN** 「ツール名 str_replace、対象 src/main.rs」の呼び出し事実は背骨に含まれる(何をしたかの追跡可能性)

### Requirement: CAGT-03 サブエージェント会話の取り込み

サブエージェント(sidechain)のトランスクリプトも CAGT-02 と同一の背骨規則で取り込み SHALL する。メイン↔サブの親子関係(親セッション参照・sidechain フラグ)は Observation のメタデータとして保持し、Projection がスレッド構造を復元できる形とする。

#### Scenario: 委譲調査の追跡可能性
- **WHEN** メイン会話がサブエージェントに調査を委譲し、サブが結論を出した
- **THEN** サブ側の指示・応答の背骨が lake に存在し、親セッションへの参照で辿れる

### Requirement: CAGT-04 観測単位・identity・時刻

per-message 粒度と SHALL する。identity key は既定形式 `source:object_id:H(canonical)` に従い、Claude Code は `claude-code:{session_id}:{message_uuid}:H(canonical)`。published はメッセージの timestamp(イベント時刻)であり、取り込み時刻を使用して SHALL NOT ならない。再取り込みは exact idempotency(全件 duplicate 判定)で冪等である。

#### Scenario: 再実行の冪等性
- **WHEN** 同一 archive スナップショットに対して importer を二度実行する
- **THEN** 二度目は全件 duplicate として報告され、lake の観測数は不変

### Requirement: CAGT-05 Codex 形式の実測確認

Codex のセッション保存場所・行スキーマは実装冒頭で実測確認 SHALL し、確認結果(パス・形式・sidechain 相当の有無)を本 spec への追記として記録する。Claude Code と共通化できる写像ロジックは共有モジュールに置く。

#### Codex 実測記録(2026-07-06 JST)

- **セッション保存場所:** `C:\Users\mitob\.codex\sessions\YYYY\MM\DD\rollout-<started-at>-<uuid>.jsonl`。実測例: `C:\Users\mitob\.codex\sessions\2026\07\05\rollout-2026-07-05T23-56-59-019f32c8-5ab6-7fd1-8660-deb45f8eec0f.jsonl`。`C:\Users\mitob\.codex\session_index.jsonl` は `id,thread_name,updated_at` の索引であり、取り込み対象の transcript 本体ではない。source archive 側は Track F の同期結果に従い `D:\userdata\docs\private\claude-source-archive\codex\sessions\...` に同じ `sessions` 木を保持する。
- **JSONL top-level schema:** 各行は `timestamp`, `type`, `payload` を持つ。実測した `type` は `session_meta`, `turn_context`, `event_msg`, `response_item`。importer は Observation の根拠を `response_item` のみから作り、`event_msg` は UI 表示・token count 等の重複/運用イベントとして取り込まない。
- **`session_meta.payload`:** main transcript では `session_id`, `id`, `timestamp`, `cwd`, `originator`, `cli_version`, `source`, `thread_source`, `model_provider`, `base_instructions`, `git` を確認した。`session_id` と `id` は main では同一。`thread_source` は main で `user`。
- **pre-session-id / legacy `session_meta.payload`:** 2025 年側の archive に `session_id` / `thread_source` を持たず、`id`, `timestamp`, `cwd`, `originator`, `cli_version`, `instructions`, `source` を持つ形式を実測した(190 files)。また 2026-05 以降の一部に `session_id` を持たず、`id`, `timestamp`, `cwd`, `originator`, `cli_version`, `source`, `thread_source`, `model_provider`, `base_instructions` 等を持つ形式を実測した(57 files)。これらの形式では `id` が唯一の transcript/session 識別子で、subagent 親子情報は存在しない。importer は `session_id` 欠落かつ `parent_thread_id` 欠落の実測形式に限り `session_id=id`, `transcript_id=id`, `thread_source=<実測値または legacy-main>`, `parent_thread_id=null` として扱う。これは任意 fallback ではなく、実測済み main transcript schema の明示分岐である。
- **`response_item.payload`:** `type=message` は `role` と `content[]` を持つ。`role=user` の本文は `content[].type=input_text`、`role=assistant` の本文は `content[].type=output_text`。`role=developer` も実測されたが、本人の指示文ではないため背骨から除外する。`type=function_call` は `id`, `name`, `arguments`, `call_id` を持つ。`arguments` は JSON 文字列だが、canonical へ入れるのは allowlist された対象参照(`workdir`, `path`, `repository_full_name` 等)だけであり、`command`, `body`, `prompt`, `content`, `output` 等の引数本体は入れない。`type=function_call_output` は `call_id`, `output` を持つツール結果であり、背骨から除外する。`type=reasoning` は背骨から除外する。
- **sidechain/subagent 相当:** 存在を実測した。実測例: main `rollout-2026-07-04T16-02-41-019f2bef-c4f4-7e81-a8e1-556858564d47.jsonl` に対し、subagent `rollout-2026-07-04T16-02-42-019f2bef-c709-7fa3-9704-965330fb4da5.jsonl` が存在した。subagent の `session_meta.payload` は `session_id=<親 session id>`, `id=<subagent transcript id>`, `parent_thread_id`, `thread_source=subagent`, `source={subagent: ...}` を持つ。importer は `session_id`, `transcript_id(id)`, `parent_thread_id`, `thread_source` を Observation payload/meta/canonical に保持し、Projection が親子構造を復元できるようにする。
- **Codex identity:** Codex は `source=codex`、object_id は `{transcript_id}:{payload.id | payload.call_id | line-N}` とする。`role=user` の `message` は `payload.id` が無い実測例があるため、同一 JSONL 内の 1-based 行番号 `line-N` を deterministic な末尾 ID として使う。identity key は既定形式 `codex:{object_id}:H(canonical)`。`published` は各行の top-level `timestamp` であり、取り込み時刻は使わない。

#### Scenario: 実測結果の spec 記録
- **WHEN** Codex importer 実装を開始する
- **THEN** 実測したセッション保存場所・行スキーマ・sidechain 相当の有無が本 spec に追記され、reviewer が推測ではなく実測根拠を確認できる

## Invariants(継承)

- Append-Only Law / Replay Law(写像は純関数、同一 JSONL → 同一 Observation 集合)
- M09 Adapter Policy の idempotency key 規約

## Failure Modes

- `MalformedTranscriptLine`(不正 JSON 行は skip+監査ログ、セッション全体を落とさない)
- `UnknownMessageType`(未知の行タイプは skip+型名を監査ログへ — スキーマ進化への耐性)
