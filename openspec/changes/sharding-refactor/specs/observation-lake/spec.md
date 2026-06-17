# Spec Delta: observation-lake

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-17

## Dependencies

- M01 Domain Kernel — 型・law の正規参照(本 spec は Idempotency Law を **完全 / 決定的** に精緻化する。意味論は破らない)
- M03 Observation Lake — 既存 `openspec/specs/observation-lake.md` の中核(append-only / ingestion pipeline / temporal validation / governance gate)は **不変**
- M08 Governance — consent / restricted capture / review の正規参照
- M09 Adapter Policy — adapter から identity_key 構成に必要な (object_id, canonical タプル) が宣言される(本 change の `adapter-policy` delta 参照)
- 正典: [sharding_refactor.md](../../../../sharding_refactor.md) §2 D1〜D9 / D12

> 本 delta は M03 の append-only / replay / governance gate / `IngestResult` 同型を **変更しない**。identity 契約・dedup 判定経路・per-leaf 永続層・rehome primitive を ADDED として規定する。

---

## ADDED Requirements

### Requirement: SHARD-01 Identity Key Contract

各 Observation は `identity_key = source : object_id : H(canonical_content)` を **持たなければならない (SHALL)**。`H` は sha256。`identity_key` は `Observation.idempotencyKey` の denormalize として `observation_json` 内にも保持され **なければならない (SHALL)**。`Observation.idempotencyKey` は optional から **必須・高エントロピー・解決可能** に格上げされる。編集は新 distinct Observation として記録 **しなければならない (SHALL)**(同 object_id、新 canonical_content → 新 identity_key)。

#### Scenario: 編集前後で identity_key が異なる

- **WHEN** Slack の同 channel:ts メッセージが編集されて再取り込みされる
- **THEN** 新 Observation の `identity_key` は元と異なり、両者が distinct な行として保存される
- **AND** 両版は `object_id`(`source:object_id` 共通)で linkable で、現在版は Projection の latest-by-published で解決可能

#### Scenario: 無変更再取り込み

- **WHEN** 同 channel:ts メッセージが内容変化なしで再取り込みされる
- **THEN** `identity_key` 一致により `IngestResult::Duplicate(existing_id)` が返る
- **AND** 新規行は append されない

### Requirement: SHARD-02 Per-Message Observation Granularity

会話 / chat / AI 会話 / append 系ログは **1 メッセージ = 1 Observation** で記録 **しなければならない (SHALL)**。会話全体を 1 Observation にしてはならない(SHALL NOT)。revisioned 文書(Slides / Docs / Sheets / Notion / Figma)は revisioned snapshot を維持し、高頻度 sensor は chunk manifest として記録する。会話のまとまりは Observation 粒度でなく `conversation_id` でグループ化する Projection 側で表現する。

#### Scenario: 追記による content hash 変化の二乗保存を防ぐ

- **WHEN** 100 メッセージの会話が export ごとに 1 メッセージずつ伸び 10 回再取り込みされる
- **THEN** 保存量は distinct メッセージ数(≤ 1000)に線形であり、二乗(会話長 × export 回数)にならない
- **AND** claude.ai 再 export の欠落は append-only により無害(既存は残る、欠けた分は no-op)

### Requirement: SHARD-03 Canonical Content Boundary

`canonical_content` は **最小・保守的** に定義 **しなければならない (SHALL)**。迷ったら false-split(余分な版を 1 つ append、append-only 下で無害)を選び、false-merge(silent loss)は絶対に避けなければならない (SHALL NOT)。

- **include:** `sender` / `body`(transport ノイズのみ正規化、ユーザ可視空白は保つ)/ `event_time`(RFC3339 UTC 固定精度)/ 添付の sha256
- **exclude:** reactions 等の独立変化 side-state / 編集 wrapper メタ / ingestion メタ(`recordedAt` / crawler cursor / export run id / claude.ai `updated_at`)
- **正規化対象は transport ノイズのみ:** NFC / CRLF→LF / JSON canonical / timestamp 表記統一

固定 serialization(canonical タプル実体)は `canonical_json` 列として保持 **しなければならない (SHALL)**。これが hash 衝突時の唯一の比較対象になる。

#### Scenario: reactions 変化が新 Observation を生まない

- **WHEN** 同 channel:ts メッセージに新しい reaction が付いて再取り込みされる
- **THEN** `identity_key` は変化せず `Duplicate` が返る
- **AND** reactions は別 Observation 列として記録される(本要件の対象外)

#### Scenario: body の transport ノイズ正規化

- **WHEN** body が CRLF / LF 表記違いだけで同一の場合
- **THEN** `canonical_content` は LF に統一されて hash 一致し、`Duplicate` が返る

### Requirement: SHARD-04 Exact Decision Dedup

dedup の正規判定は **per-leaf SQLite UNIQUE 制約による exact(決定的)冪等** で **なければならない (SHALL)**。`IngestResult = Ingested(id) | Duplicate(existing_id) | Rejected(...) | Quarantined(...)` の同型を維持し、silent drop を許してはならない (SHALL NOT)。確率的(Bloom + ε 取りこぼし)を冪等性の構成要素として用いてはならない (SHALL NOT)。

衝突時(同 `identity_key` で `canonical_json` 相違 = sha256 衝突)の比較対象は stored `canonical_json` のみで **なければならない (SHALL)**。full observation(reactions / 編集 wrapper / ingestion メタを含む)を比較してはならない (SHALL NOT)。

| 結果 | 条件 |
| --- | --- |
| `Duplicate(existing_id)` | UNIQUE 違反 + 既存 `canonical_json` と incoming 一致 |
| `Quarantined(reason="sha256-collision")` | UNIQUE 違反 + `canonical_json` 相違(真の sha256 衝突) |

#### Scenario: 編集経路で silent drop が起きない

- **WHEN** Slack 編集メッセージが取り込まれる
- **THEN** identity_key が変わるので新 Observation が `Ingested` として保存される
- **AND** `Duplicate` で捨てられない

#### Scenario: 偽 Conflict が reactions 変化で起きない

- **WHEN** 同 identity の Observation を reactions のみ差分を持って再投入する
- **THEN** `Duplicate(existing_id)` が返る
- **AND** `Quarantined`(`Conflict`)にならない(canonical タプル一致のため)

### Requirement: SHARD-05 SQLite-Authoritative Per-Leaf Persistence

各 leaf の永続層は **SQLite を authoritative**(正規ストア)として運用 **しなければならない (SHALL)**。in-memory `LakeStore` は authoritative であってはならない (SHALL NOT)。dedup 判定は SQLite UNIQUE 違反に一本化し、補償ロールバックを使ってはならない (SHALL NOT)。

in-memory 構造を保持する場合は **per-leaf 非永続キャッシュ** に降格 **しなければならない (SHALL)**。起動時に `load_observations` から再構築するか、廃止する。

#### Scenario: 多 leaf 環境で RAM に全 Observation を載せない

- **WHEN** 10 leaf × leaf あたり 100 万 Observation の構成で selfhost を起動する
- **THEN** 起動時の RAM 使用量が「全 Observation 数 × Observation サイズ」に比例しない
- **AND** dedup 判定は SQLite UNIQUE B-tree への INSERT で原子的に決まる

### Requirement: SHARD-06 Leaf-Local Append Sequence Schema

各 leaf の `observations` テーブルは以下の列を **持たなければならない (SHALL)**:

```sql
CREATE TABLE observations (
    append_seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- leaf-local 単調 commit cursor (rowid alias)
    id TEXT NOT NULL UNIQUE,                       -- UUIDv7
    identity_key TEXT NOT NULL UNIQUE,             -- exact index = この UNIQUE B-tree
    canonical_json TEXT NOT NULL,                  -- hash 入力 canonical タプル実体 (衝突比較対象)
    recorded_at TEXT NOT NULL,                     -- provenance、cursor には使わない
    observation_json TEXT NOT NULL                 -- 全実体保持
);
```

- `append_seq` は **`INTEGER PRIMARY KEY AUTOINCREMENT`** で **なければならない (SHALL)**(SQLite が INSERT で自動採番、AUTOINCREMENT で delete 後も再利用しない)。`INTEGER NOT NULL` 単体では採番されず watermark cursor が機能しない。
- `recorded_at` を watermark / propagation cursor に使ってはならない (SHALL NOT)(rehome で frontier 下に沈む)。
- `canonical_json` は `observation_json` に含めない派生 column(D9.3、R12)。
- disaster rebuild の依存は「`observation_json` 内の `identity_key`」と「`canonical_json` column」の可読性。どちらも読めない破損は `fail-fast → re-crawl` を既定とする(D9.3 / D12.2)。

#### Scenario: rehome で `append_seq` が必ず新採番される

- **WHEN** stored Observation を rehome で着地 leaf に内部 append する
- **THEN** 着地 leaf の `append_seq` が新採番され、stored の `id` / `published` / `recorded_at` は保持される
- **AND** watermark cursor `WHERE append_seq > ?` が rehome した Observation を必ず拾う

#### Scenario: SQLite UNIQUE 違反の原子性

- **WHEN** 同 `identity_key` の INSERT が並行 2 本走る
- **THEN** 1 本だけが成功し、もう 1 本は UNIQUE 違反で `Duplicate(existing_id)` を follow-up SELECT で取得する
- **AND** 並行に AUTOINCREMENT の採番欠番は起きうるが、単調性は維持される

### Requirement: SHARD-07 Rehome Primitive (Single Migration Primitive)

stored Observation の **rehome は唯一の migration primitive** で **なければならない (SHALL)**。backfill / split 再配置 / failover drain / blue-green keyspec 変更はすべて rehome に還元される。

rehome は次の **2 レール契約** を守らなければならない (SHALL):

- **Rail-1 fresh ingest**(adapter → 新 `id` + 新 `recorded_at`): **初出キャプチャ専用**、migration では使ってはならない (SHALL NOT)。
- **Rail-2 rehome**(stored Observation を再 route → 着地 leaf に内部 append): stored `id` / `published` / `recorded_at` / `consent` を **保持しなければならない (SHALL)**。新採番されるのは着地 leaf の `append_seq` のみ。

rehome は 2 モードを持つ:

- **mode (a) stored identity_key 信頼**: keyspec 不変(split / failover drain / 同 keyspec rebuild)。adapter 不要。
- **mode (b) canonical_content から再計算**: identity_keyspec 変更(blue/green)。`identity_key` column / `canonical_json` column / **`observation_json.idempotency_key`** の 3 箇所を同時に新 keyspec で再シリアライズ **しなければならない (SHALL)**(R14)。

#### Scenario: rehome で id / published / recorded_at が保持される

- **WHEN** split 中の rehome(mode a)を実行する
- **THEN** 着地 leaf の行は元の `id`、元の `published`、元の `recorded_at` をそのまま持つ
- **AND** 着地 leaf の `append_seq` のみが新採番される

#### Scenario: fresh ingest 経路を migration に使わない

- **WHEN** rehome を adapter → `IngestRequest` 経路で実装しようとする
- **THEN** スキーマ違反として CI / property test が fail する(`new_id()` と新 `recorded_at` の付与は migration では禁止)
- **AND** 専用の rehome / 内部 append API のみが許容される

#### Scenario: mode (b) で 3 箇所が同期する

- **WHEN** blue/green migration で identity_keyspec を新版へ移行する
- **THEN** rehome 後の行は `identity_key` column / `canonical_json` column / `observation_json.idempotency_key` のすべてが新 keyspec で再シリアライズされ、3 箇所が一致する
- **AND** 旧 keyspec の値は新構造には残らない(metadata として旧 keyspec version のみ履歴保持)

### Requirement: SHARD-08 Disposable Store and Re-crawl

既存 LETHE 永続データは disposable として扱い、本 change の Phase 1 開始時に **破棄して source から再取得 (re-crawl)** **しなければならない (SHALL)**。旧 → 新の後方互換 shim は作ってはならない (SHALL NOT)。寮 Slack 本体の初回取り込みは migration ではなく通常 ingest として行う。

#### Scenario: 旧 schema の Observation を読まない

- **WHEN** Phase 1 後の selfhost が旧 schema の `observations` テーブルを起動時に検出する
- **THEN** fail-fast で起動を拒否し、re-crawl を要求する
- **AND** 旧 schema からの暗黙 migration は実行されない

### Requirement: SHARD-09 Blue/Green Keyspec Change

identity_keyspec / routing_keyspec の変更は **blue/green migration** として実施 **しなければならない (SHALL)**。in-place mutation は禁止 (SHALL NOT)。

手順:

1. 新 keyspec で新構造(新 partition log + 新 leaf 群)を立てる。
2. 全観測を rehome mode (b)(`id` / `published` / `recorded_at` 保持、identity 表現は新 keyspec で再シリアライズ)で新構造へ migrate。
3. exact dedup が安全・冪等を保証する(着地 leaf の `identity_key` UNIQUE)。
4. read を cutover し、旧構造を retire(物理 leaf を削除)。
5. 旧 keyspec + 旧 partition log の version を metadata として履歴保持。

#### Scenario: cutover 後の replay 等価性

- **WHEN** blue/green cutover 完了後に新構造を replay する
- **THEN** migration 前の現状態が決定的に再現される(Replay Law)
- **AND** 旧物理 leaf は削除されており、新構造の `initialize` + `split_commit` の fold で `tree(L)` が一意復元される

---

## MODIFIED Behaviors (parent spec 参照)

既存 `openspec/specs/observation-lake.md` の以下の節は、本 delta の要件の影響で実装が変わる(契約は維持):

- **§3 Storage Architecture / §3.3 MVP Storage**: SQLite が補助 index から **per-leaf authoritative ストア** に格上げ(SHARD-05)。
- **§4.1 Ingestion Gate の責務 / §4.2 API / §4.5 IngestResult**: `IngestResult` 同型は維持。dedup は per-leaf SQLite UNIQUE に一本化(SHARD-04)。
- **§4.3 Observation 追加リクエスト**: `idempotencyKey` を optional → 必須・高エントロピー・解決可能に格上げ(SHARD-01)。
- **§6 Event Ordering / §6.1 Late Arrival**: read 順序の `(published, recordedAt, id)` は維持。watermark / propagation 検出 cursor は本 change で `append_seq` に置き換わる(`dag-propagation` delta 参照)。
- **§9 Invariants**: 不変条件 1 / 4 / 5 / 6 / 8 は維持。本 delta により次が追加される: 「identity_key NOT NULL UNIQUE が dedup の正規判定」「`append_seq` は AUTOINCREMENT で leaf-local 単調」「rehome は stored の id / published / recorded_at を保持する内部 append」。
