# Tasks: sharding-refactor

**Change:** sharding-refactor
**Date:** 2026-06-17

実施順は `proposal.md` の Rollout に従う。各 Phase の完了が次 Phase の merge gate となる。要件 ID は `specs/*/spec.md` の SHARD-* / SHARD-PROP-* / SHARD-ADAPT-* / SHARD-RT-* を参照。各 Gate の受け入れ基準は対応する Scenario をテスト化したものとする。

横断不変条件は [sharding_refactor.md §1](../../../docs/architecture/sharding.md) を正典とし、本 change の全 Phase で破ってはならない(不変条件1〜10)。

## Phase 0 — Keyspec Freeze (single leaf=root)

> 前提: `design.md` U1「現行コードとの差分マッピング」を先に実施し、SHARD-04(exact dedup)と現行 [crates/engine/src/lake/ingestion.rs](../../../crates/engine/src/lake/ingestion.rs) の `IngestResult` 同型性を確認してから 0.1 に着手する。

- [x] 0.1 `routing_keyspec` と `identity_keyspec` の version pin スキーマを定義(SHARD-RT-01 / SHARD-01)
- [x] 0.2 partition log の `initialize` event 形式を定義(両 keyspec を pin)、SQLite テーブル `partition_log` を追加(SHARD-RT-04)
- [x] 0.3 単一 leaf = root の bootstrap path を実装(ディレクトリ分割なし、keyspec のみ完全形で確定)(SHARD-RT-04 §単一インスタンス開始)
- [x] 0.4 `domain_algebra.md` Idempotency Law を「完全(決定的)冪等」へ精緻化(`domain-kernel` 親 spec 更新)

**Gate P0:** partition log に `initialize` が記録され、両 keyspec の不変性が DB 制約で強制されること(SHARD-RT-04「`initialize` の不変性」)。

## Phase 1 — Identity Contract Vertical Slice (Slack 1 本)

- [x] 1.1 `observations` テーブルを SHARD-06 の schema へ migrate
      - `append_seq INTEGER PRIMARY KEY AUTOINCREMENT`(rowid alias)
      - `id TEXT NOT NULL UNIQUE`(UUIDv7)
      - `identity_key TEXT NOT NULL UNIQUE`(exact index)
      - `canonical_json TEXT NOT NULL`(衝突比較対象)
      - `recorded_at TEXT NOT NULL`(provenance、cursor には使わない)
      - `observation_json TEXT NOT NULL`
- [ ] 1.2 SQLite を per-leaf authoritative 化、in-memory `LakeStore` を非永続キャッシュへ降格(SHARD-05)
- [x] 1.3 dedup を SQLite UNIQUE 違反 → `Duplicate(existing_id)` に一本化(補償ロールバック除去)(SHARD-04)
- [x] 1.4 衝突判定を `same_idempotent_observation`(full observation 比較) → stored `canonical_json` のみの exact compare に置換(SHARD-04 §canonical タプル衝突判定、R4)
- [x] 1.5 Slack adapter を per-message 化(channel:ts ベース)、(object_id, canonical タプル) 宣言契約へ書き換え(SHARD-02 / SHARD-ADAPT-01)
- [x] 1.6 canonical_content の境界(include: sender / body / event_time / 添付 sha256、exclude: reactions / 編集 wrapper / ingestion meta、正規化: NFC / CRLF→LF / JSON canonical / timestamp 表記統一)を実装(SHARD-03)
- [x] 1.7 **rehome primitive** を fresh ingest と別レールで実装(SHARD-07)
      - stored `id` / `published` / `recorded_at` / `consent` を保持
      - 着地 leaf で `append_seq` のみ新採番
      - mode (a) stored identity_key 信頼 / mode (b) canonical 再計算 + `observation_json.idempotency_key` 再シリアライズ
      - fresh ingest 経路は通さない(R1)
- [x] 1.8 property test: 編集なし再取り込み → `Duplicate`、編集 → 新 distinct Observation、claude.ai 再 export 欠落 → no-op

**Gate P1:**
- Slack 編集を含む再取り込みで silent drop / 偽 merge / 偽 split が発生しないこと(SHARD-01「編集 = 新 distinct」/ SHARD-04「無変更 → Duplicate」)
- rehome primitive で stored Observation の id / published / recorded_at が保持されること(SHARD-07「rehome は fresh ingest と別レール」)
- 衝突時に reactions 変化が偽 `Conflict` を起こさないこと(SHARD-04「canonical タプルのみ比較」)

## Phase 2 — Adapter Horizontal Roll-out

- [x] 2.1 Google Slides adapter を canonical タプル宣言契約へ作り直し(revisioned snapshot は維持)(SHARD-ADAPT-01)
- [x] 2.2 claude.ai zip importer を新規実装(message uuid / 欠落時は conversation_uuid + parent_message_uuid チェーン位置から決定的に導出)(SHARD-ADAPT-02)
- [x] 2.3 generic adapter framework(object_id 抽出 trait + canonical タプル trait)(SHARD-ADAPT-01)
- [x] 2.4 adapter conformance test: 同 source の reactions / 編集 wrapper 変化で `identity_key` が変わらないこと
- [x] 2.5 `Observation.idempotencyKey` を optional → 必須・高エントロピー・解決可能に格上げ(SHARD-01)

**Gate P2:** Slack / Google Slides / claude.ai の 3 adapter 共通 conformance test 通過(SHARD-ADAPT-01「同 source 再取り込み」)。

## Phase 3 — Multi-Leaf Sharding

- [x] 3.1 Patricia trie の永続表現(`tree(L)` = `initialize` + `split_commit` の fold)(SHARD-RT-02)
- [x] 3.2 `routing_key` 計算実装: `coarse(month) : coarse(year) : source : container : fine(published)`(SHARD-RT-01)
- [ ] 3.3 lazy split 実装(容量駆動、中身ごと全再配置 = rehome mode (a) を子 leaf へ)(SHARD-RT-02 §lazy split)
- [ ] 3.4 **split atomic cutover protocol**: `split_prepare`(子 build / 親へ route) → catch-up(差分 rehome) → write barrier(短い freeze) → `split_commit`(route 子へ原子的切替、`bit_index` 記録)(SHARD-RT-03、R9)
- [x] 3.5 partition log events: `split_prepare` / `split_commit` を `event_seq` + optional control-plane timestamp で永続(SHARD-RT-04、R10)
- [x] 3.6 split 中の crash: `prepare` だけで `commit` なしは `tree(L)` に影響しない(replay test)
- [x] 3.7 leaf id = 不透明 `lake:<uuid>`、path / 責任ビット範囲は log から計算(SHARD-RT-04)

**Gate P3:**
- 容量到達 → split → cutover → 親 retire のシナリオで dedup が割れないこと(SHARD-RT-03「split 後の route 一貫性」)
- `tree(L)` が `initialize` + `split_commit` の fold で一意復元できること(SHARD-RT-04「partition log replay」)
- split 中の crash で未 commit split が tree に現れないこと

## Phase 4 — Resolver and Incremental Propagation

- [x] 4.1 `candidate_leaves(filter, log)` 実装: routing 軸 prune + published time-window を `(month, year)` bucket 集合に展開して部分木 union(SHARD-RT-06、R3)
- [x] 4.2 streaming k-way merge by `(published, recorded_at, id)` 実装(SHARD-RT-07)
- [x] 4.3 resolver を Imperative Shell に配置、Kernel は論理 lake のみを見る(SHARD-RT-07 §Effect Isolation)
- [ ] 4.4 watermark を `projection_watermarks` テーブルから per-(projection × leaf) cursor table へ migrate(SHARD-PROP-01)
- [x] 4.5 cursor を `recorded_at` / `published` → leaf-local `append_seq` に置換(SHARD-PROP-02、R2)
- [ ] 4.6 propagation 検出: per-leaf `WHERE append_seq > ?` で tail を読む(SHARD-PROP-04)
- [ ] 4.7 propagation 適用: 可換 + 冪等 fold に限定(順序依存 fold は core サポート外)、at-least-once + 冪等 apply 契約(SHARD-PROP-03)
- [x] 4.8 split 後の全 Observation 再配送を baseline で許容(SHARD-PROP-05、R11)、profiling で β(frontier 子移管)を後付け可能に
- [x] 4.9 supplemental index は明示宣言の read mode のみ(暗黙 full-scan fallback 禁止)(SHARD-RT-08、R6)
- [x] 4.10 placement に解決済みエンティティ(person / subject / project)を入れない(SHARD-RT-08 §placement 原則)

**Gate P4:**
- 多 leaf 環境で projection 全鑑 + filter クエリが既存単一 leaf と同一結果を返すこと(SHARD-RT-07「touch leaf 群の streaming merge」)
- rehome した古い `recorded_at` の Observation が watermark frontier 下に沈まないこと(SHARD-PROP-02「append_seq cursor」、R2)
- 冪等 fold の二重 apply で結果が変わらないこと(SHARD-PROP-03「at-least-once + 冪等」)

## Phase 5 — Failover Spool

- [x] 5.1 喪失 leaf 専用 spool ストア(`spool:<failover-id>` SQLite)を実装、行は `spool_seq INTEGER PRIMARY KEY AUTOINCREMENT` + `identity_key` + `canonical_json` + `id` + `published` + `recorded_at` + `observation_json`(SHARD-RT-05、R13、R15)
- [x] 5.2 spool は append-only、`identity_key` UNIQUE / `append_seq` 単調は持たない(spool 自体は dedup しない)(SHARD-RT-05)
- [x] 5.3 `failover` / `recover` event を partition log の control-plane `event_seq` で記録(SHARD-RT-04、R5)
- [x] 5.4 drain: `ORDER BY spool_seq` で 1 件ずつ rehome mode (a)(stored identity_key を信頼して現 tree へ再 route → 着地 leaf 内部 append)(SHARD-RT-05 §drain)
- [x] 5.5 fresh ingest 経路は通さない(R1) + drain 後 spool を retire
- [x] 5.6 transient dup が排出時に exact index で決定的に除去される property test(SHARD-RT-05 §最終状態決定的)

**Gate P5:**
- 喪失 leaf 復旧後の最終状態に dup が残らないこと(SHARD-RT-05「window 境界と排出順の再現」)
- failover window と drain 順序が partition log + `spool_seq` で replay 再現できること(SHARD-RT-05、R15)

## Phase 6 — Blue/Green Keyspec Migration

- [ ] 6.1 新 keyspec で新構造(新 partition log + 新 leaf 群)を立てる
- [ ] 6.2 全観測を rehome mode (b)(canonical タプル再計算、`identity_key` column + `canonical_json` column + `observation_json.idempotency_key` を新 keyspec で再シリアライズ)で新構造へ migrate(SHARD-07 §mode (b)、R14)
- [ ] 6.3 iterative catch-up(bulk → 差分反復 → 短い freeze → cutover)、または freeze。いずれも rehome primitive 上で(SHARD-07)
- [ ] 6.4 read を cutover、旧構造を retire(物理 leaf を削除)
- [ ] 6.5 旧 keyspec + 旧 partition log の version を metadata として履歴保持

**Gate P6:**
- 新構造の replay で migration 前後の現状態が決定的再現されること(Replay Law)
- mode (b) rehome で `identity_key` column / `canonical_json` column / `observation_json.idempotency_key` の 3 箇所が乖離しないこと(SHARD-07、R14)

## Phase 7 — CDC / Merkle (Optional)

内部編集される大型文書専用の content-model マイルストーン。本 change の他 Phase の前提ではないので、必要が顕在化したときに着手する。

- [x] 7.1 CDC / Merkle content-model を新 schema として定義
- [x] 7.2 既存 revisioned snapshot との共存ポリシー

## Index Update (merge 時)

`openspec/specs/_index.md` への追記案:

| #   | Module                    | Spec File                         | Scope                                                    | MVP? |
| --- | ------------------------- | --------------------------------- | -------------------------------------------------------- | ---- |
| —   | (既存 M03 / M06 / M09 / M15) | observation-lake.md / dag-propagation.md / adapter-policy.md / runtime.md | 本 change により sharding 関連要件(SHARD-*)を取り込み | ✓    |

依存関係追記: SHARD-* は M03 / M06 / M09 / M15 を横断する。意味論の正典は依然として [Domain algebra](../../../docs/architecture/domain-algebra.md) / [System overview](../../../docs/architecture/system-overview.md) / [Runtime reference](../../../docs/architecture/runtime-reference.md) / [ADR backlog](../../../docs/decisions/adr-backlog.md)、本 change の sharding 観点上の正典は [Sharding design](../../../docs/architecture/sharding.md)。

## Frozen During This Change

- M07 Write-Back の実装着手(Phase 2 完了 = adapter contract 凍結後に解除)
- 既存 LETHE 永続データ(D12.2 により disposable、Phase 1 開始時に re-crawl 前提で破棄)
