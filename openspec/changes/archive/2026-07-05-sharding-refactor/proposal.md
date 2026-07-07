# Change Proposal: sharding-refactor

**Version:** 0.1 (draft)
**Date:** 2026-06-17
**Status:** Proposed
**Type:** Architectural refactor (System Laws 強化、契約は不変)
**Source:** [Sharding design](../../../../docs/architecture/sharding.md) — D0〜D12 すべて LOCKED の決定台帳

---

## Why

LETHE を大きめのワークスペース(寮 Slack)に投入するにあたり、**複数インスタンス(物理 Lake 分割)を、ハードコードな mapping に頼らず・再現性(replay)を保ったまま・完全な冪等性を保証して導入する**必要がある。現行実装は次の固定化を抱えている。

| 固定化 | 現状 | 問題 |
| --- | --- | --- |
| dedup 意味論 | `idempotencyKey` は free-form / optional ([crates/engine/src/lake/ingestion.rs](../../../../crates/engine/src/lake/ingestion.rs))、確率的(SimHash + Bloom)前提が draft に残存 | 同 channel:ts の編集が silent drop / 偽 merge を起こす |
| identity key 構成 | content hash 非含有([crates/adapters/slack/src/slack/mapper.rs](../../../../crates/adapters/slack/src/slack/mapper.rs)) | Slack 編集を「duplicate」で捨てる(silent 欠損) |
| 衝突判定 | full observation(reactions / meta 含む)を比較([crates/engine/src/lake/store.rs](../../../../crates/engine/src/lake/store.rs)) | reactions の独立変化で偽 `Conflict` |
| 永続層 | in-memory `LakeStore`(Vec + HashMap)が authoritative ([crates/engine/src/lake/store.rs](../../../../crates/engine/src/lake/store.rs))、SQLite は補償ロールバック | 多 leaf で RAM に収まらず破綻 |
| watermark cursor | グローバル Vec への `usize` position([crates/engine/src/propagation/watermark.rs](../../../../crates/engine/src/propagation/watermark.rs)) | 分割するとグローバル position が消える |
| leaf 解決 | 単一 Vec の線形 filter([crates/engine/src/lake/store.rs](../../../../crates/engine/src/lake/store.rs))、resolver 未実装 | 分割不能 |
| placement | 固定ビット trie / SimHash routing(draft) | route がタイミング依存になり dedup が割れる |
| migration primitive | fresh ingest が `new_id()` + 新 `recordedAt` を毎回付与([crates/engine/src/lake/ingestion.rs](../../../../crates/engine/src/lake/ingestion.rs)) | split / failover drain / blue-green で identity と propagation cursor が壊れる |

本 change は、[Sharding design](../../../../docs/architecture/sharding.md) §2 の D0〜D12 を normative な仕様要件として確定し、**System Laws(特に Idempotency / Replay / Append-Only)を強化**するためのリファクタリング要件を定義する。

## What Changes

- **ADDED:** 新規 capability `sharding`(要件 SHARD-01〜SHARD-26、本 change の `specs/*` 配下に分散)
- **MODIFIED:** M03 Observation Lake — identity key 契約、exact dedup、append_seq、SQLite-authoritative per leaf、rehome primitive
- **MODIFIED:** M06 DAG Propagation — watermark を global `usize` → per-(projection × leaf) `append_seq` cursor
- **MODIFIED:** M09 Adapter Policy — adapter が (object_id, canonical タプル) を宣言、`idempotencyKey` を NOT NULL に格上げ
- **MODIFIED:** M15 Runtime — routing keyspec、Patricia trie、partition log、failover spool、logical→physical resolver
- **UPDATED:** `domain_algebra.md` の Idempotency Law を確率的 → 完全(決定的)冪等に精緻化
- **OVERRIDE:** draft 段階の SimHash routing / 確率的冪等性 / 固定ビット trie / `idempotencyKey` optional を上書き

## Capabilities

### Modified Capabilities

- **`observation-lake`** (M03): identity = `source : object_id : H(canonical_content)` を per-message で必須化。per-leaf SQLite を authoritative 化、exact index(`identity_key` UNIQUE)を冪等性の正規判定に。`append_seq INTEGER PRIMARY KEY AUTOINCREMENT` を watermark cursor として追加。`canonical_json` 列で衝突判定。**rehome primitive**(stored Observation を id/published/recorded_at 保持で再 route する内部 append)を fresh ingest と別レールとして導入。
- **`dag-propagation`** (M06): watermark を per-(projection × leaf) `append_seq` cursor に再定義。検出と適用を分離、適用は可換 + 冪等 fold に限定。split 後は baseline で全量再配送を許容(β で frontier 子移管)。
- **`adapter-policy`** (M09): adapter が (object_id, canonical タプル) を宣言、core が H して `identity_key` を組立。canonical_content の境界(include / exclude / 正規化規則)を契約化。
- **`runtime`** (M15): `routing_key = coarse(published month) : coarse(published year) : source : container : fine(published)`(寮の cross-year same-month 串刺し読み co-locate)。Patricia trie + lazy split + atomic cutover protocol。partition log(`initialize` / `split_prepare` / `split_commit` / `failover` / `recover`、control-plane `event_seq`)。AP + reconcile failover + 専用 spool。`candidate_leaves` + streaming k-way merge resolver。

## Impact

- Affected specs: `observation-lake`, `dag-propagation`, `adapter-policy`, `runtime`, `domain-kernel`(Idempotency Law 精緻化)
- Affected code: `src/lake/*`, `src/propagation/*`, `src/adapter/*`, `src/runtime/*`, `src/self_host/persistence.rs`
- Breaking changes:
  - **BREAKING(internal data)**: 既存 LETHE ストアは disposable → re-crawl(D12.2)。旧→新 shim は作らない。
  - **BREAKING(adapter contract)**: `to_observations` が free-form `idempotencyKey` を返す経路から、(object_id, canonical タプル) を宣言する経路に変更。Slack / Google Slides adapter は作り直し。
  - **BREAKING(DB schema)**: `observations` テーブルは `append_seq` PK + `identity_key` NOT NULL UNIQUE + `canonical_json` 列に再定義。

## What Does NOT Change (Non-goals)

- System Laws(Append-Only / Replay / Effect Isolation / Explicit Authority / No Direct Mutation / Filtering-before-Exposure)— Idempotency Law は precision を上げるが意味論は破らない
- M01 Domain Kernel の型・failure model(正規参照先のまま)
- M08 Governance の policy 判定経路
- M12 Identity Resolution の confidence 契約(D11.4 で「placement に解決済みエンティティを使わない」と整合)
- マルチテナント / dynamic plugin load / Postgres 参照実装昇格は scope 外
- M07 Write-Back は本 change の間 **凍結** する(adapter contract 凍結後に再開)
- CDC / Merkle ベースの大型文書 content-model は本 change の最後(任意マイルストーン、D12 フェーズ7)

## Rollout / Sequencing

依存上、identity 計算は adapter の canonical タプル生成に依存するため、identity 契約と adapter を **縦スライス** で通す。詳細タスクは `tasks.md`、設計判断と未確定事項は `design.md`。

```
Phase 0: keyspec 凍結(routing + identity、完全形)、単一 leaf=root              ← 後付け不可
Phase 1: 3部 identity 契約を Slack 1本で縦に通す + rehome primitive 実装        ← per-message + exact dedup + append_seq
Phase 2: adapter 横展開(gslides 作り直し / claude.ai zip importer 新規)         ← canonical タプル宣言契約
Phase 3: 多 leaf 化(partition log + Patricia + lazy split + 全再配置)           ← rehome mode (a) を split に適用
Phase 4: resolver(D11) + per-(proj × leaf) watermark propagation(D10)         ← logical→physical 解決と incremental
Phase 5: failover spool + drain(D8)                                            ← AP + reconcile、rehome mode (a)
Phase 6: blue/green keyspec migration(D12.3)                                    ← rehome mode (b)
Phase 7: CDC/Merkle(内部編集される大型文書専用、別 content-model)              ← 任意
```

## System Laws Affected

本 change は System Laws を **変更しない**。ただし以下の Law の適用面が **強化** または **精緻化** される:

- **Idempotency Law**(強化): 確率的(SimHash + Bloom で ε 取りこぼし)を撤回し、per-leaf exact index(`identity_key` UNIQUE)で完全(決定的)冪等に。衝突判定は full observation でなく canonical タプルのみを exact compare(R4)。
- **Replay Law**(強化): `tree(L)` を `initialize` + `split_commit` の fold で一意復元、全 leaf の merge-sort by `(published, recorded_at, id)` で決定的再現。failover window 境界は control-plane `event_seq` で再現(R5)。
- **Append-Only Law**(保存): 各 leaf も append-only、split は再配置(更新でない)、blue/green は旧 retire。rehome は内部 append であり mutation ではない。
- **Effect Isolation Law**(保存): resolver は Imperative Shell、Kernel は論理 lake のみを見る(D11.3)。

詳細根拠は [Sharding design](../../../../docs/architecture/sharding.md) §1.1 の対照表を参照。
