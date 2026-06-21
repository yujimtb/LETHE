# Change Proposal: generalize-platform

**Version:** 0.1 (draft)
**Date:** 2026-06-13
**Status:** Proposed
**Type:** Refactoring (semantics-preserving / System Laws 不変)

---

## Why

LETHE は学生寮を初期コンテキストとして設計されたが、仕様の中核
(Observation Lake / Registry / Projection / Governance / System Laws)は
すでにドメイン非依存である。一方で現行実装には次の固定化が残っており、
汎用データ基盤として再利用する際の障壁になっている。

| 固定化 | 現状 | 問題 |
| --- | --- | --- |
| ドメイン語彙 | `person_page`、寮前提の Entity Type、`/api/persons` ルート | コアが特定ドメインの型を知っている |
| デプロイ前提 | 単一バイナリ + SQLite + ローカル blob + 認証なし API | internal-only 前提が破れると即座に安全性を失う |
| アダプタ | Slack / Google Slides がコードに直結 | 新 source 追加が trait 実装ではなくコア改修になる |
| 障害処理 | 同期中の失敗を即時返却(全体停止)、個別対処的 rollback | 観測単位の失敗隔離・再開がなくスケールしない |
| 設定 | `.env` の平坦な環境変数 | source を N 個に一般化できない |

本 change は、これらを **System Laws と既存モジュール契約(M01–M15)を
変えずに** 解消するためのリファクタリング要件を定義する。

## What Changes

- **ADDED:** 新規 capability `platform-generalization`(要件 GEN-01〜GEN-08)
  および `platform-robustness`(要件 ROB-01〜ROB-09)
- **MODIFIED:** M13 Person Page を「コア外の参照 Projection 実装」へ降格
  (spec 自体は保持、配置と依存方向のみ変更)
- **MODIFIED:** M14 API Serving に認証・認可要件を必須として追加
  (normative な定義は `platform-robustness` ROB-01)
- **MODIFIED:** M09 Adapter Policy を trait ベースの plugin contract に具体化
  (normative な定義は `platform-generalization` GEN-03)
- **RENAMED(提案):** モジュール表へ M16 Platform Generalization /
  M17 Platform Robustness を追加し `_index.md` の Dependency DAG を更新
  (`tasks.md` の Index Update を参照)

## Capabilities

### New Capabilities

- `platform-generalization`: ドメイン語彙の分離 / レイヤード workspace /
  pluggable adapter contract / storage effect ports / schema 進化 /
  derivation provider 抽象化 / identity 解決の一般化 / 構造化 multi-source
  設定(要件 GEN-01〜GEN-08)
- `platform-robustness`: API 認証認可 / IngestionGate / 冪等取り込み /
  失敗隔離と再開可能 sync / 原子性境界 / スケーラブル bootstrap /
  migration & replay / 可観測性と監査経路 / secret 管理とリソース上限
  (要件 ROB-01〜ROB-09)

### Modified Capabilities

- `person-page`: M13 を `lethe-projection-person` crate としてコア外へ配置し、
  コア crate からの依存を禁止する placement 制約を追加(delta:
  `specs/person-page/spec.md`)

> M14 API Serving と M09 Adapter Policy の振る舞い変更は、それぞれ
> `platform-robustness` ROB-01 と `platform-generalization` GEN-03 として
> 新 capability 側に normative に定義する。既存 `openspec/specs/api-serving.md`
> / `adapter-policy.md` は親 overview として保持し、実装着手時に
> 相互参照を追記する(`tasks.md` の Index Update で反映)。

## Impact

- Affected specs: `adapter-policy`, `api-serving`, `person-page`,
  `runtime`, `governance`(接続点のみ), `_index.md`
- Affected code: `src/` 全域(workspace 分割)、`Cargo.toml`,
  `scripts/public-release-audit.ps1`(クロスプラットフォーム化)
- Breaking changes: **BREAKING** `/api/persons/*` ルートは deprecation
  期間を経て `/api/projections/{id}/*` へ移行(互換 alias を 1 リリース維持)

## What Does NOT Change (Non-goals)

- M01 Domain Kernel の型・law・failure model(正規参照先のまま)
- Append-Only / Replay / Effect Isolation / Explicit Authority /
  No Direct Mutation / Filtering-before-Exposure の各 Law
- 既存 Observation の永続フォーマット(migration で互換読み取りを保証)
- M07 Write-Back は本 change の間 **凍結** する(GEN-03 の adapter contract
  凍結後に再開)
- マルチテナント(namespace 分離)/ dynamic plugin load / Postgres 参照実装
  昇格は本 change の scope 外(`design.md` の Open Questions で扱う)

## Rollout / Sequencing

依存関係に基づく着手順(各 Phase が後続の merge gate)。詳細タスクは
`tasks.md`、設計上の判断は `design.md` を参照。

```
Phase 0: GEN-02 workspace 分割 + GEN-04 storage port 化   ← 以降の全変更の足場
Phase 1: ROB-01 認証/認可 + ROB-02 IngestionGate 接続      ← 公開前提の安全条件
Phase 2: GEN-03 adapter contract + ROB-03/ROB-04 失敗隔離  ← 汎用 source 追加の実用性
Phase 3: GEN-01 person_page 切り出し + GEN-05〜08          ← ドメイン分離の完成
Phase 4: ROB-05〜09                                        ← 運用品質
```

## System Laws Affected

本 change は System Laws を **変更しない**。ただし以下の Law の適用面が
明確化・強化される:

- **Filtering-before-Exposure Law:** ROB-01(blob 配信)/ ROB-08(filtering 監査)
- **Replay Law:** GEN-04(blob content-addressing)/ GEN-06(pinned replay)/
  ROB-07(golden replay test)
- **Append-Only Law:** ROB-02(quarantine)/ ROB-03(冪等取り込み)
- **Explicit Authority Law:** ROB-01(認可)/ ROB-02(PolicyEngine 接続)
