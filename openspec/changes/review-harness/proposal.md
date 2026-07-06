# Change Proposal: review-harness

**Version:** 1.0
**Date:** 2026-07-06
**Status:** Proposed
**Repository:** LETHE 発、規約として全リポジトリ(agent-runtime 含む)へ展開
**Type:** CI ハーネス+規約(プロダクトコード変更なし)
**Source:** 仕様決定セッション 2026-07-06(Q21)。v2 設計記録 C18(要件被覆行列)

---

## Why

今回の change 群は3〜4本が Codex への並列委譲で同時に走る。change ① では要件被覆表(requirements-coverage.md)を手動生成したが、これを手動のままにすると、被覆検証というレビュー作業が本人のボトルネックとして回帰する — まさにこのシステム全体が消そうとしている律速そのものである。SHALL 単位の被覆を機械的に検証・報告する CI ハーネスに置き換える。

## What Changes

- **ADDED:** spec パーサ(openspec の spec delta から SHALL 要件 ID を抽出)
- **ADDED:** 被覆アノテーション規約(test コードに要件 ID を宣言する属性/コメント形式。例: `// covers: MCPW-02`)
- **ADDED:** 被覆行列ジェネレータ(要件 ID × test の対応表を生成し、未被覆 SHALL を CI で fail させる。evidence 型: automated test / manual evidence 記録の2種を許容し、manual は tasks.md への evidence 記載を検出)
- **ADDED:** CI 統合(LETHE と agent-runtime の両 CI に組み込み。PR ごとに被覆差分を報告)

## Non-Goals

- test の品質評価(被覆は対応の存在確認であり、test の十分性はレビューの領分)
- 既存 change ① への遡及適用の強制(被覆表は既に手動生成済み。アノテーション移行は機会的に行う)

## Rollout

完全独立。change ② と並行着手可 — 先に通れば ②③④ の全 change が被覆行列レビューの恩恵を受けながら進むため、着手順の先頭に置く。

---

# Design(小規模のため proposal に併記)

## D1. 検証の対象は「対応の存在」

ハーネスが保証するのは「全 SHALL に judgement+evidence が存在する」ことのみ。change ① の I3 で本人が手動で行った確認の機械化であり、それ以上(test が要件を正しく検証しているか)はレビューに残す — ここを機械化しようとすると LLM 評価が入り、CI の決定性が壊れる。

## D2. evidence 2型

automated(要件 ID アノテーション付き test の存在と pass)と manual(tasks.md 内の要件 ID への evidence 記載の存在)。公開面の実機確認のような自動化不能な受け入れは manual 型で被覆される。

## D3. 規約としての展開

ハーネス本体は LETHE リポジトリに置き、agent-runtime は CI 設定でこれを取り込む。spec 形式・アノテーション形式は両リポジトリ共通規約とする。

# Spec Delta

## RVH-01

spec delta ファイルから SHALL 要件 ID を機械抽出できる SHALL。抽出漏れ(ID 形式不正)は CI エラーとする SHALL。
受け入れ: 本 change 群の全 spec ファイルのパース test。

## RVH-02

test コードの被覆アノテーションと tasks.md の manual evidence を検出し、要件 ID × evidence の被覆行列を生成する SHALL。未被覆 SHALL が存在する場合 CI を fail させる SHALL。
受け入れ: 未被覆 fixture での fail test。

## RVH-03

PR ごとに被覆差分(新規要件・新規被覆・被覆喪失)を報告する SHALL。
受け入れ: 差分レポートの test。

# Tasks

- [ ] T1 spec パーサ+ID 規約(RVH-01)
- [ ] T2 アノテーション検出+行列生成+fail 判定(RVH-02)
- [ ] T3 CI 統合(LETHE)+PR 差分レポート(RVH-03)
- [ ] T4 agent-runtime CI への展開(Track 0 完了後)+規約ドキュメント
