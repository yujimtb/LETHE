# Change Proposal: personal-lake-ingestion

**Version:** 0.1 (draft)
**Date:** 2026-07-04
**Status:** Proposed
**Type:** New deployment instance + adapter additions (System Laws 不変)
**Source:** 2026-07-04 設計セッション — 決定台帳 P1〜P13 は本 change の design.md に収録

---

## Why

プロジェクト状態が頭の中に不完全な形で滞留しており、外部化の速度がボトルネックになっている。方針: **lake は一つ、取り出し口は好きなだけ**。個人 LETHE インスタンス(寮 lake とは完全に別インスタンス)を常設し、主要な思考の痕跡 2 系統 — claude.ai 会話と GitHub 開発履歴(issue / PR / commit / review / timeline)— を ingest 可能にする。

ゴールは「全データが入った状態」ではなく「**入れ続けられる配管が通った状態**」。append-only + exact idempotency により、取り込みの遅延は無損失であることがアーキテクチャで保証されているため、配管開通後の網羅は差分作業になる。

## 境界原理(本 change の正規化判断の根拠)

git と LETHE は同族(content-addressed / append-only な観測ストア)であるため、境界を原理で固定する:

> **コミュニケーション / 意思決定の痕跡(横断結合の対象)は lake へ。成果物の内容(他所で耐久的に version 管理済み)は一次ストアに残し、lake は参照(content address)を持つ。**

- LETHE の価値は保存ではなく**横断**(単一 identity 空間・単一時間軸・単一 Projection エンジンでの結合)。
- diff / ソースコード本体は git が権威的一次ストア。lake への複製は情報を増やさない。Projection が内容を要する場合は ADR-002 の source-native read パターンで解決する。
- 差分エンコーディングは identity 層ではなく格納層の関心事(git の blob=全量 snapshot / packfile=delta 分離と同型。LETHE では Phase 7 CDC/Merkle に対応)。canonical 形に delta を置くと re-crawl 起点依存で `H(canonical)` が再現されず Idempotency Law が壊れるため、SHALL NOT。
- git の append-only は慣習であって保証ではない(force-push / rebase / repo 削除)。ゆえに commit の**出来事スパイン**(sha / author / message / 時刻)は lake に写す価値がある。
- 系の対偶: 一次ストアが再クロール不能な source(claude.ai)は、生 export を private git リポジトリ(source archive)に保存し、耐久一次ストアの代替を自前で構成する。

## What Changes

- **ADDED:** 個人 lake インスタンス(Docker / 個人 PC、将来個人 NAS へ移設)。寮 lake と物理分離 — consent 境界 = インスタンス境界
- **ADDED:** `lethe-import-claude` CLI(`apps/tools`、実装済み `ClaudeAiImporter` の配線)
- **ADDED:** `lethe-import-github` CLI(`apps/tools`)+ `gh api` dump スクリプト(fetch / mapping 分離)
- **ADDED:** claude.ai source archive リポジトリ(private git)の運用規律
- **MODIFIED:** なし(M01〜M17 契約・System Laws は不変。SHARD-ADAPT-01 の canonical tuple 契約に新 adapter が準拠する)

## Out of Scope(明示的除外 — 判断済み、遅延ではない)

- L9 自動化(Browser-Use → Gmail Observer)— 配管開通後の別 change
- L10 GitHub Observer adapter(poll 常駐化)— 本 change の CLI mapper を再利用する
- Phase C 取り出し口(MCP / API 公開)、ダッシュボード Projection
- バックアップ自動化(個人 NAS 導入時。P7 参照 — archive repo が claude 系の回復不能性を先に塞ぐ)

## Open Questions

1. commit `published` の author date / committer date 選択の実運用検証(P8 では author date を採用、committer date は payload 保持)
2. 個人 NAS 導入時の移設手順(SQLite ローカル volume 制約は寮 NAS と同一)
