# Change Proposal: cognition-substrate

**Version:** 1.0
**Date:** 2026-07-06
**Status:** Proposed
**Repository:** LETHE
**Type:** 取り込み経路の追加+projection 4本+MCP write+registry 拡張(System Laws 不変)
**Source:** 認知外部化システム仕様決定セッション(2026-07-06、Q1–Q24)。決定台帳は本 change の design.md に収録

---

## Why

認知外部化システムの LETHE 側基盤の第二段。change ① で「書けて読める」配管が閉じたが、claim queue は空のままである — 書く主体(抽出パス・検証 dispatcher)と、その材料になる取り込み経路(claude.ai / ChatGPT のチャット履歴)、消費側が読む projection 群がまだ存在しない。本 change は LLM を呼ばない純粋なデータ仕事(取り込み・決定的 fold・書き込み口)を全て LETHE に揃え、LLM を呼ぶ判断コンポーネントは change ④ agent-runtime(新リポジトリ)へ完全に分離する。

## What Changes

- **ADDED:** ChatGPT 会話 parser(claude.ai インポート経路と共用の取り込みパイプラインに載せる)
- **ADDED:** ブラウザ自動化エクスポートの取り込み経路(claude.ai / ChatGPT とも日次。archive の `chatgpt/` ディレクトリを実運用化)
- **ADDED:** 鮮度 projection(ソース別最新観測時刻の fold。欠測検知・Eos wakeup 死活監視を兼ねる)
- **ADDED:** 再開スナップショット projection(session-summary / parking / open claim のプロジェクト単位 fold。LLM なし)
- **ADDED:** plan-state projection(open claim / parking / 決定台帳のプロジェクト単位 fold。次アクション合成の素材)
- **ADDED:** カードキュー projection(reply-draft / 承認イベント / 送信記録チェーンの fold。最初の承認で状態確定)
- **ADDED:** MCP write ツール `write_supplemental`(汎用1本、scope は `write:supplemental` 流用、全登録 kind、HTTP と同一の registry 検証、アンカー必須)
- **ADDED:** Supplemental Kind Registry への per-kind アンカーポリシー(`anchor_required`)と新 kind 群(reply-draft@1 / reply-approval@1 / send-record@1 / nudge-event@1 / eos-state-transition@1 / mode-transition@1 / briefing-issue@1)
- **ADDED:** 抽出バックフィルの範囲指定実行に対する取り込み側の対応(backfill フラグの搬送)
- **MODIFIED:** なし(M01〜M17 の既存契約は不変)

## Non-Goals

- 抽出パス・検証 dispatcher・ブリーフィング生成・返信パイプライン本体(change ④ agent-runtime)
- 通信チャネルレジストリと Slack / Gmail / Discord adapter(change ③ comms-channels)
- FTS インデックス(regex grep の現行不具合修正後に broad クエリが 500ms 予算を超過し続ける場合のみ再開)
- embedding / VectorIndex の個人 lake 導入(「語を思い出せない情報」の検索需要が具体化した時に再開。拡張点のみ spec に予約)
- per-kind の認可 scope 分割(書き手が全員本人のデーモンである間は受益者不在。外部の書き込み主体が現れた時に再開)

## Affected System Laws

違反なし。Append-Only Law: 遅延結合・状態遷移は全て fold 時の決定的処理で、レコードの書き換え・照合ジョブは存在しない。Filtering-before-Exposure Law: MCP write は FilteringGate の下流に置かず(書き込みは露出ではない)、読みは従来通り projection 経由のみ。Explicit Authority Law: MCP write は既存 `write:supplemental` scope で正当化。

## Module References

M02 Registry / M04 Supplemental Store / M05 Projection Engine / M09 Adapter Policy / M14 API Serving。spec delta は specs/ 配下 4 本。

## Rollout

change ⑤ review-harness と並行着手可(相互依存なし)。change ③ とも独立。change ④ は本 change の projection 契約確定後に着手。
