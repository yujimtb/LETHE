# Change Proposal: supplemental-write-and-mcp-read

**Version:** 1.0
**Date:** 2026-07-05
**Status:** Proposed
**Type:** New public write surface + MCP read port + adapter additions(System Laws 不変)
**Source:** 認知外部化システム設計記録 v2(2026-07-05)の change ①。決定台帳は本 change の design.md に収録

---

## Why

認知外部化システムの中核配管。思考の終端産物(claim / decision / parking)を lake の Supplemental に書き込め、AI エージェントがどのサーフェスからでも読める状態を作る。書けて読めるループが通れば、後続の抽出パス・検証 dispatcher・ブリーフィングは全てこの上に載る。

あわせてコーディングエージェント(Claude Code / Codex)の会話履歴を取り込み対象に加える。Claude Code のトランスクリプトは既定 30 日で自動削除され、削除不具合の報告も複数あるため、保全は緊急性を持つ。

## What Changes

- **ADDED:** Supplemental 書き込み API(`POST /supplementals`、scope `write:supplemental`)
- **ADDED:** Supplemental Kind Registry(M02 Registry の拡張。kind ごとの JSON Schema 検証、初期 6 種: claim / decision / parking / verification-result / claim-transition / session-summary)
- **ADDED:** Claim Queue Projection(supplemental チェーンの畳み込み。重複解消・状態計算・同源グループ化)
- **ADDED:** MCP read port(同一プロセス別リスナー、Streamable HTTP、OAuth 2.1 リソースサーバ、Tailscale Funnel で当該ポートのみ公開、厳選 5 ツール)
- **ADDED:** `lethe-import-claude-code` / `lethe-import-codex`(apps/tools。会話の背骨のみ取り込み)
- **ADDED:** source archive リポジトリへのコーディングエージェント生 JSONL 日次同期(ディレクトリ追加: `claude-code/` `codex/` `chatgpt/`)
- **ADDED:** 個人 lake での Corpus Projection 有効化(テキストを持つ全観測を対象)
- **MODIFIED:** なし(M01〜M17 の既存契約は不変。SupplementalStore の不変条件はそのまま API 契約に昇格)

## Non-Goals

- 事後抽出パス・検証 dispatcher・ブリーフィング生成(change ② cognition-loop)
- ChatGPT 会話インポータ(change ②。ただし archive の `chatgpt/` ディレクトリは本 change で予約)
- 通信チャネル群・返信パイプライン(change ③④)
- 再開スナップショット projection(change ②)
- claude.ai ブラウザ自動化エクスポート(既存 issue の系)
- per-kind の認可粒度(単一 scope で開始。拡張は将来判断)
- MCP write ツール(読み取り専用で開始)

## Affected System Laws

違反なし。関与するのは:

- **Explicit Authority Law:** 書き込みは `write:supplemental` scope で正当化。MCP 経由の読みは OAuth トークン検証で正当化
- **Filtering-before-Exposure Law:** MCP の読みは全て Projection 経由(生 Supplemental の直接読み出しで行動する消費者を SHALL NOT で禁止)
- **Append-Only Law:** claim の状態遷移は上書きでなく追記チェーン。不変

## Module References

M02 Registry / M04 Supplemental Store / M05 Projection Engine / M09 Adapter Policy / M14 API Serving。spec delta は specs/ 配下 5 本。

## Rollout

週末一単位。トラック並列実装(tasks.md の依存グラフ参照)。archive 同期 cron は Day 0 に単独先行。
