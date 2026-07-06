# Change Proposal: comms-channels

**Version:** 1.0
**Date:** 2026-07-06
**Status:** Proposed
**Repository:** LETHE
**Type:** チャネルレジストリ+通信 ingest adapter 3本(System Laws 不変)
**Source:** 仕様決定セッション 2026-07-06(Q18, Q23)および v2 設計記録の通信ループ決定(C20–C22)

---

## Why

返信 SLO 30 分を全チャネルで守るには、着信が lake に観測として入り続けることが前提になる。返信の起草・承認・送信は change ④ agent-runtime の仕事だが、その材料である着信観測と、チャネルという概念の一元管理(consent_scope・SLO 設定・break-glass)は data substrate 側の責務である。本 change は通信チャネルを generic なレジストリで管理し、Slack / Gmail / Discord の3チャネルを ingest+返信 SLO 対象として立ち上げる。

## What Changes

- **ADDED:** チャネルレジストリ(チャネル識別子・種別・consent_scope・SLO 値・break-glass ホワイトリスト・有効/無効を一元管理。ops 設定で宣言、起動時に registry へ載る)
- **ADDED:** Slack ingest adapter(既存の Slack 取り込み機構を個人 lake のチャネル文脈で構成。DM・メンション・所属チャネルの着信)
- **ADDED:** Gmail ingest adapter(受信メールの観測化。スレッド構造の保持)
- **ADDED:** Discord ingest adapter(DM・所属サーバの着信。カード承認面の用途が主でも、観測の取り込み口は同じ機構に載せる)
- **ADDED:** 着信観測への SLO メタデータ付与(着信時刻・チャネル参照・返信要否判定の素材。判定自体は runtime)
- **MODIFIED:** なし

## Non-Goals

- 返信の起草・承認カード・送信実行(change ④ agent-runtime)
- 送信用トークンの管理と送信 API(runtime 側。LETHE は読み取り専用の観測基盤に徹する)
- Teams 等の追加チャネル(チャネルレジストリへの adapter 一枚追加として将来対応)
- エスカレーション判定ロジック(runtime。LETHE は break-glass ホワイトリストの保持のみ)

## Affected System Laws

違反なし。着信は通常の観測として IngestionGate を通過し、consent_scope はチャネルレジストリの宣言に従って付与される(組織 Slack = org_federated、DM・メンション・自発言 = personal を推奨既定とする)。Filtering-before-Exposure Law はそのまま適用される。

## Module References

M02 Registry / M03 Ingestion / M05 Projection Engine / M09 Adapter Policy。spec delta は specs/ 配下 2 本。

## Rollout

change ② と独立・並行可。change ④ の返信パイプラインは本 change の着信観測契約に依存する。
