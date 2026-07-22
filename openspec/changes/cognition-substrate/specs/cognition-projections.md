# Spec Delta: cognition-projections(cognition-substrate)

対象: 鮮度 / 再開スナップショット / plan-state / カードキューの projection 4本。全て LLM を含まない決定的 fold であり、replay 決定性(同一入力 → 同一状態集合)を満たす SHALL。

## FRSH-01: 鮮度 projection

ソース識別子ごとに最新観測の published と recordedAt を畳み、ソース別閾値(config。初期値: claude.ai 系 36h / ChatGPT 系 36h / コーディングエージェント系 48h / 通信チャネル系はチャネル登録時設定)との比較結果を返す SHALL。閾値超過ソースは欠測として一覧化される SHALL。

受け入れ: fixture の時刻操作で欠測判定が閾値通りに変わる test。replay 決定性 test。

## FRSH-02: 鮮度 projection の読み取り API

`GET /projections/freshness` を追加し、全ソースの鮮度と欠測一覧を返す SHALL。MCP read の既存 5 ツールからは既存ツール(get_record 等)の対象外とし、専用ツール追加は行わない(消費者は runtime とブリーフィング)。

受け入れ: contract test。

## RSNP-01: 再開スナップショット projection

session-summary / parking / open claim をプロジェクト単位に畳み、プロジェクトごとに「最終活動時刻・直近 session-summary・宙吊り parking 一覧・open claim 一覧」を返す SHALL。プロジェクトへの帰属はレコードの project メタデータ(欠落時は uncategorized)による SHALL。LLM 呼び出しを含まない SHALL。

受け入れ: 同一プロジェクトの複数セッション fixture が1カードに畳まれる test。replay 決定性 test。

## PLST-01: plan-state projection

open claim / parking / 決定台帳(supersedes チェーン解決済みの現行決定)をプロジェクト単位に畳み、ポートフォリオ俯瞰(全プロジェクトの宙吊り数・滞留時間・現行決定一覧)を返す SHALL。再開スナップショットとの用途差(復帰 vs 俯瞰)を spec 上明記する SHALL。

受け入れ: superseded な決定が現行一覧に現れない test。滞留時間計算の test。

## CARD-01: カードキュー projection の状態機械

reply-draft@1 / reply-approval@1 / send-record@1 のチェーンを時刻順に畳み、カード状態(pending / approved / sent / skipped / expired)を計算する SHALL。不正遷移は skip し監査ログに記録する SHALL。

カードは reply-draft@1 の `created_by` が `agent:<name>` 形式の場合に
`agent_name=<name>` を含める SHALL。`created_by` に有効な agent 帰属がない
場合は、末尾 `/agent/<name>` 形式の `lineage` をフォールバックとして使用して
よい。いずれも該当しない場合は `agent_name=null` とする SHALL。

受け入れ: 正常系遷移 test、不正遷移 skip test、replay 決定性 test。

## CARD-02: 多面承認の first-approval-wins

複数インターフェース(Slack / Discord / Tailscale web)からの reply-approval@1 が同一 draft に対して複数追記された場合、published 最古の承認で状態を確定し、以降の承認イベントは冪等に吸収する SHALL。承認・却下(skip)が競合した場合も同じ時刻順規則で決定する SHALL。

受け入れ: 3面から順不同で届く承認 fixture が単一の確定状態に畳まれる test。

## CARD-03: カードキューの読み取り API

`GET /projections/card-queue` を追加し、状態フィルタ・チャネルフィルタ・ページングに対応する SHALL。自動送信された send-record(承認イベントなしで sent へ遷移したもの)を区別して返せる SHALL(翌朝ブリーフィングの自動送信一覧の材料)。

受け入れ: contract test。自動送信フィルタ test。

## CLQ-07(claim queue 拡張): backfill 棚卸し枠

claim queue projection は supplemental の backfill フラグを保持し、状態フィルタと直交する backfill フィルタを読み取り API に追加する SHALL(朝トリアージ主枠と棚卸し枠の分離)。

受け入れ: backfill=true のみ / false のみのフィルタ test。
