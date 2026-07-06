# Design: cognition-substrate

**Date:** 2026-07-06
**Session:** 仕様決定 grill-me(全24問。本 change には LETHE 側の決定を収録。runtime 側は change ④ の design.md を参照)
**Format:** 各決定は参照なしで読めるよう全文で記す。番号は本セッション全体の通し番号(Q 番号)を併記する。

---

## 決定台帳

### D1. 実装配置の原則 — LLM を呼ぶものは LETHE に置かない(Q3)

抽出パス・検証 dispatcher・ブリーフィング生成は全て LLM API を呼ぶ判断ロジックであり、change ④ で新設する個人エージェントランタイムリポジトリに置く。LETHE には取り込み(parser・importer)と projection(決定的 fold)と書き込み口という純粋なデータ仕事だけを残す。これは Rule_bot Stage 2 で確立した原則 —「LETHE はデータ基盤に徹し、エージェントの判断はハーネス側」— の個人システムへの適用であり、以後「どこに何を足すか」を毎回考えずに済む境界として維持する。v2 設計書(2026-07-05)の「cognition-loop は LETHE リポジトリの tools 層」という記述は本決定により正式に廃止される。

### D2. チャット履歴の取り込みは両サービスともブラウザ自動化・日次(Q5)

claude.ai と ChatGPT のチャット履歴は、どちらもブラウザ自動化による日次エクスポートで取得する。ダウンロード経路は共通で、claude.ai の既存インポート経路(パイプライン・ゲート通過・identity key 規則)をそのまま使い、ChatGPT 分は parser の追加のみで載せる。エクスポート成果物は source archive リポジトリの予約済み `chatgpt/` ディレクトリ(claude.ai 分は既存ディレクトリ)に置き、importer はそこを入力とする。公式エクスポート(リクエスト→メール→ダウンロード)は人手の割り込みが残るため採用しない。

### D3. ブラウザ自動化の故障検知は鮮度 fold を主、exit code 通知を従(Q14)

ブラウザ自動化は UI 変更で断続的に死ぬことが既知の性質であり、静かな故障(成功を返しながら空・部分データを出す)は exit code をすり抜ける。したがって検知の主系は lake 側の事実で判定する: ソースごとの最新観測時刻を畳む鮮度 projection を持ち、ソース別閾値(初期値: claude.ai 系 36 時間、ChatGPT 系 36 時間、コーディングエージェント系 48 時間、通信チャネル系はチャネル登録時に設定)を超過したソースを「入力系の欠測」として報告する。報告の消費者は change ④ の朝ブリーフィング(冒頭に掲載)と Slack DM 即時通知。従系として、エクスポートジョブ自体の exit code 失敗も Slack DM に即時通知する。同じ鮮度 projection が Eos wakeup の死活監視(nudge 観測が朝の時間帯に無音なら Eos wakeup が死んでいる)にもそのまま使われる。

### D4. corpus 検索は regex grep を継続(Q13)

2026-07-06 時点の実測で broad な一語クエリが「regex execution exceeded 500ms」で失敗したが、当該不具合は現在修正中であるため、FTS 化の判断は保留する。再開条件: 修正の検証後も broad クエリが実行予算を超過し続ける場合、corpus projection の materialization として FTS インデックス(tantivy を第一候補)を追加する。さらに先の拡張点として、意味的類似検索(語を思い出せない情報の再発見)の需要が具体化した時点で VectorIndex materialization(Rule_bot Stage 2 で設計済みの型)への拡張を検討する。いずれも projection の materialization という既存概念の内側で行い、原則は壊さない。

### D5. MCP write は汎用ツール1本・既存 scope 流用・全 kind(Q16)

MCP 面に `write_supplemental` ツールを1本追加する。kind を引数で受け、HTTP API `POST /supplementals` と完全に同一の registry 検証・Store 不変条件を通す(入口が違っても不変条件は一つ)。scope は新設せず HTTP API と同じ `write:supplemental` を流用する。kind 別の型付きツールへの分割や scope 分割は行わない — 動機は GPT Pro を含む外部チャットサーフェスを一級の書き込みクライアントにすることであり、汎用1本が最も広く対応する。ツール説明文には D6 のワークフロー(取り込み済み観測への後処理としての書き込み)を明記し、アンカー必須であることをスキーマで強制する。

### D6. MCP write のワークフロー — live-enrichment ではなく事後処理(Q17)

ブラウザのチャットから「その会話」についてのリアルタイム書き込みは行わない(live-enrichment 型は change ① 設計時に正式廃案済みであり、本セッションで再確認された)。書き込み対象は常に lake に取り込み済みの観測である: 会話 → 日次エクスポートで lake 投入 → 別セッション・別系統のモデルが lake を MCP read で読み、要約や claim を MCP write で書く。したがってアンカーは HTTP API と同じく解決可能な既存観測を必須とし、未解決参照 422 の不変条件は MCP 面でも無傷である。処理者の選定には交差規則(D7)が適用される: 会話を生成した系統のモデルは、その会話の要約・抽出を行わない。

### D7. 処理系統の交差規則(Q11。runtime 側 change ④ と共有する原則)

抽出・検証を含む全ての解釈処理は、会話を生成した系統の反対側のベンダーが行う。Claude 系の会話(claude.ai / Claude Code)は OpenAI 系が処理し、GPT 系の会話(ChatGPT / Codex)は Anthropic 系が処理する。独立性の軸を「抽出者 vs 検証者」ではなく「会話の生成者 vs 処理者」に置く — 自分の発話を自分で採点させない。どちらの系統も生成に関与していない中立ソース(Slack・Gmail・Discord・GitHub 等)は、抽出=Anthropic、検証=OpenAI に分割する(可逆な既定値として設定。ops で変更可能)。

### D8. projection は純粋 fold、解釈は supplemental か消費時整形(Q12)

全 projection は lake 観測+supplemental からの再計算可能な決定的 fold であり、LLM 呼び出しを含まない。LLM の解釈産物は supplemental として書かれた時点で観測扱いになり、fold の入力になる。朝の再開文面のような自然言語整形は change ④ の runtime が消費時に行う。これにより change ① C2 で確立した replay 決定性(同一入力 → 同一状態集合)が全 projection で保たれる。

### D9. 新設 projection 4本の責務

**鮮度 projection:** ソース識別子ごとに最新観測の published / recordedAt を畳み、閾値超過ソースの一覧を返す(D3)。
**再開スナップショット projection:** session-summary / parking / open claim をプロジェクト単位に畳み、「このプロジェクトはどこで止まっていて、何が宙吊りか」の再開カード素材を返す。
**plan-state projection:** open claim / parking / 決定台帳(supersedes 解決済み)をプロジェクト単位に畳み、次アクション合成(change ④)と判例検索の素材を返す。再開スナップショットとの違いは粒度と用途 — 再開スナップショットは中断からの復帰(セッション単位・直近性重視)、plan-state はポートフォリオ俯瞰(プロジェクト単位・全量)。
**カードキュー projection:** reply-draft@1 / reply-approval@1 / send-record@1 のチェーンを時刻順に畳み、カード状態(pending / approved / sent / skipped / expired)を計算する。承認インターフェースは Slack / Discord / Tailscale web の3面等価であり、どの面の承認イベントも同一に扱い、最初の承認で状態を確定する(以降の面には承認済みとして表示される)。不正遷移は claim queue と同じく skip+監査ログ。

### D10. registry の per-kind アンカーポリシーと新 kind 群

runtime と Eos wakeup の system イベント(nudge 発火・状態遷移・モード遷移・送信記録・ブリーフィング発行)は、アンカーすべき先行観測を持たない場合がある。このため Supplemental Kind Registry に `anchor_required`(既定 true)を追加し、system イベント系 kind のみ false+origin メタデータ(発生主体・時刻・文脈識別子)必須とする。既存 6 kind の意味は不変。新規登録 kind: reply-draft@1(anchor= 対象着信観測、必須)/ reply-approval@1(anchor= 対象 draft、必須)/ send-record@1(anchor= 対象 draft、必須。自動送信は approval なしで draft から直接遷移し、三条件審査の記録を payload に含む)/ nudge-event@1・eos-state-transition@1・mode-transition@1・briefing-issue@1(anchor 任意、origin 必須)。

### D11. Eos wakeup の学習状態は lake fold(Q20)

Eos wakeup の nudge 発火・5状態遷移・成果(覚醒→離宅所要時間)は全て supplemental(D10 の kind 群)として lake に書き、バンディットの posterior は fold で再計算可能にする。ルーター VM 側には実行時キャッシュと、ネットワーク断に備えた追記スプール(change ① D8 の failover spool と同型。復帰時 flush)のみを置く。これにより週次の自己拡張(lake から新戦略を合成)が成立し、VM は使い捨て可能になり、鮮度 projection が死活監視を兼ねる。

### D12. バックフィルは kind で分ける(Q22)

過去履歴への抽出バックフィルの範囲: decision と session-summary は全履歴(判例DBと再開素材としての価値が減衰しない)、claim と parking は直近 30 日のみ(古い claim の大半は既に死んでおり、初日の claim queue を腐った項目で溢れさせるとトリアージ制度自体が信用を失う)。バックフィルは日次バッチと同じコードパスの範囲指定実行とし、夜間に予算内で消化する。バックフィル起源の supplemental には backfill フラグを付け、claim queue projection はこれを保持して朝トリアージの主枠と分離した「棚卸し枠」に回せるようにする。実行主体は change ④ の抽出パスであり、LETHE 側の責務はフラグの搬送と projection でのフィルタ提供。

### D13. TTS エンジン(Q8。委譲判断としてここに記録)

ブリーフィング音声はローカル TTS とし、一次エンジンは AivisSpeech(日本語品質首位タイ、VOICEVOX 互換 HTTP API、CPU 動作可で GPU を LLM 系と取り合わない、二話者は話者モデル切り替えで実現)。感情スタイル制御が必要になった場合は Style-BERT-VITS2、本人音声のクローンを使いたくなった場合は Qwen3-TTS を差し替え先とする。呼び出しは runtime 内の一関数に閉じるため差し替えは軽微。参考: 2026-03 の5種実測比較(VOICEVOX / Style-BERT-VITS2 / AivisSpeech / CosyVoice / Qwen3-TTS)。実装は change ④。

### D14. change 分割と着手順(Q24)

② cognition-substrate(本 change、LETHE)/ ③ comms-channels(LETHE)/ ④ agent-runtime(新リポジトリ)/ ⑤ review-harness(LETHE 発、規約として全リポジトリ展開)の4本。⑤ と ② は並行着手可、③ は ② と独立なので並列度次第で同時、④ は ②③ の契約確定後。③ を ② に併合しない — 並列委譲の単位として分かれている方が、Codex への発注とレビューがそれぞれ独立に閉じ、被覆確認の一回あたり負荷が change ① 相当に保たれる。

---

## 再開条件台帳(本 change で凍結したもの)

- **FTS インデックス:** regex grep の現行不具合修正の検証後も、broad クエリが実行予算を超過し続ける場合に開く(D4)
- **VectorIndex / ANN:** 「語を思い出せない情報」の意味的検索需要が具体化した時に開く(D4)
- **per-kind 認可 scope:** 本人のデーモン以外の書き込み主体が現れた時に開く
- **MCP write の kind 制限:** 外部クライアントによる機構系 kind(claim-transition 等)の誤用が実際に観測された場合、registry に per-kind の書き込み経路制限を追加する形で開く

## 未決事項

- 新リポジトリ名(change ④。本人が決める — 判断コストをかける価値のある楽しい判断として委譲しない)
- 鮮度閾値の初期値の妥当性(2週間の運用データで見直し)
