# Design: supplemental-write-and-mcp-read

**Date:** 2026-07-05
**Session:** 要件定義 grill-me(認知外部化システム設計記録 v2 の後続)
**Format:** 各決定は参照なしで読めるよう全文で記す。番号は本 change 内でのみ有効。

---

## 決定台帳

### D1. 書き込み API は汎用エンドポイント+kind レジストリによるスキーマ検証

`POST /supplementals` の一本とし、claim / decision のようなドメイン語彙をエンドポイント名に焼き込まない(LETHE は薄いプリミティブを晒すデータ基盤、という既定の設計思想)。ただし payload は不透明 JSON にせず、Registry に登録されたバージョン付き kind スキーマ(`claim@1` 等)で検証する。個人 lake インスタンスでは未登録 kind の書き込みを拒否する。理由: 検証モード(check/generate)を欠いた claim が入ると後続の検証 dispatcher が分岐できず、「正しいか分からない思考が頭に戻る」という解消対象の問題が再発するため。スキーマ強制の位置は書き込み点が唯一正しい(複雑性の先送り回避)。

### D2. 全ての書き込みは lake 既在コンテンツへのアンカーを持つ。ライブ会話からの書き込みは行わない

SupplementalStore の既存不変条件 —「derivedFrom は空であってはならない」「参照する observation は lake に実在しなければならない」— を一切緩めず、そのまま HTTP API の契約に昇格する。要約・抽出は取り込み済みの生会話に対して別モデルが事後に行うものであり(当該会話中の AI は精度が落ちるため使わない)、書き込み時点で参照先が常に存在する。これに伴い、2026-06-15 のセッションで検討された「MCP を会話末尾の enrichment 書き込みチャネルにする」構想は正式に破棄(superseded)。

### D3. 解釈は supplemental、系の出来事は observation

claim の意味的状態(未検証→検証済み等)は AppendOnly の supplemental チェーンで表現する: 遷移のたびに新レコード(`claim-transition@1` / `verification-result@1`)を積み、`derivedFrom.supplementals` で対象 claim を参照する。現在状態は Projection の畳み込みで計算する(下記 D6)。一方、dispatcher が投げた・カードが提示された・承認された、といった解釈を含まない運用イベントは observation として lake に入る(ループ自己計測の対象。実装は change ④)。両者は claim の ID で突合可能。ManagedCache による status 上書き方式は棄却 — 滞留時間の再計算には遷移時刻の全履歴が要り、version 履歴をイベントログとして使うのは AppendOnly の再発明であるため。

### D4. Supplemental の ID は UUID。書き込み時の重複排除はしない

決定的 ID(内容ハッシュ由来)による書き込み時 dedup は採らない。チャット履歴の重複は取り込み前(source archive の git リポジトリ段階)で解消済みであり、observation 層は exact idempotency 済み。supplemental 重複の実質的な発生源はバッチの再実行とクラッシュ再試行のみで、これは読み取り側の畳み込みで吸収する(D5, D6)。

### D5. 読みは全て Projection 経由。生 supplemental を読んで行動する消費者を恒久禁止

dispatcher・MCP エージェント・将来のあらゆる消費者は、生の SupplementalRecord を列挙して行動してはならない(SHALL NOT)。必ず Projection(畳み込み済みビュー)を経由する。これは D4 が安全に成立する条件そのもの: dedup 点が畳み込み一箇所に集約されるからこそ、重複 claim の二重 dispatch(検証 API の二重課金)が構造的に防がれる。生読みが必要なのはデバッグ・監査のみで、行動の根拠にしてはならない。

### D6. Claim Queue Projection の畳み込み意味論

同一性判定: 種別(kind)・由来元(derivedFrom の集合)・正規化 payload のハッシュが一致すれば同一 claim。抽出モデルのバージョンは同一性判定に**含めない**。含めると、モデル更新後の再抽出で言い回しの揺れた claim が別物としてキューに湧き、バッチ提示(一度に見せる件数を絞る)というワーキングメモリ保護要求に直撃するため。旧モデル claim の自動 superseded 遷移も棄却 — 新モデルが同じ claim を拾い直す保証がなく、拾い漏れ=誰にも検証されないまま閉じる事故が「全思考は 24 時間以内に終端」の原則を静かに破る。

提示: 同じ由来元(同じ会話)から導出された claim 群を「同源グループ」として束ねて返す。状態管理は claim 単位のまま、判断カード 1 枚にグループ 1 つが対応できる形。

状態計算: claim を根とし、`claim-transition@1` / `verification-result@1` の追記チェーンを時刻順に畳み込んで現在状態を得る。状態値: open / dispatched / verified / refuted / inconclusive / terminated / parked。

### D7. 書き込みの認可は単一 scope `write:supplemental`

kind 別スコープ(認可語彙が kind 追加のたびに動く)も、トークンごとの許可 kind 設定(ops 依存 — ops はつなぎであり恒久設定を持たせない)も採らない。既存の `authorize_headers` 機構にスコープ文字列を一つ足すだけ。最小権限の細分化は必要が実証されてから。

### D8. `created_by` はパイプライン/クライアント身元、モデルは `model_version` フィールド

actor をモデル名にすると同一パイプラインのモデル更新で帰属が分裂する。`actor:extraction-pass` のような安定した身元を created_by に、実際に使ったモデルは既存の model_version フィールドに記録する。

### D9. 初期 kind スキーマ 6 種

- `claim@1` — 必須: statement(主張本文)、verification_mode(check = 既存原則との整合確認で済む / generate = 原理の生成が要る)。任意: context、source_quote
- `decision@1` — 必須: statement。任意: rationale、alternatives、supersedes(置き換える過去の決定への参照)
- `parking@1` — 必須: statement、resume_context(再開用の最小文脈)。resume_context を必須にするのは「棚上げは再開最小文脈つきでのみ許す」という終端プロトコルの定義そのものであるため
- `verification-result@1` — 必須: verdict(consistent / inconsistent / inconclusive)、reasoning。アンカーは対象 claim の supplemental
- `claim-transition@1` — 必須: to_state。アンカーは対象 claim の supplemental
- `session-summary@1` — 会話単位の要約。事後抽出パス(change ②)を「会話→要約→個別 claim」の二段にするための親アンカー。要約はブリーフィングと再開スナップショットの入力にも再利用される。本 change では登録のみ行い、生成は change ②

### D10. MCP read port は公開エンドポイント。認可サーバはマネージド基盤に委譲

2026-07 時点の公式仕様で確認済みの制約: カスタムコネクタは claude.ai ブラウザ版・Desktop・モバイル・Cowork のいずれから使う場合も**Anthropic のクラウド基盤から接続される**ため、公開到達性が必須(Tailscale 内限定案はそもそも不成立)。認証は OAuth 2.1+PKCE 必須、固定 Bearer トークン不可、ユーザー同意なしの機械間認証不可。サーバ側は保護リソースメタデータ(`/.well-known/oauth-protected-resource`)を公開し認可サーバを発見させる。

構成: LETHE 側は**トークン検証のみを行うリソースサーバ**に徹し、トークン発行・動的クライアント登録・同意画面はマネージド ID 基盤(Auth0 無料枠等、DCR 有効化)に委譲する。OAuth 認可サーバのフル自作は個人ツールとして過大な実装面積であり、失敗時の影響が lake の入口全体に及ぶため棄却。

### D11. 公開経路は Tailscale Funnel、公開するのは MCP ポートのみ

PC は固定 IP を持たないためトンネルが必要。既に Tailscale を使う環境なので、追加部品ゼロ・ドメイン購入不要の Funnel を採用。制約として lake は PC 稼働中しか読めないが、これは既知の割り切り(将来の個人 NAS 移設で解消)であり公開後も変わらない点を明記。

### D12. MCP サーバは selfhost と同一プロセス・別リスナー

管理用エンドポイント(同期発火等)と同一ポートに `/mcp` を足す案は、Funnel がポート単位公開であるため管理面まで公開に晒すことになり棄却。別プロセス案は投影読みに HTTP 一往復を挟み可動部が過剰で棄却。同一プロセス内で MCP 専用リスナーを別ポートに張り、Funnel はそのポートだけを公開する。トランスポートは Streamable HTTP(SSE 単独はコネクタ基盤で非対応)。

### D13. MCP ツールは厳選 5 種

search_lake(全文検索)/ get_record(レコード本文)/ get_thread(前後文脈)/ claim_queue(未終端 claim 一覧、同源グループ形)/ search_decisions(決定台帳検索)。全ルートの機械的ツール化はツール一覧の肥大で選択精度を下げるため棄却。最小 2 種(検索+取得)では「いま未検証の claim は何か」を検索クエリの工夫で拾うことになり確実性がないため、中核概念には専用ツールとして名前を与える。再開スナップショットは change ② でここに追加。

### D14. 個人 lake の検索対象はテキストを持つ観測すべて

会話も GitHub 系(issue / PR / コメント / commit メッセージ)もコーディングエージェント履歴も全部。寮 lake では同意管理が範囲を縛ったが、個人 lake は読み手が本人とそのエージェントのみで絞る理由がなく、「会話で決めたことと実装の突き合わせ」という横断こそが価値の本体。

### D15. コーディングエージェント履歴は「会話の背骨」のみ取り込む

取り込むもの: 本人の指示文、エージェントの応答文、ツール呼び出しのメタデータ(ツール名と対象の参照 — ファイルパス等)。取り込まないもの: ツール実行結果の中身(ファイル内容・コマンド出力)。根拠は三つの重なり: 境界原理(成果物の内容は git が一次ストアで、lake への複製は情報を増やさない)、容量(トランスクリプトの大半はツール結果)、安全性(公式文書が明言する通り、ツール結果には .env の値やコマンド出力経由の認証情報が混入しうる。公開 MCP から全文検索可能な場所に流し込むのは D10 の公開構成と正面衝突する)。

### D16. サブエージェント会話も背骨を取り込む

メイン会話だけでなくサブエージェント(子エージェント)のトランスクリプトも同じ背骨規則で取り込む。委譲した調査の過程と結論が追跡可能になるため。メイン↔サブの親子関係はセッションメタデータ(sidechain / 親セッション参照)として canonical に保持する。

### D17. 生 JSONL は source archive リポジトリへ日次同期。lake はそこから取り込む

Claude Code のトランスクリプトは既定 30 日で起動時に自動削除され、削除無効化設定(0 指定)は現行バージョンで弾かれ、mtime 基準の削除により保持期間内でも消える不具合が報告されている。よって保持期間設定に頼る案は棄却(保全をベンダーの掃除ロジックの正しさに賭けることになる)。既存の claude.ai 生エクスポート保全と同じ規律に載せる: 日次 cron で生 JSONL を private の source archive リポジトリへ同期し、lake は archive から取り込む。archive が生(ツール結果込み全文)、lake が正規化後(背骨のみ)、という役割分担。フック機構によるリアルタイム転送は Codex 側に同等保証がなく二系統別実装になるため棄却。

### D18. archive リポジトリは既存の一本に同居

ディレクトリ構成: `claude-ai/`(既存)、`claude-code/`、`codex/`、`chatgpt/`(change ② 用に予約)。archive の役割は「再クロール不能または揮発する一次ストアの生データ保全」という単一規律であり、ソースごとにリポジトリを増やす理由がない。認証情報混入の条件は claude.ai エクスポートと同じで既に private 前提。分割が必要な規模になればその時に行い、lake は archive の中身から取り込むため場所の変更は lake に影響しない。

### D19. 観測単位と identity(コーディングエージェント)

既定のパターンに従う: per-message 粒度、identity key は `source:object_id:H(canonical)` 形式。Claude Code は `claude-code:{session_id}:{message_uuid}:H(canonical)`、published はメッセージの timestamp(イベント時刻。取り込み時刻は使わない — routing key の既定則)。Codex のセッション保存場所・行形式は実装時に実測確認する(文書化が Claude Code ほど安定していないため。tasks の先頭項目)。

---

## 未決事項(本 change のスコープ外として明示)

- マネージド ID 基盤の具体選定(Auth0 / Logto 等)は実装時に無料枠・DCR 対応で比較して決める。spec は「OAuth 2.1 リソースサーバとして振る舞う」要求のみ固定
- per-kind 認可の将来拡張(必要が実証されたら)
- claim キューのページング・上限パラメータの初期値(実測後に ops へ)
