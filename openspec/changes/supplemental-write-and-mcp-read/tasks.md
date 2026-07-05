# Tasks: supplemental-write-and-mcp-read

**並列実装の原則:** トラック間の依存は「仕様(契約)への依存」に限定してある。spec は本 change で確定済みなので、**全トラックは同時着手可能**。実装同士の待ち合わせは統合フェーズ(Track I)のみ。各タスクは 2 時間以内、agent role は implementer(Codex)/ reviewer(要件被覆の検証)。

## 依存グラフ

```
Day 0(単独先行・openspec 外の運用作業を含む)
  F1 archive 同期 cron ──────────────┐
                                      │
Track A(kind registry)              │         Track D(claude-code importer)
  A1 → A2 → A3                        │           D1 → D2 → D3 → D4
       │                              │         Track E(codex importer)
       ▼                              │           E1(実測)→ E2 → E3
Track B(write API)                  │           ※ D2 完了後、共有写像モジュールを E が利用
  B1 → B2 → B3                        │
       │                              ▼
Track C(claim queue projection)    Track G(corpus 有効化)
  C1 → C2 → C3 → C4                    G1 → G2
       │                              │
Track H(MCP port)                   │
  H1 → H2 → H3 → H4 ──────────────────┤
                                      ▼
                          Track I(統合・E2E)
                            I1 → I2 → I3
```

- A は B の前提(B の検証パスが A の関数を呼ぶ)。ただし B1(handler 骨格+authz)は A と並行可
- C は A のスキーマ定義(spec 上の fixture)にのみ依存 — B の実装を待たず fixture レコードで開発する
- H のうち H1〜H2(リスナー・OAuth 検証)は何にも依存しない。H3(ツール)のうち search/get/thread は既存 corpus projection に、claim_queue/search_decisions は C の API 契約(spec 確定済み)に依存 — C 実装中は契約に対するスタブで進める
- D・E・F・G は完全独立

---

## 0. Day 0(openspec を待たない即日運用)

- [x] F1 source archive リポジトリに `claude-code/` `codex/` `chatgpt/` を作成し、`~/.claude/projects/` 以下と Codex セッションディレクトリを追記ミラーする日次 cron(rsync --ignore-existing 系+git commit)を設置する。手動で初回同期を実行し、既存セッションが archive に入ったことを確認する
  - Spec: coding-agent-adapters CAGT-01 / 受け入れ: ローカルの JSONL を一つ手動削除しても archive に残る
  - **30 日の削除窓が現在も過去を削っているため、本タスクのみ即日実施**

## Track A. Supplemental Kind Registry

- [ ] A1 `SupplementalKindSchema` 型と Registry 拡張(登録・取得・バージョン規則)を実装する
  - Spec: SKIND-01 / 受け入れ: バージョン規則違反の登録が拒否される unit test
- [ ] A2 payload 検証純関数(JSON Schema 準拠判定、違反フィールド列挙)を実装する
  - Spec: SKIND-02 / 受け入れ: 必須欠落・型違反・enum 違反の検出 test
- [ ] A3 初期 6 kind(claim@1 / decision@1 / parking@1 / verification-result@1 / claim-transition@1 / session-summary@1)を登録し、`reject_unregistered_kinds` 設定を実装する
  - Spec: SKIND-03, SKIND-04 / 受け入れ: parking の resume_context 欠落拒否、未登録 kind 拒否の contract test

## Track B. 書き込み API

- [ ] B1 `POST /supplementals` handler 骨格+`write:supplemental` スコープ認可を実装する(検証はまず Store 既存分のみ)
  - Spec: SUPW-01, SUPW-02 / 受け入れ: スコープ不足 403、正常系 201→ 再起動後読める E2E
- [ ] B2 Store 不変条件のエラー写像(空アンカー/未解決参照 → 422+詳細、AppendOnly 衝突 → 409)を実装する
  - Spec: SUPW-03, SUPW-05 / 受け入れ: 各エラーパスの contract test、同一内容別 UUID の二重書き込みが両方 201
- [ ] B3 kind スキーマ検証(A2)を書き込みパスに結線する
  - Spec: SUPW-04 / 受け入れ: verification_mode 欠落 claim の 422 test(A3 完了後)

## Track C. Claim Queue Projection

- [ ] C1 `crates/projections/claim-queue` を作成し、fixture supplemental 集合に対する重複解消 fold(kind+derivedFrom+payload ハッシュ、model_version 除外、代表 ID+吸収 ID 一覧)を実装する
  - Spec: CLQ-02 / 受け入れ: バッチ再実行 fixture で 1 件に縮約、吸収 ID が監査可能
- [ ] C2 状態機械 fold(transition / verification-result チェーンの時刻順畳み込み、不正遷移 skip+監査ログ)を実装する
  - Spec: CLQ-03 / 受け入れ: replay 決定性 test(同一入力 → 同一状態集合)
- [ ] C3 同源グループ化と決定台帳ビュー(supersedes チェーン解決、全文検索)を実装する
  - Spec: CLQ-04, CLQ-05 / 受け入れ: 同一会話 3 claim が 1 グループ、置換済み決定に superseded_by 付与
- [ ] C4 読み取り API(`GET /projections/claim-queue`, `GET /projections/decisions`)を追加する
  - Spec: CLQ-06 / 受け入れ: 状態フィルタ・ページングの contract test

## Track D. Claude Code importer

- [ ] D1 `apps/tools/lethe-import-claude-code` を作成し、JSONL 行のパース(user / assistant / tool_use / メタデータ行の型判別、不正行 skip)を実装する
  - Spec: CAGT-02, Failure Modes / 受け入れ: 実セッション JSONL のパース test(壊れ行混入でも完走)
- [ ] D2 背骨写像(指示文・応答文・ツール呼び出しメタデータのみ。ツール結果・引数本体の除外)を**共有モジュール**として実装する
  - Spec: CAGT-02 / 受け入れ: .env 読み取りを含む fixture で、生成 canonical に内容が一切現れない negative test
- [ ] D3 identity key(`claude-code:{session_id}:{message_uuid}:H(canonical)`)と published=メッセージ timestamp、sidechain 親子メタデータを実装する
  - Spec: CAGT-03, CAGT-04 / 受け入れ: 再実行で全件 duplicate の idempotency test
- [ ] D4 archive ワーキングコピーを入力とする取り込み CLI(既存 lethe-import-claude と同型のゲート通過)を完成させ、実データで初回取り込みを行う
  - Spec: CAGT-01, CAGT-04 / 受け入れ: 実 archive からの E2E、観測数と duplicate 数の報告

## Track E. Codex importer

- [ ] E1 Codex のセッション保存場所・行スキーマを実測確認し、結果を coding-agent-adapters spec に追記する(サブエージェント相当の有無を含む)
  - Spec: CAGT-05 / 受け入れ: spec 追記がレビュー可能な形で存在
- [ ] E2 D2 の共有写像モジュールに Codex 形式のフロントエンドを実装する
  - Spec: CAGT-02 / 受け入れ: D2 と同じ negative test を Codex fixture で通過
- [ ] E3 `apps/tools/lethe-import-codex` CLI を完成させ、実データで初回取り込みを行う
  - Spec: CAGT-04 / 受け入れ: idempotency test+E2E

## Track G. 個人 lake の corpus 有効化

- [ ] G1 corpus-projection のインスタンス設定で、テキストを持つ全観測(会話・GitHub 系・コーディングエージェント)を対象化する
  - Spec: MCPR-05 / 受け入れ: 各ソース種別 1 件以上が grep でヒットする E2E
- [ ] G2 コーディングエージェント観測のスレッド復元(get_thread がセッション+sidechain 構造を返す)を corpus 側で確認・必要なら拡張する
  - Spec: CAGT-03, MCPR-04 / 受け入れ: 親子セッションを跨ぐ thread test

## Track H. MCP read port

- [ ] H1 selfhost 内に MCP 専用リスナー(別ポート、Streamable HTTP)を実装する。config に mcp_bind_addr を追加
  - Spec: MCPR-01, MCPR-02 / 受け入れ: 内部 API ポートと MCP ポートの分離を確認する test
- [ ] H2 OAuth リソースサーバ(JWT 検証: 署名・exp・audience。`/.well-known/oauth-protected-resource` 公開。401+WWW-Authenticate)を実装する。認可サーバはマネージド基盤(DCR 有効)を選定し issuer を config 化
  - Spec: MCPR-03 / 受け入れ: 期限切れ・audience 不一致・正常トークンの 3 パス test(認可サーバはモック鍵で)
- [ ] H3 5 ツール(search_lake / get_record / get_thread / claim_queue / search_decisions)を実装する。claim 系 2 ツールは C4 完了までは CLQ-06 契約へのスタブで進めてよい
  - Spec: MCPR-04 / 受け入れ: ツール説明文が spec レビューを通過、各ツールの contract test
- [ ] H4 Tailscale Funnel で MCP ポートのみを公開し、実際の claude.ai カスタムコネクタ登録 → OAuth フロー → ツール呼び出しの手動疎通を行う
  - Spec: MCPR-01, MCPR-06 / 受け入れ: ブラウザ版 claude.ai から search_lake が実データを返す

## Track I. 統合

- [ ] I1 E2E: 手書きの claim を POST → claim_queue projection に open で現れる → claim-transition を POST → 状態が変わる、の一連を通す
  - Spec: SUPW-01, CLQ-03 / 受け入れ: シナリオ test が CI で通る
- [ ] I2 E2E: コーディングエージェント取り込み済み観測にアンカーする decision を POST → search_decisions で引ける
  - Spec: SUPW-03, CLQ-05, CAGT-04 / 受け入れ: 横断シナリオ test
- [ ] I3 要件被覆の抜き取り確認(本人): 各 spec の SHALL に対する test 対応表を生成し、公開面(Funnel 対象ポート・トークン検証)を実機で最終確認する
  - 受け入れ: 全 SHALL に judgement+evidence が存在
