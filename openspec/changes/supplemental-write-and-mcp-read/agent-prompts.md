# Agent Prompts: supplemental-write-and-mcp-read

この文書は、`/goal` で Track ごとに別エージェントへ投げるための実装依頼文です。

各エージェントには「共通プロンプト」と「担当 Track プロンプト」を両方渡してください。統合担当は Track I を担当し、各 Track のハンドオフを受けて統合します。

---

## 共通プロンプト

```text
/goal OpenSpec change `supplemental-write-and-mcp-read` の担当 Track を実装してください。

作業場所:
- リポジトリ: D:\userdata\docs\projects\skcollege_database
- change: openspec/changes/supplemental-write-and-mcp-read

必ず最初に読むこと:
- AGENTS.md またはスレッド冒頭の AGENTS.md instructions
- openspec/changes/supplemental-write-and-mcp-read/proposal.md
- openspec/changes/supplemental-write-and-mcp-read/design.md
- openspec/changes/supplemental-write-and-mcp-read/tasks.md
- 自分の担当 Track に対応する specs/*/spec.md

実装方針:
- 仕様書の SHALL に厳密に従うこと。
- 明示されていない後方互換、互換レイヤ、エイリアス、silent fallback、場当たり的な代替経路を追加しないこと。
- 安全に継続できない場合は failing fast すること。`os.getenv()` などへデフォルト引数を入れて設定不足を隠さないこと。
- 読み取り消費者は Projection 経由に限定すること。生 supplemental を行動根拠にしないこと。
- 既存のコード構造、命名、テストパターン、設定パターンを先に確認し、それに合わせて最小変更で実装すること。
- unrelated な変更は触らないこと。既存の未コミット変更を勝手に戻さないこと。

完了条件:
- 担当 Track の tasks.md チェック項目を完了状態へ更新すること。
- 関連する spec/design/ops/README 等、実装結果に合わせる必要があるドキュメントを更新すること。
- 担当 Track の受け入れ条件に対応するテストを追加または更新し、実行すること。
- テストできない外部実機項目がある場合は、理由と手動確認手順を明記すること。
- 最後に `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-<ID>.md` を作成し、次を記録すること:
  - 実装した内容
  - 変更ファイル一覧
  - 実行したテストと結果
  - 未完了または統合担当に引き継ぐ事項
  - 仕様 SHALL と evidence の対応

統合担当への注意:
- 統合担当は Track I を担当するため、Track I の E2E を壊すような暫定実装を残さないこと。
- スタブを使ってよいと tasks.md に明記された場合でも、最終ハンドオフではスタブ箇所を明示すること。
```

---

## Day 0 / Track F: archive 同期 cron

```text
/goal `supplemental-write-and-mcp-read` の Day 0 / F1 を実装してください。

担当範囲:
- tasks.md の `0. Day 0` / F1 のみ。
- spec: `specs/coding-agent-adapters/spec.md` の CAGT-01。

実装内容:
- 既存の private source archive リポジトリを特定する。
- archive に `claude-code/`, `codex/`, `chatgpt/` を作成する。
- `~/.claude/projects/` 以下を `claude-code/` へ追記的にミラーする日次 cron を設置する。
- Codex セッションディレクトリを実測で特定し、`codex/` へ追記的にミラーする日次 cron を設置する。
- `chatgpt/` は change ② 用の予約ディレクトリとして README 付きで作成する。
- 同期は archive 側の削除を行わない。一次ストア側の削除を archive に伝播させてはならない。
- 手動で初回同期を実行し、既存セッションが archive に入ったことを確認する。

禁止事項:
- `rsync --delete` 相当の削除伝播は禁止。
- 設定不足をデフォルト値で隠す fallback は禁止。
- archive に含める raw JSONL を lake 正規化済みデータで置き換えないこと。archive は生データ保全が役割。

受け入れ条件:
- ローカルの JSONL を一つ手動削除しても archive 側に残ることを確認できる。
- cron または Windows タスク等、実環境で日次実行される設定が存在する。
- CAGT-01 の SHALL に対する evidence を handoff に記録する。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-f.md` を作成する。
```

---

## Track A: Supplemental Kind Registry

```text
/goal `supplemental-write-and-mcp-read` の Track A を実装してください。

担当範囲:
- tasks.md の Track A: A1, A2, A3。
- spec: `specs/supplemental-kind-registry/spec.md`。

実装内容:
- `SupplementalKindSchema` 型を追加する。
- Registry に supplemental kind schema の登録、取得、バージョン規則検証を追加する。
- バージョン規則は既存 Schema Registry と同一にする。
  - 任意フィールド追加は minor。
  - 必須フィールド追加または削除は major。
- payload 検証を純関数として実装する。
  - JSON Schema 準拠判定。
  - 失敗時は違反フィールドを列挙。
- 初期 6 kind を登録する。
  - `claim@1`: required `statement`, `verification_mode`; enum `check`, `generate`; optional `context`, `source_quote`
  - `decision@1`: required `statement`; optional `rationale`, `alternatives`, `supersedes`
  - `parking@1`: required `statement`, `resume_context`
  - `verification-result@1`: required `verdict`, `reasoning`; enum `consistent`, `inconsistent`, `inconclusive`
  - `claim-transition@1`: required `to_state`; enum `open`, `dispatched`, `verified`, `refuted`, `inconclusive`, `terminated`, `parked`; optional `reason`
  - `session-summary@1`: required `summary`; optional `topics`
- `supplemental.reject_unregistered_kinds` 設定を導入し、個人 lake 設定では true にする。
- `verification-result@1` と `claim-transition@1` は、対象 claim の supplemental ID が `derivedFrom.supplementals` に少なくとも一つ含まれる検証を行う。

禁止事項:
- 未登録 kind の silent accept は禁止。
- payload を不透明 JSON として素通ししないこと。
- 検証失敗時に最初の 1 件だけで打ち切ってよい設計にしないこと。違反フィールド列挙が必要。

受け入れ条件:
- バージョン規則違反の登録が拒否される unit test。
- 必須欠落、型違反、enum 違反の検出 test。
- `parking` の `resume_context` 欠落拒否 test。
- 未登録 kind 拒否の contract test。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-a.md` を作成する。
- Track B が呼ぶべき公開関数、型、エラー型を明記する。
```

---

## Track B: 書き込み API

```text
/goal `supplemental-write-and-mcp-read` の Track B を実装してください。

担当範囲:
- tasks.md の Track B: B1, B2, B3。
- spec: `specs/supplemental-write-api/spec.md`。
- Track A の実装が未完了の場合でも、B1 と B2 は先に進める。B3 は Track A の検証関数へ結線する。

実装内容:
- selfhost に `POST /supplementals` を追加する。
- リクエストボディは SupplementalRecord の JSON 表現とする。
  - `id`, `kind`, `derived_from`, `payload`, `created_by`, `mutability`, `model_version`, `consent_metadata`, `lineage`
- 成功時は 201 と格納済みレコードのエンベロープを返す。
- 既存 `authorize_headers` 機構で `write:supplemental` scope を要求する。
- Store 不変条件を API 契約に昇格する。
  - `derivedFrom` 空は 422。
  - 存在しない observation 参照は 422、未解決 ID を詳細に列挙。
  - 存在しない supplemental 参照は 422、未解決 ID を詳細に列挙。
  - AppendOnly の同一 ID 上書きは 409。
- ID はクライアント採番の UUID、形式は `sup:{uuid}` とする。
- 書き込み時の内容ベース重複排除は行わない。
- Track A の SKIND-02 検証関数を Store 投入前に呼ぶ。
- `created_by` は安定した pipeline/client actor、モデル名は `model_version` に置く規約を docs または API contract に明記する。

禁止事項:
- kind 別 scope は実装しない。
- 内容ベース dedup を書き込み側に入れない。
- 空アンカーを許可しない。ライブ会話からの直接書き込み概念を追加しない。
- スキーマ検証を Store 投入後に回さない。

受け入れ条件:
- scope 不足が 403 で、書き込みが発生しない contract test。
- 正常系 201、再起動後に読める E2E。
- 空アンカー、未解決 observation、未解決 supplemental が 422 と詳細を返す test。
- 同一 ID 再 POST が 409。
- 同一内容、別 UUID の二重書き込みが両方 201。
- `verification_mode` 欠落 claim が 422 で違反フィールドを返す test。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-b.md` を作成する。
- Track I が使う POST サンプル、必要な auth 設定、fixture 作成方法を明記する。
```

---

## Track C: Claim Queue Projection

```text
/goal `supplemental-write-and-mcp-read` の Track C を実装してください。

担当範囲:
- tasks.md の Track C: C1, C2, C3, C4。
- spec: `specs/claim-queue-projection/spec.md`。

実装内容:
- `crates/projections/claim-queue` を作成する。
- supplemental 集合から claim queue と decision view を再構築可能な Projection として実装する。
- 行動用の読み口は Projection 経由に限定する。生 SupplementalRecord 列挙を行動根拠にしない。
- claim 重複解消の等価関係:
  - `kind`
  - `derivedFrom` の集合
  - 正規化 payload のハッシュ
- `model_version` は等価判定に含めない。
- 代表 ID は最初に観測されたレコード ID とし、吸収された ID 一覧を保持する。
- 状態 fold:
  - 初期状態は `open`。
  - `claim-transition@1` と `verification-result@1` を `created_at` 順に畳み込む。
  - `verification-result.verdict` は `consistent -> verified`, `inconsistent -> refuted`, `inconclusive -> inconclusive` として解釈する。
  - 不正遷移は skip し、監査ログに記録する。Projection 全体は落とさない。
- 同源グループ:
  - derivedFrom の会話または観測単位で claim を束ねる。
  - 状態管理は claim 単位、提示は group 単位。
- decision view:
  - `decision@1` の `statement`, `rationale` を全文検索可能にする。
  - `supersedes` チェーンを解決し、置き換え済み決定へ `superseded_by` を付ける。
- selfhost に読み取り API を追加する。
  - `GET /projections/claim-queue`
  - `GET /projections/decisions?q=`
- 認可は既存 read 系 scope に合わせる。

禁止事項:
- materialized view を ground truth として直接更新しない。
- replay 結果が入力順の偶然に依存する設計にしない。
- `model_version` を dedup key に含めない。
- ProjectionStale を隠して空結果にしない。

受け入れ条件:
- バッチ再実行 fixture で claim が 1 件に縮約され、吸収 ID が監査可能。
- 同一入力からの replay 決定性 test。
- 同一会話 3 claim が 1 group で返る test。
- 置換済み decision に `superseded_by` が付く test。
- 状態フィルタとページングの contract test。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-c.md` を作成する。
- Track H が MCP tool から呼ぶ API 仕様、レスポンス例、ProjectionStale の表現を明記する。
```

---

## Track D: Claude Code importer

```text
/goal `supplemental-write-and-mcp-read` の Track D を実装してください。

担当範囲:
- tasks.md の Track D: D1, D2, D3, D4。
- spec: `specs/coding-agent-adapters/spec.md` の CAGT-02, CAGT-03, CAGT-04 と関連 Failure Modes。
- Day 0 / F1 の archive ワーキングコピーを入力とする。F1 が未完了の場合は、既存ローカル JSONL と fixture で進め、ハンドオフに実データ E2E 未実施を明記する。

実装内容:
- `apps/tools/lethe-import-claude-code` を作成する。
- JSONL 行を型判別して parse する。
  - user
  - assistant
  - tool_use
  - メタデータ行
  - 不正 JSON 行
  - 未知 type
- 不正 JSON 行と未知 type は skip し、監査ログへ記録する。セッション全体は落とさない。
- 背骨写像を共有モジュールとして実装する。
  - 含める: 本人の指示文、エージェントの応答文、ツール呼び出しのメタデータ。
  - ツールメタデータはツール名と対象参照、例えばファイルパスやパターン等。
  - 含めない: ツール実行結果の中身、ファイル内容、コマンド出力、ツール呼び出し引数の本体、書き込み内容。
- `.env` 読み取りを含む fixture で、生成 canonical に秘密値やファイル内容が一切現れない negative test を作る。
- identity key:
  - `claude-code:{session_id}:{message_uuid}:H(canonical)`
- published:
  - メッセージ timestamp。取り込み時刻を使わない。
- サブエージェント sidechain の親子関係をメタデータとして保持する。
- archive ワーキングコピーを入力とする CLI を完成させる。
- 既存 `lethe-import-claude` と同型の gate / idempotency / duplicate 報告パターンに合わせる。

禁止事項:
- ツール実行結果を canonical content に含めない。
- ツール引数の本体を丸ごと含めない。
- published に取り込み時刻を使わない。
- 不正行 1 件でセッション全体を失敗させない。
- D2 の共有モジュールを Claude Code 専用に閉じない。Track E が利用できる形にする。

受け入れ条件:
- 実セッション JSONL または実形式 fixture の parse test。
- 壊れ行混入でも完走する test。
- `.env` 内容が canonical に出ない negative test。
- 再実行で全件 duplicate になる idempotency test。
- 実 archive からの E2E と、観測数、duplicate 数の報告。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-d.md` を作成する。
- Track E が使う共有写像モジュールの場所、入力型、出力型、制約を明記する。
```

---

## Track E: Codex importer

```text
/goal `supplemental-write-and-mcp-read` の Track E を実装してください。

担当範囲:
- tasks.md の Track E: E1, E2, E3。
- spec: `specs/coding-agent-adapters/spec.md` の CAGT-02, CAGT-04, CAGT-05。
- Track D の D2 共有写像モジュールを利用する。未完成の場合は、先に E1 を完了し、D2 の公開 interface に合わせて E2/E3 を結線する。

実装内容:
- Codex のセッション保存場所を実測確認する。
- Codex の行スキーマを実測確認する。
- sidechain またはサブエージェント相当の有無を確認する。
- 確認結果を `specs/coding-agent-adapters/spec.md` に追記する。
- D2 の共有写像モジュールに Codex 形式のフロントエンドを実装する。
- 背骨規則は Claude Code と同じ。
  - 含める: 本人の指示文、エージェントの応答文、ツール呼び出しメタデータ。
  - 含めない: ツール実行結果の中身、ツール引数本体、ファイル内容、コマンド出力。
- `apps/tools/lethe-import-codex` CLI を作成する。
- archive ワーキングコピーを入力にできるようにする。
- per-message 粒度、既定 identity key 形式 `source:object_id:H(canonical)` に従う。
- published はメッセージ timestamp。取り込み時刻を使わない。

禁止事項:
- Codex の保存場所や schema を推測だけで実装しない。必ず実測結果を spec に残す。
- 共有写像を迂回して Codex 専用の別ルールを作らない。
- ツール結果や秘密値が canonical に混入する fallback を作らない。

受け入れ条件:
- Codex セッション保存場所、行スキーマ、sidechain 相当の有無が spec に追記されている。
- Codex fixture で D2 と同じ negative test が通る。
- idempotency test。
- 実データ E2E。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-e.md` を作成する。
- 実測した Codex パス、schema、未確定点、実データ E2E 結果を明記する。
```

---

## Track G: 個人 lake の corpus 有効化

```text
/goal `supplemental-write-and-mcp-read` の Track G を実装してください。

担当範囲:
- tasks.md の Track G: G1, G2。
- spec: `specs/mcp-read-port/spec.md` の MCPR-05。
- spec: `specs/coding-agent-adapters/spec.md` の CAGT-03。

実装内容:
- 個人 lake の corpus-projection 設定を変更し、テキストを持つ全観測を対象にする。
  - claude.ai 会話
  - GitHub issue
  - GitHub PR
  - GitHub コメント
  - commit メッセージ
  - Claude Code 会話
  - Codex 会話
- 寮 lake の同意管理向け選別フィルタを個人 lake に持ち込まない。
- コーディングエージェント観測の thread 復元を確認する。
- `get_thread` がセッションと sidechain 構造を返せるか確認し、必要なら corpus 側を拡張する。
- 親子セッションを跨ぐ thread test を追加する。

禁止事項:
- 個人 lake の検索範囲を一部ソースに限定しない。
- corpus 未反映を空検索結果としてごまかさない。
- sidechain を平坦化して親子関係を失わせない。

受け入れ条件:
- 各ソース種別 1 件以上が corpus 検索でヒットする E2E。
- 親子セッションを跨ぐ `get_thread` test。
- MCPR-05 と CAGT-03 の SHALL に対する evidence。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-g.md` を作成する。
- Track H の `search_lake`, `get_record`, `get_thread` が呼ぶべき corpus API とレスポンス例を明記する。
```

---

## Track H: MCP read port

```text
/goal `supplemental-write-and-mcp-read` の Track H を実装してください。

担当範囲:
- tasks.md の Track H: H1, H2, H3, H4。
- spec: `specs/mcp-read-port/spec.md`。
- Track C の C4、Track G の corpus API と統合する。C4 未完了時は `claim_queue` と `search_decisions` の内部接続だけ CLQ-06 契約スタブで進めてよい。最終 handoff でスタブ有無を明記する。

実装内容:
- selfhost と同一プロセス内に MCP 専用リスナーを追加する。
- 内部 API / 管理 API とポートを共有しない。
- config に `mcp_bind_addr` を追加する。設定不足を fallback で隠さない。
- Streamable HTTP トランスポートを実装する。
- OAuth 2.1 リソースサーバとして実装する。
  - `/.well-known/oauth-protected-resource` を公開する。
  - issuer は managed ID provider の URL を config 化する。
  - Bearer JWT の署名、exp、audience を検証する。
  - 401 では `WWW-Authenticate` を返す。
  - トークン発行、DCR、同意画面は実装しない。
- 固定 API key 認証は実装しない。
- 読み取り専用の 5 tool だけを提供する。
  - `search_lake`
  - `get_record`
  - `get_thread`
  - `claim_queue`
  - `search_decisions`
- 全 tool は Projection 経由のみを読む。
- 生 supplemental、生 observation store へ直接アクセスしない。
- tool description は AI の選択精度に関わる contract として、短く明確に書く。
- Tailscale Funnel は MCP ポートのみを公開する。
- README / ops 文書に「PC 稼働中のみ到達可能」を明記する。

禁止事項:
- 内部 API ポートに `/mcp` を足さない。
- 管理面を Funnel に晒さない。
- 固定 Bearer token や API key 認証を実装しない。
- MCP write tool を追加しない。
- ProjectionStale や RecordNotFound を隠して空結果にしない。

受け入れ条件:
- 内部 API ポートと MCP ポートが分離されている test。
- 期限切れ JWT、audience 不一致、正常 JWT の 3 パス test。
- protected resource metadata の contract test。
- 5 tool それぞれの contract test。
- tool description の spec レビュー。
- 可能なら実機で claude.ai カスタムコネクタ登録、OAuth flow、`search_lake` 呼び出しの手動疎通。
- 実機疎通が環境上できない場合は、未実施理由と再現手順を handoff に残す。

ハンドオフ:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-h.md` を作成する。
- Funnel 対象ポート、OAuth config、mock key test 方法、手動疎通結果を明記する。
```

---

## Track I: 統合担当

```text
/goal `supplemental-write-and-mcp-read` の Track I を統合担当として実施してください。

担当範囲:
- tasks.md の Track I: I1, I2, I3。
- 全 Track の handoff を読み、仕様と実装を統合確認する。

前提:
- 各 Track は `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-*.md` を残す。
- 統合担当は、スタブ、未実施外部確認、設定不足、仕様 SHALL 未被覆を見つけたら、統合前に明確に差し戻す。

実施内容:
- 全 Track の handoff を読む。
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md` の完了状態と実装実態を照合する。
- 各 spec の SHALL に対する test evidence 表を作成する。
- E2E I1:
  - 手書き claim を `POST /supplementals`。
  - `claim_queue` projection に `open` で現れることを確認。
  - `claim-transition` を `POST /supplementals`。
  - 状態が変わることを確認。
  - シナリオ test を CI で通す。
- E2E I2:
  - コーディングエージェント取り込み済み観測にアンカーする `decision` を `POST /supplementals`。
  - `search_decisions` で引けることを確認。
  - 横断シナリオ test を追加または更新する。
- I3:
  - 各 spec の SHALL に judgement と evidence を付ける。
  - Funnel 対象ポートが MCP ポートのみであることを実機または設定で確認する。
  - JWT 検証が署名、exp、audience を見ていることを確認する。
  - README / ops 文書に PC 稼働中制約が明記されていることを確認する。

禁止事項:
- Track の未完了を統合側で黙って迂回しない。
- スタブのまま E2E を通ったことにしない。
- 仕様 SHALL に evidence がない状態で tasks.md を完了にしない。

成果物:
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-i.md`
- SHALL coverage table
- Track I の E2E test
- 必要な docs 更新
- 最終的な tasks.md 完了状態
```

---

## 推奨投入順

1. Day 0 / Track F
2. Track A, C, D, G, H を並列開始
3. Track B を A と並列開始。ただし B3 は A2/A3 完了後に結線
4. Track E は E1 を即開始。E2/E3 は D2 の共有写像モジュールに合わせる
5. 全 handoff が出た後に Track I

---

## 統合担当の受領チェックリスト

- [ ] `handoffs/track-f.md` がある
- [ ] `handoffs/track-a.md` がある
- [ ] `handoffs/track-b.md` がある
- [ ] `handoffs/track-c.md` がある
- [ ] `handoffs/track-d.md` がある
- [ ] `handoffs/track-e.md` がある
- [ ] `handoffs/track-g.md` がある
- [ ] `handoffs/track-h.md` がある
- [ ] Track B は Track A の実 API に結線済み
- [ ] Track H は Track C/G の実 API に結線済み
- [ ] Track E は Track D の共有写像モジュールを利用
- [ ] スタブが残っていない、または残存理由が明示され統合対象外として差し戻し済み
- [ ] 全 SHALL に judgement と evidence がある
- [ ] Track I の E2E が通っている
