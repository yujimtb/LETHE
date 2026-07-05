# Spec Delta: mcp-read-port

**Change:** supplemental-write-and-mcp-read
**Module:** (new) mcp-read-port(M14 API Serving と同居、別リスナー)
**Scope:** 公開 MCP 読み取りサーバ(OAuth 2.1 リソースサーバ、Streamable HTTP、厳選 5 ツール)
**Dependencies:** M05 Projection Engine, M14 API Serving, corpus-projection(既存), claim-queue-projection
**Agent:** Spec Designer(ツール契約)→ Implementer(リスナー+authz+ツール)→ Reviewer(公開面・認可検証)

---

## ADDED Requirements

### Requirement: MCPR-01 別リスナーでの提供

MCP サーバは selfhost と同一プロセス内の**専用リスナー(別ポート)**で提供 SHALL する。管理用・内部 API のリスナーとポートを共有して SHALL NOT ならない。公開(Tailscale Funnel)は MCP ポートのみを対象とし、内部 API はこれまで通り LAN / Tailscale 内に閉じる。

#### Scenario: 公開面の分離
- **WHEN** Funnel 経由の外部クライアントが内部 API のポートへ到達を試みる
- **THEN** 到達不能である(Funnel の公開対象は MCP ポートのみ)

### Requirement: MCPR-02 トランスポート

MCP エンドポイントは Streamable HTTP トランスポートを実装 SHALL する(Anthropic のコネクタ基盤からの接続要件。接続元は claude.ai / Desktop / モバイルのいずれの利用でも Anthropic クラウド側であるため、公開到達性が前提)。

#### Scenario: Streamable HTTP での接続
- **WHEN** MCP client が Streamable HTTP で MCP endpoint に接続する
- **THEN** server は同 transport で tool list と tool call を処理し、SSE 単独 transport を前提にしない

### Requirement: MCPR-03 OAuth 2.1 リソースサーバ

MCP サーバはリソースサーバとして振る舞い SHALL する: (1) `/.well-known/oauth-protected-resource` で保護リソースメタデータを公開し、認可サーバ(マネージド ID 基盤)を指す。(2) 受信リクエストの Bearer トークン(JWT)を認可サーバの公開鍵で検証する(署名・有効期限・audience)。(3) トークン発行・動的クライアント登録・同意画面は実装しない(認可サーバ側の責務)。固定 API キー認証を実装して SHALL NOT ならない。

#### Scenario: 無効トークンの拒否
- **WHEN** 期限切れまたは audience 不一致の JWT でツール呼び出しが届く
- **THEN** 401 と WWW-Authenticate ヘッダ(保護リソースメタデータへの誘導)が返る

#### Scenario: メタデータ発見
- **WHEN** クライアントが `/.well-known/oauth-protected-resource` を取得する
- **THEN** 認可サーバの issuer URL を含む有効なメタデータが返る

### Requirement: MCPR-04 ツールセット(読み取り専用・5 種)

提供ツールは以下の 5 種と SHALL し、書き込みツールを提供して SHALL NOT ならない:

| ツール | 内容 | 背後の Projection |
|--------|------|------------------|
| `search_lake` | 全文検索(クエリ、ソース種別フィルタ任意) | corpus-projection |
| `get_record` | レコード ID 指定の本文取得 | corpus-projection |
| `get_thread` | レコードの前後文脈(同一会話/スレッド)取得 | corpus-projection |
| `claim_queue` | 未終端 claim の一覧(状態フィルタ、同源グループ形) | claim-queue-projection |
| `search_decisions` | 決定台帳の検索(supersedes 解決済み) | claim-queue-projection |

全ツールは Projection のみを読み、生 supplemental・生 observation ストアへ直接アクセスして SHALL NOT ならない(Filtering-before-Exposure)。ツール説明文は AI の選択精度を左右する契約物として spec レビュー対象に含める。

#### Scenario: エージェントの claim 取得
- **WHEN** 接続済みの Claude が「いま未検証の主張は何か」に answering するため claim_queue を verification_mode = generate フィルタで呼ぶ
- **THEN** 同源グループ形の open claim 一覧が返る

### Requirement: MCPR-05 個人 lake の検索範囲

corpus-projection の個人 lake インスタンス設定は、テキストを持つ全観測(claude.ai 会話・GitHub issue / PR / コメント / commit メッセージ・コーディングエージェント会話)を対象と SHALL する。寮 lake のような選別フィルタは適用しない(読み手が本人とそのエージェントのみであるため。横断検索こそが価値の本体)。

#### Scenario: 横断検索対象
- **WHEN** 個人 lake で claude.ai 会話・GitHub issue・Codex 会話の各観測が corpus に存在する
- **THEN** `search_lake` は同一クエリで source 種別を跨いだ結果を返せる

### Requirement: MCPR-06 稼働制約の明示

lake は本 PC 上で稼働するため、MCP エンドポイントは PC 稼働中のみ到達可能である。この制約は既知の割り切りとして README / ops 文書に明記 SHALL する(将来の個人 NAS 移設で解消予定)。

#### Scenario: PC 停止時の既知制約
- **WHEN** README / ops 文書を確認する
- **THEN** MCP endpoint は本 PC 稼働中のみ到達可能であり、常時稼働 SLA を提供しないことが明記されている

## Invariants(継承)

- Filtering-before-Exposure Law / Explicit Authority Law

## Failure Modes

- 401 `InvalidToken`(WWW-Authenticate 付き)/ 404 `RecordNotFound` / `ProjectionStale` の明示伝搬(エージェントに鮮度を偽らない)
