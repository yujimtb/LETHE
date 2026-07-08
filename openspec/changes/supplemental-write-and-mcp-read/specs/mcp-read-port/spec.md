# Spec Delta: mcp-read-port

**Change:** supplemental-write-and-mcp-read
**Module:** (new) mcp-read-port(M14 API Serving と同居、別リスナー)
**Scope:** 公開 MCP サーバ(OAuth 2.1 リソースサーバ、Streamable HTTP、read 5 ツール + write 1 ツール)
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

MCP サーバはリソースサーバとして振る舞い SHALL する: (1) `/.well-known/oauth-protected-resource` と `/.well-known/oauth-protected-resource/mcp` で保護リソースメタデータを公開し、認可サーバ(マネージド ID 基盤)を指す。(2) 受信リクエストの Bearer トークン(JWT)を認可サーバの公開鍵で検証する(署名・有効期限・issuer・audience・権限 grant)。(3) 権限 grant は JWT の `scope` claim と Auth0 RBAC/API permission 用の `permissions` claim から読む。(4) トークン発行・動的クライアント登録・同意画面・refresh token exchange は実装しない(認可サーバ側の責務)。固定 API キー認証を実装して SHALL NOT ならない。

#### Scenario: 無効トークンの拒否
- **WHEN** 期限切れまたは audience 不一致の JWT でツール呼び出しが届く
- **THEN** 401 と WWW-Authenticate ヘッダ(保護リソースメタデータへの誘導と必要 scope)が返る

#### Scenario: メタデータ発見
- **WHEN** クライアントが `/.well-known/oauth-protected-resource` を取得する
- **THEN** 認可サーバの issuer URL を含む有効なメタデータが返る

#### Scenario: Auth0 RBAC permission claim
- **WHEN** Auth0 が refresh token flow などで `permissions = ["mcp:read", ...]` を持つ access token を発行する
- **THEN** MCP サーバは `permissions` の grant を `scope` と同じ認可入力として扱い、該当 tool の scope check に使用する

### Requirement: MCPR-04 ツールセット(read 5 種 + write 1 種)

提供ツールは以下の 6 種と SHALL する:

| ツール | 種別 | 必須 scope | 内容 | 背後の Projection / Store |
|--------|------|------------|------|---------------------------|
| `search_lake` | read | `mcp:read` | 全文検索(クエリ、ソース種別フィルタ任意) | corpus-projection |
| `get_record` | read | `mcp:read` | レコード ID 指定の本文取得 | corpus-projection |
| `get_thread` | read | `mcp:read` | レコードの前後文脈(同一会話/スレッド)取得 | corpus-projection |
| `claim_queue` | read | `mcp:read` | 未終端 claim の一覧(状態フィルタ、同源グループ形) | claim-queue-projection |
| `search_decisions` | read | `mcp:read` | 決定台帳の検索(supersedes 解決済み) | claim-queue-projection |
| `write_supplemental` | write | `write:supplemental` | 既存 observation/blob/supplemental anchor から派生した supplemental record を 1 件作成 | supplemental store + projection refresh |

read ツールは Projection のみを読み、生 supplemental・生 observation ストアへ直接アクセスして SHALL NOT ならない(Filtering-before-Exposure)。`write_supplemental` は HTTP `POST /supplementals` と同じ検証・永続化・projection refresh 経路を使い、anchor 未解決または未登録 kind は明示的に拒否 SHALL する。ツール説明文・annotations・`securitySchemes` は AI の選択精度とクライアント側確認 UI を左右する契約物として spec レビュー対象に含める。

#### Scenario: ChatGPT write action discovery
- **WHEN** ChatGPT.com が `tools/list` を取得する
- **THEN** `write_supplemental` は `annotations.readOnlyHint = false`, `annotations.destructiveHint = false`, `annotations.openWorldHint = false`, `securitySchemes = [{ type = "oauth2", scopes = ["write:supplemental"] }]`, `_meta.securitySchemes` mirror を持つ

#### Scenario: tool-level 再認可要求
- **WHEN** `mcp:read` のみを持つ access token で `write_supplemental` が呼ばれる
- **THEN** JSON-RPC error ではなく MCP tool result として `isError = true` と `_meta["mcp/www_authenticate"]` を返し、`error="insufficient_scope"` と `scope="write:supplemental"` を含む challenge で ChatGPT の再認可 UI を起動できる

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
