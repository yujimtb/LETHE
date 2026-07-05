# Spec Delta: claim-queue-projection

**Change:** supplemental-write-and-mcp-read
**Module:** (new) claim-queue-projection(M05 Projection Engine 上)
**Scope:** supplemental チェーンを畳み込み、未終端 claim のキューと決定台帳ビューを提供する Projection+読み取り API
**Dependencies:** M01 Domain Kernel, M04 Supplemental Store, M05 Projection Engine, M14 API Serving, SKIND 系
**Agent:** Spec Designer(fold 意味論)→ Implementer(Projector+API)→ Reviewer(dedup・状態機械の replay 検証)

---

## ADDED Requirements

### Requirement: CLQ-01 読みは Projection 経由(生読み禁止)

supplemental を行動の根拠として消費する全てのクライアント(検証 dispatcher、MCP エージェント、将来の消費者)は本 Projection またはその後継 Projection を経由 SHALL する。生の SupplementalRecord 列挙を行動の根拠にして SHALL NOT ならない(デバッグ・監査を除く)。これは「ID は UUID・書き込み時 dedup なし」という決定が安全に成立する唯一の条件である(dedup 点の一極集約により、重複 claim の二重 dispatch を構造的に防ぐ)。

#### Scenario: dispatcher の読み口
- **WHEN** 検証 dispatcher(change ②)が check 系の open claim を取得する
- **THEN** 取得は本 Projection の API 経由であり、重複解消済みの claim 集合のみが渡る

### Requirement: CLQ-02 重複解消の等価関係

畳み込みは次の 3 組が一致する claim レコード群を同一 claim とみなし 1 件に縮約 SHALL する: (kind, derivedFrom の集合, 正規化 payload のハッシュ)。model_version は等価判定に**含めない**。縮約時は最初に観測されたレコードの ID を代表 ID とし、吸収されたレコード ID の一覧を保持する(監査可能性)。

#### Scenario: バッチ再実行の吸収
- **WHEN** 同一会話への抽出バッチが再実行され、同一内容・別 UUID の claim が 2 件存在する
- **THEN** キューには 1 件だけ現れ、代表 ID は先行レコードのもの

#### Scenario: モデル更新後の言い回し揺れ
- **WHEN** 新モデルによる再抽出で、同一会話由来だが本文の異なる claim が生じる
- **THEN** 別 claim として扱われるが、CLQ-04 の同源グループにより提示上は 1 グループに束なる

### Requirement: CLQ-03 状態機械の畳み込み

各 claim の現在状態は、当該 claim をアンカーする `claim-transition@1` / `verification-result@1` レコードを created_at 順に畳み込んで決定 SHALL する。状態値: open(初期)/ dispatched / verified / refuted / inconclusive / terminated / parked。verification-result は verdict に応じて verified / refuted / inconclusive への遷移として解釈する。同一入力集合に対する畳み込み結果は決定的である(Replay Law)。

#### Scenario: 検証結果による状態遷移
- **WHEN** open の claim に verdict = inconsistent の verification-result が追記される
- **THEN** 再計算後のキューで当該 claim は refuted になる

#### Scenario: replay 決定性
- **WHEN** 同一の supplemental 集合から Projection を再構築する
- **THEN** 全 claim の状態・縮約結果が完全に一致する

### Requirement: CLQ-04 同源グループ提示

キュー API は claim を由来元(derivedFrom の会話/観測)ごとに「同源グループ」として束ねた形で返却 SHALL する。状態管理は claim 単位のまま、グループは提示単位である(判断カード 1 枚 = 1 グループに対応。一度に見せる件数を絞るワーキングメモリ保護要求に接続)。

#### Scenario: グループ形の返却
- **WHEN** 同一会話から 3 件の open claim が導出されている
- **THEN** キュー応答は 1 グループ(member 3 件、それぞれの状態・verification_mode 付き)として返る

### Requirement: CLQ-05 決定台帳ビュー

本 Projection は `decision@1` レコードの検索可能ビューを併せて提供 SHALL する(statement・rationale の全文検索、supersedes チェーンの解決 — 置き換え済み決定は現行版への参照付きで返す)。

#### Scenario: 置き換えチェーンの解決
- **WHEN** 決定 A が決定 B に置き換えられている状態で A がヒットする
- **THEN** 応答は A に superseded_by = B を付して返す

### Requirement: CLQ-06 読み取り API

selfhost に以下を追加 SHALL する(いずれも既存 read 系スコープで認可): `GET /projections/claim-queue`(状態フィルタ・同源グループ形・ページング)、`GET /projections/decisions?q=`(決定台帳検索)。

#### Scenario: 状態フィルタとページング
- **WHEN** open / parked / verified の claim が混在する Projection に `GET /projections/claim-queue?state=open&limit=2` を呼ぶ
- **THEN** 応答は open claim の同源グループのみを最大 2 件返し、次ページがある場合は cursor を含む

#### API Contract

- `GET /projections/claim-queue?state=<state>&limit=<n>&cursor=<offset>` は `read:corpus` スコープで認可する。`state` は省略可で、指定時は `open` / `dispatched` / `verified` / `refuted` / `inconclusive` / `terminated` / `parked` のいずれかである。`limit` は 1 以上かつ selfhost の `resource_limits.max_page_size` 以下で、`cursor` は数値 offset 文字列である。
- 成功時の `data` は `{ "groups": [...], "total": <filtered_group_count>, "limit": <n>, "next_cursor": "<offset>", "audit_log": [...] }` を返す。`groups[].members[]` は `representative_id`, `absorbed_ids`, `state`, `verification_mode`, `derived_from`, `source_refs`, `payload_hash`, `state_history` を含み、同源グループ内でも状態管理は member 単位で表す。
- `GET /projections/decisions?q=<query>&limit=<n>` は `read:corpus` スコープで認可する。`q` は必須かつ non-blank、`limit` は同じ上限規則に従う。
- 成功時の `data` は `{ "query": "<query>", "matches": [...], "total": <match_count>, "limit": <n>, "audit_log": [...] }` を返す。`matches[]` は `id`, `statement`, `rationale`, `supersedes`, `superseded_by`, `derived_from`, `created_by`, `created_at` を含む。
- いずれの成功応答も `projection_metadata.projection_id = "proj:claim-queue"` の ResponseEnvelope で返す。Projection が stale の場合は空結果にせず HTTP 503 と `{ "error": "projection_stale", "detail": "...", "retry_after": 30 }` を返す。

## Invariants(継承)

- No Direct Mutation Law: materialization を ground truth として更新しない(常に supplemental からの再計算で復元可能)
- Replay Law / Filtering-before-Exposure Law

## Failure Modes

- `ProjectionStale`(watermark 遅延の明示)/ 不正 state 値の遷移レコードは畳み込みで skip し監査ログに記録(Projection を落とさない)
