# Spec Delta: supplemental-write-api

**Change:** supplemental-write-and-mcp-read
**Module:** M14 API Serving(拡張)+ M04 Supplemental Store(契約昇格)
**Scope:** `POST /supplementals` 書き込みエンドポイント
**Dependencies:** M01 Domain Kernel, M02 Registry(SKIND 系), M04 Supplemental Store, M14 API Serving
**Agent:** Spec Designer(API 契約)→ Implementer(handler+authz)→ Reviewer(不変条件・エラー写像検証)

---

## ADDED Requirements

### Requirement: SUPW-01 書き込みエンドポイント

selfhost は `POST /supplementals` を提供 SHALL する。リクエストボディは SupplementalRecord の JSON 表現(id, kind, derived_from, payload, created_by, mutability, model_version, consent_metadata, lineage)。`created_at` は selfhost が作成時刻を設定し、書き込み API の入力には含めない。成功時は 201 と格納済みレコードのエンベロープを返す。

#### Scenario: 正常系の書き込み
- **WHEN** 登録済み kind・有効なアンカー・スキーマ準拠 payload の claim を POST する
- **THEN** 201 が返り、レコードは SQLite に永続化され、直後の再起動後も読み出せる

### Requirement: SUPW-02 認可

エンドポイントは既存の `authorize_headers` 機構でスコープ `write:supplemental` を要求 SHALL する。スコープ不足は 403。kind によらず単一スコープとする(per-kind 認可は本 change の Non-goal)。

#### Scenario: スコープ不足の拒否
- **WHEN** `read:corpus` のみのトークンで POST する
- **THEN** 403 が返り、レコードは書き込まれない

### Requirement: SUPW-03 Store 不変条件の API 契約への昇格

SupplementalStore の既存検証 —(1)derivedFrom は空であってはならない、(2)derivedFrom.observations の参照先は lake に実在しなければならない、(3)derivedFrom.supplementals の参照先は Store に実在しなければならない、(4)AppendOnly レコードの同一 ID 上書き禁止 — を API のエラー契約として明文化 SHALL する。緩和は行わない。検証失敗は 422、AppendOnly 衝突は 409 に写像する。

#### Scenario: 空アンカーの拒否
- **WHEN** derivedFrom が空の supplemental を POST する
- **THEN** 422 が返る(全ての supplemental は lake 既在コンテンツへのアンカーを持つ — ライブ会話からの書き込みという概念は存在しない)

#### Scenario: 存在しない observation への参照の拒否
- **WHEN** lake に存在しない observation ID をアンカーに含む supplemental を POST する
- **THEN** 422 が返り、未解決の参照 ID がエラー詳細に列挙される

#### Scenario: 存在しない supplemental への参照の拒否
- **WHEN** Store に存在しない supplemental ID をアンカーに含む supplemental を POST する
- **THEN** 422 が返り、未解決の参照 ID がエラー詳細に列挙される

### Requirement: SUPW-04 kind スキーマ検証の適用

書き込みは Store 投入前に SKIND-02 の検証関数を通過 SHALL する。違反は 422 と違反フィールド列挙。

#### Scenario: verification_mode 欠落 claim の拒否
- **WHEN** verification_mode のない payload を kind `claim@1` で POST する
- **THEN** 422 が返り、違反フィールドが列挙される(検証 dispatcher の分岐不能なレコードを入口で断つ)

### Requirement: SUPW-05 ID 採番と重複の扱い

ID はクライアント採番の UUID(`sup:{uuid}`)と SHALL する。サーバは書き込み時の内容ベース重複排除を行わない(重複解消は Projection の責務 — claim-queue-projection spec 参照)。同一 ID への再 POST は AppendOnly 衝突として 409。

#### Scenario: 内容が同一で ID が異なる二重書き込み
- **WHEN** バッチ再実行により同一内容・別 UUID の claim が二度 POST される
- **THEN** 両方 201 で格納される(重複の吸収は読み取り側の畳み込みが行い、書き込み側は関知しない)

### Requirement: SUPW-06 created_by の帰属規約

created_by にはパイプライン/クライアントの安定した身元(例 `actor:extraction-pass`)を設定し、使用モデルは model_version フィールドに記録する規約と SHALL する(API は形式検証のみ、意味の強制はレビュー規約)。

#### Scenario: モデル更新で actor が分裂しない
- **WHEN** 同一 extraction pipeline が異なる model_version で supplemental を書き込む
- **THEN** `created_by` は同じ pipeline actor のままで、使用モデルの差分は `model_version` に記録される

## Invariants(継承)

- Append-Only Law / Explicit Authority Law / Effect Isolation Law(検証は純関数、I/O は shell)

## Failure Modes

- 401 / 403(認可)、409 `AppendOnlyConflict`、422 `EmptyAnchor` | `UnresolvedAnchor { ids }` | `KindNotRegistered` | `PayloadSchemaViolation { violations }`
