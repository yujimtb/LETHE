# M02: Registry

**Module:** registry
**Scope:** EntityType / Schema / Observer / Source Contract / Projection Catalog のメタデータ管理
**Dependencies:** M01 Domain Kernel
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §3
**Agent:** Spec Designer (schema 設計) → Implementer (DB + API) → Reviewer (制約検証)

---

## 1. Module Purpose

LETHE のすべてのメタデータを管理する **Registry** を定義する。
Registry は「何を」「どの形式で」「誰が」「どの契約で」観測し、「どのような Projection が存在するか」を一元管理する。

---

## 2. Entity Type Registry

### 2.1 Purpose

観測対象の型定義。誰でも新しい型を登録できる（P1: Open Entity Model）。

### 2.2 Schema

```yaml
EntityType:
  id: string             # "et:{name}" 形式
  name: string
  description: string
  parent: EntityTypeRef? # is-a 関係
  attributes: [string]   # 推奨属性名
  registered_by: string?
  registered_at: Timestamp?
```

### 2.3 基盤型（システム初期登録）

| ID | Name | Description |
|---|---|---|
| `et:person` | Person | 寮に関わる人物 |
| `et:space` | Space | 物理空間 |
| `et:artifact` | Artifact | 物理的・デジタルな対象物 |
| `et:document` | Document | デジタル文書 |
| `et:message` | Message | メッセージ（Slack 等） |
| `et:observer` | Observer | Observer 自身 |

### 2.4 型の階層

`et:room` is-a `et:space` のように継承関係を持てる。Projection は親型でフィルタすると配下の全サブタイプを取得できる。

### 2.5 API

| Method | Path | Description |
|---|---|---|
| GET | `/api/registry/entity-types` | 一覧取得 |
| GET | `/api/registry/entity-types/{id}` | 個別取得 |
| POST | `/api/registry/entity-types` | 新規登録 |
| PATCH | `/api/registry/entity-types/{id}` | 属性更新（id/name 変更不可） |

---

## 3. Schema Registry

### 3.1 Purpose

Observation の payload 形式を JSON Schema で定義する。

### 3.2 Schema

```yaml
ObservationSchema:
  id: string             # "schema:{name}" 形式
  name: string
  version: SemVer
  subject_type: EntityTypeRef   # "et:*" で汎用
  target_type: EntityTypeRef?
  payload_schema: JsonSchema
  source_contracts:
    - observer_id: ObserverRef
      adapter_version: SemVer
      compatible_range: string
  attachments:
    required: boolean
    accepted_types: [MimeType]
  registered_by: string?
  registered_at: Timestamp?
```

### 3.3 MVP 必須 Schema

| ID | Name | Subject Type | MVP Source |
|---|---|---|---|
| `schema:workspace-object-snapshot` | Workspace Object Snapshot | `et:document` | Google Slides |
| `schema:slack-message` | Slack Message | `et:message` | Slack |
| `schema:slack-channel-snapshot` | Slack Channel Snapshot | `et:*` | Slack |
| `schema:observer-heartbeat` | Observer Heartbeat | `et:observer` | 全 Observer |

### 3.4 Version Rules

| Change Type | Action |
|---|---|
| Optional フィールド追加 | Minor bump (1.0 → 1.1) |
| Required フィールド追加 | Major bump (1.x → 2.0) |
| フィールド削除 | Major bump |
| 型変更 | Major bump |

過去の Observation は書き込み時の schemaVersion を永久に保持する。

### 3.5 API

| Method | Path | Description |
|---|---|---|
| GET | `/api/registry/schemas` | 一覧取得（version 含む） |
| GET | `/api/registry/schemas/{id}` | 個別取得 |
| GET | `/api/registry/schemas/{id}/versions` | 全 version 取得 |
| POST | `/api/registry/schemas` | 新規登録 |
| POST | `/api/registry/schemas/{id}/versions` | 新 version 追加 |

---

## 4. Observer & Source Contract Registry

### 4.1 Purpose

「誰が」「どの source から」「どの契約で」取り込むかを管理する。

### 4.2 Schema

```yaml
Observer:
  id: string             # "obs:{name}" 形式
  name: string
  observer_type: enum[crawler, connector, bot, sensor-gateway, human]
  source_system: SourceSystemRef
  adapter_version: SemVer
  schemas: [SchemaRef]   # "*" で任意
  schema_bindings:
    - schema: SchemaRef
      versions: string
  authority_model: AuthorityModel
  capture_model: CaptureModel
  owner: string
  trust_level: enum[automated, human-verified, crowdsourced]

SourceSystem:
  id: string             # "sys:{name}" 形式
  name: string
  provider: string?
  api_version: string?
  source_class: enum[mutable-multimodal, mutable-text, immutable-multimodal, immutable-text]
```

### 4.3 MVP 必須 Source Contracts

| Observer | Source System | Authority | Capture |
|---|---|---|---|
| `obs:gslides-crawler` | `sys:google-slides` | source-authoritative | snapshot |
| `obs:slack-crawler` | `sys:slack` | lake-authoritative | event |

### 4.4 Source Contract Evolution

1. adapter は versioned contract を持つ
2. adapter minor/patch は既存 `schema_bindings` の範囲内でのみ emit できる
3. schema major bump を emit する場合は adapter major bump か新 observer contract を要求する
4. Observation には `schemaVersion` と `meta.sourceAdapterVersion` を残す
5. 過去 snapshot はその時点の contract で永続保存

### 4.5 API

| Method | Path | Description |
|---|---|---|
| GET | `/api/registry/observers` | Observer 一覧 |
| GET | `/api/registry/observers/{id}` | 個別取得 |
| POST | `/api/registry/observers` | 新規登録 |
| GET | `/api/registry/source-systems` | Source System 一覧 |
| POST | `/api/registry/source-systems` | 新規登録 |

---

## 5. Projection Catalog

### 5.1 Purpose

構築された Projection の発見・利用・DAG 可視化・API 契約管理。

### 5.2 Schema

```yaml
ProjectionCatalogEntry:
  id: ProjectionRef      # "proj:{name}" 形式
  name: string
  description: string
  created_by: string
  created_at: Timestamp
  version: SemVer
  status: enum[building, active, stale, degraded, archived]
  kind: ProjectionKind
  engine: string
  sources: [ProjectionInputDecl]
  outputs: [OutputSpec]
  read_modes: [ReadModePolicy]
  doi: string?
  tags: [string]
  health: enum[healthy, stale, degraded, broken]
  depth: integer         # DAG depth (auto-calculated)
```

### 5.3 DAG Integrity

- 登録時に循環依存チェックを実施
- depth は自動計算
- upstream archive 時は downstream を degraded に変更

### 5.4 API

| Method | Path | Description |
|---|---|---|
| GET | `/api/catalog/projections` | 一覧（filter, search, tag 対応） |
| GET | `/api/catalog/projections/{id}` | 個別取得 |
| GET | `/api/catalog/projections/{id}/versions` | version 一覧 |
| GET | `/api/catalog/projections/{id}/dag` | DAG 依存関係 |
| POST | `/api/catalog/projections` | 登録 |
| PATCH | `/api/catalog/projections/{id}` | status 更新 |

---

## 6. Invariants

- EntityType / Schema の id は一度登録したら変更不可
- Schema の payload_schema は major version 間で互換性チェックを実施
- Observer は必ず存在する SourceSystem を参照する
- Projection Catalog の DAG は非巡回性を常に保証する
- 全 Registry 操作は audit log を残す

---

## 7. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | 新規 EntityType 登録 | 成功、id が `et:` prefix | |
| 2 | 重複 id で EntityType 登録 | ConflictFailure | |
| 3 | 存在しない parent で EntityType 登録 | ValidationFailure | |
| 4 | Schema minor bump | 成功、旧 version 保持 | |
| 5 | Schema major bump (breaking) | 成功、互換性 warning | |
| 6 | 循環 Projection DAG の登録 | 拒否 | |
| 7 | Observer 登録（存在しない source_system） | ValidationFailure | |
| 8 | schema major bump without adapter major bump | ValidationFailure | |
| 9 | Projection status を archived に変更 | downstream health が degraded に | |

---

## 8. Module Interface

### Provides

- EntityType / Schema / Observer / SourceSystem / ProjectionCatalogEntry の CRUD API
- DAG 非巡回性チェック
- Schema version 互換性チェック
- Audit event 発行

### Requires

- M01 Domain Kernel: EntityRef, SchemaRef, AuthorityModel, CaptureModel, ProjectionKind
