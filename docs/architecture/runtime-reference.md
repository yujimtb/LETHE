# LETHE Runtime Reference Architecture

## Purpose

この文書は、LETHE をどのような runtime topology で動かすと実装しやすいかを示す **reference implementation** です。  
ここでいう runtime は交換可能であり、守るべき本質は `system-overview.md` と `domain-algebra.md` の意味論と law です。

Lake を物理分割（sharding）する場合の routing layer・partition rule log・exact index / failover spool の運用・logical→physical resolver・per-leaf watermark propagation の確定設計は [Sharding design](sharding.md) を参照してください。

---

## 1. Design Principles

### 1.1 Runtime Is Subordinate to Semantics

実装は次を壊してはいけません。

- append-only な canonical capture
- academic-pinned の deterministic replay
- source-native authority の尊重
- filtering-before-exposure
- capability-scoped operation

### 1.2 Functional Core / Imperative Shell Mapping

| Layer | Runtime Role | Examples |
|---|---|---|
| **Domain Kernel** | 型と変換の意味論 | Observation validation, projection fold |
| **Policy Engine** | pure decision service | consent check, review requirement |
| **Ports** | effect interface | blob store, source API, projection catalog |
| **Adapters** | effect interpreter | Google connector, Slack crawler, MinIO adapter |
| **Orchestrators** | workflow / scheduling | job queue, replay worker, cache refresher |

---

## 2. Reference Topology

```text
                        +-----------------------------+
                        |        Authoring UI         |
                        |   GUI + coding agent + API  |
                        +--------------+--------------+
                                       |
                                       v
                        +-----------------------------+
                        | Sandbox / Draft Workspace   |
                        | spec draft, dry-run, review |
                        +--------------+--------------+
                                       |
                                       v
+----------------+      +-----------------------------+      +--------------------+
| Source Systems | ---> | Ingestion / Write Gate      | ---> | Audit / Lineage    |
| Google / Slack |      | auth, schema, policy, dedup |      | service            |
| Figma / Sensor |      +--------------+--------------+      +--------------------+
+----------------+                     |
                                       v
                  +--------------------+--------------------+
                  |                                         |
                  v                                         v
       +-------------------------+             +---------------------------+
       | Observation Lake        |             | Supplemental Derivation   |
       | append-only event store |             | transcript / OCR / cache  |
       +------------+------------+             +-------------+-------------+
                    |                                            |
                    +-------------------+------------------------+
                                        |
                                        v
                         +-------------------------------+
                         | Projection Runner / Catalog   |
                         | build, version, publish       |
                         +---------------+---------------+
                                         |
                                         v
                         +-------------------------------+
                         | Projection API / Export Layer |
                         | latest, pinned, cached reads  |
                         +-------------------------------+
```

---

## 3. Runtime Components

### 3.1 Ingestion / Write Gate

責務:

- observer / user / service identity の確認
- schema validation
- consent / access / review policy 呼び出し
- idempotency check
- blob upload orchestration
- append request 生成
- source-native write request 生成

非責務:

- transcript / OCR / LLM 解釈
- name resolution の最終判断
- Projection materialization の直接更新

### 3.2 Observation Lake

最小構成:

- append-only log
- cold storage export
- schema/version aware partitioning
- replay 可能な ordering metadata

現行 Rust self-host 実装では、SQLite が durable store です。起動時に `partition_log.initialize` を 1 件だけ記録し、`routing_keyspec` / `identity_keyspec` の完全形を pin します。`partition_log` は append-only で、UPDATE / DELETE は DB trigger で拒否します。

`observations` table は `append_seq INTEGER PRIMARY KEY AUTOINCREMENT` を leaf-local cursor とし、`identity_key TEXT NOT NULL UNIQUE` と `canonical_json TEXT NOT NULL` を持ちます。self-host の通常 ingest は SQLite INSERT を先に行い、成功した行だけを in-memory Lake cache に反映します。`identity_key` UNIQUE 違反時は stored `canonical_json` と incoming `canonical_json` の exact compare で `Duplicate(existing_id)` / collision quarantine を分けます。

sharding runtime の実装対応:

- `crates/runtime/src/runtime/partition.rs`: routing keyspec / identity keyspec、Patricia trie replay、capacity split plan、split cutover protocol、failover / recover event payload。
- `crates/runtime/src/runtime/resolver.rs`: `candidate_leaves(filter, tree)` と `(published, recorded_at, id)` streaming k-way merge。resolver は imperative shell 側に置き、Domain Kernel は物理 leaf を見ません。
- `crates/engine/src/propagation/watermark.rs` / `crates/engine/src/propagation/scheduler.rs`: per-(projection, leaf) append_seq cursor と per-leaf tail detection。split 後の baseline は child leaf cursor を 0 から開始して全 Observation 再配送を許容します。
- `apps/selfhost/src/self_host/failover_spool.rs`: dedicated spool SQLite。spool は append-only で dedup せず、drain は `spool_seq` 順に `rehome_observation(..., StoredIdentity)` だけを使います。
- `crates/runtime/src/runtime/index_policy.rs`: secondary index は明示 read mode 必須です。暗黙 full-scan fallback は禁止し、placement axis に `person` / `subject` / `project` を含めません。
- `crates/runtime/src/runtime/keyspec_migration.rs`: blue/green keyspec migration の状態機械。bulk rehome mode (b)、iterative catch-up、freeze、cutover、retire の順序と旧 keyspec metadata 保持を固定します。

Lake は user-facing query 面ではなく、**capture and replay substrate** として設計します。

### 3.3 Supplemental Derivation Store

ここには以下を置きます。

- expensive but reusable derivations
- shared interpretation caches
- annotation and proposal support data
- name resolution candidates

mutability は 2 系統に分けます。

- **append-only supplemental:** provenance を強く保持したい記録
- **managed cache:** 再計算で上書きしてよいキャッシュ

### 3.4 Projection Runner

Projection Runner は次を実行します。

- spec lint
- dependency DAG validation
- build sandbox preparation
- deterministic build execution
- lineage manifest generation
- publish / archive / rollback metadata update

公開 Projection 用 build は、少なくとも lockfile、entrypoint、source version pin を保持すべきです。

### 3.5 Projection Catalog and API Layer

Catalog は次の責務を持ちます。

- Projection discovery
- version / dependency browsing
- read mode contract documentation
- access scope discovery
- downstream compatibility metadata

API layer は latest/pinned/cached の contract を明示します。

### 3.6 Sandbox / Draft Workspace

Sandbox は LETHE の利用体験上かなり重要です。

- 非技術ユーザーが terminal なしで Projection を試作できる
- coding agent が capability-scoped に spec や build を支援できる
- canonical write と publish の前に review を挟ける

`system-overview.md` にある通り、agent の主戦場は Lake ではなく sandbox です。

---

## 4. Main Runtime Flows

### 4.1 Capture Flow

```text
source / observer
  -> adapter
  -> ingestion gate
  -> validate + dedup + policy
  -> store blob if needed
  -> append lake observation
  -> emit audit event
```

### 4.2 Projection Build Flow

```text
spec draft
  -> lint
  -> resolve dependencies
  -> sandbox build
  -> lineage manifest
  -> catalog register
  -> serve / export
```

### 4.3 Write-Back Flow

```text
GUI / API / agent action
  -> normalize command
  -> evaluate review + authority
  -> derive effect plan
  -> apply lake append or source-native call
  -> recapture or rebuild
  -> refresh projection view
```

### 4.4 Serving Flow

```text
client request
  -> access policy
  -> choose read mode
  -> filter if restricted
  -> read projection / cache / source-native
  -> return data + revision metadata
```

---

## 5. Isolation and Reproducibility

### 5.1 Build Isolation

推奨ルール:

- build は ephemeral sandbox で実行
- network は default deny
- CPU / memory / storage / runtime upper bound を設定
- build image digest を記録
- artifact hash と build log を保存

### 5.2 Public Projection Requirements

次を満たすものを公開系 Projection の基準とします。

- spec と code が Git 管理されている
- dependency lock がある
- source pin rule がある
- deterministic mode を明示している
- failure surface が定義されている

---

## 6. Reference Technology Mapping

### 6.1 MVP

| Concern | Candidate |
|---|---|
| Registry / Catalog | SQLite or small PostgreSQL |
| Lake storage | Parquet on local/NAS storage |
| Object storage | Local FS or MinIO |
| Projection engine | DuckDB |
| API layer | FastAPI |
| Authoring UI | simple web app + agent integration |
| Build runner | local process or lightweight container |

### 6.2 Growth

| Concern | Candidate |
|---|---|
| Registry / Catalog | PostgreSQL |
| Object storage | MinIO |
| Supplemental store | PostgreSQL + Parquet + vector store |
| Sandbox | containerized jobs |
| Auth | OAuth2 / Keycloak |
| Queue | Redis or durable job queue |

### 6.3 Scale

| Concern | Candidate |
|---|---|
| Event ingestion | Kafka or equivalent |
| Cold lake storage | Parquet on object store |
| Metadata plane | PostgreSQL |
| Specialized projections | Neo4j, TimescaleDB, pgvector, DuckDB |
| Audit / lineage | dedicated service or warehouse tables |

重要なのは「どの製品を選ぶか」ではなく、「semantic law を壊さないか」です。

---

## 7. Operational Controls

### 7.1 Observability

少なくとも次を追跡します。

- append success / failure
- build success / failure
- source-native write conflict
- policy denial
- export event
- review queue lag
- approval_to_projection_freshness_p99

### 7.2 Observer Health and Gap Detection

Observer がサイレントに停止した場合の検知手段を定義します。

#### Heartbeat Observation

各 Observer は定期的に `schema:observer-heartbeat` を Lake に投入します。

```yaml
- id: "schema:observer-heartbeat"
  name: "Observer Heartbeat"
  version: "1.0.0"
  subject_type: "et:observer"
  payload_schema:
    type: object
    properties:
      status: { enum: ["alive", "degraded", "shutting-down"] }
      last_successful_capture_at: { type: string, format: date-time }
      pending_count: { type: integer }
    required: ["status"]
```

#### Gap Alert

monitoring service が heartbeat の途絶を検知し、alert を発行します。閾値は Observer ごとに source contract で定義します。

#### Projection 側の Gap Awareness

Projection Spec に `gapPolicy` を宣言できるようにします。

```yaml
spec:
  sources:
    - ref: "lake"
      filter:
        schemas: ["schema:room-entry"]
      gapPolicy:
        action: "warn"           # warn | block | fill-null
        maxGapDuration: "PT1H"   # 1時間以上の gap で発動
```

### 7.3 Backup and Recovery

- Lake と Registry は定期スナップショットを取る
- blob store は content-addressable 前提でバックアップする
- catalog / lineage / audit は整合性を持って保存する
- public Projection は rebuild 手順を持つ

### 7.3 Replaceable Boundaries

以下は交換可能です。

- queue 実装
- blob store 製品
- vector engine
- graph engine
- web framework

以下は交換しにくいコア契約です。

- Observation append-only
- read mode meaning
- command normalization
- provenance completeness
- filtering-before-exposure

---

## 8. Relationship to Other Documents

- 全体仕様: [System overview](system-overview.md)
- 意味論と law: [Domain algebra](domain-algebra.md)
- ガバナンスと capability: [Governance capability model](governance-capability-model.md)
- 未確定論点: [ADR backlog](../decisions/adr-backlog.md)
