# M15: Runtime

**Module:** runtime
**Scope:** Runtime topology, build isolation, sandbox, reference technology mapping, operational controls
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M05 Projection Engine
**Parent docs:** [Runtime reference](../../docs/architecture/runtime-reference.md)
**Agent:** Spec Designer (topology 設計) → Implementer (infrastructure) → Reviewer (isolation 検証)
**MVP:** △ (MVP は最小構成のみ)

---

## 1. Module Purpose

LETHE を実際に稼働させるための runtime topology・build isolation・sandbox・observer health 検知・
技術選定ガイドラインを定義する。

> **Runtime Is Subordinate to Semantics:** 実装は交換可能であり、
> 守るべき本質は `docs/architecture/system-overview.md` と `docs/architecture/domain-algebra.md` の意味論と law。

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

## 3. Functional Core / Imperative Shell Mapping

| Layer | Runtime Role | Examples |
|---|---|---|
| **Domain Kernel** | 型と変換の意味論 | Observation validation, projection fold |
| **Policy Engine** | pure decision service | consent check, review requirement |
| **Ports** | effect interface | blob store, source API, projection catalog |
| **Adapters** | effect interpreter | Google connector, Slack crawler, MinIO adapter |
| **Orchestrators** | workflow / scheduling | job queue, replay worker, cache refresher |

---

## 4. Runtime Components

### 4.1 Ingestion / Write Gate

**責務:**
- observer / user / service identity の確認
- schema validation
- consent / access / review policy 呼び出し
- idempotency check
- blob upload orchestration
- append request 生成

**非責務:**
- transcript / OCR / LLM 解釈
- name resolution の最終判断
- Projection materialization の直接更新 (No Direct Mutation Law)

### 4.2 Projection Runner

**責務:**
- spec lint
- dependency DAG validation
- build sandbox preparation
- deterministic build execution
- lineage manifest generation
- publish / archive / rollback metadata update

公開 Projection の build は lockfile + entrypoint + source version pin を保持する。

### 4.3 Projection Catalog

**責務:**
- Projection discovery
- version / dependency browsing
- read mode contract documentation
- access scope discovery
- downstream compatibility metadata

### 4.4 Sandbox / Draft Workspace

**重要度:** 高 — LETHE の利用体験上のコア

- 非技術ユーザーが terminal なしで Projection を試作可能
- coding agent が capability-scoped に spec / build を支援
- canonical write / publish 前に review を挟む
- agent の主戦場は Lake ではなく sandbox

---

## 5. Main Runtime Flows

### 5.1 Capture Flow

```text
source / observer → adapter → ingestion gate
  → validate + dedup + policy → store blob → append lake observation → emit audit event
```

### 5.2 Projection Build Flow

```text
spec draft → lint → resolve dependencies → sandbox build
  → lineage manifest → catalog register → serve / export
```

### 5.3 Write-Back Flow (MVP 外)

```text
GUI / API / agent action → normalize command → evaluate review + authority
  → derive effect plan → apply lake append or source-native call
  → recapture or rebuild → refresh projection view
```

### 5.4 Serving Flow

```text
client request → access policy → choose read mode → filter if restricted
  → read projection / cache / source-native → return data + revision metadata
```

---

## 6. Build Isolation

### 6.1 推奨ルール

- build は ephemeral sandbox で実行
- network は default deny
- CPU / memory / storage / runtime upper bound を設定
- build image digest を記録
- artifact hash と build log を保存

### 6.2 Public Projection Requirements

公開 Projection の基準:
- spec と code が Git 管理
- dependency lock がある
- source pin rule がある
- deterministic mode を明示
- failure surface が定義

### 6.3 MVP Build Isolation

MVP では最小限:
- local process 実行 (container は Growth 以降)
- timeout enforcement
- build log 保存
- artifact hash 記録

---

## 7. Reference Technology Mapping

### 7.1 MVP

| Concern | Candidate |
|---|---|
| Registry / Catalog | SQLite |
| Lake storage | Parquet on local FS |
| Object storage | Local FS or MinIO |
| Projection engine | DuckDB |
| API layer | FastAPI |
| Authoring UI | simple web app + agent integration |
| Build runner | local process |
| Language | Python |

### 7.2 Growth

| Concern | Candidate |
|---|---|
| Registry / Catalog | PostgreSQL |
| Object storage | MinIO |
| Supplemental store | PostgreSQL + Parquet + vector store |
| Sandbox | containerized jobs |
| Auth | OAuth2 / Keycloak |
| Queue | Redis or durable job queue |

### 7.3 Scale

| Concern | Candidate |
|---|---|
| Event ingestion | Kafka or equivalent |
| Cold lake storage | Parquet on object store |
| Metadata plane | PostgreSQL |
| Specialized projections | Neo4j, TimescaleDB, pgvector, DuckDB |
| Audit / lineage | dedicated service or warehouse tables |

> **重要:** 選択基準は「semantic law を壊さないか」

---

## 8. Operational Controls

### 8.1 Observability

追跡すべきメトリクス:
- append success / failure
- build success / failure
- source-native write conflict
- policy denial
- export event
- review queue lag
- approval_to_projection_freshness_p99

### 8.2 Observer Health & Gap Detection

#### Heartbeat Observation

各 Observer は定期的に `schema:observer-heartbeat` を Lake に投入:

```yaml
schema: "schema:observer-heartbeat"
version: "1.0.0"
subject_type: "et:observer"
payload:
  status: "alive" | "degraded" | "shutting-down"
  last_successful_capture_at: "2026-05-01T10:30:00+09:00"
  pending_count: 0
```

#### Gap Alert

- monitoring service が heartbeat 途絶を検知
- 閾値は Observer ごとに source contract で定義

#### Projection 側の Gap Awareness

```yaml
spec:
  sources:
    - ref: "lake"
      gapPolicy:
        action: "warn"           # warn | block | fill-null
        maxGapDuration: "PT1H"
```

### 8.3 Backup & Recovery

- Lake / Registry: 定期スナップショット
- blob store: content-addressable 前提でバックアップ
- catalog / lineage / audit: 整合性保護
- Projection: rebuild 手順を保持

---

## 9. Replaceable vs Core Boundaries

### 交換可能

- queue 実装
- blob store 製品
- vector engine
- graph engine
- web framework

### 交換不可 (コア契約)

- Observation append-only
- read mode meaning
- command normalization
- provenance completeness
- filtering-before-exposure

---

## 10. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | runtime は Domain Kernel の law を壊さない | law compliance test |
| 2 | build は environment isolation で実行 | sandbox check |
| 3 | build artifact に hash が付与される | build log check |
| 4 | heartbeat 途絶はアラートを発火 | gap detection test |
| 5 | コア契約は技術選定に関わらず保持 | architecture review |

---

## 11. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | Capture flow end-to-end | Observation in Lake | adapter → gate → lake |
| 2 | Projection build | artifact + lineage manifest | spec → build → publish |
| 3 | Observer heartbeat 送信 | heartbeat Observation in Lake | |
| 4 | Observer heartbeat なし (timeout) | Gap alert | |
| 5 | Build timeout | build 中断 + error log | |
| 6 | Health check endpoint | status ok + projection statuses | |
| 7 | Backup → restore → verify | Lake / Registry 整合性 | |

---

## 12. Module Interface

### Provides

- Runtime topology specification
- Build isolation / sandbox rules
- Technology mapping per maturity phase (MVP / Growth / Scale)
- Operational control requirements (observability, alerting, backup)
- Observer health detection mechanism
- Replaceable / core boundary classification

### Requires

- M01 Domain Kernel: System Laws (validation)
- M02 Registry: Observer & Source contracts (health thresholds)
- M03 Observation Lake: append API, heartbeat schema
- M05 Projection Engine: build runner integration
