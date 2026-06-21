# M05: Projection Engine

**Module:** projection-engine
**Scope:** Projection 意味論 / spec format / lifecycle / build / source 宣言
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M04 Supplemental Store
**Parent docs:** [plan.md](../../plan.md) §5, [domain_algebra.md](../../domain_algebra.md) §3.3–3.4, §5
**Agent:** Spec Designer (spec format) → Implementer (runner 実装) → Reviewer (replay 検証)

---

## 1. Module Purpose

Observation Lake / Supplemental / source-native / 他の Projection をソースとして、**派生データベース / API** を構築するエンジン。
LETHE の利用面の中核。

---

## 2. Core Interpretation

academic-pinned の Projection は概念的に:

```text
ProjectionResult =
  inputs
  |> selectByReadMode(spec.readMode)
  |> pinOrResolve(spec.sources)
  |> sortDeterministically
  |> validateInputs(spec)
  |> foldl(applyInput, initialState(spec))
  |> finalize(spec)
```

- `applyInput` と `finalize` は **純粋関数**
- source access / DB 書き込みはこの式の外 (shell)

---

## 3. Projection Kinds

| Kind | Meaning | Use |
|---|---|---|
| **PureProjection** | 入力と変換ロジックに閉じる | academic build |
| **CachedProjection** | materialization / cache を持つ | operational API |
| **WritableProjection** | write surface を持つ (command に正規化) | GUI 編集面 |

---

## 4. Read Mode Semantics

| Read Mode | Determinism | Goal | Source-native Direct Read |
|---|---|---|---|
| AcademicPinned | 必須 | 再現性、引用 | **禁止** |
| OperationalLatest | 緩い | 鮮度、運用 | **許可** |
| ApplicationCached | 中間 | 低コスト、高速 | cache miss は明示エラー |

### 4.1 Source-Native Read Contract

```yaml
sources:
  - ref: "source-native:sys:google-slides"
    readMode: "operational-latest"
    freshnessSla: "best-effort"
    lineageCapture: "input-refs"
```

### 4.2 Failure Semantics

source-native、Lake snapshot、Projection cache は spec で宣言された入力だけを使用する。取得失敗や cache miss を別経路で代替してはならず、build / read を明示的に失敗させる。

### 4.3 Multi-Source Reconciliation

- 同一 Projection が `lake` と `source-native:*` を併用する場合、`spec.reconciliation` を **必須** とする
- `AcademicPinned` は source-native 直接読み禁止のため、`reconciliation.policy = "lake-first"` のみ許可
- `OperationalLatest` では `source-latest` または `dual-track` を許可する
- policy 未指定の multi-source spec は validation failure

---

## 5. Projection Spec Format

```yaml
apiVersion: "lethe/v1"
kind: "Projection"

metadata:
  id: "proj:{name}"
  name: string
  created_by: string
  version: SemVer
  tags: [string]
  description: string

spec:
  # ── ソース宣言 ──
  sources:
    - ref: "lake"
      filter:
        schemas: [SchemaRef]
        subject_types: [EntityTypeRef]?
        window: { start: date, end: date }?
    - ref: "supplemental"
      filter:
        derivations: [SupplementalKind]
      versionPins:
        - derivation: SupplementalKind
          recordVersion: string
          modelVersion: string?
    - ref: "proj:{other}"           # DB on DBs
      version: ">=1.0.0"
    - ref: "source-native:sys:{name}"
      readMode: ReadMode

  # ── 構築設定 ──
  engine: string                    # "duckdb", "neo4j", "python", etc.
  build:
    type: string                    # "sql-migration", "python-script", etc.
    entrypoint: path?
    projector: path

  # ── 出力定義 ──
  outputs:
    - format: string                # "sql", "parquet", "json", "graphql"
      tables: [string]?
      schedule: string?
      location: path?

  # ── API 契約 ──
  interface:
    primaryAccess:
      type: string                  # "http", "sql", "bolt"
      path: string?
    readModes: [ReadModePolicy]
    compatibility:
      downstreamVersioning: "major-pinned"

  # ── 複数ソース整合 ──
  reconciliation:
    policy: "lake-first" | "source-latest" | "dual-track"
    conflictResolution: "union" | "intersection" | "left-bias"
    onConflict: "fail-build" | "mark-stale"

  # ── 再現性 ──
  reproducibility:
    deterministicIn: [ReadMode]
    seed: int?
    rebuild_command: string?

  # ── Gap Policy ──
  gapPolicy:
    action: enum[warn, block, fill-null]
    maxGapDuration: duration?

  # ── Write-Back (optional, see M07) ──
  writeBack:
    enabled: boolean
    mode: WriteMode?
    # ... (see write-back.md for full spec)
```

---

## 6. Projection Input Types

```text
ProjectionInput
  = LakeInput ObservationSelector
  | SupplementalInput SupplementalSelector
  | SourceRevisionInput SourceRevisionSelector
  | ProjectionInputRef ProjectionVersionSelector
```

Projection が読むのは常に **宣言済み入力集合**。

---

## 7. Projection Lifecycle

```
1. Define    → Projection Spec を記述 (YAML + code)
2. Register  → Projection Catalog に登録 (DAG 非巡回性チェック)
3. Build     → 変換実行、DB 構築
4. Serve     → クエリ可能。他の Projection のソースにもなれる
5. Version   → Spec 更新 → version bump → rebuild
6. Archive   → 非活性化、最終 snapshot export、DOI 凍結
```

### 7.1 Build Contract

build は少なくとも以下を明示:
- source declaration
- read mode policy
- deterministic input ordering rule
- supplemental version pins（ManagedCache を読む場合）
- supported schema / projection versions
- multi-source reconciliation policy
- output contract
- lineage capture strategy
- failure mode

### 7.2 Build Isolation

- build は ephemeral sandbox で実行
- network は default deny
- CPU / memory / runtime upper bound 設定
- build image digest 記録
- artifact hash + build log 保存

---

## 8. Projection Patterns

| Pattern | Example | Engine |
|---|---|---|
| Social Network | 同時滞在グラフ | Neo4j / NetworkX |
| Time Series | センサー集計 | TimescaleDB / DuckDB |
| Behavioral Embedding | 行動パターン vector | pgvector / FAISS |
| Text Corpus | 全文検索 | SQLite FTS / Elasticsearch |
| Multimedia Index | 画像 meta + vector | pgvector + MinIO |
| Operational View | 現在の部屋割り | PostgreSQL |
| Composite Analysis | クロス分析 | DuckDB / Jupyter |

---

## 9. Projection Runner

### 9.1 Responsibilities

- spec lint
- dependency DAG validation
- build sandbox preparation
- deterministic build execution
- lineage manifest generation
- publish / archive / rollback metadata update

### 9.2 API

| Method | Path | Description |
|---|---|---|
| POST | `/api/projections/{id}/build` | build 実行 |
| POST | `/api/projections/{id}/rebuild` | full rebuild |
| GET | `/api/projections/{id}/build/status` | build 状態 |
| GET | `/api/projections/{id}/build/log` | build ログ |
| GET | `/api/projections/{id}/lineage` | lineage manifest |

---

## 10. Lineage

### 10.1 Lineage Granularity

| Level | Content | Use |
|---|---|---|
| Projection-level | 使用した Lake window, Projection version, supplemental | 標準 |
| Row-level | 出力行 → 元 Observation / source revision | 重要 Projection |
| Blob-anchor | 画像の bounding box, 動画の time range, 文書の object id | deep tracing |

### 10.2 Lineage Record

```text
LineageManifest =
  { projectionId  : ProjectionRef
  , version       : SemVer
  , buildId       : BuildId
  , builtAt       : Timestamp
  , sources       : [SourceSnapshot]
  , inputRefs     : [ObservationRef | SupplementalRef]
  , inputCount    : int
  , outputCount   : int
  , deterministic : boolean
  , seed          : int?
  }
```

---

## 11. Schema Compatibility

Projection は対応する Schema version range を宣言:

```yaml
sources:
  - ref: "lake"
    filter:
      schemas:
        - name: "schema:room-entry"
          versions: ">=1.0.0, <3.0.0"
```

major bump 後も旧データで動作継続。新形式は Projection 更新が必要。

---

## 12. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | AcademicPinned build は同一入力 → 同一結果 | replay test |
| 2 | Projection DAG は非巡回 | catalog registration check |
| 3 | Projection materialization への直接 write 禁止 | SQL audit |
| 4 | build は sandbox 内で実行 | isolation check |
| 5 | lineage manifest は全 build で生成 | build pipeline check |
| 6 | multi-source spec は reconciliation を必須宣言 | spec validation |
| 7 | ManagedCache supplemental を academic-pinned で読む場合は version pin 必須 | spec validation |
| 8 | `lineage_ref` は入力参照を含む取得可能な manifest を指す | API integration test |

---

## 13. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | Valid Projection Spec 登録 | Catalog に追加 | |
| 2 | 循環依存 Spec 登録 | 拒否 | |
| 3 | build 実行 (DuckDB projector) | 出力テーブル生成 + lineage | |
| 4 | AcademicPinned replay | 同一結果 | |
| 5 | upstream archive → downstream status | degraded | |
| 6 | Schema version 範囲外 data | warning / skip | |
| 7 | `lake` + `source-native` 併用で reconciliation 未指定 | validation failure | |
| 8 | academic-pinned + ManagedCache supplemental で version pin なし | validation failure | |
| 9 | multi-source + `dual-track` | lineage に双方の source snapshot が残る | |

---

## 14. Module Interface

### Provides

- Projection Spec parser / validator
- Projection runner (build / rebuild)
- Lineage manifest generator
- Build isolation sandbox
- Schema compatibility checker

### Requires

- M01 Domain Kernel: ProjectionKind, ReadMode, ProjectionSpec 型
- M02 Registry: Projection Catalog, Schema Registry
- M03 Observation Lake: Observation query, watermark
- M04 Supplemental Store: Supplemental query
