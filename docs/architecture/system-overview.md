
---

# LETHE (旧称:DOKP; Architecture Design Specification: Dormitory Observation & Knowledge Platform)

**Version:** 4.0
**Date:** 2026-03-09
**Context:** 学生寮データ統合基盤 & 学術的知識資産プラットフォーム

---

## 0. How to Read This Specification

### 0.1 Problem, Scope, and Non-Goals

この仕様の中心課題は、**mutable な外部 source と append-only な学術再現性をどう両立させるか** にある。  
LETHE は「Lake に全部を直接見せるシステム」ではなく、**Observation を不変に capture し、Projection / API / Sandbox で使うための知識基盤**として設計する。

この文書で扱う scope:

- canonical capture の意味論
- Lake / supplemental / source-native の境界
- Projection の read / build / replay / write semantics
- governance, consent, capability, review の基本方針
- reference implementation に落とすための runtime 分離

この文書で直接固定しないもの:

- 特定製品へのロックイン（Kafka, MinIO, DuckDB などは交換可能）
- 各 source adapter の詳細 API 差分
- GUI の画面設計
- すべての未確定論点の即時決着

### 0.2 Document Map

本仕様群は、意味論と運用詳細を分けるために複数文書へ分割する。

| File | Role | Position |
|---|---|---|
| [System overview](system-overview.md) | 親仕様。全体像、主要レイヤ、read/write モデル | Authoritative overview |
| [Domain algebra](domain-algebra.md) | core algebra, system laws, failure model, storage semantics | Normative semantic companion |
| [Governance capability model](governance-capability-model.md) | consent, filtering, capability, review, retention, secret handling | Normative policy companion |
| [Runtime reference](runtime-reference.md) | runtime topology, sandbox, workers, reference stack | Reference implementation |
| [ADR backlog](../decisions/adr-backlog.md) | 未確定論点、優先順位、次に決めること | Decision backlog |
| [Design questions](../archive/design-questions.md) | 回答付きの raw working sheet | Archived working material |

### 0.2.1 Current implementation

現行リポジトリはvirtual Cargo workspaceであり、次の境界をコード所有単位として
強制する。

- `crates/core`, `policy`, `registry`, `engine`, `api`, `runtime`
- `crates/storage/*`, `adapters/*`, `derivations/*`, `projections/*`
- `apps/selfhost`と`apps/tools/*`
- `tests/e2e`

実装はRust、Axum、SQLite、content-addressed local blob storageを使用する。
workspace rootはsourceを持たず、crate間依存はDAG検査で検証する。

### 0.3 Functional Core / Imperative Shell

この仕様は、次の構図で読むと一貫する。

| Layer | Responsibility | Design Rule |
|---|---|---|
| **Domain Kernel** | Observation / Projection / Command / Lineage の意味論 | 純粋関数として定義する |
| **Policy Layer** | consent, access, review, retention, approval | IO を起こさず判定する |
| **Effect Ports** | blob save, source read, source-native write, DB materialize | interface として切り出す |
| **Adapters** | Google, Slack, Figma, sensor, storage, API adapters | effect interpreter に留める |
| **Runtime / Scheduler** | crawl, build, replay, refresh, queue | 順序管理に徹し意味論を変えない |

### 0.4 System Laws

実装や運用を変えても、少なくとも次の law は守る。

| Law | Meaning |
|---|---|
| **Append-Only Law** | Canonical Observation を破壊的更新しない |
| **Replay Law** | pin された同一入力から同一 Projection 結果を得る |
| **Effect Isolation Law** | ドメイン解釈は hidden mutable state に依存しない |
| **Explicit Authority Law** | すべての write は authority model で正当化する |
| **No Direct Mutation Law** | Projection materialization を正史として更新しない |
| **Filtering-before-Exposure Law** | restricted data は表示・配布前に filtering projection を通す |

詳細な law、command algebra、failure model は [domain_algebra.md](domain-algebra.md) を参照。

### 0.5 Open Decisions

未確定事項は本文に埋め込まず、[adr_backlog.md](../decisions/adr-backlog.md) で追跡する。
現在の重点は、multimodal canonicalization、high-frequency capture policy、agent playground / capability model、API / serving examples の 4 点である。

---

## 1. System Vision

### 1.1 What This System Is

学生寮に関わるあらゆる **「観測（Observation）」** を普遍的に蓄積し、そこから誰でも自由にデータベースを構築・合成できる **開放的な知識基盤**。

### 1.2 Why This System Exists

| 問題 | 本システムによる解決 |
|---|---|
| 寮内で個別に乱立するDB・スプレッドシートが統一的に扱えない | canonical capture layer に集約し、利用面は Projection API に統一される |
| 蓄積されたデータに学術的引用可能性がない | 全 Projection に DOI 付与・再現性を保証 |
| 年度交代でデータ資産が断絶する | Lake と Registry が永続し、誰でも過去データを発見・再利用できる |

### 1.3 Design Principles

| # | Principle | Guarantees |
|---|---|---|
| P1 | **Open Entity Model** | 任意の関心対象（人、モノ、場所、概念）を自由に登録・観測できる |
| P2 | **Sovereign Projection** | 誰でも既存データから自分の DB を自由に構築できる |
| P3 | **DB on DBs** | Projection は他の Projection をソースにできる（DAG 構造で無限合成） |
| P4 | **Multimodal First** | テキスト、画像、音声、動画、センサーデータを区別なく扱える |
| P5 | **Source Agnostic** | 新しいデータソース（センサー、Bot、人間入力、外部 API）を自由に追加できる |
| P6 | **API-First Projection** | Projection は DB であると同時に安定した API / export 契約を持つ |
| P7 | **Interpretation Separation** | 一次資料、補助的導出結果、利用者向け解釈を層として分離する |

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Registry (Meta Layer)                            │
│                                                                              │
│ EntityType / Schema / Observer / Source Contract / Projection / Governance  │
└───────────────────────────────┬──────────────────────────────────────────────┘
      │
  ┌───────────────────────┼────────────────────────┐
  ▼                       ▼                        ▼
┌─────────────────────┐ ┌──────────────────────┐ ┌──────────────────────────┐
│ Source-Native       │ │ Canonical Capture    │ │ Supplemental Derivation  │
│ Systems             │ │ Layer                │ │ Store                    │
│ Google / Slack /    │ │ Observation Lake     │ │ transcript / OCR /       │
│ Sensors / Figma ... │ │ + Blob Storage       │ │ embeddings / confidence  │
└──────────┬──────────┘ └──────────┬───────────┘ └─────────────┬────────────┘
       │                       ▲                           │
       │                       │                           │
       ▼                       │                           ▼
┌─────────────────────┐            │                 ┌────────────────────────┐
│ Observers           │────────────┘                 │ Projection / API Layer │
│ crawler / connector │                              │ academic / operational │
│ sensor gateway /    │─────────────────────────────►│ DB on DBs / exports    │
│ human form          │                              └────────────┬───────────┘
└─────────────────────┘                                           │
                                   ▼
                              ┌────────────────────┐
                              │ GUI / Clients /    │
                              │ Other Projections  │
                              └────────────────────┘
```

固定の「L1→L2→L3」階層ではなく、**canonical capture を根にした DAG（有向非巡回グラフ）** として任意の深さで合成できる。Projection の「深度」は自動計算されるメタデータに過ぎず、設計上の制約にはならない。

重要なのは、**source の性質、capture の方法、利用時の読取経路を分離して設計する** ことである。Lake は保存と再構成のための backing layer であり、通常の利用面は Projection API / export / sandbox に置く。ただし、Projection は用途に応じて Lake、supplemental store、source-native API のいずれを主読取経路にするかを明示しなければならない。

### 2.1 Core Separation Model

本仕様では、混線しやすい論点を次の 3 軸に分離して扱う。

| Axis | Question | Typical Values |
|---|---|---|
| **Authority Model** | 正史はどこにあるか | `lake-authoritative`, `source-authoritative`, `dual-reference` |
| **Capture Model** | observer が何を Lake に保存するか | `event`, `snapshot`, `chunk-manifest`, `restricted` |
| **Read Mode** | 利用者・API がどの経路で読むか | `academic-pinned`, `operational-latest`, `application-cached` |

これにより、mutable / immutable、multimodal / text、academic / operational の違いを同一レイヤーで混ぜずに表現できる。

### 2.2 Observer and Source System

Observation は「observer が source system から何を取得したか」の記録である。

- **source system:** Google Docs / Sheets / Slides / Drive / Forms / Photos / Calendar、Notion、Figma、Canva、Slack、センサー backend など、元データの authority を持つ系
- **observer:** crawler、connector、sensor gateway、人手入力フォームなど、source system からデータを取得して Lake に記録する主体

Google 共有ドキュメントのような mutable source では、crawler / connector を observer として運用するのが基本である。これにより、人間入力と自動取得を同じ Observation モデルで扱える。

### 2.3 Read Modes

同じ source に対しても用途により読取経路は異なる。したがって、academic / operational は Observation 自体の種類ではなく、**API または Projection の read mode** として指定する。

| Read Mode | Primary Path | Goal |
|---|---|---|
| **academic-pinned** | Lake / pinned raw manifest | 再現性、DOI、lineage |
| **operational-latest** | source-native API / live source | 鮮度、実運用 |
| **application-cached** | Projection cache / Lake snapshot | 応答性、コスト抑制 |

### 2.4 Source Contract Matrix

代表的な source は次のように整理する。

| Source Class | Examples | Authority | Capture | Academic | Operational |
|---|---|---|---|---|---|
| **Mutable + Multimodal** | Google Slides, Figma, Canva, Google Meet recording, latest sensor state | `source-authoritative` or `dual-reference` | `snapshot` / `chunk-manifest` | pinned snapshot / pinned manifest を利用 | latest を返す |
| **Mutable + Text / Structured** | Google Docs, Google Sheets, Google Calendar, Notion page/database | `source-authoritative` with `dual-reference` | `snapshot` | revision snapshot を利用 | latest を返す |
| **Immutable + Multimodal** | sensor raw accumulation, photo archive | `lake-authoritative` or raw store + Lake manifest | `chunk-manifest` / `snapshot` | Lake / raw manifest を利用 | Lake または source cache |
| **Immutable + Text** | Slack archive, append-only logs | `lake-authoritative` | `event` | Lake を利用 | Lake / cache を利用 |

Google Photos の共有アルバムのように、見かけ上アーカイブでも追加・削除・共有範囲変更が起こるものは、運用上は mutable source として扱う方が安全である。

---

## 3. Registry (Meta Layer)

### 3.1 Entity Type Registry — 「何について」観測するかの型定義

プロジェクトメンバーは誰でも新しい Entity Type を登録できる（P1: Open Entity Model）。

```yaml
# === 基盤型（システム初期登録） ===
- id: "et:person"
  name: "Person"
  description: "寮に関わる人物（寮生、スタッフ、来訪者等）"
  attributes: ["name", "affiliation", "cohort"]

- id: "et:space"
  name: "Space"
  description: "物理空間（部屋、共有スペース、建物等）"
  subtypes: ["et:room", "et:common-area", "et:dining-hall", "et:building"]
  attributes: ["building", "floor", "capacity", "purpose"]

- id: "et:artifact"
  name: "Artifact"
  description: "物理的・デジタルな対象物"
  subtypes: ["et:book", "et:equipment", "et:document", "et:furniture"]
  attributes: ["name", "category"]

# === 利用者が自由に追加する型の例 ===
- id: "et:meal-event"
  name: "Meal Event"
  description: "食堂での食事イベント"
  registered_by: "tanaka"
  registered_at: "2026-05-15"

- id: "et:study-group"
  name: "Study Group"
  description: "自主的に形成された学習グループ"
  registered_by: "yamamoto"
  registered_at: "2026-06-01"
```

**型の階層（is-a）:** `et:room` is-a `et:space` のように継承関係を持てる。Projection は親型でフィルタすることで配下の全サブタイプを取得できる。

### 3.2 Schema Registry — 「どのような形式で」観測するかの定義

各 Observation は必ずいずれかの Schema に適合しなければならない。Schema は JSON Schema で記述される。

```yaml
- id: "schema:room-entry"
  name: "Room Entry/Exit"
  version: "1.0.0"
  subject_type: "et:person"       # 誰が
  target_type: "et:room"          # どこに
  payload_schema:
    type: object
    properties:
      action: { enum: ["enter", "exit"] }
      method: { enum: ["key-card", "manual", "auto-sensor"] }
    required: ["action"]

- id: "schema:temperature-reading"
  name: "Temperature Reading"
  version: "1.0.0"
  subject_type: "et:space"        # どこの
  payload_schema:
    type: object
    properties:
      celsius: { type: number }
      humidity_pct: { type: number }
    required: ["celsius"]

- id: "schema:photo-record"
  name: "Photo Record"
  version: "1.0.0"
  subject_type: "et:*"            # 任意のエンティティについて
  payload_schema:
    type: object
    properties:
      description: { type: string }
      labels: { type: array, items: { type: string } }
    required: ["description"]
  attachments:
    required: true
    accepted_types: ["image/jpeg", "image/png", "image/webp"]

- id: "schema:freeform-note"
  name: "Freeform Note"
  version: "1.0.0"
  subject_type: "et:*"
  payload_schema:
    type: object
    properties:
      text: { type: string }
      tags: { type: array, items: { type: string } }
    required: ["text"]

- id: "schema:document-snapshot"
  name: "Document Snapshot"
  version: "1.0.0"
  subject_type: "et:document"
  payload_schema:
    type: object
    properties:
      provider: { enum: ["google-slides", "google-sheets", "spreadsheet", "other"] }
      document_type: { enum: ["slide-deck", "spreadsheet", "document", "other"] }
      source_document_id: { type: string }
      source_revision_id: { type: string }
      source_modified_at: { type: string, format: date-time }
      capture_mode: { enum: ["native-structure", "rendered-snapshot", "hybrid"] }
      native_structure_ref: { type: string }
      render_refs:
        type: array
        items: { type: string }
    required: ["provider", "document_type", "source_document_id", "capture_mode"]
  attachments:
    required: false
    accepted_types: ["application/json", "application/pdf", "application/vnd.openxmlformats-officedocument.presentationml.presentation", "image/png"]
```

**拡張の自由度:** 新しい Schema は誰でも登録できる。`subject_type: "et:*"` を指定すればあらゆるエンティティに対して使える汎用スキーマとなる。

### 3.3 Observer & Source Contract Registry — 「誰が」「どの契約で」取り込むか

```yaml
- id: "obs:door-sensor-gateway-a"
  name: "Building A Sensor Gateway"
  observer_type: "sensor-gateway"
  source_system: "sys:door-sensor-bldg-a"
  schemas: ["schema:room-entry"]
  authority_model: "lake-authoritative"
  capture_model: "event"
  owner: "facilities-team"
  trust_level: "automated"        # automated | human-verified | crowdsourced

- id: "obs:dining-bot"
  name: "Dining Hall Usage Bot"
  observer_type: "bot"
  source_system: "sys:dining-hall"
  schemas: ["schema:dining-entry", "schema:dining-exit"]
  authority_model: "lake-authoritative"
  capture_model: "event"
  owner: "infra-team"
  trust_level: "automated"

- id: "obs:manual-tanaka"
  name: "Tanaka Manual Entry"
  observer_type: "human"
  source_system: "sys:manual-entry"
  schemas: ["*"]                  # 任意の Schema で投稿可能
  authority_model: "lake-authoritative"
  capture_model: "event"
  owner: "tanaka"
  trust_level: "human-verified"

- id: "obs:weather-crawler"
  name: "External Weather Crawler"
  observer_type: "crawler"
  source_system: "sys:weather-api"
  schemas: ["schema:weather-reading"]
  authority_model: "dual-reference"
  capture_model: "snapshot"
  owner: "infra-team"
  trust_level: "automated"

- id: "obs:gslides-crawler"
  name: "Google Slides Crawler"
  observer_type: "crawler"
  source_system: "sys:google-slides"
  schemas: ["schema:document-snapshot"]
  authority_model: "source-authoritative"
  capture_model: "snapshot"
  owner: "infra-team"
  trust_level: "automated"
```

**新規 source の追加（P5）:** source system と observer を分けて Registry に登録する。source contract には、authority model、capture model、対応 schema、read 制約を含める。

**Mutable な外部ソース:** Google Slides、Figma、Google Docs、Google Sheets のような mutable source は、現在値を上書き保存するのではなく、crawler / connector が各 revision/timepoint を表す **snapshot Observation** として Lake に append する。Operational 利用では source-native latest を読めるが、academic 利用では pin された revision snapshot を用いる。

### 3.4 Projection Catalog — 構築された DB の発見・利用

```yaml
- id: "proj:social-graph-2026s"
  name: "Social Co-presence Graph (Spring 2026)"
  description: |
    共有スペースの入退室データから同時滞在ネットワークを構築。
    ノード＝Person、エッジ＝同一空間への同時滞在（15分以上）。
  created_by: "yamamoto"
  created_at: "2026-05-01"
  version: "1.2.0"
  engine: "neo4j"
  sources:
    - type: "lake"
      filter:
        schemas: ["schema:room-entry"]
        window: { start: "2026-04-01", end: "2026-09-30" }
  connection:
    protocol: "bolt"
    endpoint: "bolt://192.168.x.x:7687"
  outputs:
    - format: "cypher"
      description: "Native graph queries"
    - format: "parquet"
      description: "Weekly snapshot exports"
      location: "/exports/social-graph/2026s/"
  doi: "10.xxxxx/lethe-sg-2026s"   # 学術引用用 DOI
  tags: ["network", "social", "co-presence"]
  status: "active"                 # active | archived | building
```

---

## 4. Observation Lake (Canonical Capture Layer)

### 4.1 Core Concept

システムの根幹となる不変の capture ストア。全てのデータは **Observation（観測）** という統一的な単位で記録される。

> **Observation =「ある observer が、ある source system から、ある契約に従って何かを capture した記録」**

Observation は用途中立である。academic / operational の区別は Observation 自体に埋め込まず、Projection や API の read mode で指定する。

ただし、Lake は **利用者が直接読むための主データ面ではなく、canonical capture と replay の基盤** である。利用者やエージェントが日常的に触るのは Projection / API / Sandbox 層であり、Lake はその背後で再現性と発見性を保証する。

### 4.2 Observation Schema

```
┌──────────────────────────────────────────────────────────────┐
│                        Observation                            │
├──────────────────────────────────────────────────────────────┤
│ id             : UUID v7 (time-sortable)                     │
│ schema         : SchemaRef   → Schema Registry               │
│ schemaVersion  : SemVer      (e.g. "1.0.0")                 │
│                                                              │
│ observer       : ObserverRef → Observer Registry             │
│ sourceSystem   : SystemRef?  → Source System Registry        │
│ actor          : EntityRef?  (human who triggered, if any)   │
│ authorityModel : enum        (lake | source | dual)          │
│ captureModel   : enum        (event | snapshot | chunk |     │
│                               restricted)                    │
│                                                              │
│ subject        : EntityRef   (PRIMARY: what this is about)   │
│ target         : EntityRef?  (SECONDARY: related entity)     │
│                                                              │
│ payload        : JSON        (validated against schema)      │
│ attachments    : [BlobRef]   (→ Object Storage)              │
│                                                              │
│ published      : ISO 8601    (claimed event time)            │
│ recordedAt     : ISO 8601    (system ingestion time, auto)   │
│                                                              │
│ consent        : ConsentRef  → Consent Ledger                │
│ idempotencyKey : string      (deduplication key)             │
│ meta           : JSON        (source revision / anchors /    │
│                               operational metadata)          │
└──────────────────────────────────────────────────────────────┘
```

**EntityRef format:** `{type}:{id}` — 例: `person:tanaka-2026`, `room:A-301`, `meal-event:2026-05-01-lunch`

**Read mode は Observation に含めない:** same observation を academic-pinned と operational-latest の両方で参照できるようにするため、用途の区別は API / Projection contract 側に置く。

### 4.3 Concrete Examples

```jsonc
// Example 1: Door sensor records a room entry
{
  "id": "019577a0-...",
  "schema": "schema:room-entry",
  "schemaVersion": "1.0.0",
  "observer": "obs:door-sensor-gateway-a",
  "sourceSystem": "sys:door-sensor-bldg-a",
  "authorityModel": "lake",
  "captureModel": "event",
  "subject": "person:tanaka-2026",
  "target": "room:A-301",
  "payload": { "action": "enter", "method": "key-card" },
  "attachments": [],
  "published": "2026-05-01T08:30:00+09:00",
  "recordedAt": "2026-05-01T08:30:00.123+09:00",
  "consent": "consent:tanaka-2026-main",
  "idempotencyKey": "door-a301-tanaka-20260501-0830",
  "meta": { "sensor_firmware": "v2.1" }
}

// Example 2: Manual photo record of whiteboard
{
  "id": "019577b2-...",
  "schema": "schema:photo-record",
  "schemaVersion": "1.0.0",
  "observer": "obs:manual-suzuki",
  "sourceSystem": "sys:manual-entry",
  "actor": "person:suzuki-2026",
  "authorityModel": "lake",
  "captureModel": "event",
  "subject": "room:common-B2",
  "payload": {
    "description": "ミーティング後のホワイトボード",
    "labels": ["meeting", "project-alpha"]
  },
  "attachments": ["blob:sha256:a1b2c3d4..."],
  "published": "2026-05-01T14:23:00+09:00",
  "recordedAt": "2026-05-01T14:23:05.456+09:00",
  "consent": "consent:suzuki-2026-main",
  "idempotencyKey": "suzuki-photo-20260501-1423",
  "meta": {}
}

// Example 3: External API weather data
{
  "id": "019577c3-...",
  "schema": "schema:weather-reading",
  "schemaVersion": "1.0.0",
  "observer": "obs:weather-crawler",
  "sourceSystem": "sys:weather-api",
  "authorityModel": "dual",
  "captureModel": "snapshot",
  "subject": "space:campus",
  "payload": { "celsius": 22.5, "humidity_pct": 65, "condition": "cloudy" },
  "attachments": [],
  "published": "2026-05-01T12:00:00+09:00",
  "recordedAt": "2026-05-01T12:00:02.789+09:00",
  "idempotencyKey": "weather-campus-20260501-1200",
  "meta": { "api_provider": "openweathermap" }
}
```

### 4.4 Layered Storage Architecture

| Data Type | Storage | Format | Retention |
|---|---|---|---|
| Canonical Observations | Event Store (append-only log) | JSON / Avro | **永続** |
| Binary Attachments | Object Storage (MinIO / S3互換) | Original format, CAS hash | ポリシーベース |
| Supplemental Derivation Store | KV / Document DB / Parquet / Vector Store | JSON / Parquet / embedding formats | ポリシーベース |
| High-Frequency Raw Store | Object Storage / TSDB / Data Lakehouse | Parquet / Zarr / Arrow / native TSDB blocks | ポリシーベース |
| Cold Archive | Partitioned files | Parquet (per schema, per month) | **永続** |

**Content-Addressable Storage (CAS):** バイナリデータは SHA-256 ハッシュをキーとして保存される。同一ファイルは自動的に重複排除される。参照形式: `blob:sha256:<hash>`

**Supplemental Derivation Store:** transcript、OCR、face detection、embedding、confidence、名寄せ候補など、再利用価値は高いが canonical source ではない補助情報を保存する領域。原則として canonical capture とは分離し、必要に応じて append-only または managed cache として運用する。

Lake / supplemental / source-native のより厳密な意味境界は [domain_algebra.md](domain-algebra.md) に分離して定義する。

高頻度な生時系列は、必ずしも 1 サンプル = 1 Observation として Lake に格納する必要はない。Lake の役割は canonical な発見性・参照性・再現性の確保であり、大容量の連続系列本体は外部 raw store に置き、その存在と意味を Observation として登録してよい。実運用上 latest を返す必要がある場合は operational-latest で source-native / live endpoint を読む。

### 4.5 Ingestion Pipeline

```
Source System / Observer
  │
  ▼
Ingestion Gate ──────────────────────────────────────
  │
  ├─ 1. Authenticate observer    (Observer Registry)
  ├─ 2. Resolve source contract  (authority / capture policy)
  ├─ 3. Validate payload         (Schema Registry: JSON Schema check)
  ├─ 4. Check consent status     (Consent Ledger / restricted capture policy)
  ├─ 5. Deduplicate              (idempotencyKey で重複検知)
  ├─ 6. Store blobs → get BlobRefs  (Object Storage)
  ├─ 7. Assign recordedAt        (system timestamp)
  ├─ 8. Assign id                (UUID v7)
  │
  ▼
Canonical Capture Layer (append)
```

Ingestion Gate は **一次資料の capture にのみ責務を持つ**。解釈、補助的推論、名寄せ、confidence 付与、顔認識、transcript 生成などは ingestion の責務ではなく、Supplemental Derivation または Projection の責務とする。

### 4.5.1 Mutable External Artifact and Workspace Ingestion

Google Slides、Google Docs、Google Sheets、Google Drive、Google Forms、Google Calendar、Notion、Figma、Canva のような revision を持つ SaaS source は、イベント差分ではなく **revisioned snapshot source** として扱う。正史は source-native system 側にあり、crawler / connector が observer として revision snapshot を Lake に保存する。

取り込み時の原則:

1. **Native Snapshot を取得**
   Source API が返す構造情報をそのまま取得する。
   例: Google Slides の page/pageElement/objectId/text/speaker notes/layout/master 情報。

2. **Rendered Snapshot を取得**
   人間が見ている見た目を保存するため、同一 revision の PDF、PNG、PPTX 等を取得する。

3. **Hybrid Observation として append**
   1 と 2 を同一 source revision に紐づく 1 つの Observation として Lake に保存する。

4. **Semantic Enrichment は行わない**
   OCR、captioning、embedding、要約、分類などの意味付けは Ingestion では実施しない。これらは downstream Projection として扱う。

この原則により、Lake には「解釈前の一次資料」のみが入り、後段の multimodal/LLM 処理を何度でも再実行できる。transcript や OCR などの補助結果を保存したい場合は、Lake ではなく Supplemental Derivation Store に保存する。Operational 利用は source-native latest を返してよいが、academic 利用では pin された revision snapshot を用いる。

```jsonc
{
  "schema": "schema:document-snapshot",
  "observer": "obs:gslides-crawler",
  "sourceSystem": "sys:google-slides",
  "authorityModel": "source",
  "captureModel": "snapshot",
  "subject": "document:gslide:deck-abc123",
  "payload": {
    "provider": "google-slides",
    "document_type": "slide-deck",
    "source_document_id": "deck-abc123",
    "source_revision_id": "rev-017",
    "source_modified_at": "2026-03-07T09:10:00+09:00",
    "capture_mode": "hybrid",
    "native_structure_ref": "blob:sha256:native-json...",
    "render_refs": [
      "blob:sha256:deck-pdf...",
      "blob:sha256:slide-1-png...",
      "blob:sha256:slide-2-png..."
    ]
  },
  "attachments": [
    "blob:sha256:native-json...",
    "blob:sha256:deck-pdf...",
    "blob:sha256:slide-1-png...",
    "blob:sha256:slide-2-png..."
  ],
  "published": "2026-03-07T09:10:00+09:00",
  "idempotencyKey": "gslides:deck-abc123:rev-017"
}
```

#### 4.5.1.1 Representative SaaS Sources

| Service | Canonical subject | Canonical capture | Attachments | Typical supplemental |
|---|---|---|---|---|
| Google Docs | `document:gdoc:<id>` | block / paragraph / suggestion tree、comment anchor、revision metadata | PDF / DOCX export | summary、embedding、suggestion classification |
| Google Sheets | `document:gsheet:<id>` | sheet / cell / formula / named range / validation rule、revision metadata | XLSX / CSV export | table normalization、anomaly tag |
| Google Slides | `document:gslide:<id>` | page / element / speaker note / layout / master、revision metadata | PDF / PPTX / PNG export | OCR、caption、embedding |
| Google Drive | `file:gdrive:<id>` | file metadata、folder hierarchy、permission、primary blob pointer | original file / thumbnail | content classification |
| Google Forms | `form:gform:<id>` | form structure、question id、branching、response schema | PDF export | response summary |
| Google Photos / Album | `asset:gphotos:<id>` / `album:gphotos:<id>` | asset metadata、album membership、sharing state | original media / thumbnail | face cluster、caption、embedding |
| Google Calendar | `calendar-event:gcal:<id>` | event body、recurrence、attendee、conference data、calendar scope | ICS / PDF export | availability inference、meeting summary |
| Notion | `page:notion:<id>` / `database:notion:<id>` | block tree、property、relation、comment、revision metadata | Markdown / PDF export | embedding、semantic label |
| Figma | `design:figma:<id>` | node tree、component id、comment anchor、style token、revision metadata | PNG / PDF / SVG export | OCR、semantic label |
| Canva | `design:canva:<id>` | page tree、element graph、comment、brand / template ref、revision metadata | PDF / PNG / PPTX export | OCR、semantic label |

これらはすべて同じ Observation envelope を使い、service ごとの差分は schema と `payload.native` に閉じ込める。canonical capture は source-native object graph と revision metadata、必要なら render/export を保持し、名寄せ・要約・embedding・顔認識・meeting summary などの解釈結果は Supplemental Derivation Store に置く。

#### 4.5.1.2 Recommended Observation Structure

revisioned SaaS source では、Observation の `payload` を少なくとも次のまとまりに分けることを推奨する。

- `artifact`: provider / service / object type / source-local identifier / container
- `revision`: source revision id / modified timestamp / capture mode
- `native`: source API が返す構造を lossless に保持する本体
- `relations`: parent-child、attendee、backlink、space など source 由来の関係
- `rights`: visibility、sharing、owner、space policy などの制約
- `attachment_roles`: `attachments` に入れた原本・render・export の意味付け

```jsonc
{
  "schema": "schema:workspace-object-snapshot",
  "observer": "obs:gcalendar-crawler",
  "sourceSystem": "sys:google-calendar",
  "authorityModel": "source",
  "captureModel": "snapshot",
  "subject": "calendar-event:gcal:event-abc123",
  "payload": {
    "artifact": {
      "provider": "google",
      "service": "calendar",
      "object_type": "event",
      "source_object_id": "event-abc123",
      "container_id": "calendar:residents-2026",
      "canonical_uri": "https://calendar.google.com/calendar/event?eid=event-abc123"
    },
    "revision": {
      "source_revision_id": "etag:347829",
      "source_modified_at": "2026-03-07T09:10:00+09:00",
      "capture_mode": "snapshot"
    },
    "native": {
      "encoding": "inline-json",
      "content": {
        "summary": "Resident interview",
        "start": { "dateTime": "2026-03-21T13:00:00+09:00" },
        "end": { "dateTime": "2026-03-21T13:30:00+09:00" },
        "attendees": [
          { "email": "resident@example.jp", "responseStatus": "accepted" }
        ],
        "recurrence": [],
        "conferenceData": { "type": "google-meet" }
      }
    },
    "relations": {
      "participants": ["person:resident-2026"],
      "related_spaces": ["space:meeting-room-a"]
    },
    "rights": {
      "visibility": "restricted",
      "sharing": "calendar-members"
    },
    "attachment_roles": {
      "rendered_exports": ["blob:sha256:event-ics..."]
    }
  },
  "attachments": ["blob:sha256:event-ics..."],
  "published": "2026-03-07T09:10:00+09:00",
  "idempotencyKey": "gcal:event-abc123:etag-347829",
  "meta": {
    "capture_scope": "calendar:residents-2026",
    "crawler_cursor": "syncToken:abc",
    "canonical_hash": "sha256:9f3d..."
  }
}
```

大きな native payload では `payload.native` を blob ref 化してよい。`attachments` は原本・render・export の superset を保持し、`payload.attachment_roles` でそれぞれの役割を明示する。

  ### 4.5.2 Sensor and High-Frequency Source Ingestion

  センサーや高頻度更新ソースは、固定の Hz 閾値で一律に分岐するのではなく、**source contract に従って capture する**。同じ source でも academic-pinned と operational-latest で読む経路が異なってよい。

  基本方針:

  1. **Lake は再構成に必要な canonical capture を必ず保持する**
    最低限、どの source が、どの期間・どの revision・どの raw chunk を生成したかは Lake から辿れる必要がある。

  2. **高頻度 raw 本体は raw store に置いてよい**
    連続系列本体は High-Frequency Raw Store に置き、Lake には manifest Observation を置く。

  3. **Projection は read mode に応じて読取経路を切り替えてよい**
    `academic-pinned` では Lake または pin された raw manifest を読む。`operational-latest` では source-native endpoint や raw store の latest view を読んでよい。`application-cached` では Projection cache や Lake snapshot を主に用いる。

  4. **Current Value は canonical data にしない**
    "最新温度" "現在の在室状態" のような見かけ上 mutable な値は、Lake 内の正史ではなく Projection / API が導出する。

  5. **Late Arrival / Duplicate / Re-send を前提にする**
    センサーは遅延送信、再送、順不同到着を起こしうるため、`published` を event time、`recordedAt` を ingestion time として分離し、`idempotencyKey` で重複排除する。

  6. **補正は再書き込みではなく correction Observation で表現する**
    校正ミス、単位変換ミス、異常値除外、欠損補完は元 Observation を直接更新せず、参照付きの correction / retraction Observation で表現する。

  7. **capture policy は schema / source ごとに registry で定義する**
    どの source が event 単位、interval 単位、snapshot 単位、chunk manifest 単位で capture されるかは source contract として管理する。

  この規則により、Lake は常に再計算可能な一次データの索引を保持し、運用上必要な live read や最新値 API は Projection 層が read mode に応じて担う。

  ```jsonc
  {
    "schema": "schema:raw-timeseries-chunk",
    "observer": "obs:env-sensor-gateway-a301",
    "sourceSystem": "sys:env-sensor-a301",
    "authorityModel": "dual",
    "captureModel": "chunk",
    "subject": "space:room:A-301",
    "payload": {
     "series_type": "environment",
     "sensor_id": "env-a301",
     "time_range": {
      "start": "2026-03-07T09:00:00+09:00",
      "end": "2026-03-07T09:00:59.999+09:00"
     },
     "sample_count": 600,
     "sampling_hz": 10,
     "encoding": "parquet",
     "raw_ref": "blob:sha256:env-a301-20260307T0900-parquet...",
     "channels": ["celsius", "humidity_pct"]
    },
    "attachments": ["blob:sha256:env-a301-20260307T0900-parquet..."],
    "published": "2026-03-07T09:00:59.999+09:00",
    "recordedAt": "2026-03-07T09:00:01.245+09:00",
    "idempotencyKey": "env-a301-20260307T0900-chunk-0001",
    "meta": {
     "sequence_start": 482391,
     "sequence_end": 482990,
     "firmware": "v1.8.2"
    }
  }
  ```

  最新状態が必要な場合は、例えば次のような Projection を構築する。

  - room-latest-environment: 各部屋について最新 reading を 1 行に射影
  - room-environment-rollup-1m: 1 分窓で平均・最小・最大を集計
  - occupancy-intervals: 入退室イベント列から在室区間を再構成

  分析時には、Projection engine が manifest Observation を辿って raw chunk を読み込めればよい。これにより、Lake は軽量な control plane、外部 raw store は重量級 data plane として役割分担できる。

### 4.6 Immutability & Corrections

Lake は **Append-Only** であり、過去の Observation を変更・削除しない。訂正は新しい Observation で表現する。

| Intent | Mechanism |
|---|---|
| 新規データの記録 | 新しい Observation |
| 過去の誤りを訂正 | 新しい Observation + `meta.corrects: "<original-id>"` |
| データの撤回 | 新しい Observation + `meta.retracts: "<original-id>"` |
| オプトアウト | 年度末確認で Consent Ledger を更新 → filtering / restricted Projection で反映 |

### 4.7 Event Ordering (Determinism)

Observation の適用順序（Projection が再生する際の順序）：

1. `published` (Event Time — 事象の発生時刻)
2. `recordedAt` (System Time — 記録時刻)
3. `id` (UUID v7 — 時刻ソート可能な ID のバイト順)

### 4.8 Time Zone Normalization Policy

- `published`: 元の offset 付き ISO 8601 をそのまま保存する（例: `2026-05-01T08:30:00+09:00`）
- `recordedAt`: システムが UTC で付与する
- Projection が時間計算する際は UTC に正規化して比較する
- 表示時はユーザーのローカルタイムに変換する
- schema で timezone-naive な timestamp を使うことは非推奨とし、schema validation で offset の存在を検証する

---

## 5. Projection Ecosystem

### 5.1 What is a Projection?

Projection は、Observation Lake、Supplemental Derivation Store、source-native API、または他の Projection をソースとして **宣言済み read mode** のもとで変換ロジックを適用し構築された **派生データベース / サービス / API**。

- **Owned:** 個人またはチームが作成・管理する
- **Registered:** Projection Catalog に登録され、誰でも発見できる
- **Reproducible:** `academic-pinned` ではソース宣言 + 変換ロジックから再構築可能
- **Disposable:** いつでも破棄・再構築できる（Ground Truth は Lake にある）
- **Composable:** 他の Projection のソースになれる（P3: DB on DBs）
- **API-First:** 可能な限り stable API / export 契約を持つ
- **Isolated:** user / agent は sandbox から構築し、直接 Lake を編集しない

### 5.1.1 Authority and Read Path

Projection は source の authority model を尊重しなければならない。

- `lake-authoritative` source は Lake を正史とする
- `source-authoritative` source は source-native system を正史とし、Lake は revisioned capture を保持する
- `dual-reference` source は source-native と Lake snapshot の両方を持ち、用途により読取経路を選ぶ

したがって、「Projection は常に Lake だけを読む」わけでも「常に source-native を読む」わけでもない。読取経路は read mode と authority model の組み合わせで決まる。

### 5.2 Source Declaration & DAG

```
                   Lake / Source-native / Supplemental
                    /       |            \
                    /        |             \
                  Proj:A      Proj:B        Proj:C    ← depth 1
            |         |
            +----+----+
                 |
              Proj:D                    ← depth 2 (A + B を合成: DB on DBs)
                 |
              Proj:E                    ← depth 3 (D をさらに加工)
```

- **depth 1:** Lake / supplemental / source-native のいずれかのみをソースとする Projection
- **depth N:** 少なくとも1つの depth N-1 の Projection をソースに含む
- **非巡回性の強制:** 循環依存は Catalog 登録時にシステムが拒否する

### 5.2.1 DAG Change Propagation Model

上流 Observation が追加・更新された場合の下流 Projection への伝播は、以下のモデルに従う。

#### 伝播戦略

**第一優先: Incremental Propagation（差分伝播）**

更新された record のみを下流 Projection に伝播する。全データの rebuild を避け、データ増加時のスケーラビリティを確保する。

- Projection は前回 build 時点の watermark（`lastProcessedRecordedAt` / `lastProcessedId`）を保持する
- 新規 Observation は watermark 以降のレコードのみを対象に incremental apply する
- incremental apply が不可能な Projection（集計全体が変わるもの等）は設計段階で rebuild コストとレスポンスを考慮する

**第二優先: Scheduled Rebuild**

cron 等で定期的に full rebuild を実行する。batch workload 向き。incremental apply と併用することで、drift 補正としても機能する。

**非推奨: Lazy Invalidate**

upstream 更新時に stale フラグだけ付け、次回アクセス時に rebuild する方式は、レスポンスの観点から原則として採用しない。ただし、アクセス頻度が極めて低い Projection に限り、コスト抑制目的で許可する。

#### 通知チャネル（段階的実装）

| Phase | Mechanism | Rationale |
|---|---|---|
| MVP | **Poll-based** — 各 Projection が定期的にソースの最新 watermark を確認 | 実装最小。追加インフラ不要 |
| Growth | **Event-driven** — Lake append 時に notification event を発行し、依存 Projection が subscribe | freshness 改善。必要な Projection だけ処理 |
| Scale | **CDC + DAG scheduler** — change data capture と DAG-aware scheduler で topological order 実行 | 大規模 DAG での効率的 propagation |

#### Upstream Breaking Change への対応

- Upstream が **archive** → downstream は最終 snapshot をそのまま保持（degraded read）
- Upstream が **major version bump** → downstream は旧 version pin で動作継続。新 version 対応は明示的 migration
- Projection Catalog に **health status** を表示する（healthy / stale / degraded / broken）

#### 設計時の考慮事項

全データを対象とする集計・統計型 Projection は、incremental apply が困難になりやすい。そのような Projection は設計段階で以下を明示しなければならない。

- 想定データ量と rebuild 所要時間の見積もり
- incremental apply の可否と方式
- scheduled rebuild の頻度
- レスポンス要件（リアルタイム / near-realtime / batch）

### 5.3 Projection Definition File

各 Projection は宣言的な Spec ファイルで定義される。

```yaml
apiVersion: "lethe/v1"
kind: "Projection"

metadata:
  id: "proj:dining-patterns-2026"
  name: "Dining Pattern Analysis"
  created_by: "suzuki"
  version: "1.0.0"
  tags: ["dining", "behavioral", "timeseries"]
  description: |
    食堂利用パターンの分析。入退出観測を集計し、
    時間帯別利用率と個人別利用頻度プロファイルを生成。

spec:
  # ── ソース宣言 ──
  sources:
    - ref: "lake"
      filter:
        schemas: ["schema:dining-entry", "schema:dining-exit"]
        subject_types: ["et:person"]
        window: { start: "2026-04-01", end: "2026-09-30" }
    - ref: "supplemental"
      filter:
        derivations: ["asr-transcript", "person-resolution-candidate"]
    - ref: "proj:person-directory-2026"   # ← DB on DBs: 別の Projection を利用
      version: ">=1.0.0"

  # ── 構築設定 ──
  engine: "duckdb"
  build:
    type: "sql-migration"
    entrypoint: "./projections/dining/migrate.sql"
    projector: "./projections/dining/projector.py"

  # ── 出力定義 ──
  outputs:
    - format: "sql"
      tables: ["hourly_occupancy", "person_frequency", "peak_analysis"]
    - format: "parquet"
      schedule: "weekly"
      location: "/exports/dining-patterns/"

  interface:
    primaryAccess:
      type: "http"
      path: "/api/projections/dining-patterns-2026"
    readModes:
      - name: "academic-pinned"
        sourcePolicy: "lake-only-or-pinned-manifest"
      - name: "operational-latest"
        sourcePolicy: "source-native-preferred"
      - name: "application-cached"
        sourcePolicy: "projection-cache-preferred"
    compatibility:
      downstreamVersioning: "major-pinned"

  # ── 再現性 ──
  reproducibility:
    deterministicIn: ["academic-pinned"]
    seed: 42
    rebuild_command: "make rebuild-dining"
```

### 5.4 Projection Lifecycle

```
1. Define    → Projection Spec を記述（YAML + 変換コード + read mode 契約）
2. Register  → Projection Catalog に登録（DAG の非巡回性チェック）
3. Build     → 変換を実行、対象 DB を構築
4. Serve     → クエリ可能な状態。他の Projection のソースにもなれる
5. Version   → Spec 更新 → バージョンアップ → 再ビルド
6. Archive   → 非活性化、最終スナップショットをエクスポート、DOI を凍結
```

### 5.5 Typical Projection Patterns

| Pattern | Example | Engine |
|---|---|---|
| **Social Network** | 同時滞在グラフ（入退室データから） | Neo4j / NetworkX |
| **Time Series** | センサーデータ集計、利用率推移 | TimescaleDB / DuckDB |
| **Behavioral Embedding** | 行動パターンのベクトル表現、クラスタリング | pgvector / FAISS |
| **Text Corpus** | ノート、コメント、アンケート回答の全文検索 | Elasticsearch / SQLite FTS |
| **Multimedia Index** | 画像メタデータ + 視覚的埋め込みベクトル | pgvector + MinIO |
| **Operational View** | 現在の部屋割り、設備稼働状況 | PostgreSQL |
| **Composite Analysis** | ソーシャル × 食生活 × 学業のクロス分析 | DuckDB / Jupyter + Parquet |

### 5.6 Access Patterns

Projection を利用する際の手段：

| Pattern | Description | Use Case |
|---|---|---|
| **Projection API** | HTTP / RPC / GraphQL 等の stable interface を使う | GUI、アプリケーション、agent 利用 |
| **Native Query** | Manifest の `connection` で直接クエリ（SQL, Cypher 等） | リアルタイム分析 |
| **Bulk Export** | Parquet / CSV スナップショットをダウンロード | 大規模バッチ処理 |
| **Source Reference** | 自分の Projection Spec の `sources` に宣言 | DB on DBs 合成 |
| **Sandbox Session** | 分離された playground で下書き・試作する | 非専門ユーザー、agent-assisted authoring |

Projection API は read mode を受け取れることが望ましい。少なくとも `academic-pinned` と `operational-latest` の違いを契約上区別できるようにする。

### 5.7 API-First Projection Contract

Projection は可能な限り API としても公開される。これにより、Lake と user-facing 利用面を分離しつつ、他の Projection や GUI から透過的に利用できる。

最低限持つべき契約:

- access method（HTTP / SQL / export / hybrid）
- supported read modes（`academic-pinned`, `operational-latest`, `application-cached`）
- source policy（各 read mode が Lake / source-native / cache のどれを優先するか）
- compatibility policy（他 Projection からの参照は major version pin を基本とする）
- snapshot / revision identifier
- access scope（public / internal / restricted）

### 5.8 Sandbox and Agent-Assisted Authoring

Projection の作成・試作・公開前検証は、Lake から分離された **Sandbox / Playground** で行う。coding agent はこの sandbox を主戦場とし、直接 Lake を編集しない。

Sandbox の役割:

- Projection Spec の雛形生成
- SQL / projector / write adapter の試作
- dry-run build
- publish 前レビュー
- 非技術ユーザー向け GUI からの構築支援

agent capability と review 境界の詳細は [governance_capability_model.md](governance-capability-model.md) を参照。

---

## 6. Multimodal Support

### 6.1 Architecture

```
Observation Event (structured, in Lake)
    │
    ├── payload: { "description": "Meeting whiteboard", "labels": ["project-x"] }
    │
    └── attachments:
         ├── blob:sha256:a1b2c3... → image/jpeg  (3.2MB, Object Storage)
         └── blob:sha256:d4e5f6... → audio/wav   (12MB,  Object Storage)
```

Observation 自体は常に構造化メタデータであり、バイナリデータは Object Storage に格納されて BlobRef で参照される。

### 6.2 Blob Metadata

```json
{
  "hash": "sha256:a1b2c3d4e5f6...",
  "mime_type": "image/jpeg",
  "size_bytes": 3355443,
  "original_filename": "whiteboard_2026-05-01.jpg",
  "uploaded_at": "2026-05-01T14:23:00Z",
  "uploaded_by": "src:manual-suzuki",
  "dimensions": { "width": 4032, "height": 3024 }
}
```

### 6.3 Supplemental Derivation Layer

マルチモーダルデータの加工結果は、常に Projection だけに閉じ込める必要はない。再利用価値が高く、再計算コストが高い補助結果は **Supplemental Derivation Store** に保存してよい。

ここに保存するものの例:

- transcript / OCR text
- face / object detection result
- embedding / vector
- confidence / verification metadata
- 名寄せ候補
- chunk summary / keyframe index

Supplemental Derivation は canonical data ではない。したがって、以下を最低限記録する。

- `derived_from`（元 Observation / BlobRef / source revision）
- derivation method / model version
- created_at
- mutability policy（append-only か managed cache か）

### 6.4 Downstream Processing

マルチモーダルデータの加工は Projection として実装する。必要に応じて、その中間成果物を Supplemental Derivation Store に保存する。

| Projection Type | Processing | Output |
|---|---|---|
| Image Projection | OCR / CLIP で埋め込みベクトル生成 | Text + Vector DB |
| Audio Projection | 音声認識（Whisper 等）で文字起こし | Text corpus |
| Video Projection | キーフレーム抽出、活動検出 | Metadata + Image refs |
| Sensor Fusion | 複数センサーデータの統合・補間 | Time series |

### 6.5 Mutable Multimodal Documents

Google Slides のような mutable かつ multimodal な source は、次の 2 層を canonical data として扱う。

| Layer | Contains | Role |
|---|---|---|
| **Native Canonical** | Source API の構造情報（object graph, text, notes, layout, ids） | 逆変換・差分検出・構造検索の基盤 |
| **Render Canonical** | PNG/PDF/PPTX などの視覚スナップショット | 人間視点の保存、multimodal LLM 入力 |

この 2 層は同じ source revision に属する一次資料であり、どちらも Lake に保存してよい。一方、以下は canonical data に含めず Projection として扱う。

- OCR 結果
- Image caption
- Embedding / vector
- LLM 要約
- LLM によるレイアウト解釈や意味ラベル

したがって、multimodal LLM は canonical render snapshot を読んでよいが、その出力は canonical source ではなく派生データである。transcript、OCR、caption のような高コスト補助情報は Supplemental Derivation Store に保存し、user-facing Projection がそこから再利用してよい。

Google Workspace / Photos / Calendar、Notion、Figma、Canva、Slack 等も同じ原則で扱う。mutable source からは crawler / connector が revisioned snapshot を canonical capture として取り込み、補助解釈は別層に置く。

---

## 7. Governance & Ethics

本節では governance の基本方針を概説する。**詳細な policy 定義、capability model、review matrix、retention / takedown ladder、secret handling** については [governance_capability_model.md](governance-capability-model.md) を参照のこと。

### 7.1 基本方針

1. **Capture before interpretation** — 一次資料を保ち、解釈は serving 前に行う
2. **Filtering before exposure** — restricted data は表示・共有・export の前で制御する
3. **Explicit authority** — write の行き先を隠さない
4. **Least privilege** — 人間・agent・service に capability-scoped な権限を与える
5. **Auditable decisions** — deny / approve / export / delete / publish は理由付きで追跡する

### 7.2 Consent と Opt-Out

- 既定運用は **restricted canonical capture + 年度末 opt-out 確認**
- 年度中は restricted capture を蓄積し、名寄せ・face / speaker resolution を filtering 精度向上のための内部補助として使う
- 公開・閲覧・実験適用は filtering Projection と年度末確認後の承認を経る
- Opt-out 戦略は `drop` / `anonymize` / `pseudonymize` の 3 種
- Consent 撤回時の Supplemental への影響は filtering projection で対処する（supplemental record は lineage 維持のため保持し、exposure 前の filtering で除外する）

### 7.3 Access Control（概要）

| Role | Lake Write | Lake Read | Projection Access | Registry |
|---|---|---|---|---|
| **System Admin** | via registered observer | Full | All | Full CRUD |
| **Researcher** | via registered observer | Restricted / filtered | Own + shared | Read + Register |
| **Resident** | via approved observer | Own data only | Approved only | Read |
| **External** | ✗ | ✗ | Published exports only | Read Catalog |

### 7.4 Data Retention（概要）

| Data | Default Retention | Override |
|---|---|---|
| Lake Observations | 永続 | IRB 条件に従い短縮可能 |
| Binary Attachments | 5年 | Projection が参照中は延長 |
| Projection Snapshots | Projection の Archive まで | DOI 付与済みは永続保存 |
| Consent Records | 永続 | 法的要件に従う |

---

## 8. Schema Evolution

### 8.1 Versioning Rules

| Change Type | Allowed? | Action |
|---|---|---|
| Optional フィールド追加 | ✓ | Minor version bump (e.g. 1.0 → 1.1) |
| Required フィールド追加 | 新バージョン | Major version bump (e.g. 1.x → 2.0) |
| フィールド削除 | 新バージョン | Major version bump |
| 型変更 | 新バージョン | Major version bump |

過去の Observation は書き込み時の schemaVersion を永久に保持する。

### 8.2 Projector Compatibility

Projection は対応する Schema バージョン範囲を宣言する。

```yaml
sources:
  - ref: "lake"
    filter:
      schemas:
        - name: "schema:room-entry"
          versions: ">=1.0.0, <3.0.0"   # v1.x と v2.x に対応
```

破壊的変更（Major bump）後も、既存 Projection は旧データで動作し続ける。新形式のデータを取り込むには Projection 側の更新が必要。

### 8.3 Source Contract Evolution

本システムでは、Schema Evolution だけでなく **Source Contract Evolution** を明示的に扱う。現実には、新しい EntityType が増える場面よりも、既存 source が返す payload や API shape が拡張・変更される場面の方が多い。

原則:

1. **Source adapter は versioned contract を持つ**
  crawler / connector は source API version と変換ルールを明示する。

2. **加法的変更は minor として吸収する**
  新規 optional field の追加は既存 schema の minor 更新または supplemental field として吸収する。

3. **破壊的変更は新 contract / 新 schema として扱う**
  payload shape や意味が変わる場合は、新 schemaVersion または新 source adapter version を発行する。

4. **過去 snapshot はその時点の contract で永続保存する**
  source-native API が後に変化しても、過去に capture された revision の解釈は変えない。

---

## 9. Academic Integrity

### 9.1 Reproducibility

再現性は **academic-pinned read mode** に対して保証する。すなわち、同一の pin された source、同一 Projection Spec、同一 seed から、同一の結果が再構築できることを意味する。

- Projection Spec（YAML + 変換コード）は Git 管理
- pin された Lake window、source revision、raw manifest は Spec 内に明記
- 非決定的要素（ランダムシード、外部 API 呼び出し等）は Spec に記録

`operational-latest` は鮮度を優先するため、同一結果の再構築を保証しない。ここでは freshness が主要要件であり、再現性は Lake 側に残された capture によって後から academic build として取り直せることを要件とする。

### 9.2 Citability

- 全 Projection に **DOI** (Digital Object Identifier) を付与可能
- Projection Catalog が学術的 provenance record を兼ねる
- 引用例: `Yamamoto, S. (2026). Social Co-presence Graph, Spring 2026. LETHE. doi:10.xxxxx/lethe-sg-2026s`

### 9.3 Lineage Tracking

任意の Projection の出力レコードから、academic-pinned では原始 Observation または pin された raw manifest まで辿れるようにする。source-authoritative source では、必要に応じて source revision まで辿れるようにする。

```
Proj:E の結果行 → Proj:D の結果行 → Proj:A/B の結果行 → Lake の Observation → Source System
```

これは Projection Spec の `sources` 宣言、Lake の `id` チェーン、必要に応じて source revision anchor により実現される。

Lineage の粒度は少なくとも次の 3 段階を持てる。

- **Projection-level lineage:** どの Lake window、どの Projection version、どの supplemental derivation を使ったか
- **Row-level lineage:** ある出力行が、どの Observation / source revision / anchor に由来するか
- **Blob-anchor lineage:** 画像の bounding box、動画の time range、文書の object id など、blob 内部の根拠位置まで辿る

例:

- ある dining-analysis 行が、`schema:dining-entry` の Observation 群と `proj:person-directory-2026` v1.2 から導出された
- ある slide annotation が、`document:gslide:deck-abc123` rev-017 の `slide-2 / objectId:title-9` を根拠にしている
- ある動画要約が、`blob:sha256:...` の `00:10:32 - 00:11:08` に由来している

---

## 10. Practical Workflows

### 10.1 「Xについて研究したい」（新しい研究テーマの立ち上げ）

```
1. Registry を確認:
   → X に対応する EntityType は存在するか？
     → Yes: ステップ 3 へ
     → No:  新しい EntityType を登録

2. Registry を確認:
   → X を観測するための Schema は存在するか？
     → Yes: ステップ 3 へ
     → No:  新しい Schema を定義・登録

3. データ収集を開始:
  → source system と observer を登録（センサー / Bot / crawler / 手動フォーム / API）
   → Observation を Lake に投入開始

4. Projection を構築:
   → Projection Spec を記述
   → 変換ロジックを実装
   → Projection Catalog に登録
   → Build & Serve

5. (Optional) 既存 Projection と合成:
   → Catalog で関連 Projection を発見
   → DB on DBs で合成 Projection を構築
```

### 10.2 「誰かのデータを使いたい」

```
1. Projection Catalog を閲覧
2. 対象 Projection の description / schema docs / DOI を確認
3. アクセス手段を選択:
   a. Native Query（エンジンに直接接続）
   b. Bulk Export（Parquet / CSV スナップショット）
   c. 自分の Projection の source として宣言（DB on DBs）
4. 論文等で引用する場合は DOI を使用
```

### 10.3 年度を跨いだ継続運用

```
Year N:
  - Lake は継続的に成長
  - 新しい Schema / EntityType / observer / source contract が自由に追加される
  - Projection が作られ、研究に利用される

Year N → N+1 引き継ぎ:
  - 前年チームが自身の Projection を Archive（最終スナップショット + DOI 凍結）
  - 次年チームが引き継ぐもの:
      ・ Lake（過去の全 Observation）
      ・ Registry（全ての型 / スキーマ / ソース定義）
      ・ Projection Catalog（過去の全 Projection、active / archived）
  - 次年チームができること:
      ・ 過去の任意の Projection をソースから再構築
      ・ 過去データ＋新データで新しい Projection を構築
      ・ Schema / EntityType を自由に拡張
```

---

## 11. Reference Implementation Recommendations

本章は **reference implementation** の提案であり、意味論そのものを固定する章ではない。  
守るべき semantic contract は Sections 0-10, 12-13 と、[domain_algebra.md](domain-algebra.md) / [governance_capability_model.md](governance-capability-model.md) に置く。
**技術スタックの詳細、runtime topology、sandbox 構成、operational controls** は [runtime_reference_architecture.md](runtime-reference.md) を参照。

> **Implementation note:** この章の技術マッピングは reference であり、現行リポジトリの実装言語やライブラリ構成を拘束しない。現在の実装は Rust crate 上で MVP の semantic kernel、adapter pipeline、projection pipeline を検証している。

### 11.1 Full Stack (推奨構成)

| Component | Technology | Rationale |
|---|---|---|
| Lake (Event Store) | Apache Kafka → Parquet on MinIO | ストリーミング取り込み + クエリ可能なコールドストレージ |
| Object Storage | MinIO (S3 互換) | セルフホスト、マルチモーダル Blob 保存 |
| Supplemental Derivation Store | PostgreSQL / Parquet / Vector DB | transcript、OCR、embedding、confidence の共有保存 |
| Schema Registry | Confluent Schema Registry | Avro/JSON Schema の検証・互換性管理 |
| Registry DB | PostgreSQL | 構造化メタデータストア |
| Projection Engines | 用途別に選定 | Neo4j / TimescaleDB / DuckDB / pgvector 等 |
| Projection Catalog / API Gateway | PostgreSQL + Web UI + HTTP API | 発見・ドキュメント・DAG 可視化・API 契約 |
| Sandbox Runner | Containers / isolated jobs | user / agent の試作環境を分離 |
| Authoring UI | Web UI + coding agent integration | ターミナル非前提の Projection 作成・公開 |
| Version Control | Git | Projection Spec / Schema / 変換コード |
| Auth | Keycloak / OAuth2 | observer 認証・ユーザー認可 |

### 11.2 Minimal Viable Stack（学生チーム向け最小構成）

```
┌──────────────────────────────────────────────────────────┐
│  Rust workspace         → semantic / effect boundaries   │
│  SQLite                 → observation + runtime state    │
│  Local content store    → sha256-addressed blobs         │
│  Axum                   → authenticated Projection API   │
│  OpenSpec + Git         → specification and review       │
│  Cargo test             → law / contract / replay checks │
└──────────────────────────────────────────────────────────┘
```

**単一マシンで全アーキテクチャを実装可能。** 規模拡大に応じてコンポーネントをスケールアウトする。

### 11.3 Migration Path

```
Phase 1 (current): Rust + SQLite + local CAS + Axum
Phase 2 (Growth):  PostgreSQL + object storage + container sandbox
Phase 3 (Scale):   Full Stack (Kafka + MinIO + Keycloak + API gateway + Neo4j + ...)
```

### 11.4 MVP End-to-End Scenario

最初に動かす end-to-end シナリオとして、**Google Slides + Slack のデータ取り込み → 名寄せ → 個人ページ作成** を定義する。

```
目標: 2 つの mutable source → canonical capture → 名寄せ → 個人ページ Projection
      を最短で動かす

Step 1: Registry 初期化
  - EntityType: et:person, et:document, et:message を登録
  - Schema: schema:workspace-object-snapshot, schema:slack-message を登録
  - Observer: obs:gslides-crawler, obs:slack-crawler を登録
  - Source contract: sys:google-slides (source-authoritative), sys:slack (lake-authoritative) を登録

Step 2: Google Slides 取り込み
  - gslides-crawler が対象 deck の revision snapshot を取得
  - native structure + render (PDF/PNG) を Observation として Lake に append
  - Supplemental: OCR text を生成・保存

Step 3: Slack 取り込み
  - slack-crawler が対象 channel のメッセージ履歴を取得
  - 各メッセージを Observation として Lake に append
  - Supplemental: 必要に応じて thread summary を生成

Step 4: 名寄せ (Identity Resolution)
  - proj:person-resolution を構築
    - source: Lake (slides の author/editor 情報 + Slack の user 情報)
    - Supplemental の OCR text から登場人物を抽出
    - Slack display name / email と Google account を突合
    - 名寄せ候補を Supplemental に保存
    - 解決済み identity graph を Projection として公開

Step 5: 個人ページ Projection
  - proj:person-page を構築（DB on DBs: proj:person-resolution を利用）
    - 各人物について:
      - 関連する Slides (author/editor/mentioned)
      - 関連する Slack messages (sender/mentioned)
      - activity timeline
    - API で /api/projections/person-page/{person_id} を公開

Step 6: 検証
  - Slides の新 revision が取り込まれ、個人ページに反映されることを確認
  - Slack の新メッセージが取り込まれ、個人ページに反映されることを確認
  - 名寄せが正しく機能していることを確認
  - lineage: 個人ページ → person-resolution → Lake Observation の追跡を確認

完了条件:
  - Google Slides と Slack のデータが Lake に格納されている
  - 名寄せ Projection が人物を正しく紐づけている
  - 個人ページ Projection が各人物の関連データを表示する
  - 新規データ追加後に incremental propagation で反映される
  - lineage query で元 Observation まで辿れる
```

#### MVP 後の拡張順序

| Phase | 追加要素 | 目的 |
|---|---|---|
| MVP+1 | Google Docs / Sheets crawler + 追加 schema | mutable SaaS source の拡充 |
| MVP+2 | Writable Projection + Write Adapter | write-back の検証 |
| MVP+3 | Filtering Projection + consent + 年度末 opt-out | governance の検証 |
| MVP+4 | Agent sandbox integration | coding agent の動作確認 |
| MVP+5 | Sensor data + high-frequency capture | IoT データの統合 |

---

## 12. Write Paths: Lake-Mediated and Source-Native

> **Implementation note:** Write-Back (M07) の共通 contract と Notion adapter は
> `crates/adapters/api`、`crates/adapters/notion`、`apps/selfhost` に実装済みである。
> 本章と §13 は、他 source adapter を含む一般 contract と target semantics を示す。

Projection 上の UI やアプリケーションからの変更は、**Projection に直接書き込まない**。ただし、書き込み先は 1 つではなく、次の 2 系統を持つ。

書き込み経路の選択は authority model に従う。`lake-authoritative` source は Lake-mediated、`source-authoritative` source は source-native を原則とする。`dual-reference` source は、正史の置き場に応じてどちらか一方を選ぶ。

### 12.1 Lake-Mediated Write-Back

寮内の正規事実を追加・修正・撤回する場合は、必ず新しい Observation として Lake に投入し、Projector 経由で反映させる。

```
UI で「部屋割り変更」操作
  │
  ▼
Command: Observation { schema: "schema:room-assignment", ... }
  │
  ▼
Ingestion Gate → Lake (append)
  │
  ▼
Projector (room-management) が検知 → Projection を更新
```

### 12.2 Source-Native Write-Back

Google Slides、Figma、Google Docs、Google Sheets、その他 mutable external source に対する編集は、**source-native API に透過的に適用する** ことを許可する。

```
UI で「Slide タイトル変更」操作
  │
  ▼
Projection Write Adapter
  │
  ▼
Source-native API request を生成
  │
  ▼
Google Slides / Figma / Docs / Sheets 側に反映
  │
  ▼
Crawler / Connector が新 revision を再取得
  │
  ▼
Lake に新 snapshot Observation を append
```

この方式では、変更の正史は source-native system 側にあり、Lake はその結果生じた新 revision を capture する。必要に応じて、write command 自体の監査ログを supplemental または governance log に残してよい。academic-pinned でこの変更を参照したい場合は、再 capture 後の pin された revision を使う。

### 12.3 Selection Rule

- 内部ドメイン事実の更新: Lake-mediated
- mutable external source の編集: Source-native
- lossless inversion ができない編集要求: proposal / annotation に降格

これにより、監査可能性・再現性を維持しながら、mutable source との自然な操作感も確保できる。

---

## 13. Functional Projection Model & Writable Views

本節は Projection の関数型解釈と write-back の概要を示す。**型定義、command algebra、write mode mapping、consistency law、concurrency protocol** の詳細は [domain_algebra.md](domain-algebra.md) §5–7 を参照のこと。

### 13.1 Functional Interpretation

> **Projection = 宣言された入力集合を read mode に従って読み込み、決定的な状態または出力を返す導出関数**

academic-pinned では、同一の pin された入力集合から同一の結果が再構築できなければならない。operational-latest では latest read を許可するため、決定性は要件ではない。

副作用（observer 認証、blob 保存、recordedAt 付与、UUID 採番、DB materialization）は Projection 本体ではなく shell に隔離する。

### 13.2 Projection Mutability Rules

| Layer | Allowed Write Path |
|---|---|
| **Canonical Capture Layer** | Observation append only |
| **Supplemental Derivation Store** | append または managed cache |
| **Projection Materialization** | 直接更新禁止 |
| **Source-Native System** | source-native API のみ |
| **UI View / Draft Workspace** | Command 発行のみ |

### 13.3 Write Modes

| Mode | Meaning | Primary persistence |
|---|---|---|
| **canonical** | 正規のドメイン事実を追加・修正・撤回 | Lake append or source-native revision |
| **annotation** | 派生結果への注釈・ラベル・レビュー付与 | Supplemental / annotation observation |
| **proposal** | 即時変換できない追加・修正案の保持 | Proposal observation / review queue |

### 13.4 Writable Projection の必須条件

1. 操作が Lake Observation 列 or source-native API request 列へ逆変換できること
2. 逆変換規則が Projection Spec に宣言されていること
3. Replay 後に同じ結果へ収束すること
4. provenance が保存されること

### 13.5 Concurrency Control

**楽観的ロック（Optimistic Concurrency Control）** を標準とする。`visibleRowHash` / `baseRevision` を用い、競合時は `ConflictFailure` を返してユーザーに再操作を促す。source-native write-back の自動 merge は annotation mode に限定し、canonical mode ではユーザー確認を必須とする。詳細は [domain_algebra.md](domain-algebra.md) §6.6 を参照。

### 13.6 Projection Spec Extension for Write-Back

Writable Projection は、既存の Projection Spec を次のように拡張して宣言できる。

```yaml
apiVersion: "lethe/v1"
kind: "Projection"

metadata:
  id: "proj:room-assignment-view"
  name: "Room Assignment View"
  version: "1.0.0"

spec:
  sources:
    - ref: "lake"
      filter:
        schemas: ["schema:room-assignment", "schema:room-assignment-change"]

  engine: "duckdb"
  build:
    type: "sql-migration"
    projector: "./projections/room_assignment/projector.py"

  reproducibility:
    deterministic: true

  writeBack:
    enabled: true
    mode: "canonical"
    acceptedCommands:
      - "assign-room"
      - "change-room-assignment"
      - "vacate-room"
    inverseMapping:
      adapter: "./projections/room_assignment/write_adapter.py"
    reviewPolicy:
      required: false
    lineage:
      captureProjectionContext: true
      captureVisibleRowHash: true
```

複合分析 Projection の例:

```yaml
writeBack:
  enabled: true
  mode: "annotation"
  acceptedCommands:
    - "tag-derived-record"
    - "attach-review-note"
  inverseMapping:
    adapter: "./projections/composite_analysis/write_adapter.py"
  reviewPolicy:
    required: false
```

逆に、正規事実へ戻せない Projection は次のように明示的に read-only とする。

```yaml
writeBack:
  enabled: false
  reason: "derived metrics cannot be losslessly inverted into canonical observations"
```

### 13.7 Command and Observation Requirements

Write Adapter が生成する Command / Observation / source-native request には、少なくとも以下を含めなければならない。

- `source`: どの UI / user / automation が操作したか
- `actor`: 操作主体
- `schema`: 生成対象の Schema
- `published`: 利用者が主張する事象時刻、または操作時刻
- `idempotencyKey`: 重複追加防止用キー
- `meta.projectionContext.projectionId`: どの Projection 上で操作したか
- `meta.projectionContext.visibleRowHash`: 利用者が見ていた行のハッシュ
- `meta.projectionContext.writeMode`: canonical / annotation / proposal

source-native write-back の場合は、さらに以下を保持する。

- `meta.sourceNative.targetSystem`
- `meta.sourceNative.targetObjectId`
- `meta.sourceNative.baseRevision`

必要に応じて以下も持てる。

- `meta.corrects`
- `meta.retracts`
- `meta.proposalId`
- `meta.reviewStatus`

### 13.8 Insert / Update / Delete Semantics

Projection 上の編集操作は、Lake-mediated の場合は Lake 上で、source-native の場合は source system 上で次の意味に正規化される。

| UI Operation | Normalized Semantics |
|---|---|
| Insert | 新しい Observation の追加、または source-native create |
| Update | correction Observation の追加、または source-native update |
| Delete | retraction / 終了 Observation の追加、または source-native delete / archive |

したがって、どの Projection でも「行を消す」「行を書き換える」という操作は、Ground Truth の破壊的変更を意味しない。source-native system で状態が変わる場合も、その結果は新 revision capture として再び観測可能でなければならない。

### 13.9 Consistency Laws

Writable Projection と Write Adapter が満たすべき law は [domain_algebra.md](domain-algebra.md) §7 に定義する。主要な law: Replay Law, No Direct Mutation Law, Put-Then-Get Law, Idempotency Law, Provenance Law。

### 13.10 Draft Workspace

Projection とは別に **Draft Workspace** を持ってよい。Draft は Ground Truth ではなく、Publish 時に Command / Observation 群へ変換される。

これにより、人間にとっては spreadsheet 的編集体験を維持しつつ、システム全体としては append-only と deterministic replay を保てる。

### 13.11 Composite Projection Insert Policy

他の Projection を合成した DB に対して新規レコードを追加したい場合、次の判定順序を適用する。

1. **Lossless Inversion Possible**
   追加行を canonical Observation 群へ損失なく逆変換できる場合は canonical mode を使う。

2. **Derived Annotation Only**
   追加行が派生結果への注釈・判定・レビューを表す場合は annotation mode を使う。

3. **Ambiguous Semantics**
   元の事実へ一意に戻せない場合は proposal mode を使う。

4. **No Valid Inversion**
   上記のいずれにも該当しない場合、その Projection は当該操作を reject しなければならない。

### 13.12 Inverse Mapping Policy for Mutable Multimodal Sources

Google Slides、スプレッドシート、その他 mutable な multimodal source に対する write-back は、通常の writable projection より厳格に扱う。

基本方針:

- mutable multimodal source は **default read-only**
- write-back を許可するのは、source-native operation に **損失なく** 逆変換できる場合のみ
- 画像や LLM 解釈のみに依存する編集要求は canonical にしてはならない
- 成功した source-native write-back は、その後の crawler / connector により新 revision snapshot として Lake に再 capture されなければならない

Google Slides の場合、source-native write-back を許可できるのは次のような操作である。

- 既知 `objectId` を持つ text box のテキスト更新
- 既知 `objectId` を持つ shape/image の属性更新
- slide の並び替え
- speaker notes の更新
- 既知の linked chart の refresh

一方、次のような操作は proposal または annotation に降格する。

- slide 画像だけを見て「このあたりにタイトルを追加してほしい」といった曖昧な編集
- 合成 DB の行から、対応する slide object を一意に特定できない編集
- LLM による意味解釈を前提とした自由レイアウト変更

source-native write-back の必須条件:

1. `presentationId` / `source_document_id` が特定できること
2. 対象 slide の `pageObjectId` が特定できること
3. 対象 element の `objectId`、または同等の stable anchor が特定できること
4. 元の `source_revision_id` または等価な snapshot hash が保持されていること
5. Write Adapter が deterministic な source-native API request 列へ変換できること

運用上、Google Slides への source-native write-back は、画像から slide を再生成することではなく、**native structure に anchored された API request を生成すること** を意味する。

### 13.13 Recommended Default Policy

システム全体の既定値として、以下を推奨する。

- 全 Projection は default read-only
- `writeBack.enabled: true` を明示した Projection のみ writable
- canonical mode は運用チームが管理する Projection に限定
- source-native write-back は managed projection と sandbox review を経たものに限定
- 個人研究用 / 実験用 Projection は annotation mode または proposal mode を優先
- 正規 Schema が未定義の新規追加要求は、まず proposal mode で受ける

さらに、非技術ユーザーのために、write-back や publish は coding agent を組み込んだ GUI / sandbox から実行できることを推奨する。ターミナル操作は必須要件ではない。

この方針により、関数型プログラミングに近い「不変の入力 + 純粋な導出 + 副作用の隔離」という構造を保ちながら、現実的な追加・編集要求を受け止められる。

---

## 14. Supporting Documents and ADR Backlog

本仕様は、今後の更新容易性のために意図的に複数文書へ分割する。  
`plan.md` は親仕様として残し、詳細は以下に委譲する。

| File | Main Focus |
|---|---|
| [domain_algebra.md](domain-algebra.md) | algebra, laws, write command model, storage semantics, failure taxonomy |
| [governance_capability_model.md](governance-capability-model.md) | consent, filtering, capability, review, retention, secrets |
| [runtime_reference_architecture.md](runtime-reference.md) | runtime topology, sandbox, queues, build isolation, reference stack |
| [adr_backlog.md](../decisions/adr-backlog.md) | unresolved decisions, decision status, next examples to prepare |
| [sharding_refactor.md](sharding.md) | Lake physical sharding の確定設計（identity/routing keyspec, exact index, Patricia split, failover, watermark propagation, logical→physical resolver, migration）。§11 の「keyspec をセットアップ時に確定し tree は leaf 1個で動かす」を sharding 全体へ展開した正典 |

本文から見て未確定な論点は、原則として `adr_backlog.md` に昇格させる。  
`design_questions.md` は raw な検討履歴として保持し、ADR 候補を育てるためのワークシートとして扱う。

特に次の論点は、次回の仕様更新で優先して具体例を追加したい。

- multimodal canonicalization boundary
- source-native latest / academic-pinned の API 具体例
- row-level lineage の具体例
- agent playground の approval route

---

## Appendix A: Glossary

| Term | Definition |
|---|---|
| **Observation** | Lake に記録される不変の capture 単位。observer が source system から何を取得したかを記録する |
| **Observer** | source system からデータを取得して Lake に記録する主体。crawler、connector、人手入力、sensor gateway など |
| **Source System** | 元データの authority を持つ系。Google Docs / Calendar、Notion、Figma、Canva、Slack、センサー backend など |
| **Entity** | 観測の対象。EntityType で型付けされる（例: person:tanaka-2026） |
| **EntityType** | Entity の分類（例: et:person, et:room）。誰でも追加可能 |
| **Schema** | Observation の payload 形式を定義する JSON Schema |
| **Authority Model** | 正史をどこに置くかを示す分類。`lake-authoritative`、`source-authoritative`、`dual-reference` |
| **Capture Model** | observer が Lake に何を保存するかを示す分類。`event`、`snapshot`、`chunk-manifest`、`restricted` |
| **Read Mode** | 利用時にどの経路で読むかを示す指定。`academic-pinned`、`operational-latest`、`application-cached` |
| **Projection** | Lake / 他の Projection から構築される派生 DB |
| **Projection Spec** | Projection の定義ファイル（YAML + 変換コード） |
| **Lake** | Observation の不変ストア（Observation Lake） |
| **Registry** | EntityType / Schema / Observer / Source Contract / Projection のメタデータ管理層 |
| **Blob** | Object Storage に格納されるバイナリデータ（画像、音声等） |
| **BlobRef** | Blob への参照。`blob:sha256:<hash>` 形式 |
| **DOI** | Digital Object Identifier。学術引用用の永続識別子 |
| **DAG** | Directed Acyclic Graph。Projection 間のソース依存関係 |

## Appendix B: Design Decisions Log

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| UUID v7（v4 ではなく） | 時刻ソート可能、イベント順序と自然に整合 | UUID v4（ソート不可）、ULID |
| EntityRef 形式 `type:id` | 可読性と型安全性の両立 | URI 形式、数値 ID |
| Schema を JSON Schema で定義 | 広く普及、バリデーションツールが豊富 | Avro（バイナリ最適化寄り）、Protobuf |
| DAG（固定レイヤーではなく） | DB on DBs の柔軟性を最大化 | L1/L2/L3 固定階層（合成の深さが制限される） |
| CAS (Content-Addressable) for Blobs | 自動重複排除、参照の不変性 | パスベース保存（重複・リネーム問題） |
| Opt-Out を 3 段階に分離 | 研究用途では Anonymize/Pseudonymize が Drop より有用 | Drop のみ（ネットワーク分析でグラフ構造が崩壊する） |
