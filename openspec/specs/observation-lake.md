# M03: Observation Lake

**Module:** observation-lake
**Scope:** Canonical capture layer / ingestion pipeline / storage architecture / ordering
**Dependencies:** M01 Domain Kernel, M02 Registry, M08 Governance
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §4, [Domain algebra](../../docs/architecture/domain-algebra.md) §4
**Agent:** Spec Designer (ingestion 契約) → Implementer (append + gate 実装) → Reviewer (append-only 検証)

---

## 1. Module Purpose

システムの根幹となる **不変の capture ストア**。全データは Observation として記録される。
Lake は利用者が直接読むための主データ面ではなく、**canonical capture と replay の基盤** として設計する。

---

## 2. Core Concept

> **Observation =「ある observer が、ある source system から、ある契約に従って何かを capture した記録」**

- Observation は用途中立。academic / operational の区別は API / Projection の read mode で指定する
- Lake は capture and replay substrate
- 利用者が触るのは Projection / API / Sandbox 層

---

## 3. Storage Architecture

### 3.1 Storage Layout

| Data Type | Storage | Format | Retention |
|---|---|---|---|
| Canonical Observations | Event Store (append-only) | JSON / Avro | **永続** |
| Binary Attachments | Object Storage (CAS) | Original format | ポリシーベース (default: 5年) |
| Cold Archive | Partitioned files | Parquet (per schema, per month) | **永続** |

### 3.2 Content-Addressable Storage (CAS)

- バイナリは SHA-256 ハッシュキーで保存
- 同一ファイルは自動重複排除
- 参照形式: `blob:sha256:<hash>`

### 3.3 MVP Storage

- Observations: Parquet files on local/NAS
- Blobs: Local filesystem or MinIO
- Index: SQLite or PostgreSQL

---

## 4. Ingestion Pipeline

```
Source System / Observer
  │
  ▼
Ingestion Gate
  ├─ 1. Authenticate observer         (Observer Registry)
  ├─ 2. Resolve source contract        (authority / capture policy)
  ├─ 3. Validate payload               (Schema Registry: JSON Schema)
  ├─ 4. Evaluate governance policy     (consent / restricted capture / review)
  ├─ 5. Deduplicate                    (idempotencyKey)
  ├─ 6. Store blobs → get BlobRefs     (Object Storage)
  ├─ 7. Assign recordedAt              (UTC system timestamp)
  ├─ 8. Validate temporal bounds       (`published <= recordedAt + PT10M`)
  ├─ 9. Assign id                      (UUID v7)
  │
  ▼
Lake (append)
  │
  ▼
Audit Event emit
```

### 4.1 Ingestion Gate の責務

**責務:**
- observer 認証
- schema validation
- consent / access / review policy 呼び出し
- idempotency check
- blob upload orchestration
- temporal validation (`published` / `recordedAt`)
- append request 生成

**非責務 (これらはやらない):**
- transcript / OCR / LLM 解釈
- 名寄せの最終判断
- Projection materialization の直接更新

### 4.2 Ingestion Gate API

| Method | Path | Description |
|---|---|---|
| POST | `/api/lake/observations` | Observation 追加 |
| POST | `/api/lake/observations/batch` | バッチ追加 |
| POST | `/api/lake/blobs` | Blob アップロード |
| GET | `/api/lake/blobs/{hash}` | Blob 取得 |

### 4.3 Observation 追加リクエスト

```json
{
  "schema": "schema:slack-message",
  "schemaVersion": "1.0.0",
  "observer": "obs:slack-crawler",
  "sourceSystem": "sys:slack",
  "authorityModel": "lake",
  "captureModel": "event",
  "subject": "message:slack:C01ABC-1234567890.123456",
  "payload": { ... },
  "attachments": [],
  "published": "2026-05-01T08:30:00+09:00",
  "idempotencyKey": "slack:C01ABC:1234567890.123456",
  "meta": {}
}
```

### 4.4 Synchronous Policy / Temporal Gate

- M08 policy evaluation は **同期** で行う
- `Allow` の場合のみ append 継続
- `Deny PolicyFailure` は `Rejected`
- `RequireReview` は `Quarantined` として review queue に回す
- `published > recordedAt + PT10M` は `Quarantined(reason="clock-skew")`

### 4.5 Ingestion Result

```text
IngestResult
  = Ingested { id: ObservationId, recordedAt: Timestamp }
  | Duplicate { existingId: ObservationId }
  | Rejected { error: ValidationFailure | PolicyFailure }
  | Quarantined { ticket: QuarantineTicket }
```

---

## 5. Immutability & Corrections

Lake は **Append-Only**。過去の Observation を変更・削除しない。

| Intent | Mechanism |
|---|---|
| 新規記録 | 新しい Observation |
| 誤り訂正 | 新 Observation + `meta.corrects: "<original-id>"` |
| 撤回 | 新 Observation + `meta.retracts: "<original-id>"` |
| オプトアウト | Consent Ledger 更新 → filtering Projection で反映 |

---

## 6. Event Ordering

Projection が再生する際の順序:

1. `published` (Event Time — 事象の発生時刻)
2. `recordedAt` (System Time — 記録時刻)
3. `id` (UUID v7 — time-sortable バイト順)

### 6.1 Late Arrival / Duplicate / Re-send

- `published` を event time、`recordedAt` を ingestion time として分離
- `idempotencyKey` で重複排除
- 補正は correction Observation で表現（元 Observation は更新しない）

---

## 7. Mutable External Source Ingestion

### 7.1 SaaS Revisioned Snapshot Pattern

Google Slides / Docs / Sheets / Drive / Calendar、Notion、Figma、Canva 等の mutable source は **revisioned snapshot** として取り込む。

原則:
1. **Native Snapshot 取得** — Source API の構造情報をそのまま取得
2. **Rendered Snapshot 取得** — PDF、PNG、PPTX 等の人間可読形式を取得
3. **Hybrid Observation として append** — 1 と 2 を同一 revision に紐づく 1 Observation として保存
4. **Semantic Enrichment は行わない** — OCR / caption / embedding は downstream Projection の責務

### 7.2 High-Frequency Source Ingestion

センサー / 高頻度ソースの原則:
1. Lake は再構成に必要な canonical capture を保持
2. 高頻度 raw 本体は raw store に置いてよい
3. Lake には manifest Observation を置く
4. Projection は read mode に応じて読取経路を切り替え
5. Current Value は canonical data にしない（Projection が導出）
6. Late arrival / duplicate を前提にする

---

## 8. Query Interface (Internal)

Lake は user-facing query 面ではない。内部 query は以下を提供:

| Method | Path | Description |
|---|---|---|
| GET | `/api/lake/observations` | filter by schema, subject, time range, observer |
| GET | `/api/lake/observations/{id}` | 個別取得 |
| GET | `/api/lake/observations/watermark` | 最新 watermark (recordedAt, id) |
| GET | `/api/lake/observations/since` | watermark 以降の差分取得 |

---

## 9. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | Observation は一度保存したら変更不可 | UPDATE/DELETE SQL がないこと |
| 2 | 全 Observation は有効な SchemaRef を持つ | schema validation |
| 3 | 全 Observation は有効な ObserverRef を持つ | observer registry lookup |
| 4 | idempotencyKey 重複は Duplicate 返却 | dedup index |
| 5 | recordedAt は常に UTC | server-side timestamp |
| 6 | `published <= recordedAt + PT10M` を満たすか quarantine | temporal gate |
| 7 | Governance の `RequireReview` は `Quarantined` で surface | review queue check |
| 8 | Blob は CAS hash で一意 | hash check on upload |

---

## 10. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | Valid Observation POST | Ingested + id 返却 | |
| 2 | 同一 idempotencyKey で再送 | Duplicate + existingId | |
| 3 | 無効 schema での POST | Rejected (ValidationFailure) | |
| 4 | 未登録 observer での POST | Rejected (ValidationFailure) | |
| 5 | Blob upload + Observation 参照 | BlobRef で取得可能 | |
| 6 | watermark 取得 → 新 Observation → since 取得 | 差分に含まれる | |
| 7 | Correction Observation (meta.corrects) | 元 Observation は変更なし | |
| 8 | future `published` (`recordedAt + 10m` 超過) | Quarantined(reason=clock-skew) | |
| 9 | policy = RequireReview | Quarantined + review ticket | |
| 10 | 1000 件 batch 投入 | 全件 Ingested、順序保存 | |

---

## 11. Module Interface

### Provides

- Observation append API (single + batch)
- Blob upload / download API
- Observation query API (filter, watermark, since)
- IngestResult 型

### Requires

- M01 Domain Kernel: Observation 型、AuthorityModel、CaptureModel、FailureClass
- M02 Registry: Schema validation、Observer lookup、source contract lookup
- M08 Governance: consent / review policy evaluation
