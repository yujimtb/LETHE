# M14: API Serving

**Module:** api-serving
**Scope:** API レイヤー の read mode 制御、serving フロー、FastAPI 構成
**Dependencies:** M01 Domain Kernel, M05 Projection Engine, M08 Governance
**Parent docs:** [Runtime reference](../../docs/architecture/runtime-reference.md) §3.5 / §4.4, [System overview](../../docs/architecture/system-overview.md) §5.6–5.7
**Agent:** Spec Designer (read mode 契約) → Implementer (FastAPI, middleware) → Reviewer (access / filtering)
**MVP:** ✓

---

## 1. Module Purpose

Projection の公開 API を提供するレイヤー。
read mode 選択・access policy 適用・filtering-before-exposure を一貫して処理する。

---

## 2. Serving Flow

```text
client request
  → authentication
  → access policy evaluation (M08 Governance)
  → read mode resolution
  → projection query
  → filtering-before-exposure (restricted fields)
  → response + projection metadata
```

---

## 3. Read Modes

| Mode | Description | Source Policy | Use Case |
|---|---|---|---|
| `operational-latest` | 最新の materialized data | source-native-preferred | GUI 表示 |
| `academic-pinned` | pin 時点のデータで再現可能 | lake-only + pin | 論文引用 |

### 3.1 Read Mode Resolution

```python
def resolve_read_mode(request, projection_spec) -> ReadMode:
    """
    1. request query param ?mode= を確認
    2. なければ projection_spec.interface.readModes[0] (default)
    3. academic-pinned の場合: ?pin=<version> が必須
    """
```

### 3.2 Failure Semantics

operational-latest で最新データ取得に失敗した場合:

```text
1. stale cache や別 source へ暗黙に切り替えない
2. Projection が利用不能なら 503 を返す
3. 取得・解析・設定エラーは原因を保持して失敗させる
```

---

## 4. Response Envelope

全 API レスポンスに Projection metadata を付与:

```json
{
  "data": { ... },
  "projection_metadata": {
    "projection_id": "proj:person-page",
    "version": "1.0.0",
    "built_at": "2026-05-01T12:00:00+09:00",
    "read_mode": "operational-latest",
    "stale": false,
    "lineage_ref": "lineage:proj-person-page:build-42"
  }
}
```

### 4.1 Headers

| Header | Description |
|---|---|
| `X-LETHE-Projection-Id` | Projection ID |
| `X-LETHE-Read-Mode` | 使用された read mode |
| `X-LETHE-Built-At` | 最新 build timestamp |
| `X-LETHE-Lineage-Ref` | lineage 参照 |

### 4.2 Protected Blob and Lineage Access

- `GET /api/projections/{projection_id}/blobs/{sha256}` は認証と Projection read scope を要求する
- blob は filter 済み Projection 出力内に参照が存在する場合だけ返す
- raw CAS を hash だけで公開してはならない
- `GET /api/projections/{projection_id}/lineage` は response の `lineage_ref` が指す manifest を返す

---

## 5. Access Control Integration

### 5.1 Middleware Chain

```python
# FastAPI middleware order:
# 1. AuthenticationMiddleware → identity 確認
# 2. AccessPolicyMiddleware  → capability check (M08)
# 3. FilteringMiddleware     → restricted field masking (Filtering-before-Exposure Law)
```

### 5.2 Filtering-before-Exposure

restricted flag が付いた field は Projection API 経由で公開する前に filtering:

| Restricted Level | Action |
|---|---|
| `unrestricted` | そのまま返却 |
| `restricted` | field masking or exclusion |
| `consent-required` | consent 確認後に返却 |

---

## 6. Axum Structure

### 6.1 Application Layout

```text
crates/api/src/api/
├── envelope.rs
├── health.rs
├── pagination.rs
└── read_mode.rs

apps/selfhost/src/self_host/
├── server.rs                     # Axum router / authentication boundary
└── app/                          # Projection query service
```

### 6.2 Main App

```rust
Router::new()
    .route("/health", get(health))
    .route("/admin/sync", post(sync_now))
    .route(
        "/api/projections/{projection_id}/records",
        get(projection_records),
    )
```

---

## 7. Error Responses

| Status | Condition | Body |
|---|---|---|
| 400 | Invalid query parameter | `{ "error": "bad_request", "detail": "..." }` |
| 401 | Authentication failure | `{ "error": "unauthorized" }` |
| 403 | Access denied | `{ "error": "forbidden", "detail": "..." }` |
| 404 | Resource not found | `{ "error": "not_found" }` |
| 503 | Projection build in progress | `{ "error": "service_unavailable", "retry_after": 30 }` |

---

## 8. Health Check

```json
GET /health

{
  "status": "ok",
  "version": "0.1.0",
  "projections": {
    "person-resolution": { "status": "built", "built_at": "..." },
    "person-page": { "status": "built", "built_at": "..." }
  }
}
```

---

## 9. Pagination

全 list 系エンドポイントは共通 pagination:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `offset` | integer | 0 | 開始位置 |
| `limit` | integer | 20 | 取得件数 (max 100) |
| `sort` | string | varies | ソート field |
| `order` | string | "desc" | "asc" / "desc" |

Response:

```json
{
  "data": [...],
  "total": 42,
  "offset": 0,
  "limit": 20,
  "projection_metadata": { ... }
}
```

---

## 10. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | Filtering-before-Exposure Law: restricted data は必ず filtering | middleware integration test |
| 2 | 全レスポンスに projection_metadata 付与 | response schema test |
| 3 | read mode は projection spec で宣言されたもののみ | validation |
| 4 | blob は認証済みかつ filter 済み Projection から参照される場合のみ返す | API integration test |
| 5 | `lineage_ref` は取得可能な manifest を指す | lineage endpoint test |
| 6 | pagination limit ≤ 100 | validation |

---

## 11. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | Valid GET /api/projections/proj:person-page/records | 200 + person list + metadata | |
| 2 | Invalid auth token | 401 | |
| 3 | Access denied (no capability) | 403 | |
| 4 | ?mode=operational-latest | read_mode = operational-latest | |
| 5 | ?mode=academic-pinned&pin=v1 | pinned data 返却 | |
| 6 | ?mode=unknown | 400 | |
| 7 | Projection not built | 503 + retry_after | |
| 8 | restricted field in person | field masked | |
| 9 | GET /api/health | status ok | |
| 10 | 未認証 blob request | 401 | |
| 11 | Projection 未参照の raw CAS blob | 404 | |
| 12 | lineage endpoint | input Observation / Supplemental refs を含む manifest | |

---

## 12. Module Interface

### Provides

- FastAPI application
- Request → Response serving pipeline
- Read mode resolution
- Response envelope with projection metadata
- Filtering middleware
- Projection-scoped blob serving
- Lineage manifest serving
- Pagination utilities
- Health check endpoint

### Requires

- M01 Domain Kernel: ReadMode type
- M05 Projection Engine: Projection catalog, build status
- M08 Governance: Access policy, capability check, restricted field metadata
- M13 Person Page: Person API routes (router)
