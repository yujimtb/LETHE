# M06: DAG Propagation

**Module:** dag-propagation
**Scope:** Incremental propagation / watermark 管理 / DAG scheduler / upstream breaking change
**Dependencies:** M01 Domain Kernel, M03 Observation Lake, M04 Supplemental Store, M05 Projection Engine, M08 Governance
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §5.2.1, [issues/R2-01](../../docs/archive/issues/round-2/R2-01_incremental_propagation_watermark.md)
**Agent:** Spec Designer (watermark 仕様) → Implementer (propagation 実装) → Reviewer (determinism 検証)

---

## 1. Module Purpose

上流 Observation の追加・更新が下流 Projection に伝播する仕組みを定義する。
全データ rebuild を回避し、データ増加時のスケーラビリティと freshness を確保する。

---

## 2. Propagation Strategy

### 2.1 第一優先: Incremental Propagation

更新された record のみを下流に伝播する。

- Projection は watermark (`lastProcessedRecordedAt` / `lastProcessedId`) を保持
- 新規 Observation は watermark 以降のみ incremental apply
- incremental apply 不可の Projection は設計段階で rebuild コストを考慮

### 2.2 第二優先: Scheduled Rebuild

cron で定期 full rebuild。batch workload 向き。incremental と併用で drift 補正。

### 2.3 非推奨: Lazy Invalidate

upstream 更新時に stale フラグだけ付け、次回アクセスで rebuild する方式は原則非採用。
アクセス頻度が極めて低い Projection に限り許可。

---

## 3. Watermark Management

### 3.1 Watermark Schema

```text
WatermarkState =
  { projectionId         : ProjectionRef
  , lastProcessedRecordedAt : Timestamp
  , lastProcessedId      : ObservationId
  , supplementalVersionPins : json?
  , lastBuildAt          : Timestamp
  , lastBuildStatus      : enum[success, partial, failed]
  , pendingCount         : int?
  }
```

### 3.2 Watermark Update Protocol

```
1. Projection runner が現在の watermark を読む
2. Projection spec から required supplemental version pins を解決
3. Lake から watermark 以降の Observation を取得
4. incremental apply 実行
5. 成功 → watermark と supplementalVersionPins を最新に更新
6. 失敗 → watermark は更新しない (retry)
```

### 3.3 Storage

MVP: SQLite テーブル `projection_watermarks`

```sql
CREATE TABLE projection_watermarks (
  projection_id TEXT PRIMARY KEY,
  last_processed_recorded_at TEXT NOT NULL,
  last_processed_id TEXT NOT NULL,
  supplemental_version_pins TEXT,
  last_build_at TEXT NOT NULL,
  last_build_status TEXT NOT NULL,
  pending_count INTEGER
);
```

---

## 4. Notification Channel (Phase-based)

| Phase | Mechanism | Rationale |
|---|---|---|
| MVP | **Poll-based** | 各 Projection が定期的に watermark 確認。追加インフラ不要 |
| Growth | **Event-driven** | Lake append 時に notification event。依存 Projection が subscribe |
| Scale | **CDC + DAG scheduler** | change data capture + topological order 実行 |

### 4.1 MVP Poll Mechanism

```text
for each active projection:
  current_wm = get_watermark(projection_id)
  lake_wm = get_lake_watermark()
  if lake_wm > current_wm:
    new_observations = lake.query(since=current_wm)
    projection.incremental_apply(new_observations)
    update_watermark(projection_id, lake_wm)
```

Poll interval: configurable per projection (default: 30s)

Governance 連携ルール:
- M08 の `writeApprovalSla = 60s` を満たすため、approved write を提供する Projection の poll interval は `<= 30s`
- approval 完了後の freshness は `approval_to_projection_freshness_p99` で監視する

---

## 5. DAG-Aware Propagation

### 5.1 Topological Order

dependencyが上流→下流になるよう topological sort で実行順を決定。

```text
execution_order = topological_sort(projection_dag)
for projection in execution_order:
  if has_pending_upstream_changes(projection):
    incremental_apply(projection)
```

### 5.2 Upstream Breaking Change

| Event | Downstream Response |
|---|---|
| Upstream **archive** | 最終 snapshot 保持 (degraded read) |
| Upstream **major version bump** | 旧 version pin で動作継続。新 version 対応は明示的 migration |
| Upstream **build failure** | downstream も stale 状態 |

### 5.3 Health Status

Catalog に以下の health status を表示:

| Status | Meaning |
|---|---|
| healthy | 最新 watermark、build 成功 |
| stale | watermark が古い (threshold 超過) |
| degraded | upstream に問題あり |
| broken | build 失敗 |

---

## 6. Rebuild Cost Estimation

全データ集計型 Projection は設計段階で以下を明示:

```yaml
spec:
  buildCost:
    estimatedDataVolume: "10GB"
    estimatedRebuildTime: "PT30M"
    incrementalApply: true          # or false
    incrementalMethod: "watermark-append"
    scheduledRebuildFrequency: "P1D"
    responseRequirement: "near-realtime"  # realtime | near-realtime | batch
```

---

## 7. API

| Method | Path | Description |
|---|---|---|
| GET | `/api/propagation/watermarks` | 全 Projection の watermark 一覧 |
| GET | `/api/propagation/watermarks/{projId}` | 個別 watermark |
| POST | `/api/propagation/trigger/{projId}` | 手動 incremental trigger |
| POST | `/api/propagation/rebuild/{projId}` | 手動 full rebuild |
| GET | `/api/propagation/dag` | DAG 依存図と health |
| GET | `/api/propagation/health` | 全 Projection health summary |

---

## 8. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | watermark は単調増加 | watermark update check |
| 2 | incremental apply 後の結果は full rebuild と一致 | periodic reconciliation |
| 3 | DAG は topological order でのみ実行 | scheduler check |
| 4 | build 失敗時は watermark 不変 | rollback check |
| 5 | upstream archive → downstream degraded | catalog health check |
| 6 | approval 完了済み write は SLA 内に Projection へ反映 | freshness metric |

---

## 9. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | 新 Observation → poll | incremental apply 実行 | |
| 2 | watermark 取得 → 変更なし → poll | skip (no-op) | |
| 3 | incremental 後 full rebuild | 同一結果 | |
| 4 | upstream build failure | downstream = stale | |
| 5 | 2段 DAG (A→B) で A に追加 | A 先に rebuild → B | |
| 6 | 手動 trigger API | incremental 実行 | |
| 7 | approved write | 60s 以内に latest read へ反映 | |

---

## 10. Module Interface

### Provides

- Watermark CRUD API
- Poll-based propagation scheduler
- DAG-aware execution ordering
- Health status tracking
- Manual trigger / rebuild API

### Requires

- M01 Domain Kernel: Timestamp, ObservationId
- M03 Observation Lake: watermark query, since query
- M04 Supplemental Store: version-pinned supplemental lookup
- M05 Projection Engine: build / rebuild execution
- M08 Governance: write approval SLA
