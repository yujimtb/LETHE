# M09: Adapter Policy

**Module:** adapter-policy
**Scope:** Source adapter の共通パターン・契約・テスト要件
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §4.5, [issues/R2-02](../../docs/archive/issues/round-2/R2-02_slack_schema_adapter.md), [issues/R2-06](../../docs/archive/issues/round-2/R2-06_google_slides_adapter.md)
**Agent:** Spec Designer (共通契約) → Reviewer (contract 検証)

---

## 1. Module Purpose

全 source adapter が従うべき **共通パターンと契約** を定義する。
個別 adapter (M10 Slack, M11 Google Slides) はこの policy を実装する。

---

## 2. Adapter Responsibility

### 2.1 Adapter がやること

- source system からデータを取得
- 取得データを Observation envelope に変換
- blob を Object Storage にアップロード
- Ingestion Gate 経由で Lake に append
- heartbeat Observation を定期投入

### 2.2 Adapter がやらないこと

- OCR / transcript / embedding 生成 → Supplemental Store の責務
- 名寄せの最終判断 → Identity Resolution Projection の責務
- Projection materialization → Projection Engine の責務

---

## 3. Common Adapter Contract

### 3.1 Configuration

```yaml
AdapterConfig:
  observer_id: ObserverRef
  source_system_id: SourceSystemRef
  adapter_version: SemVer
  authority_model: AuthorityModel
  capture_model: CaptureModel
  schemas: [SchemaRef]
  schema_bindings:
    - schema: SchemaRef
      versions: string
  poll_interval: duration          # e.g. "PT5M"
  heartbeat_interval: duration     # e.g. "PT1M"
  rate_limit:
    requests_per_second: int
    burst: int
  retry:
    max_retries: int
    backoff: "exponential"
    max_wait: duration
  credential_ref: SecretRef        # secret manager 参照
```

### 3.2 Adapter Interface

```python
class SourceAdapter(Protocol):
    def fetch_incremental(self, cursor: Cursor | None) -> FetchResult:
        """cursor 以降の差分を取得"""
        ...

    def fetch_snapshot(self, target_id: str) -> SnapshotResult:
        """特定オブジェクトの最新 snapshot を取得"""
        ...

    def to_observations(self, raw: RawData) -> list[ObservationDraft]:
        """生データを Observation envelope に変換"""
        ...

    def get_cursor(self) -> Cursor:
        """現在の cursor (watermark) を返す"""
        ...

    def heartbeat(self) -> ObservationDraft:
        """heartbeat Observation を生成"""
        ...
```

### 3.3 Adapter / Schema Binding Rule

- adapter は `adapter_version` と `schema_bindings` を必ず宣言する
- Optional field 追加などの加法的変更は、binding 範囲内なら adapter minor で吸収可
- payload shape / semantics が変わる場合は schema major または adapter major を上げ、binding を更新する
- 生成 Observation には `meta.sourceAdapterVersion` を付与する

### 3.4 FetchResult

```text
FetchResult
  = { items: [RawData], nextCursor: Cursor, hasMore: boolean }
  | FetchError { error: RetryableEffectFailure | NonRetryableEffectFailure }
```

---

## 4. Source Classification

| Source Class | Examples | Authority | Capture | Mutable? |
|---|---|---|---|---|
| Mutable + Multimodal | Google Slides, Figma, Canva | source-authoritative / dual | snapshot | Yes |
| Mutable + Text | Google Docs/Sheets, Notion | source-authoritative | snapshot | Yes |
| Immutable + Multimodal | sensor raw, photo archive | lake-authoritative | chunk-manifest / snapshot | No |
| Immutable + Text | Slack archive, append-only logs | lake-authoritative | event | No |

---

## 5. IdempotencyKey Generation Rules

| Source Type | Key Pattern | Example |
|---|---|---|
| Slack message | `slack:{channel}:{ts}` | `slack:C01ABC:1234567890.123456` |
| Slack message edit | `slack:{channel}:{ts}:edit:{edit_ts}` | |
| Google Slides revision | `gslides:{presentationId}:rev:{revisionId}` | |
| Google Calendar event | `gcal:{eventId}:etag:{etag}` | |
| Sensor chunk | `{sensor_id}:{start_ts}:chunk:{seq}` | |

**必須条件:**
- 同一 source の同一データから常に同じ key を生成
- key は adapter 内で閉じ、他 adapter と衝突しない
- key 生成は deterministic

---

## 6. Heartbeat Pattern

各 adapter は定期的に `schema:observer-heartbeat` を投入:

```json
{
  "schema": "schema:observer-heartbeat",
  "observer": "obs:{name}",
  "subject": "observer:{name}",
  "payload": {
    "status": "alive",
    "last_successful_capture_at": "2026-05-01T08:30:00Z",
    "pending_count": 0
  }
}
```

heartbeat 途絶は monitoring service が検知し alert を発行。

---

## 7. Error Handling

| Error Type | Action |
|---|---|
| Rate limit (429) | backoff + retry |
| Auth failure (401/403) | stop + alert + audit |
| Network timeout | retry with backoff |
| Malformed response | quarantine + alert |
| Partial failure (batch) | 成功分は commit、失敗分は retry / quarantine |

---

## 8. Adapter Testing Requirements

全 adapter は以下のテストを持つこと:

| Test Category | Content |
|---|---|
| Unit: to_observations | 生データ → Observation 変換の正確性 |
| Unit: idempotencyKey | 同一入力 → 同一 key |
| Unit: cursor management | 正しい cursor 生成・更新 |
| Contract: adapter/schema binding | `adapter_version` と `schemaVersion` の組み合わせ検証 |
| Integration: fetch + ingest | source → Lake end-to-end |
| Integration: deduplication | 同一データ再取得 → Duplicate |
| Contract: schema validation | 生成 Observation が schema に適合 |
| Contract: heartbeat | heartbeat が定期投入される |

---

## 9. MVP Adapter Requirements

MVP では以下を実装:
- Slack adapter (M10)
- Google Slides adapter (M11)

各 adapter は最低限:
- incremental fetch (cursor-based)
- Observation 生成
- blob upload
- idempotencyKey 生成
- heartbeat
- retry with backoff
- 基本テストスイート

---

## 10. Module Interface

### Provides

- SourceAdapter protocol / base class
- AdapterConfig schema
- IdempotencyKey generator utilities
- Retry / backoff utilities
- Heartbeat generator

### Requires

- M01 Domain Kernel: Observation, AuthorityModel, CaptureModel, FailureClass
- M02 Registry: Observer lookup, Schema validation
- M03 Observation Lake: Ingestion Gate API, Blob upload API
