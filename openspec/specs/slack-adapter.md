# M10: Slack Adapter

**Module:** slack-adapter
**Scope:** Slack source adapter — message / channel / thread / edit / delete の取り込み
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M09 Adapter Policy
**Parent docs:** [issues/R2-02](../../issues/R2-02_slack_schema_adapter.md)
**Agent:** Spec Designer (schema + contract) → Implementer (API client + adapter) → Reviewer (idempotency / authority 検証)
**MVP:** ✓

---

## 1. Module Purpose

Slack workspace のメッセージ・チャンネル・スレッド・編集・削除を Observation として Lake に取り込む adapter。

---

## 2. Source Contract

```yaml
Observer:
  id: "obs:slack-crawler"
  observer_type: "crawler"
  source_system: "sys:slack"
  schemas:
    - "schema:slack-message"
    - "schema:slack-channel-snapshot"
  authority_model: "lake-authoritative"
  capture_model: "event"
  trust_level: "automated"

SourceSystem:
  id: "sys:slack"
  provider: "Slack"
  api_version: "2024+"
  source_class: "immutable-text"    # messages are append-only (edits/deletes are events)
```

---

## 3. Schemas

### 3.1 schema:slack-message

```yaml
id: "schema:slack-message"
version: "1.0.0"
subject_type: "et:message"
payload_schema:
  type: object
  properties:
    channel_id:
      type: string
      description: "Slack channel ID"
    channel_name:
      type: string
    ts:
      type: string
      description: "Slack message timestamp (unique ID)"
    thread_ts:
      type: string
      description: "Parent thread timestamp (null if not in thread)"
    user_id:
      type: string
      description: "Slack user ID"
    user_name:
      type: string
    text:
      type: string
    message_type:
      type: string
      enum: ["message", "edit", "delete", "reaction_add", "reaction_remove",
             "file_share", "channel_join", "channel_leave"]
    edited:
      type: object
      properties:
        user: { type: string }
        ts: { type: string }
    reactions:
      type: array
      items:
        type: object
        properties:
          name: { type: string }
          count: { type: integer }
          users: { type: array, items: { type: string } }
    files:
      type: array
      items:
        type: object
        properties:
          id: { type: string }
          name: { type: string }
          mimetype: { type: string }
          size: { type: integer }
          blob_ref: { type: string }
    reply_count:
      type: integer
    reply_users_count:
      type: integer
  required: ["channel_id", "ts", "user_id", "text", "message_type"]
attachments:
  required: false
  accepted_types: ["*/*"]
```

### 3.2 schema:slack-channel-snapshot

```yaml
id: "schema:slack-channel-snapshot"
version: "1.0.0"
subject_type: "et:*"
payload_schema:
  type: object
  properties:
    channel_id: { type: string }
    channel_name: { type: string }
    purpose: { type: string }
    topic: { type: string }
    member_count: { type: integer }
    members: { type: array, items: { type: string } }
    is_archived: { type: boolean }
    snapshot_at: { type: string, format: date-time }
  required: ["channel_id", "channel_name", "snapshot_at"]
```

---

## 4. Observation Mapping

### 4.1 Regular Message

```json
{
  "schema": "schema:slack-message",
  "schemaVersion": "1.0.0",
  "observer": "obs:slack-crawler",
  "sourceSystem": "sys:slack",
  "authorityModel": "lake",
  "captureModel": "event",
  "subject": "message:slack:{channel_id}-{ts}",
  "payload": {
    "channel_id": "C01ABC",
    "channel_name": "general",
    "ts": "1234567890.123456",
    "user_id": "U01XYZ",
    "user_name": "tanaka",
    "text": "Hello everyone!",
    "message_type": "message"
  },
  "published": "2026-05-01T08:30:00+09:00",
  "idempotencyKey": "slack:C01ABC:1234567890.123456"
}
```

### 4.2 Message Edit

```json
{
  "schema": "schema:slack-message",
  "observer": "obs:slack-crawler",
  "subject": "message:slack:{channel_id}-{original_ts}",
  "payload": {
    "message_type": "edit",
    "ts": "1234567890.123456",
    "text": "Hello everyone! (edited)",
    "edited": { "user": "U01XYZ", "ts": "1234567891.000000" }
  },
  "idempotencyKey": "slack:C01ABC:1234567890.123456:edit:1234567891.000000",
  "meta": { "corrects": null }
}
```

**Note:** edit は correction ではなく新規 event として記録。名寄せ時に同一 `ts` の最新 edit を採用。

### 4.3 Message Delete

```json
{
  "payload": {
    "message_type": "delete",
    "ts": "1234567890.123456"
  },
  "idempotencyKey": "slack:C01ABC:1234567890.123456:delete",
  "meta": { "retracts": "message:slack:C01ABC-1234567890.123456" }
}
```

### 4.4 File Share

- file metadata は payload に記録
- file binary は blob upload → BlobRef で attachments に記録

---

## 5. IdempotencyKey Rules

| Event | Key Pattern |
|---|---|
| New message | `slack:{channel_id}:{ts}` |
| Edit | `slack:{channel_id}:{ts}:edit:{edit_ts}` |
| Delete | `slack:{channel_id}:{ts}:delete` |
| Reaction add | `slack:{channel_id}:{ts}:react:{user}:{emoji}` |
| File share | `slack:{channel_id}:{ts}:file:{file_id}` |

---

## 6. Crawl Strategy

### 6.1 Initial Load

```
for each target channel:
  cursor = None
  while True:
    result = slack_api.conversations_history(channel, cursor)
    observations = adapter.to_observations(result.messages)
    lake.ingest_batch(observations)
    if not result.has_more: break
    cursor = result.next_cursor
```

### 6.2 Incremental

```
for each target channel:
  last_ts = adapter.get_cursor(channel)
  result = slack_api.conversations_history(channel, oldest=last_ts)
  observations = adapter.to_observations(result.messages)
  lake.ingest_batch(observations)
  adapter.update_cursor(channel, latest_ts)
```

### 6.3 Thread Handling

- `thread_ts` がある message は thread reply として記録
- thread の parent message は通常 message として先に取得
- `conversations.replies` で thread 内全 message を取得

---

## 7. Rate Limiting

Slack API は tier-based rate limiting:
- Tier 1: 1 req/min
- Tier 2: 20 req/min
- Tier 3: 50 req/min
- Tier 4: 100 req/min

`conversations.history` は Tier 3。`conversations.replies` は Tier 3。

adapter は `Retry-After` header を尊重し、exponential backoff で対応。

Slack timestamp は `seconds.microseconds` の形式を必須とする。不正形式を壁時計やゼロへ補完してはならず、`MalformedResponse` として ingestion を失敗させる。

---

## 8. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | 通常メッセージ取得 | schema:slack-message Observation 生成 | |
| 2 | 同一 ts 再取得 | Duplicate | idempotency |
| 3 | edit イベント | 新 Observation (message_type=edit) | |
| 4 | delete イベント | 新 Observation (message_type=delete) | |
| 5 | thread reply | thread_ts が設定された Observation | |
| 6 | file 共有メッセージ | blob upload + BlobRef in attachments | |
| 7 | rate limit (429) | retry + 成功 | |
| 8 | channel snapshot | schema:slack-channel-snapshot Observation | |
| 9 | heartbeat | schema:observer-heartbeat Observation | |
| 10 | 不正な `ts` | `MalformedResponse` | 壁時計への代替禁止 |

---

## 9. Module Interface

### Provides

- SlackAdapter (implements SourceAdapter protocol)
- Slack API client (paginated, rate-limited)
- Slack → Observation mapper
- Cursor management for Slack channels

### Requires

- M09 Adapter Policy: SourceAdapter protocol, retry utilities
- M03 Observation Lake: Ingestion Gate API, Blob upload
- M02 Registry: Observer / Schema validation
