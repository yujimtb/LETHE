# M13: Person Page

**Module:** person-page
**Scope:** 個人ページ Projection & API — MVP 最終成果物
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M05 Projection Engine, M12 Identity Resolution
**Parent docs:** [issues/R2-04](../../docs/archive/issues/round-2/R2-04_person_page_api.md), [System overview](../../docs/architecture/system-overview.md) §11.4 Step 5
**Agent:** Spec Designer (API 契約) → Implementer (projector + API) → Reviewer (出力検証)
**MVP:** ✓

---

## 1. Module Purpose

名寄せ済みの各人物について、関連する Slides・Slack メッセージ・活動集約を統合した
**個人ページ** を提供する。MVP の最終出力であり、GUI チームに向けた API 契約を定義する。

補足: Google Slides は**自己紹介スライド**であり、slide 内容から個人情報を抽出して個人ページの中核データとする。
`proj:person-resolution` からは **承認済み `resolved_persons` のみ** を入力とし、`resolution_candidates.status = pending` は入力しない。

---

## 2. Projection Spec

```yaml
apiVersion: "lethe/v1"
kind: "Projection"

metadata:
  id: "proj:person-page"
  name: "Person Page"
  created_by: "system"
  version: "1.0.0"
  tags: ["person", "page", "mvp"]

spec:
  sources:
    - ref: "proj:person-resolution"
      version: ">=1.0.0"
    - ref: "lake"
      filter:
        schemas:
          - "schema:workspace-object-snapshot"
          - "schema:slack-message"
    - ref: "supplemental"
      filter:
        derivations: ["ocr-text", "profile-extraction"]

  engine: "duckdb"
  build:
    type: "python-script"
    projector: "./projections/person_page/projector.py"

  outputs:
    - format: "sql"
      tables:
        - "person_profiles"            # 人物プロフィール
        - "person_slides"              # 関連スライド
        - "person_messages"            # 関連メッセージ
        - "person_activity"            # 活動集約

  interface:
    primaryAccess:
      type: "http"
      path: "/api/projections/proj:person-page/records"
    readModes:
      - name: "operational-latest"
        sourcePolicy: "source-native-preferred"

  reproducibility:
    deterministicIn: ["academic-pinned"]

  gapPolicy:
    action: "warn"
    maxGapDuration: "PT1H"
```

---

## 3. Output Tables

### 3.1 person_profiles

| Column | Type | Description |
|---|---|---|
| person_id | TEXT PK | "person:{merged_id}" (M12 と同一) |
| display_name | TEXT | 正規名 |
| self_intro_text | TEXT | 自己紹介スライドから抽出されたテキスト |
| self_intro_slide_id | TEXT | 自己紹介スライドの document_id |
| self_intro_thumbnail | TEXT | スライド thumbnail の blob ref |
| identities | JSON | `[{system, external_id}]` |
| source_count | INT | 関連 source 数 |
| last_activity | TIMESTAMP | 最終活動日時 |
| profile_updated_at | TIMESTAMP | |

### 3.2 person_slides

| Column | Type | Description |
|---|---|---|
| id | TEXT PK | `ps:{append_seq:020}:{ObservationId}:{claim}` |
| person_id | TEXT FK | → person_profiles |
| document_id | TEXT | "document:gslide:{id}" |
| title | TEXT | presentation title |
| role | TEXT | "editor" / "viewer" / "owner" |
| last_seen_revision | TEXT | 最新 revision ID |
| slide_count | INT | |
| thumbnail_ref | TEXT | 代表 thumbnail blob ref |
| last_modified | TIMESTAMP | |

### 3.3 person_messages

| Column | Type | Description |
|---|---|---|
| id | TEXT PK | `pm:{append_seq:020}:{ObservationId}` |
| person_id | TEXT FK | → person_profiles |
| channel | TEXT | Slack channel name |
| text | TEXT | message text (truncated for listing) |
| ts | TIMESTAMP | message timestamp |
| thread_ts | TEXT | thread parent (nullable) |
| has_attachments | BOOLEAN | |

### 3.4 person_activity

| Column | Type | Description |
|---|---|---|
| person_id | TEXT PK | |
| total_slides_related | INT | |
| total_messages | INT | |
| first_activity | TIMESTAMP | |
| last_activity | TIMESTAMP | |
| active_channels | JSON | channel 一覧 |

---

## 4. API Endpoints

### 4.1 一覧

| Method | Path | Description |
|---|---|---|
| GET | `/api/projections/proj:person-page/records` | 名寄せ済み人物一覧 |
| GET | `/api/projections/proj:person-page/records/{person_id}` | 個人詳細ページ |
| GET | `/api/projections/proj:person-page/records/{person_id}/slides` | 関連 Slides 一覧 |
| GET | `/api/projections/proj:person-page/records/{person_id}/messages` | 関連メッセージ一覧 |
| GET | `/api/projections/proj:person-page/records/{person_id}/timeline` | Activity timeline |

### 4.2 個人詳細レスポンス

```json
{
  "person_id": "person:tanaka-2026",
  "display_name": "田中太郎",
  "self_introduction": {
    "text": "3年生の田中太郎です。趣味は...",
    "slide_id": "document:gslide:deck-abc123",
    "thumbnail_url": "/api/blobs/sha256:slide-1-png..."
  },
  "identities": [
    { "system": "slack", "external_id": "U1234567" },
    { "system": "google", "external_id": "tanaka@example.jp" }
  ],
  "related_slides": [
    {
      "document_id": "document:gslide:deck-abc123",
      "title": "自己紹介スライド",
      "role": "editor",
      "last_seen_revision": "rev-017",
      "slide_count": 3,
      "thumbnail_url": "/api/blobs/sha256:thumb..."
    }
  ],
  "recent_messages": [
    {
      "channel": "general",
      "text": "明日の会議について...",
      "ts": "2026-05-01T10:30:00+09:00"
    }
  ],
  "activity_summary": {
    "total_slides_related": 5,
    "total_messages": 128,
    "first_activity": "2026-04-01T09:00:00+09:00",
    "last_activity": "2026-05-01T10:30:00+09:00",
    "active_channels": ["general", "random", "project-a"]
  },
  "projection_metadata": {
    "projection_id": "proj:person-page",
    "version": "1.0.0",
    "built_at": "2026-05-01T12:00:00+09:00",
    "read_mode": "operational-latest"
  }
}
```

### 4.3 一覧レスポンス

```json
{
  "persons": [
    {
      "person_id": "person:tanaka-2026",
      "display_name": "田中太郎",
      "source_count": 2,
      "total_slides": 5,
      "total_messages": 128,
      "last_activity": "2026-05-01T10:30:00+09:00",
      "thumbnail_url": "/api/blobs/sha256:thumb..."
    }
  ],
  "total": 42,
  "offset": 0,
  "limit": 20
}
```

### 4.4 Timeline レスポンス

```json
{
  "person_id": "person:tanaka-2026",
  "events": [
    {
      "type": "slide_edit",
      "document_id": "document:gslide:deck-abc123",
      "title": "自己紹介スライド",
      "ts": "2026-05-01T09:10:00+09:00"
    },
    {
      "type": "message",
      "channel": "general",
      "text": "明日の会議...",
      "ts": "2026-05-01T10:30:00+09:00"
    }
  ],
  "total": 133
}
```

---

## 5. Self-Introduction Extraction

### 5.1 Pipeline

自己紹介スライドから個人情報を抽出するパイプライン:

```text
Google Slides Observation (M11)
  → slide thumbnail (blob)
  → OCR pipeline (Supplemental M04)
  → profile extraction (Supplemental M04)
  → person_profiles.self_intro_text
```

### 5.2 Profile Extraction

OCR テキストから抽出する情報:
- 名前 (日本語/英語)
- 学年・学部
- 自己紹介テキスト
- 趣味・特技

抽出結果は Supplemental に `derivation: "profile-extraction"` として保存。

---

## 6. Query Patterns

### 6.1 Person List

```sql
SELECT p.person_id, p.display_name, p.source_count,
       a.total_slides_related, a.total_messages, a.last_activity
FROM person_profiles p
JOIN person_activity a ON p.person_id = a.person_id
ORDER BY a.last_activity DESC
LIMIT :limit OFFSET :offset
```

### 6.2 Person Detail

```sql
SELECT * FROM person_profiles WHERE person_id = :id;
SELECT * FROM person_slides WHERE person_id = :id ORDER BY last_modified DESC;
SELECT * FROM person_messages WHERE person_id = :id ORDER BY ts DESC LIMIT 20;
SELECT * FROM person_activity WHERE person_id = :id;
```

---

## 7. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | person_id は M12 resolved_persons と一致 | FK check |
| 2 | slides は Lake 内の既存 Observation にのみリンク | 存在チェック |
| 3 | messages の text は restricted flag に従い filtering | Filtering-before-Exposure Law |
| 4 | API は projection_metadata を必ず返却 | response schema check |
| 5 | pending identity candidate は person page に露出しない | approved identity join check |
| 6 | pagination が正しく機能する | offset/limit test |
| 7 | message/slide IDはperson IDとperson内ordinalに依存しない | late-bridge regression |
| 8 | message順はappend_seq、同値時はfact IDで決定する | strict row validation |

---

## 8. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | 名寄せ済み person (2 sources) | 個人詳細 JSON 返却 | identity, slides, messages |
| 2 | `/api/projections/proj:person-page/records` | 一覧返却 (paginated) | |
| 3 | 自己紹介スライドあり person | self_introduction.text 含む | |
| 4 | slides endpoint | 関連 slide 一覧 | |
| 5 | messages endpoint | 関連メッセージ一覧 | |
| 6 | timeline endpoint | 時系列イベント一覧 | |
| 7 | restricted message | text masked or excluded | Filtering Law |
| 8 | 存在しない person_id | 404 | |
| 9 | pending medium candidate | projection recordsに出ない | |
| 10 | incremental: 新 slide capture → rebuild | person_slides 更新 | |

---

## 9. Module Interface

### Provides

- Person Page projector
- Person API endpoints (5 routes)
- Person detail / list / slides / messages / timeline response schemas

### Requires

- M12 Identity Resolution: `proj:person-resolution` (resolved_persons, person_identifiers)。pending candidate は upstream で join 対象外
- M03 Observation Lake: Slack / Slides Observation query
- M04 Supplemental Store: OCR text, profile extraction
- M05 Projection Engine: build runner, catalog registration
- M14 API Serving: FastAPI integration, read mode middleware
