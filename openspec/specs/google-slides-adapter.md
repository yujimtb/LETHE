# M11: Google Slides Adapter

**Module:** google-slides-adapter
**Scope:** Google Slides source adapter — revision snapshot / native structure / render の取り込み
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M04 Supplemental Store, M09 Adapter Policy
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §4.5.1, [issues/R2-06](../../docs/archive/issues/round-2/R2-06_google_slides_adapter.md)
**Agent:** Spec Designer (capture 仕様) → Implementer (API client + adapter) → Reviewer (snapshot 検証)
**MVP:** ✓

---

## 1. Module Purpose

Google Slides の presentation を revision snapshot として取り込む adapter。
native structure + rendered exports を Hybrid Observation として Lake に append する。

---

## 2. Source Contract

```yaml
Observer:
  id: "obs:gslides-crawler"
  observer_type: "crawler"
  source_system: "sys:google-slides"
  schemas:
    - "schema:workspace-object-snapshot"
  authority_model: "source-authoritative"
  capture_model: "snapshot"
  trust_level: "automated"

SourceSystem:
  id: "sys:google-slides"
  provider: "Google"
  api_version: "v1"
  source_class: "mutable-multimodal"
```

---

## 3. Schema

### 3.1 schema:workspace-object-snapshot

SaaS Snapshot Pattern (M01 §4.2) に準拠:

```yaml
id: "schema:workspace-object-snapshot"
version: "1.0.0"
subject_type: "et:document"
payload_schema:
  type: object
  properties:
    artifact:
      type: object
      properties:
        provider: { type: string }        # "google"
        service: { type: string }         # "slides"
        objectType: { type: string }      # "presentation"
        sourceObjectId: { type: string }  # presentation ID
        containerId: { type: string }     # Drive folder ID
        canonicalUri: { type: string }    # https://docs.google.com/presentation/d/{id}
      required: ["provider", "service", "objectType", "sourceObjectId"]
    revision:
      type: object
      properties:
        sourceRevisionId: { type: string }
        sourceModifiedAt: { type: string, format: date-time }
        captureMode: { type: string, enum: ["snapshot", "hybrid"] }
      required: ["sourceRevisionId", "captureMode"]
    native:
      type: object
      properties:
        encoding: { type: string, enum: ["blob-ref", "inline-json"] }
        blobRef: { type: string }
        content: {}                       # inline の場合
      required: ["encoding"]
    relations:
      type: object
      properties:
        editors: { type: array, items: { type: string } }
        viewers: { type: array, items: { type: string } }
        owner: { type: string }
    rights:
      type: object
      properties:
        visibility: { type: string }
        sharing: { type: string }
    attachmentRoles:
      type: object
      properties:
        native_structure: { type: string }
        rendered_pdf: { type: string }
        rendered_pptx: { type: string }
        slide_thumbnails: { type: array, items: { type: string } }
  required: ["artifact", "revision", "native"]
attachments:
  required: false
  accepted_types:
    - "application/json"
    - "application/pdf"
    - "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    - "image/png"
    - "image/jpeg"
```

---

## 4. Capture Strategy

### 4.1 取り込み原則

1. **Native Structure 取得**: Google Slides API → presentation object (pages, pageElements, speakerNotes, layouts, masters)
2. **Rendered Exports 取得**: PDF, PPTX, slide ごと PNG
3. **Hybrid Observation**: 1 revision = 1 Observation (native + renders を同一 record)
4. **Semantic Enrichment は行わない**: OCR / caption / embedding は Supplemental (M04) の責務

### 4.2 Observation Example

```json
{
  "schema": "schema:workspace-object-snapshot",
  "schemaVersion": "1.0.0",
  "observer": "obs:gslides-crawler",
  "sourceSystem": "sys:google-slides",
  "authorityModel": "source",
  "captureModel": "snapshot",
  "subject": "document:gslide:deck-abc123",
  "payload": {
    "artifact": {
      "provider": "google",
      "service": "slides",
      "objectType": "presentation",
      "sourceObjectId": "deck-abc123",
      "containerId": "folder-xyz",
      "canonicalUri": "https://docs.google.com/presentation/d/deck-abc123"
    },
    "revision": {
      "sourceRevisionId": "rev-017",
      "sourceModifiedAt": "2026-03-07T09:10:00+09:00",
      "captureMode": "hybrid"
    },
    "native": {
      "encoding": "blob-ref",
      "blobRef": "blob:sha256:native-json-abc..."
    },
    "relations": {
      "editors": ["tanaka@example.jp", "suzuki@example.jp"],
      "owner": "tanaka@example.jp"
    },
    "rights": {
      "visibility": "internal",
      "sharing": "domain-viewers"
    },
    "attachmentRoles": {
      "native_structure": "blob:sha256:native-json-abc...",
      "rendered_pdf": "blob:sha256:deck-pdf-abc...",
      "rendered_pptx": "blob:sha256:deck-pptx-abc...",
      "slide_thumbnails": [
        "blob:sha256:slide-1-png...",
        "blob:sha256:slide-2-png..."
      ]
    }
  },
  "attachments": [
    "blob:sha256:native-json-abc...",
    "blob:sha256:deck-pdf-abc...",
    "blob:sha256:deck-pptx-abc...",
    "blob:sha256:slide-1-png...",
    "blob:sha256:slide-2-png..."
  ],
  "published": "2026-03-07T09:10:00+09:00",
  "idempotencyKey": "gslides:deck-abc123:rev:rev-017",
  "meta": {
    "slide_count": 12,
    "canonical_hash": "sha256:9f3d..."
  }
}
```

---

## 5. Google API Usage

### 5.1 APIs Used

| API | Method | Purpose |
|---|---|---|
| Slides API v1 | `presentations.get` | native structure 取得 |
| Drive API v3 | `files.get` | metadata, revision info |
| Drive API v3 | `revisions.list` | revision 一覧 |
| Drive API v3 | `files.export` | PDF / PPTX export |
| Slides API v1 | `presentations.pages.getThumbnail` | slide thumbnail PNG |

### 5.2 Revision Detection

```
1. Drive API revisions.list で最新 revision を取得
2. 前回 capture 時の revisionId と比較
3. 新 revision があれば capture
4. revision が同一なら skip
```

### 5.3 Rate Limiting

- Google API: 300 queries/min/user (default)
- Slides API thumbnail: per-slide rate limit あり
- adapter は `Retry-After` + exponential backoff で対応

---

## 6. IdempotencyKey Rules

| Event | Key Pattern |
|---|---|
| Presentation snapshot | `gslides:{presentationId}:rev:{revisionId}` |
| Presentation (no revision ID) | `gslides:{presentationId}:modifiedAt:{timestamp}` |

---

## 7. Blob Management

| Artifact | Blob Content | MIME Type |
|---|---|---|
| Native structure | Slides API JSON response | `application/json` |
| PDF export | Drive export | `application/pdf` |
| PPTX export | Drive export | `application/vnd.openxmlformats-officedocument.presentationml.presentation` |
| Slide thumbnail | Per-slide PNG | `image/png` |

大きな native payload は blob ref 化。小さなものは inline-json 可。

---

## 8. Supplemental Trigger

adapter 自体は supplemental を生成しないが、capture 完了後に以下を trigger 可能:
- OCR pipeline → slide thumbnail を入力 → OCR text を Supplemental に保存
- Embedding pipeline → native text + OCR → embedding vectors を Supplemental に保存

trigger は非同期。adapter の責務は Lake append まで。

---

## 9. Crawl Strategy

### 9.1 Target Management

```yaml
targets:
  - presentation_id: "deck-abc123"
    poll_interval: "PT5M"
  - drive_folder_id: "folder-xyz"
    discover_new: true
    poll_interval: "PT15M"
```

### 9.2 Incremental

```
for each target presentation:
  latest_revision = drive_api.revisions.list(presentation_id)
  if latest_revision.id == adapter.get_cursor(presentation_id):
    skip  # no change
  else:
    native = slides_api.presentations.get(presentation_id)
    pdf = drive_api.files.export(presentation_id, "application/pdf")
    pptx = drive_api.files.export(presentation_id, "application/vnd...pptx")
    thumbnails = [slides_api.pages.getThumbnail(p) for p in native.slides]
    blobs = upload_all_blobs(native, pdf, pptx, thumbnails)
    observation = build_observation(native, blobs, revision)
    lake.ingest(observation)
    adapter.update_cursor(presentation_id, latest_revision.id)
```

---

## 10. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | 既存 presentation の初回 capture | Hybrid Observation 生成 (native + PDF + thumbnails) | |
| 2 | 同一 revision の再取得 | Duplicate (skip or dedup) | |
| 3 | 新 revision の取得 | 新 Observation 生成 | |
| 4 | 10 slides の presentation | 10 thumbnail + 1 PDF + 1 PPTX + 1 native | blob count |
| 5 | Drive folder 内の新 presentation 発見 | 自動 target 追加 + capture | |
| 6 | API rate limit (429) | retry + 成功 | |
| 7 | heartbeat | observer-heartbeat Observation | |
| 8 | native structure (inline-json, small) | payload.native.encoding = "inline-json" | |
| 9 | native structure (large) | payload.native.encoding = "blob-ref" | |

---

## 11. Module Interface

### Provides

- GoogleSlidesAdapter (implements SourceAdapter protocol)
- Google API client (Slides + Drive, rate-limited)
- Presentation → Observation mapper
- Revision cursor management

### Requires

- M09 Adapter Policy: SourceAdapter protocol, retry utilities
- M03 Observation Lake: Ingestion Gate API, Blob upload
- M02 Registry: Observer / Schema validation
