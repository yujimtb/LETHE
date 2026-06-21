# M12: Identity Resolution

**Module:** identity-resolution
**Scope:** 名寄せ Projection — Slack user / Google account / slide 登場人物の同一人物判定
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M04 Supplemental Store, M05 Projection Engine, M08 Governance
**Parent docs:** [issues/R2-03](../../docs/archive/issues/round-2/R2-03_identity_resolution_projection.md), [System overview](../../docs/architecture/system-overview.md) §11.4 Step 4
**Agent:** Spec Designer (名寄せ spec) → Implementer (projector) → Reviewer (confidence / lineage 検証)
**MVP:** ✓

---

## 1. Module Purpose

複数 source に散在する人物情報を統合し、同一人物を判定する **名寄せ Projection**。
MVP の中核であり、個人ページ (M13) の前提。

---

## 2. Design Position

- 名寄せは **Projection** の領域（canonical truth ではない）
- 名寄せ候補は **Supplemental** に保存可
- 解決済み identity graph は **Projection** として公開
- ADR-004 に準拠: identity resolution は Lake ではなく Projection 寄り

---

## 3. Input Sources

### 3.1 Slack Sources

| Field | Source | EntityRef |
|---|---|---|
| user_id | schema:slack-message → payload.user_id | person:slack:{user_id} |
| user_name | schema:slack-message → payload.user_name | |
| display_name | Slack users.info API (supplemental) | |
| email | Slack users.info API (supplemental) | |
| profile_image | Slack users.info API (supplemental) | |

### 3.2 Google Slides Sources

| Field | Source | EntityRef |
|---|---|---|
| editor email | schema:workspace-object-snapshot → payload.relations.editors | person:google:{email} |
| owner email | schema:workspace-object-snapshot → payload.relations.owner | |
| mentioned name | OCR text (supplemental) から抽出 | |

### 3.3 Supplemental Sources

| Kind | Content |
|---|---|
| ocr-text | slide text → 人名抽出 |
| name-resolution-candidate | 前回の解決候補 |

---

## 4. Resolution Algorithm

### 4.1 Phase 1: Source-Internal Linking

各 source 内で同一人物を判定:

```text
Slack:
  user_id → display_name, email, profile
  → PersonCandidate(source="slack", identifiers=[user_id, email, display_name])

Google:
  editor_email → PersonCandidate(source="google", identifiers=[email])
```

### 4.2 Phase 2: Cross-Source Matching

source 間で identifier を突合:

| Match Strategy | Field | Confidence |
|---|---|---|
| Email exact match | Slack email ↔ Google editor email | High |
| Name fuzzy match | Slack display_name ↔ OCR 抽出名 | Medium |
| Domain match | email domain 一致 | Low (補助) |

### 4.3 Phase 3: Resolution Graph

```text
ResolvedPerson =
  { personId     : EntityRef          -- "person:{merged_id}"
  , canonicalName: string
  , aliases       : [string]
  , identifiers  : [SourceIdentifier]
  , confidence   : High | Medium
  , sources      : [SourceRef]
  , resolvedAt   : Timestamp
  , resolvedBy   : "projector:identity-resolution:v{version}"
  }
```

### 4.4 Ambiguity Handling

| Case | Action |
|---|---|
| 1 source identifier → 1 person | 自動解決 (High confidence) |
| 複数 identifier が一致 | 自動 merge (High confidence) |
| 名前のみ一致 (fuzzy) | `resolution_candidates` に保存 (Medium) → 人間承認後にのみ merge |
| 解決不可 | Low confidence → 個別 PersonCandidate として保持 |

---

## 5. Projection Spec

```yaml
apiVersion: "lethe/v1"
kind: "Projection"

metadata:
  id: "proj:person-resolution"
  name: "Identity Resolution"
  created_by: "system"
  version: "1.0.0"
  tags: ["identity", "resolution", "mvp"]

spec:
  sources:
    - ref: "lake"
      filter:
        schemas:
          - "schema:slack-message"
          - "schema:workspace-object-snapshot"
    - ref: "supplemental"
      filter:
        derivations: ["ocr-text", "name-resolution-candidate"]

  engine: "duckdb"
  build:
    type: "python-script"
    projector: "./projections/identity_resolution/projector.py"

  outputs:
    - format: "sql"
      tables:
        - "resolved_persons"           # 解決済み人物
        - "person_identifiers"         # identifier → person mapping
        - "resolution_candidates"      # 未解決候補
        - "resolution_log"             # 解決ログ

  interface:
    primaryAccess:
      type: "http"
      path: "/api/projections/person-resolution"
    readModes:
      - name: "operational-latest"
        sourcePolicy: "lake-latest"

  reproducibility:
    deterministicIn: ["academic-pinned"]

  gapPolicy:
    action: "warn"
    maxGapDuration: "PT1H"
```

---

## 6. Output Tables

### 6.1 resolved_persons

| Column | Type | Description |
|---|---|---|
| person_id | TEXT PK | "person:{merged_id}" |
| canonical_name | TEXT | 正規名 |
| confidence | TEXT | "high" / "medium"（medium は承認済みのみ） |
| source_count | INT | 関連 source 数 |
| resolved_at | TIMESTAMP | |

### 6.2 person_identifiers

| Column | Type | Description |
|---|---|---|
| identifier_id | TEXT PK | |
| person_id | TEXT FK | → resolved_persons |
| source | TEXT | "slack" / "google" / "ocr" |
| identifier_type | TEXT | "email" / "user_id" / "display_name" |
| identifier_value | TEXT | |

### 6.3 resolution_candidates

| Column | Type | Description |
|---|---|---|
| candidate_id | TEXT PK | |
| person_a_id | TEXT | |
| person_b_id | TEXT | |
| match_type | TEXT | "email_exact" / "name_fuzzy" / "domain" |
| confidence | TEXT | |
| status | TEXT | "pending" / "accepted" / "rejected" |

---

## 7. API

| Method | Path | Description |
|---|---|---|
| GET | `/api/projections/person-resolution/persons` | 解決済み人物一覧 |
| GET | `/api/projections/person-resolution/persons/{id}` | 人物詳細 + identifiers |
| GET | `/api/projections/person-resolution/candidates` | 未解決候補一覧 |
| POST | `/api/projections/person-resolution/candidates/{id}/accept` | 候補を承認 (annotation) |
| POST | `/api/projections/person-resolution/candidates/{id}/reject` | 候補を棄却 (annotation) |
| GET | `/api/projections/person-resolution/by-identifier` | identifier → person lookup |

---

## 8. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | 名寄せ結果は canonical truth ではない | Lake write なし |
| 2 | 同一 email は同一 person に解決 | email uniqueness check |
| 3 | 解決ログは全判定を記録 | resolution_log テーブル |
| 4 | confidence が low の person は自動 merge しない | algorithm check |
| 5 | medium confidence は承認前に `resolved_persons` へ入れない | candidate / person split check |
| 6 | incremental apply で結果が安定 | replay test |
| 7 | 同一入力では candidate ID と配列順が一致する | deterministic replay test |

---

## 9. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | Slack user + Google editor (同一 email) | 1 resolved_person (High) | |
| 2 | Slack user + Google editor (異なる email) | 2 separate persons | |
| 3 | OCR 抽出名 ≈ Slack display_name | candidate (Medium) | fuzzy match |
| 4 | 新 Slack message → incremental apply | person_identifiers 更新 | |
| 5 | candidate accept | resolved_persons に merge (confidence=medium) | |
| 6 | candidate reject | status = rejected | |
| 7 | pending medium candidate | person list には出ない | |
| 8 | by-identifier lookup (email) | 正しい person 返却 | |
| 9 | 同一入力を複数回 build | candidate ID と出力順が完全一致 | |

---

## 10. Module Interface

### Provides

- Identity resolution projector
- Resolved person API
- Candidate management API
- by-identifier lookup

### Requires

- M01 Domain Kernel: EntityRef, Confidence
- M03 Observation Lake: Slack / Slides Observation query
- M04 Supplemental Store: OCR text, name-resolution-candidate
- M05 Projection Engine: build runner, catalog registration
- M08 Governance: confidence threshold / approval rule
