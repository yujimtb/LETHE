# M08: Governance

**Module:** governance
**Scope:** Consent / access control / capability model / write review / retention / secret handling
**Dependencies:** M01 Domain Kernel
**Parent docs:** [Governance capability model](../../docs/architecture/governance-capability-model.md), [System overview](../../docs/architecture/system-overview.md) §7
**Agent:** Spec Designer (policy 設計) → Implementer (policy engine) → Reviewer (exposure 検証)

---

## 1. Module Purpose

LETHE のすべてのデータ操作に対する **policy evaluation と capability control** を定義する。
consent、access、filtering projection、agent capability、write review、retention、secret management を統一的に扱う。

---

## 2. Governance Principles

1. **Capture before interpretation** — 一次資料を保ち、解釈は serving 前に行う
2. **Filtering before exposure** — restricted data は表示・共有・export の前で制御する
3. **Explicit authority** — write の行き先を隠さない
4. **Least privilege** — 人間・agent・service に capability-scoped な権限
5. **Auditable decisions** — deny / approve / export / delete / publish は理由付きで追跡可能

---

## 3. Policy Types

### 3.1 Access Scope

```text
AccessScope = Public | Internal | Restricted | HighlySensitive
```

### 3.2 Capabilities

```text
Capability
  = ReadRegistry
  | SearchCatalog
  | ReadOwnProjection
  | ReadSharedProjection
  | RunProjectionDraft
  | RequestWritePreview
  | SubmitProposal
  | ApproveProposal
  | ExecuteManagedCanonicalWrite
  | ExecuteSourceNativeWrite
  | ExportData
  | ReadAuditTrail
```

### 3.3 Policy Outcome

```text
PolicyOutcome = Allow | Deny Reason | RequireReview ReviewRoute
```

### 3.4 Policy Inputs

- actor identity + role
- requested operation
- data scope
- consent / restriction metadata
- projection contract
- source authority model
- target environment (sandbox / production / export)

---

## 4. Consent Model

### 4.1 Scope

consent / restriction は人物だけでなく、artifact / space / group / external partner にも適用。

### 4.2 Default: Restricted Canonical Capture + 年度末 Opt-Out

- 年度中: restricted capture を蓄積
- 名寄せ / face / speaker resolution は filtering 精度向上の内部補助としてのみ使用
- review 完了前の data は experiment projection に入れない
- 年度末: opt-out 確認 → filtering basis 固定 → experiment 適用承認

### 4.3 Opt-Out Strategies

| Strategy | Meaning | Use |
|---|---|---|
| Drop | Projection から完全除外 | 強い削除要求 |
| Anonymize | 不可逆変換 | 研究公開用 |
| Pseudonymize | 可逆変換、限定的に継続 | 倫理委員会管理下 |

### 4.4 Incidental Capture

写真・動画・音声の incidental capture は避けられない。capture と serve の policy は分離:
- capture 時: restricted 保持可
- derive 時: capability + purpose 確認、名寄せ補助は filtering quality 向上に限定
- experiment approval 時: 年度末確認 + filtering basis 固定を必須化
- serve/export 時: filtering projection 必須

### 4.5 Identity Resolution Confidence Thresholds

| Confidence | Materialization rule | Operational projection | Academic / published projection | Review |
|---|---|---|---|---|
| High | `resolved_persons` へ自動昇格可 | filtering 後に利用可 | filtering basis 固定後に利用可 | 追加 review 不要 |
| Medium | `resolution_candidates` に留める | reviewer が承認した場合のみ `resolved_persons` へ昇格 | **自動利用禁止** | manual review 必須 |
| Low | candidate のまま保持 | merge 不可 | 利用禁止 | 必要時のみ調査 |

補足:
- `resolution_candidates.status = pending` の行は published/shared Projection に入力してはならない
- Medium confidence を `resolved_persons` に昇格させる操作は approval trace に残す

---

## 5. Role & Capability Matrix

### 5.1 Human Roles

| Role | Registry | Lake Read | Projection | Write | Export |
|---|---|---|---|---|---|
| System Admin | Full | Controlled full | All | Managed only | Managed only |
| Researcher | Read + register | Restricted/filtered | Own + shared | Proposal/approved | Scope-limited |
| Resident | Limited read | Own only | Approved views | Own-facing | Usually no |
| External | Catalog only | None | Published only | None | Published only |

### 5.2 Agent Capabilities

| Capability | Allowed? | Notes |
|---|---|---|
| Search registry / catalog | Yes | authoring に必要 |
| Run dry-run build | Yes | sandbox 内 |
| Generate spec / SQL draft | Yes | publish 前提ではない |
| Request write preview | Yes | effect plan 確認 |
| Submit proposal | Yes | canonical 化前の案 |
| Read raw secrets | **No** | |
| Unrestricted Lake read | **No** | selector 限定 |
| Canonical write without approval | **No** | managed route 必須 |
| Free external network during build | **No** | sandbox default deny |

---

## 6. Write Review Model

### 6.1 Review Matrix

| Write Mode | Authority | Default Policy |
|---|---|---|
| Canonical | LakeAuthoritative | managed projection or human-approved |
| Canonical | SourceAuthoritative | stable anchor + base revision + approval |
| Canonical | DualReference | precedence matrix 必須 |
| Annotation | Any | scoped self-service |
| Proposal | Any | default allowed, publication blocked until review |

### 6.1.1 DualReference Evaluation Precedence

| Condition | Effect plan | Rationale |
|---|---|---|
| stable source anchor あり + `baseRevision` あり + lossless inverse 可能 | `InvokeSourceNative` | live authority と整合 |
| source-native へ戻す必然はない + correction/retraction を Lake append で lossless に表現できる | `AppendCanonical` | append-only replay を保持 |
| stable anchor 不明 / inverse が曖昧 / destructive effect あり | `SubmitReview` | 自動 route 禁止 |

### 6.2 Mandatory Review Triggers

- high-sensitivity data export
- external publication
- first experimental use of person-related cohort data after year-end review
- irreversible delete / crypto-shred
- source-native write with destructive effect
- canonical write without stable anchor
- conflict resolution after stale base revision
- medium-confidence identity candidate の published/shared projection への昇格

### 6.3 Approval Trace

最低限残す: requester, approver, reason, confirmation cohort, source data scope, filtering basis, effect plan, execution result, timestamp

### 6.4 Write Approval SLA

- approval 完了済みの canonical / source-native write は、`operational-latest` read で **60 秒以内** に観測可能でなければならない
- poll-based propagation を使う場合、対象 Projection の poll interval は `<= 30s`
- 監視メトリクスとして `approval_to_projection_freshness_p99` を持つ

---

## 7. Data Classification & Secret Handling

### 7.1 Classification

| Class | Rule |
|---|---|
| Public | catalog / published projection で公開可 |
| Internal | authenticated users のみ |
| Restricted | filtering projection + audit 必須 |
| HighlySensitive | encryption + approval + purpose limitation 必須 |

### 7.2 Secret Management

- source credential は secret manager へ
- actor / agent に生 credential を見せない
- short-lived token 優先
- rotation event を audit

---

## 8. Retention & Takedown

### 8.1 Normal Retraction

retraction observation / filtering projection で吸収。append-only 原則は保持。

### 8.2 Emergency Suppression

1. projection serve stop
2. blob access suppression
3. export disable
4. audit trail preservation

### 8.3 Takedown Ladder

| Level | Action |
|---|---|
| Level 1 | Projection filtering / serve deny |
| Level 2 | blob ref disable / access suppression |
| Level 3 | crypto-shred or blob delete |

### 8.4 Data Retention Defaults

| Data | Default | Override |
|---|---|---|
| Lake Observations | 永続 | IRB 条件で短縮可 |
| Binary Attachments | 5年 | Projection 参照中は延長 |
| Projection Snapshots | Archive まで | DOI 付与済みは永続 |
| Consent Records | 永続 | 法的要件 |

---

## 9. Audit Events

最低限記録:
- read of restricted data
- export
- write preview / execution
- publish
- approval / rejection
- secret rotation
- takedown / physical delete

---

## 10. Policy Evaluation Sketch

```text
evaluateRequest(request, context) =
  request
  |> classifyOperation
  |> loadRelevantPolicies
  |> checkConsentAndScope
  |> checkCapabilities
  |> checkReviewRequirement
  |> emit(Allow | Deny | RequireReview)
```

---

## 11. MVP Governance Scope

MVP では以下のみ実装:
- internal-only access (全ユーザーが Internal ロール)
- restricted capture flag (boolean)
- 最小 audit log (write / export イベント)
- filtering projection の stub

MVP で実装しないもの:
- full role-based access control
- agent capability enforcement
- 年度末 opt-out batch
- external publication review

---

## 12. Module Interface

### Provides

- Policy evaluation engine
- Capability check API
- Consent status lookup
- Audit event emitter
- Filtering projection framework
- Identity confidence threshold policy
- DualReference precedence matrix
- Write approval SLA metadata

### Requires

- M01 Domain Kernel: PolicyDecision, ReviewStatus, AccessScope, Capability
