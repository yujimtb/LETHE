# LETHE Governance and Capability Model

## Purpose

この文書は、`system-overview.md` の Governance & Ethics を、**policy evaluation と capability control** の観点から整理し直した補助仕様です。
特に、consent、access、filtering projection、agent capability、write review、retention、secret management を一つの枠組みで扱います。

---

## 1. Governance Principles

1. **Capture before interpretation**  
   一次資料の capture はできるだけ保ち、解釈や匿名化は serving 前に行う。

2. **Filtering before exposure**  
   restricted data は capture 時に潰すのではなく、表示・共有・export の前で制御する。

3. **Explicit authority**  
   どの write が Lake に行くか、どの write が source-native に行くかを隠さない。

4. **Least privilege**  
   人間・agent・service のすべてに capability-scoped な権限を与える。

5. **Auditable decisions**  
   deny、approve、export、delete、publish は理由付きで追跡可能にする。

---

## 2. Policy Objects

### 2.1 Core Policy Types

```text
AccessScope
  = Public
  | Internal
  | Restricted
  | HighlySensitive

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

PolicyOutcome
  = Allow
  | Deny Reason
  | RequireReview ReviewRoute
```

### 2.2 Policy Inputs

policy engine は少なくとも次を受け取ります。

- actor identity
- actor role
- requested operation
- data scope
- consent / restriction metadata
- projection contract
- source authority model
- target environment (sandbox / production / export)

---

## 3. Consent and Restriction Model

### 3.1 Consent Is Not Only Person-Centric

LETHE では consent / restriction を人物だけに閉じません。  
マルチモーダル系では、artifact 単位・space 単位・group 単位の制約も必要になります。

対象例:

- person consent
- artifact rights
- space policy
- external partner agreement
- model training restriction

### 3.2 Filtering Projection Principle

以下を既定方針にします。

- Lake には restricted canonical capture を置いてよい
- ただし、生データ表示や export の前に filtering projection を必ず通す
- 顔認識、speaker identification、name resolution などの補助機能は filtering projection を支援するために使ってよい

### 3.3 Year-End Opt-Out Review

LETHE の既定は、person-related experimental use を upfront opt-in ではなく **年度末 opt-out 確認** で判定することです。年度の進行中は restricted canonical capture を蓄積し、名寄せ・face / speaker resolution は filtering 精度向上のための内部補助としてのみ使います。review 完了前の data は experiment projection に入れてはなりません。

### 3.3.1 Opt-Out Strategies

| Strategy | Meaning | Typical Use |
|---|---|---|
| **Drop** | Projection から完全除外 | 強い削除要求 |
| **Anonymize** | 不可逆変換で利用継続 | 研究公開用 |
| **Pseudonymize** | 可逆変換で限定的に継続 | 倫理委員会管理下 |

### 3.4 Incidental Capture

写真・動画・音声では incidental capture が避けられません。  
そのため、capture と serve の policy は分けて扱います。

- capture 時: restricted として保持可
- derive 時: capability と purpose を確認し、名寄せ補助は filtering quality 向上目的に限定する
- experiment approval 時: 年度末 opt-out 確認と filtering basis の固定を必須化
- serve/export 時: filtering projection を必須化

### 3.5 Identity Resolution Confidence Thresholds

| Confidence | Materialization rule | Operational projection | Academic / published projection | Review |
|---|---|---|---|---|
| **High** | `resolved_persons` へ自動昇格可 | filtering 後に利用可 | filtering basis 固定後に利用可 | 追加 review 不要 |
| **Medium** | `resolution_candidates` に留める | reviewer が承認した場合のみ `resolved_persons` へ昇格 | **自動利用禁止** | manual review 必須 |
| **Low** | candidate のまま保持 | merge 不可 | 利用禁止 | 必要時のみ調査 |

補足:
- `resolution_candidates.status = pending` の行は published/shared Projection に入力してはならない
- Medium confidence を `resolved_persons` に昇格させる操作は approval trace に残す

---

## 4. Role and Capability Matrix

### 4.1 Human Roles

| Role | Registry | Lake Read | Projection | Write | Export |
|---|---|---|---|---|---|
| **System Admin** | Full | Controlled full access | All | Managed only | Managed only |
| **Researcher** | Read + register | Restricted / filtered | Own + shared | Proposal / approved modes | Scope-limited |
| **Resident** | Limited read | Own only via policy | Approved views only | Own-facing actions | Usually no |
| **External** | Catalog only | None | Published projections only | None | Published only |

### 4.2 Agent Capabilities

agent は user の代理ではありますが、**自動的に同等権限にはしません**。  
特に、Projection playground を中心にした capability model を採ります。

| Capability | Allowed? | Notes |
|---|---|---|
| Search registry | Yes | spec authoring に必要 |
| Search projection catalog | Yes | DB on DBs の探索に必要 |
| Run dry-run build | Yes | sandbox で実行 |
| Generate spec / SQL / projector draft | Yes | publish 前提ではない |
| Request write preview | Yes | effect plan を確認させる |
| Submit proposal | Yes | canonical 化前の案として扱う |
| Read raw secrets | No | secret manager 経由でも不可 |
| Unrestricted Lake read | No | 必要な selector のみに限定 |
| Canonical write without approval | No | managed route 必須 |
| Free external network during build | No | sandbox default deny |

### 4.3 Preferred Agent Surface

agent の基本面は以下です。

- sandbox GUI
- Projection authoring workflow
- draft spec lint
- dry-run build
- proposal submission

Lake 直接編集や unrestricted raw browse は避けます。

---

## 5. Write Review Model

### 5.1 Review Matrix

| Write Mode | Authority | Default Policy |
|---|---|---|
| **Canonical** | LakeAuthoritative | managed projection or human-approved route |
| **Canonical** | SourceAuthoritative | stable anchor + base revision + approval |
| **Canonical** | DualReference | precedence matrix required |
| **Annotation** | Any | scoped self-service allowed |
| **Proposal** | Any | default allowed, publication blocked until review |

### 5.1.1 DualReference Evaluation Precedence

| Condition | Effect plan | Rationale |
|---|---|---|
| stable source anchor あり + `baseRevision` あり + lossless inverse 可能 | `InvokeSourceNative` | live authority と整合 |
| source-native へ戻す必然はない + correction/retraction を Lake append で lossless に表現できる | `AppendCanonical` | append-only replay を保持 |
| stable anchor 不明 / inverse が曖昧 / destructive effect あり | `SubmitReview` | 自動 route 禁止 |

### 5.2 Mandatory Review Triggers

以下は review 必須候補です。

- high-sensitivity data export
- external publication
- first experimental use of person-related cohort data after year-end review
- irreversible delete / crypto-shred
- source-native write with destructive effect
- canonical write without stable anchor
- conflict resolution after stale base revision
- medium-confidence identity candidate の published/shared projection への昇格

### 5.3 Approval Trace

approval では少なくとも次を残します。

- requester
- approver
- reason
- confirmation cohort / review window
- source data scope
- filtering basis (resolution graph / model version)
- generated effect plan
- final execution result
- timestamp

### 5.4 Write Approval SLA

- approval 完了済みの canonical / source-native write は、`operational-latest` read で **60 秒以内** に観測可能でなければならない
- poll-based propagation を使う場合、対象 Projection の poll interval は `<= 30s`
- 監視メトリクスとして `approval_to_projection_freshness_p99` を持つ

---

## 6. Secret and Sensitive Data Handling

### 6.1 Secret Management

既定方針:

- source credential は secret manager へ置く
- actor や agent に生 credential を見せない
- short-lived token を優先する
- rotation event を audit する

### 6.2 Data Classification

| Class | Typical Rule |
|---|---|
| **Public** | catalog or published projection で公開可 |
| **Internal** | authenticated users のみ |
| **Restricted** | filtering projection と audit を必須化 |
| **HighlySensitive** | encryption, approval, purpose limitation を必須化 |

### 6.3 Audit Events

最低限残したいイベント:

- read of restricted data
- export
- write preview
- write execution
- publish
- approval / rejection
- secret rotation
- takedown / physical delete

---

## 7. Retention, Retraction, and Takedown

### 7.1 Normal Retraction

通常の取り下げは retraction observation や filtering projection で吸収します。  
append-only 原則は保ちます。

### 7.2 Emergency Suppression

機微事故や運用事故では、まず exposure を止めます。

- projection serve stop
- blob access suppression
- export disable
- audit trail preservation

### 7.3 Physical Delete

physical delete は例外扱いです。  
現時点の方向性としては、blob 実体のみを対象にするのが最も安全です。

### 7.4 Consent Cascade to Supplemental Derivation

consent 撤回時の Supplemental Derivation Store への影響は、以下の方針で処理します。

**基本原則: Supplemental record は削除せず、Filtering Projection で除外する。**

理由:

- Supplemental record を削除すると、実際に opt-out すべきデータかどうかの判断根拠が失われる
- lineage の追跡が困難になる
- filtering projection での除外が、レイヤ分離の観点から最もクリーンである

Consent 撤回時の処理フロー:

| 層 | 対象 | 撤回時の処理 |
|---|---|---|
| **Lake** | Canonical Observation | retraction flag を付与。physical delete は blob のみ |
| **Supplemental** | transcript, embedding, face detection 等 | **保持する**。retracted Observation との関連を metadata で記録 |
| **Projection** | materialized views | filtering projection で当該 subject を除外 |

Supplemental 上の record は `derivedFrom` で元 Observation を参照しているため、retracted Observation に紐づく supplemental を自動的に特定できます。Projection build 時に filtering projection がこの情報を使い、opt-out 対象の supplemental 由来データを exposure から除外します。

最低限残す metadata 例:

```text
ConsentMetadata =
  { referencedObservationId
  , retractedAt?
  , optOutStrategy
  , optOutEffectiveAt?
  }
```

### 7.5 Takedown Ladder

| Level | Action |
|---|---|
| **Level 1** | Projection filtering / serve deny |
| **Level 2** | blob ref disable / access suppression |
| **Level 3** | crypto-shred or blob delete |

---

## 8. Policy Evaluation Sketch

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

ポイントは、policy engine が DB 実装や source adapter に依存せず、**意味上の操作**を受け取ることです。

---

## 9. Relationship to Open ADRs

現在 open な論点:

- agent approval tier
- row-level lineage と policy audit の接続粒度
- export 契約の具体例
- 年度末確認 batch の UI / 運用手順

詳細は [ADR backlog](../decisions/adr-backlog.md) を参照してください。
