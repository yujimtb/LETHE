# M01: Domain Kernel

**Module:** domain-kernel
**Scope:** 型定義・closed algebra・system laws・failure model・storage semantics boundary
**Dependencies:** なし（全モジュールの基盤）
**Parent docs:** [Domain algebra](../../docs/architecture/domain-algebra.md), [System overview](../../docs/architecture/system-overview.md) §0.4
**Agent:** Spec Designer (定義) → Reviewer (law 整合性検証)

---

## 1. Module Purpose

LETHE のすべてのモジュールが依存する **意味論的基盤** を定義する。
ここで定義される型・law・failure model は、他の全モジュールの設計と実装の制約条件となる。

---

## 2. Design Posture

### 2.1 Functional Core / Imperative Shell

- **Functional Core:** Observation / Projection / Command / Policy を純粋関数で解釈する
- **Imperative Shell:** source access、blob 保存、DB materialize、API 呼び出し、job 実行を副作用境界に押し出す

### 2.2 Normative vs Extensible

| 区分 | 例 | 安定度 |
|---|---|---|
| **Closed algebra** | `AuthorityModel`, `WriteMode`, `ReadMode` | 高い — 変更は ADR 必須 |
| **Open registry** | `EntityType`, `Schema`, `Observer`, `Projection` | 拡張可能 |
| **Replaceable adapter** | Google, Slack, Figma, sensor, storage | 交換可能 |
| **Reference impl** | Kafka, MinIO, DuckDB, FastAPI | 非本質 |

---

## 3. Closed Algebras

### 3.1 Core Modes

```text
AuthorityModel
  = LakeAuthoritative        -- 正史が Lake にある
  | SourceAuthoritative       -- 正史が source-native system にある
  | DualReference             -- 両方を参照

CaptureModel
  = Event                     -- 個別イベント
  | Snapshot                  -- revision snapshot
  | ChunkManifest            -- 高頻度系列の manifest
  | Restricted                -- consent 制約下の capture

ReadMode
  = AcademicPinned            -- 再現性必須、pin された入力のみ
  | OperationalLatest         -- 鮮度優先、latest 読み許可
  | ApplicationCached         -- cache 優先、低コスト

WriteMode
  = Canonical                 -- 正規ドメイン事実
  | Annotation                -- 注釈・ラベル・レビュー
  | Proposal                  -- 即時変換不可な提案
```

### 3.2 Storage and Output Kinds

```text
ObservationKind
  = CanonicalObservation
  | SupplementalRecord
  | GovernanceRecord

ProjectionKind
  = PureProjection            -- 入力と変換に閉じる
  | CachedProjection          -- materialization / cache を持つ
  | WritableProjection        -- write surface を持つ

MaterializationKind
  = SqlTables
  | Fileset
  | VectorIndex
  | GraphStore
  | HttpApi
```

### 3.3 Policy and Decision Types

```text
PolicyDecision
  = Allow
  | Deny PolicyError
  | RequireReview ReviewTask

ReviewStatus
  = Draft
  | PendingReview
  | Approved
  | Rejected
  | Superseded

CommandResult
  = Accepted EffectPlan
  | Rejected PolicyError
  | NeedsReview ReviewTask
```

### 3.4 Failure Classes

```text
FailureClass
  = ValidationFailure         -- schema / shape 不正
  | PolicyFailure             -- consent / access 違反
  | ConflictFailure           -- base revision 競合
  | DeterminismFailure        -- replay 不一致
  | RetryableEffectFailure    -- 一時的 adapter failure
  | NonRetryableEffectFailure -- 永続的 adapter failure
  | QuarantineFailure         -- 取り込み保留
```

---

## 4. Primary Domain Records

### 4.1 Observation

```text
Observation =
  { id              : ObservationId       -- UUID v7 (time-sortable)
  , schema          : SchemaRef
  , schemaVersion   : SemVer
  , observer        : ObserverRef
  , sourceSystem    : SourceSystemRef?
  , actor           : EntityRef?
  , authorityModel  : AuthorityModel
  , captureModel    : CaptureModel
  , subject         : EntityRef           -- PRIMARY: 何について
  , target          : EntityRef?          -- SECONDARY: 関連先
  , payload         : Json                -- schema-validated
  , attachments     : [BlobRef]
  , published       : Timestamp           -- event time (offset 付き ISO 8601)
  , recordedAt      : Timestamp           -- system ingestion time (UTC)
  , consent         : ConsentRef?
  , idempotencyKey  : IdempotencyKey
  , meta            : Json
  }
```

**EntityRef format:** `{type}:{id}` — 例: `person:tanaka-2026`, `room:A-301`

Observation は **解釈前の capture 記録** であり、payload に含めないもの:
- trust/confidence の解釈
- 名寄せの最終判断
- OCR / transcript / embedding
- 匿名化結果

### 4.2 SaaS Snapshot Payload Pattern

revisioned SaaS source (Google, Notion, Figma, Canva) 共通の payload 構造:

```text
SaaSSnapshotPayload =
  { artifact        : { provider, service, objectType, sourceObjectId, containerId?, canonicalUri? }
  , revision        : { sourceRevisionId, sourceModifiedAt?, captureMode }
  , native          : Json | BlobRef      -- source API object graph を lossless 保持
  , relations       : Json?               -- parent-child, attendee, backlink
  , rights          : Json?               -- visibility, sharing, owner
  , attachmentRoles : Json?               -- attachments の意味付け
  }
```

### 4.3 Supplemental Record

```text
SupplementalRecord =
  { id          : SupplementalId
  , kind        : SupplementalKind
  , derivedFrom : InputAnchorSet
  , payload     : Json
  , createdBy   : ActorRef
  , createdAt   : Timestamp
  , mutability  : AppendOnly | ManagedCache
  , recordVersion: string?
  , consentMetadata: Json?
  , lineage     : LineageRef
  }
```

### 4.4 Projection Spec

```text
ProjectionSpec =
  { id              : ProjectionRef
  , version         : SemVer
  , kind            : ProjectionKind
  , sources         : [ProjectionInputDecl]
  , readModes       : [ReadModePolicy]
  , build           : BuildSpec
  , outputs         : [OutputSpec]
  , reconciliation  : Json?
  , reproducibility : ReproducibilitySpec
  , writeBack       : WriteBackSpec?
  }
```

ManagedCache supplemental を `AcademicPinned` で読む場合は version pin を必須とし、Lake と source-native を併用する Projection は reconciliation policy を宣言する。

### 4.5 Command

```text
Command =
  { commandId          : CommandId
  , issuedBy           : ActorRef
  , issuedFrom         : CommandSurface
  , writeMode          : WriteMode
  , subject            : EntityRef?
  , target             : EntityRef?
  , payload            : Json
  , baseRevision       : RevisionAnchor?
  , idempotencyKey     : IdempotencyKey
  , projectionContext  : ProjectionContext?
  }
```

### 4.6 Effect Plan

```text
EffectPlan
  = AppendCanonical [ObservationDraft]
  | AppendSupplemental [SupplementalDraft]
  | SubmitReview ReviewDraft
  | InvokeSourceNative [SourceRequest]
  | Materialize ProjectionBuildRequest
  | EmitAudit [AuditEvent]
```

### 4.7 Error-Carrying Results

```text
IngestResult
  = Ingested ObservationId
  | Duplicate ExistingObservationId
  | Rejected { error: ValidationFailure | PolicyFailure }
  | Quarantined QuarantineTicket

PolicyResult
  = Allow
  | Deny PolicyFailure
  | RequireReview ReviewTask
```

---

## 5. System Laws

実装や運用を変えても、以下の law は **必ず守る**。

| # | Law | Meaning | Violation = |
|---|---|---|---|
| L1 | **Append-Only Law** | Canonical Observation を破壊的更新しない | DeterminismFailure / data loss |
| L2 | **Replay Law** | pin された同一入力 → 同一 Projection 結果 | DeterminismFailure |
| L3 | **Effect Isolation Law** | ドメイン解釈は hidden mutable state に依存しない | DeterminismFailure |
| L4 | **Explicit Authority Law** | すべての write は authority model で正当化 | PolicyFailure |
| L5 | **No Direct Mutation Law** | Projection materialization を正史として更新しない | append-only 違反 |
| L6 | **Filtering-before-Exposure Law** | restricted data は表示・配布前に filtering projection を通す | PolicyFailure |
| L7 | **Provenance Completeness Law** | 出力・書き込み・承認・削除理由は辿れる | 監査不能 |
| L8 | **Idempotency Law** | 同一 identity key の再送は per-leaf exact index で完全(決定的)に二重化しない。silent drop と確率的判定は禁止 | data duplication / silent loss |
| L9 | **Put-Then-Get Law** | 受理された write は再投影後の view に反映される | consistency 違反 |
| L10 | **Deterministic Interpretation Law** | academic-pinned の解釈は spec と入力で決まる | DeterminismFailure |
| L11 | **Temporal Ordering Law** | `published` は `recordedAt + MAX_CLOCK_SKEW` を超えて未来に出ない | ValidationFailure / QuarantineFailure |

### 5.1 Law Verification Checklist

Reviewer が各モジュール実装を検証する際の観点:

| Law | 検証方法 |
|---|---|
| L1 | Lake に UPDATE / DELETE SQL がないこと |
| L2 | 同一入力での replay test が通ること |
| L3 | projector が外部 state を参照していないこと |
| L4 | write path に authority model チェックがあること |
| L5 | Projection table への直接 INSERT/UPDATE がないこと |
| L6 | restricted data の exposure 前に filter 呼び出しがあること |
| L7 | audit event が write / approve / export で発行されること |
| L8 | `identity_key` NOT NULL UNIQUE を正規判定とし、衝突時は stored `canonical_json` のみを比較すること |
| L9 | write → rebuild → read の integration test があること |
| L10 | academic-pinned build がシード固定・外部非依存であること |
| L11 | `published <= recordedAt + MAX_CLOCK_SKEW` を gate で検証すること |

---

## 6. Failure Model

### 6.1 Failure Taxonomy

| Failure Class | Meaning | Example | Default Handling |
|---|---|---|---|
| ValidationFailure | schema / shape 不正 | payload mismatch | reject or quarantine |
| PolicyFailure | consent / access 違反 | no consent, forbidden export | deny + audit |
| ConflictFailure | base revision 競合 | stale source-native update | reject + refresh |
| DeterminismFailure | replay 不一致 | nondeterministic projector | block registration |
| RetryableEffectFailure | 一時 adapter failure | network timeout | retry with backoff |
| NonRetryableEffectFailure | 永続 adapter failure | malformed request | fail + surface |
| QuarantineFailure | 取り込み保留 | partially corrupted record | isolate for review |

### 6.2 Human Review Boundary

以下は必ず review に乗せる:
- ambiguous identity resolution
- medium-confidence identity candidate の公開 Projection への昇格
- irreversible external deletion
- canonical write without stable anchor
- high-sensitivity export
- policy downgrade with legal/ethics impact

### 6.3 Error Propagation Contract

| Boundary | Upstream outcome | Surface contract |
|---|---|---|
| M03 Ingestion Gate | `ValidationFailure` | `Rejected { error = ValidationFailure }` |
| M03 Ingestion Gate | `PolicyFailure` | `Rejected { error = PolicyFailure }` |
| M03 Ingestion Gate | `RequireReview` or temporal anomaly | `Quarantined { ticket = ... }` |
| M07 Write-Back | `ConflictFailure` | reject + 最新状態の refresh を要求 |
| M14 API Serving | `PolicyFailure` on read | deny または field masking。露出前に必ず surface する |

---

## 7. Storage Semantics Boundary

| Store | Meaning | Mutability | Content |
|---|---|---|---|
| **Lake** | canonical capture | append-only | Observation, snapshot, chunk manifest |
| **Supplemental** | reusable non-canonical | append-only or managed cache | transcript, OCR, embeddings |
| **Source-native** | external authority | source-defined | Google Docs, Slack, sensor backend |
| **Projection materialization** | derived view | replaceable | SQL tables, graph, vector, export |
| **Draft workspace** | editing surface | mutable | spreadsheets, notebooks, GUI drafts |

### 7.1 Placement Rules

1. 一次資料の capture → Lake
2. 共有価値の高い派生 → Supplemental
3. live authority → source-native
4. user-facing read → Projection / API
5. mutable 編集 → Draft Workspace

---

## 8. Time Model

- `published`: 元の offset 付き ISO 8601 をそのまま保存
- `recordedAt`: システムが UTC で付与
- Projection 時間計算は UTC 正規化
- 表示時はユーザーのローカルタイムに変換
- timezone-naive timestamp は非推奨（schema validation で offset 検証）

### 8.1 Event Ordering

Observation の適用順序:
1. `published` (Event Time)
2. `recordedAt` (System Time)
3. `id` (UUID v7 バイト順)

### 8.2 Temporal Validation Rule

- 既定の `MAX_CLOCK_SKEW` は `PT10M`
- `published <= recordedAt + MAX_CLOCK_SKEW` を canonical ingest の受理条件とする
- 閾値を超える future timestamp は `Quarantined` に回し、clock-skew 調査対象として扱う

---

## 9. Invariants for Implementers

本モジュールの型と law は以下のように実装に反映する:

- Closed algebra はそれぞれ enum / union type として実装し、exhaustive match を強制する
- Observation は immutable data class として実装する
- EffectPlan は Command から derive し、直接 IO を起こさない
- PolicyResult は例外ではなく戻り値として扱う
- FailureClass ごとに error handler を定義する

---

## 10. Module Interface

### Provides (他モジュールが参照するもの)

- Closed algebra の型定義（Python enum / dataclass）
- System Law の ID と検証ヘルパー
- Failure class の型と分類関数
- Observation / SupplementalRecord / Command / EffectPlan の base model
- EntityRef / BlobRef / SchemaRef 等の value object

### Requires

- なし（基盤モジュール）
