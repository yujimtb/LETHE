# LETHE Domain Algebra

## Purpose

この文書は、`system-overview.md` の意味論的な核を補強するための補助仕様です。
主に **型、law、失敗モデル、write の正規化、storage の意味境界** を扱います。

`system-overview.md` が全体像と各レイヤの関係を説明する親文書であるのに対し、この文書は「何を純粋関数として扱い、どこで副作用を起こし、どの条件を満たせば設計が壊れないか」を明示します。

---

## 1. Design Posture

### 1.1 Functional Core / Imperative Shell

LETHE は次の原則で読むと最も安定します。

- **Functional Core:** Observation / Projection / Command / Policy を純粋関数で解釈する
- **Imperative Shell:** source access、blob 保存、DB materialize、API 呼び出し、job 実行を副作用境界に押し出す

この分離により、次の利点が得られます。

- academic-pinned の再現性を仕様として説明しやすい
- write-back の責務を UI ではなく command algebra に落とせる
- source adapter の差し替えが意味論を壊しにくい
- sandbox / agent / GUI を増やしてもコア仕様を再利用できる

### 1.2 Normative vs Extensible

本仕様では、すべてを固定しません。  
以下を区別します。

| 区分 | 例 | 期待される安定度 |
|---|---|---|
| **Closed algebra** | `AuthorityModel`, `WriteMode`, `ReadMode` | 高い |
| **Open registry** | `EntityType`, `Schema`, `Observer`, `Projection` | 拡張可能 |
| **Replaceable adapter** | Google Workspace / Calendar, Notion, Figma, Canva, Slack, sensor, storage adapter | 交換可能 |
| **Reference implementation** | Kafka, MinIO, DuckDB, FastAPI | 非本質 |

---

## 2. Closed Algebras

### 2.1 Core Modes

```text
AuthorityModel
  = LakeAuthoritative
  | SourceAuthoritative
  | DualReference

CaptureModel
  = Event
  | Snapshot
  | ChunkManifest
  | Restricted

ReadMode
  = AcademicPinned
  | OperationalLatest
  | ApplicationCached

WriteMode
  = Canonical
  | Annotation
  | Proposal
```

### 2.2 Storage and Output Kinds

```text
ObservationKind
  = CanonicalObservation
  | SupplementalRecord
  | GovernanceRecord

ProjectionKind
  = PureProjection
  | CachedProjection
  | WritableProjection

MaterializationKind
  = SqlTables
  | Fileset
  | VectorIndex
  | GraphStore
  | HttpApi
```

### 2.3 Policy and Decision Types

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

### 2.4 Failure Classes

```text
FailureClass
  = ValidationFailure
  | PolicyFailure
  | ConflictFailure
  | DeterminismFailure
  | RetryableEffectFailure
  | NonRetryableEffectFailure
  | QuarantineFailure
```

---

## 3. Primary Domain Records

### 3.1 Observation

Observation は canonical capture の基本単位です。

```text
Observation =
  { id: ObservationId
  , schema: SchemaRef
  , schemaVersion: SemVer
  , observer: ObserverRef
  , sourceSystem: SourceSystemRef?
  , actor: EntityRef?
  , authorityModel: AuthorityModel
  , captureModel: CaptureModel
  , subject: EntityRef
  , target: EntityRef?
  , payload: Json
  , attachments: [BlobRef]
  , published: Timestamp
  , recordedAt: Timestamp
  , consent: ConsentRef?
  , idempotencyKey: IdempotencyKey
  , meta: Json
  }
```

Observation は **解釈前の capture 記録** であり、以下を原則として payload の責務に入れません。

- trust/confidence の解釈
- 名寄せの最終判断
- OCR / transcript / embedding のような派生意味付け
- serving 用の匿名化結果

それらは supplemental または Projection に逃がします。canonical ingest は `published <= recordedAt + PT10M` を既定受理条件とし、これを超える future timestamp は quarantine 対象とします。

### 3.1.1 Revisioned SaaS Snapshot Pattern

Google Workspace / Photos / Calendar、Notion、Figma、Canva のような revisioned SaaS source は、共通の Observation envelope を使いながら `payload` を次の形で持つことを推奨します。

```text
SaaSSnapshotPayload =
  { artifact:
      { provider: Text
      , service: Text
      , objectType: Text
      , sourceObjectId: Text
      , containerId: Text?
      , canonicalUri: Uri?
      }
  , revision:
      { sourceRevisionId: Text
      , sourceModifiedAt: Timestamp?
      , captureMode: Snapshot | Hybrid
      }
  , native: Json | BlobRef
  , relations: Json?
  , rights: Json?
  , temporal: Json?
  , attachmentRoles: Json?
  }
```

`native` には source API の object graph / block tree / event body / node tree を lossless に保持します。render / export / original binary は `Observation.attachments` に置き、`attachmentRoles` はそれらの意味付けに使います。OCR、transcript、embedding、caption、meeting summary、名寄せ候補のような解釈結果は canonical payload ではなく Supplemental へ置きます。

### 3.2 Supplemental Record

Supplemental Record は、再利用価値が高いが canonical truth ではない補助情報です。

```text
SupplementalRecord =
  { id: SupplementalId
  , kind: SupplementalKind
  , derivedFrom: InputAnchorSet
  , payload: Json
  , createdBy: ActorRef
  , createdAt: Timestamp
  , mutability: AppendOnly | ManagedCache
  , recordVersion: Text?
  , consentMetadata: Json?
  , lineage: LineageRef
  }
```

典型例:

- transcript
- OCR text
- face / object detection
- embedding
- name resolution candidate
- confidence annotation
- source trust memo

### 3.3 Projection Spec

```text
ProjectionSpec =
  { id: ProjectionRef
  , version: SemVer
  , kind: ProjectionKind
  , sources: [ProjectionInputDecl]
  , readModes: [ReadModePolicy]
  , build: BuildSpec
  , outputs: [OutputSpec]
  , reproducibility: ReproducibilitySpec
  , writeBack: WriteBackSpec?
  }
```

### 3.4 Projection Input

```text
ProjectionInput
  = LakeInput ObservationSelector
  | SupplementalInput SupplementalSelector
  | SourceRevisionInput SourceRevisionSelector
  | ProjectionInputRef ProjectionVersionSelector
```

重要なのは、Projection が読むのは常に「テーブル」ではなく **宣言済み入力集合** だという点です。ManagedCache supplemental を academic-pinned で読む場合は version pin を宣言し、Lake と source-native を混在させる場合は reconciliation policy を宣言しなければなりません。

### 3.5 Command

UI や agent の操作は、まず command に正規化します。

```text
Command =
  { commandId: CommandId
  , issuedBy: ActorRef
  , issuedFrom: CommandSurface
  , writeMode: WriteMode
  , subject: EntityRef?
  , target: EntityRef?
  , payload: Json
  , baseRevision: RevisionAnchor?
  , idempotencyKey: IdempotencyKey
  , projectionContext: ProjectionContext?
  }
```

### 3.6 Effect Plan

Command をすぐ実行するのではなく、まず effect plan に落とします。

```text
EffectPlan
  = AppendCanonical [ObservationDraft]
  | AppendSupplemental [SupplementalDraft]
  | SubmitReview ReviewDraft
  | InvokeSourceNative [SourceRequest]
  | Materialize ProjectionBuildRequest
  | EmitAudit [AuditEvent]
```

この段階で、policy engine は効果の種類と必要承認を判断できます。

---

## 4. Storage Semantics

### 4.1 Semantic Boundary Table

| Store | Meaning | Default Mutability | Typical Content | Notes |
|---|---|---|---|---|
| **Lake** | canonical capture | append-only | Observation, source revision snapshot, chunk manifest | 解釈を入れすぎない |
| **Supplemental** | reusable but non-canonical derivation | append-only or managed cache | transcript, OCR, embeddings, resolution candidates | 再計算可能でも共有価値があるもの |
| **Source-native** | external authority | source-defined | Google Docs / Calendar, Notion, Figma, Canva, Slack, sensor backend | write-back はここへ行く場合がある |
| **Projection materialization** | derived view | replaceable | SQL tables, graph index, vector index, export files | Ground Truth ではない |
| **Draft workspace** | user editing surface | mutable | spreadsheets, notebooks, GUI drafts | publish まで正史にしない |

### 4.2 Placement Rules

以下を既定ルールとして扱います。

1. 一次資料の capture は Lake
2. 派生解釈で共有価値が高いものは Supplemental
3. live な authority は source-native のまま保持
4. user-facing な read は Projection / API / GUI
5. mutable 編集体験は Draft Workspace に閉じ込める

### 4.3 Placement Examples

| Case | Recommended Placement | Reason |
|---|---|---|
| Google Slides native structure | Lake snapshot | source revision を pin できる |
| Google Calendar event snapshot | Lake snapshot | recurrence / attendee state を pin できる |
| Notion page / database block tree | Lake snapshot | block relation と revision 状態を保持できる |
| Figma / Canva node tree | Lake snapshot + render attachment | native design graph と見た目を両方保持できる |
| Rendered PDF/PNG snapshot | Lake attachment | 人間可読な一次資料 |
| OCR text for slides | Supplemental | 再利用価値はあるが canonical ではない |
| Name resolution candidate | Supplemental or Projection | 解釈を含むため |
| Trust/confidence score | Supplemental or Projection | source truth ではないため |
| Resident-facing current roster | Projection | serving 用 view |

### 4.4 Supplemental Mutability Judgment Table

Supplemental record を AppendOnly と ManagedCache のどちらで運用するかは、以下の判定表に従う。**既定は AppendOnly** とする。

| 条件 | 分類 | 理由 |
|---|---|---|
| 再計算コストが高く、入力が pin 可能 | **AppendOnly** | lineage が安定し、academic-pinned で参照可能 |
| 再計算コストが高いが、入力が頻繁に変わる | **ManagedCache** + version tag | 最新を上書きしつつ version で辿れるようにする |
| 再計算コストが低い | **ManagedCache** または 都度再計算 | 保存しなくてもよい |
| academic-pinned Projection が参照する | **AppendOnly 必須** | pin された supplemental version を参照できなければ determinism が壊れる |

代表的な Derivation の推奨分類:

| Derivation | 推奨分類 | 理由 |
|---|---|---|
| ASR transcript | AppendOnly | 再計算コスト高、モデル version pin で再現可能 |
| OCR text | AppendOnly | 同上 |
| Embedding vector | ManagedCache + version tag | モデル更新で上書きしたい場合がある |
| Name resolution candidate | AppendOnly | 判断履歴の追跡が必要 |
| Face/object detection | AppendOnly | モデル version と結果の対応を保持したい |
| Live sensor rollup cache | ManagedCache | 鮮度優先、再計算容易 |

**academic-pinned の Projection が supplemental を読む場合は、必ず version-pinned read とする。** これにより、ManagedCache であっても academic 利用時には特定 version を参照できる。

---

## 5. Projection Semantics

### 5.1 Core Interpretation

academic-pinned の Projection は、概念的に次の式で表せます。

```text
ProjectionResult =
  inputs
  |> selectByReadMode(spec.readMode)
  |> pinOrResolve(spec.sources)
  |> sortDeterministically
  |> validateInputs(spec)
  |> foldl(applyInput, initialState(spec))
  |> finalize(spec)
```

ここで重要なのは次の 2 点です。

- `applyInput` と `finalize` は純粋関数であるべき
- source access や DB 書き込みはこの式の外で行うべき

### 5.2 Read Mode Meaning

| Read Mode | Determinism Requirement | Primary Goal |
|---|---|---|
| **AcademicPinned** | 必須 | 再現性、引用可能性 |
| **OperationalLatest** | 緩い | 鮮度、運用整合 |
| **ApplicationCached** | 中間 | 低コスト、高速応答 |

`OperationalLatest` が非決定的であっても構いません。  
ただし、その場合も **何を latest とみなしたか** を revision / timestamp / cache metadata として追跡可能にする必要があります。

### 5.3 Projection Kinds

| Kind | Meaning | Typical Use |
|---|---|---|
| **PureProjection** | すべての意味を入力集合と変換ロジックに閉じ込める | academic build |
| **CachedProjection** | serve 最適化のため materialization / cache を持つ | operational API |
| **WritableProjection** | write surface を持つが、内部では command に正規化する | GUI 編集面 |

### 5.4 Projection Build Contract

Projection build は少なくとも次を明示します。

- source declaration
- read mode policy
- deterministic input ordering rule
- supported schema / projection versions
- output contract
- lineage capture strategy
- failure mode

### 5.5 Source-Native Read Contract

Projection が source-native system を直接読む場合は、以下の契約に従う。

```yaml
# Projection Spec の sources 宣言に source-native read を明示的に書ける
sources:
  - ref: "source-native:sys:google-slides"
    readMode: "operational-latest"
    fallback: "lake-snapshot"          # source 不達時のフォールバック
    freshnessSla: "best-effort"
    lineageCapture: "timestamp-only"   # source から読んだ時刻を記録
```

Read Mode と source-native 直接読みの許可:

| Read Mode | Source-native 直接読み | 条件 |
|---|---|---|
| **AcademicPinned** | **禁止** | pin できないため。必ず Lake snapshot または pinned manifest を使う |
| **OperationalLatest** | **許可** | source が available なら直接読む。lineage には読取時刻を記録 |
| **ApplicationCached** | **Projection cache 優先** | cache miss 時は source-native に fallback 可 |

Fallback ladder:

1. Source-native latest (available なら)
2. Lake の最新 snapshot
3. Projection の前回 cache
4. Stale result + staleness warning

source-native を読んだ場合は、lineage に `sourceNativeRead { system, timestamp, revisionIfKnown }` を記録する。revision が取れない場合はタイムスタンプのみで可。Lake と source-native を同一 Projection で併用する場合は、`lake-first` / `source-latest` / `dual-track` の reconciliation policy を宣言し、未宣言なら registration を拒否する。

---

## 6. Command Algebra and Write Semantics

### 6.1 Normalized Commands

```text
WriteCommand
  = CreateFact
  | CorrectFact
  | RetractFact
  | AttachAnnotation
  | SubmitProposal
  | ApproveProposal
  | RejectProposal
  | InvokeSourceNativeChange
```

### 6.2 Basic Flow

```text
UI / Agent Action
  -> Normalize to Command
  -> Evaluate Policy
  -> Derive EffectPlan
  -> Interpret Effects
  -> Rebuild / Refresh Projection
```

この flow の利点は、GUI、API、agent、batch job のどれから来た操作でも同じ意味論で扱えることです。

### 6.3 Write Mode Mapping

| Write Mode | Typical Command | Primary Persistence |
|---|---|---|
| **Canonical** | `CreateFact`, `CorrectFact`, `RetractFact` | Lake append or source-native revision |
| **Annotation** | `AttachAnnotation` | Supplemental or annotation observation |
| **Proposal** | `SubmitProposal`, `ApproveProposal`, `RejectProposal` | Review queue + proposal record |

### 6.4 Derive Write Plan

```text
deriveWritePlan(command, context) =
  case (command.writeMode, context.authorityModel) of
    (Canonical, LakeAuthoritative) ->
      AppendCanonical(...)

    (Canonical, SourceAuthoritative) ->
      InvokeSourceNative(...)

    (Canonical, DualReference) ->
      deriveDualReferencePlan(command, context)

    (Annotation, _) ->
      AppendSupplemental(...)

    (Proposal, _) ->
      SubmitReview(...)

deriveDualReferencePlan(command, context) =
  if context.hasStableSourceAnchor
     and command.baseRevision != null
     and context.inverseMapping == "lossless"
     and context.destructiveSourceEffect == false
    then InvokeSourceNative(...)
  else if context.lakeCorrectionAllowed
    then AppendCanonical(...)
  else
    SubmitReview(...)
```

### 6.5 Non-lossless Operations

次の条件を満たさない編集は canonical として受理してはいけません。

- stable anchor が特定できない
- base revision が分からない
- deterministic な逆変換ができない
- authority model に反する

その場合は **proposal へ降格** します。

### 6.6 Concurrency and Conflict Resolution Protocol

同一リソースへの並行書き込みは、**楽観的ロック（Optimistic Concurrency Control）** を標準プロトコルとする。

#### Lake-mediated write:

1. User が Projection を読む → `visibleRowHash` を取得
2. User が編集 → Command 発行（`visibleRowHash` を添付）
3. Write Gate が Command 受理時に、現在の Projection 状態と `visibleRowHash` を比較
4. 一致 → accept → Lake append → rebuild
5. 不一致 → `ConflictFailure` を返す → User に最新状態を提示して再操作を促す

#### Source-native write-back:

1. Write Adapter が `baseRevision` を添付して source-native API を呼ぶ
2. Source API が revision conflict を返した場合:
   - Adapter が最新 revision を取得
   - 自動 merge 可能なら merge → 再送
   - 自動 merge 不可なら `ConflictFailure` を User に返す
3. 自動 merge の可否判定: field-level で衝突しなければ merge 可。同一 field への異なる変更は衝突

**自動 rebase は annotation mode に限定** し、canonical mode では常にユーザー確認を挟む。

---

## 7. System Laws and Invariants

### 7.1 Core Laws

1. **Append-Only Law**  
   Canonical Observation は破壊的更新しない。

2. **Replay Law**  
   同じ pin された入力集合からは同じ Projection 結果が得られる。

3. **Effect Isolation Law**  
   ドメイン解釈は adapter 固有の hidden state に依存しない。

4. **Explicit Authority Law**  
   すべての write は authority model を明示して正当化される。

5. **Provenance Completeness Law**  
   出力、書き込み、承認、削除理由は辿れる。

6. **Idempotency Law**  
   同一 idempotency key の再送は結果を二重化しない。  
   sharding 下では per-leaf exact index による**完全（決定的）冪等**に強化する（確率版は撤回）。idempotencyKey は per-message の `source:object_id:H(canonical_content)` で必須・解決可能。詳細は [Sharding design](sharding.md)。

7. **No Direct Mutation Law**  
   Projection materialization を正史として更新しない。

8. **Put-Then-Get Law**  
   受理された write は再投影後の view に反映される。

9. **Filtering-before-Exposure Law**  
   restricted data は capture 前ではなく、公開前 / 表示前に filtering projection を通す。

10. **Deterministic Interpretation Law**  
    academic-pinned の解釈は spec と入力で決まる。

### 7.2 Practical Implication

この law 群が守られていれば、技術スタックが変わっても設計意図は保たれます。  
逆に、stack が同じでも law を破れば LETHE ではなくなります。

---

## 8. Failure Model

### 8.1 Failure Taxonomy

| Failure Class | Meaning | Example | Default Handling |
|---|---|---|---|
| **ValidationFailure** | schema / shape が不正 | payload mismatch | reject or quarantine |
| **PolicyFailure** | consent / access / review 条件違反 | no consent, forbidden export | deny + audit |
| **ConflictFailure** | base revision 競合 | stale source-native update | reject and refresh |
| **DeterminismFailure** | replay 不一致 | nondeterministic projector | block registration |
| **RetryableEffectFailure** | effect が一時失敗 | network timeout, lock wait | retry with backoff |
| **NonRetryableEffectFailure** | 永続的な adapter failure | malformed source request | fail and surface |
| **QuarantineFailure** | 取り込み保留が必要 | partially corrupted record | isolate for review |

### 8.2 Error-Carrying Results

なるべく例外で流すのではなく、意味上の結果として扱います。

```text
IngestResult
  = Ingested ObservationId
  | Duplicate ExistingObservationId
  | Rejected ValidationFailure
  | Quarantined QuarantineTicket

PolicyResult
  = Allow
  | Deny PolicyFailure
  | RequireReview ReviewTask
```

### 8.3 Human Review Boundary

以下は review に乗せる価値があります。

- ambiguous identity resolution
- irreversible external deletion
- canonical write without stable anchor
- high-sensitivity export
- policy downgrade with legal or ethics impact

---

## 9. Extension Points and ADR Dependencies

現時点で open なのは「型体系」そのものより、各型の運用境界です。

特に次が ADR 対象です。

- multimodal canonicalization boundary
- supplemental mutability policy
- row-level lineage materialization policy
- projection API freshness examples
- source contract evolution workflow

詳細は [ADR backlog](../decisions/adr-backlog.md) を参照してください。
