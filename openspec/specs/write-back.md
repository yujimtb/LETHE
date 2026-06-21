# M07: Write-Back

**Module:** write-back
**Scope:** Command algebra / write paths / writable projections / concurrency control
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M05 Projection Engine, M08 Governance
**Parent docs:** [System overview](../../docs/architecture/system-overview.md) §12–13, [Domain algebra](../../docs/architecture/domain-algebra.md) §6
**Agent:** Spec Designer (command algebra) → Implementer (adapter 実装) → Reviewer (authority 検証)
**MVP:** — (MVP 外。MVP+2 で実装)

---

## 1. Module Purpose

Projection 上の UI / API / agent からの変更を、Lake-mediated または source-native 経路で正しくルーティングする。
Command の正規化と EffectPlan への変換を責務とする。

---

## 2. Write Paths

### 2.1 Lake-Mediated Write-Back

内部ドメイン事実の追加・修正・撤回。

```
UI 操作 → Command → Ingestion Gate → Lake (append) → Projector rebuild → Projection 更新
```

### 2.2 Source-Native Write-Back

mutable external source への編集。

```
UI 操作 → Command → Write Adapter → Source-native API → Crawler re-capture → Lake append
```

### 2.3 Selection Rule

| 条件 | Write Path |
|---|---|
| 内部ドメイン事実 | Lake-mediated |
| mutable external source | Source-native |
| lossless inversion 不可 | proposal / annotation に降格 |

---

## 3. Normalized Commands

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

### 3.1 Command Schema

```text
Command =
  { commandId          : CommandId
  , issuedBy           : ActorRef
  , issuedFrom         : CommandSurface    -- "gui", "api", "agent", "batch"
  , writeMode          : WriteMode
  , subject            : EntityRef?
  , target             : EntityRef?
  , payload            : Json
  , baseRevision       : RevisionAnchor?
  , idempotencyKey     : IdempotencyKey
  , projectionContext  : ProjectionContext?
  }
```

### 3.2 ProjectionContext

```text
ProjectionContext =
  { projectionId    : ProjectionRef
  , visibleRowHash  : Hash
  , writeMode       : WriteMode
  }
```

---

## 4. Write Mode Mapping

| Write Mode | Command Types | Primary Persistence |
|---|---|---|
| Canonical | CreateFact, CorrectFact, RetractFact | Lake append or source-native revision |
| Annotation | AttachAnnotation | Supplemental or annotation observation |
| Proposal | SubmitProposal, Approve, Reject | Review queue + proposal record |

### 4.1 Derive Write Plan

```text
deriveWritePlan(command, context) =
  case (command.writeMode, context.authorityModel) of
    (Canonical, LakeAuthoritative)     → AppendCanonical(...)
    (Canonical, SourceAuthoritative)   → InvokeSourceNative(...)
    (Canonical, DualReference)         → deriveDualReferencePlan(command, context)
    (Annotation, _)                    → AppendSupplemental(...)
    (Proposal, _)                      → SubmitReview(...)

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

### 4.2 DualReference Decision Matrix

| Situation | Result |
|---|---|
| source revision が既知で live authority を更新すべき | Source-native write |
| Lake correction/retraction として lossless に表現可能 | Lake append |
| stable anchor 不明 / inverse が曖昧 / mixed side effect | Proposal + review |

---

## 5. Concurrency Control

**楽観的ロック (OCC) を標準。**

### 5.1 Lake-Mediated

1. User が Projection を読む → `visibleRowHash` 取得
2. User が編集 → Command 発行 (`visibleRowHash` 添付)
3. Write Gate が現在状態と比較
4. 一致 → accept → Lake append → rebuild
5. 不一致 → `ConflictFailure` → 最新状態を提示

### 5.2 Source-Native Write-Back

1. Write Adapter が `baseRevision` 添付で source API 呼び出し
2. revision conflict → 最新取得 → auto merge 判定
3. field-level で衝突なし → merge → 再送
4. 衝突あり → `ConflictFailure` → User に返却

**自動 rebase は annotation mode に限定。canonical mode は常にユーザー確認。**

---

## 6. Writable Projection Spec Extension

```yaml
writeBack:
  enabled: true
  mode: "canonical"                    # canonical | annotation | proposal
  acceptedCommands:
    - "assign-room"
    - "change-room-assignment"
  inverseMapping:
    adapter: "./projections/{name}/write_adapter.py"
  reviewPolicy:
    required: boolean
  lineage:
    captureProjectionContext: true
    captureVisibleRowHash: true
```

全 Projection は **default read-only**。`writeBack.enabled: true` を明示したもののみ writable。

---

## 7. Insert / Update / Delete Semantics

| UI Operation | Lake-Mediated | Source-Native |
|---|---|---|
| Insert | 新 Observation 追加 | source-native create |
| Update | correction Observation 追加 | source-native update |
| Delete | retraction Observation 追加 | source-native delete/archive |

「行を消す」「行を書き換える」は Ground Truth の破壊的変更を意味しない。

---

## 8. Composite Projection Insert Policy

| 判定 | Mode |
|---|---|
| Lossless inversion possible | canonical |
| Derived annotation only | annotation |
| Ambiguous semantics | proposal |
| No valid inversion | **reject** |

---

## 9. Non-Lossless Operation Guard

以下を満たさない編集は canonical として受理しない → proposal に降格:
- stable anchor が特定できない
- base revision が不明
- deterministic 逆変換不可
- authority model に反する

---

## 10. Mutable Multimodal Source Write-Back

基本方針:
- default **read-only**
- source-native operation に **損失なく** 逆変換できる場合のみ write-back 許可
- 画像 / LLM 解釈のみに依存する編集は canonical にしない

Google Slides 許可例:
- 既知 `objectId` の text 更新
- shape/image 属性更新
- slide 並び替え
- speaker notes 更新

proposal に降格する例:
- 画像だけ見て「タイトル追加してほしい」
- 対応 slide object を一意に特定不可
- LLM 解釈前提の自由レイアウト変更

---

## 11. Invariants

| # | Invariant | Law |
|---|---|---|
| 1 | Projection materialization への直接 write 禁止 | L5 No Direct Mutation |
| 2 | canonical write は authority model で正当化 | L4 Explicit Authority |
| 3 | 受理 write は再投影後に反映 | L9 Put-Then-Get |
| 4 | 同一 idempotencyKey で二重化しない | L8 Idempotency |
| 5 | 自動 rebase は annotation mode のみ | concurrency safety |

---

## 12. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | DualReference + stable anchor + baseRevision | `InvokeSourceNative` | |
| 2 | DualReference + no source anchor + lake correction 可 | `AppendCanonical` | |
| 3 | DualReference + ambiguous inverse mapping | `SubmitReview` | |
| 4 | stale `visibleRowHash` | `ConflictFailure` | Lake-mediated |
| 5 | stale `baseRevision` | `ConflictFailure` | source-native |

---

## 13. Module Interface

### Provides

- Command normalizer
- Write plan deriver
- Conflict detector (OCC)
- Write adapter framework
- Writable Projection spec extension

### Requires

- M01 Domain Kernel: Command, EffectPlan, WriteMode, ConflictFailure
- M02 Registry: authority model lookup
- M03 Observation Lake: append API
- M05 Projection Engine: rebuild trigger
- M08 Governance: review requirement, DualReference precedence
