# M04: Supplemental Store

**Module:** supplemental-store
**Scope:** 派生情報ストアの意味論・mutability policy・placement rules
**Dependencies:** M01 Domain Kernel, M03 Observation Lake
**Parent docs:** [Domain algebra](../../docs/architecture/domain-algebra.md) §3.2, §4.3–4.4, [System overview](../../docs/architecture/system-overview.md) §6.3
**Agent:** Spec Designer (mutability policy) → Implementer (CRUD) → Reviewer (lineage 検証)

---

## 1. Module Purpose

再利用価値が高いが canonical truth ではない **補助情報** を保存・管理するストア。
Supplemental record は canonical Observation から派生し、multiple Projection から共有参照される。

---

## 2. Typical Contents

| Kind | Example | Mutability |
|---|---|---|
| transcript | 音声認識結果 | AppendOnly |
| ocr-text | OCR テキスト | AppendOnly |
| embedding | ベクトル表現 | ManagedCache + version tag |
| face-detection | 顔検出結果 | AppendOnly |
| name-resolution-candidate | 名寄せ候補 | AppendOnly |
| object-detection | 物体検出結果 | AppendOnly |
| chunk-summary | セグメント要約 | AppendOnly |
| live-sensor-rollup | ライブ集計キャッシュ | ManagedCache |

---

## 3. Record Schema

```text
SupplementalRecord =
  { id          : SupplementalId
  , kind        : SupplementalKind       -- "transcript", "ocr-text", etc.
  , derivedFrom : InputAnchorSet         -- 元 Observation / BlobRef
  , payload     : Json
  , createdBy   : ActorRef               -- pipeline / model / human
  , createdAt   : Timestamp
  , mutability  : AppendOnly | ManagedCache
  , recordVersion: string?               -- ManagedCache の version tag
  , modelVersion: string?                -- derivation model version
  , consentMetadata: Json?
  , lineage     : LineageRef
  }
```

### 3.1 InputAnchorSet

```text
InputAnchorSet =
  { observations : [ObservationId]
  , blobs        : [BlobRef]
  , supplementals: [SupplementalId]
  , sourceRevisions: [SourceRevisionAnchor]?
  }
```

---

## 4. Mutability Policy

**既定は AppendOnly。**

### 4.1 Judgment Table

| 条件 | 分類 | 理由 |
|---|---|---|
| 再計算コスト高 + 入力 pin 可能 | **AppendOnly** | lineage 安定、academic-pinned で参照可能 |
| 再計算コスト高 + 入力が頻繁に変化 | **ManagedCache** + version tag | 最新上書き + version 追跡 |
| 再計算コスト低 | **ManagedCache** or 都度再計算 | 保存しなくてもよい |
| academic-pinned Projection が参照 | **AppendOnly 必須** | determinism 保証 |

### 4.2 Representative Derivations

| Derivation | 推奨分類 | 理由 |
|---|---|---|
| ASR transcript | AppendOnly | 再計算コスト高、モデル version pin で再現可能 |
| OCR text | AppendOnly | 同上 |
| Embedding vector | ManagedCache + version tag | モデル更新で上書きしたい場合あり |
| Name resolution candidate | AppendOnly | 判断履歴の追跡が必要 |
| Face/object detection | AppendOnly | モデル version と結果の対応を保持 |
| Live sensor rollup cache | ManagedCache | 鮮度優先、再計算容易 |

### 4.3 Version-Pinned Read

**academic-pinned の Projection が supplemental を読む場合は、必ず version-pinned read とする。**
ManagedCache でも academic 利用時には特定 version を参照できる。

Version pin の最小単位:

```text
SupplementalVersionPin =
  { kind         : SupplementalKind
  , recordVersion: string
  , modelVersion : string?
  }
```

Projection spec からは `kind + recordVersion` を必須参照し、latest 読みは `operational-latest` のみ許可。

---

## 5. Consent Cascade

consent 撤回時の基本原則: **Supplemental record は削除せず、Filtering Projection で除外する。**

理由:
- 削除すると opt-out 判断根拠が失われる
- lineage 追跡が困難になる
- filtering projection での除外が最もクリーン

| 層 | 撤回時処理 |
|---|---|
| Lake | retraction flag 付与。physical delete は blob のみ |
| Supplemental | **保持**。retracted Observation との関連を metadata で記録 |
| Projection | filtering projection で subject 除外 |

### 5.1 Consent Metadata Contract

```text
ConsentMetadata =
  { referencedObservationId: ObservationId
  , retractedAt           : Timestamp?
  , optOutStrategy        : Drop | Anonymize | Pseudonymize
  , optOutEffectiveAt     : Timestamp?
  }
```

filtering projection は `consentMetadata` を参照して exposure を除外する。

---

## 6. API

| Method | Path | Description |
|---|---|---|
| POST | `/api/supplemental/records` | 新規 record 追加 |
| GET | `/api/supplemental/records` | filter by kind, derivedFrom, createdAt |
| GET | `/api/supplemental/records/{id}` | 個別取得 |
| GET | `/api/supplemental/records/{id}/versions` | version 一覧 (ManagedCache) |
| PUT | `/api/supplemental/records/{id}` | ManagedCache 上書き (version 自動採番) |
| GET | `/api/supplemental/by-observation/{obsId}` | 特定 Observation から派生した全 record |

---

## 7. Invariants

| # | Invariant | Verification |
|---|---|---|
| 1 | AppendOnly record は上書き不可 | PUT 拒否 |
| 2 | 全 record は derivedFrom を持つ | validation |
| 3 | academic-pinned 参照時は version-pinned | read path check |
| 4 | derivedFrom の Observation が存在する | referential integrity |
| 5 | ManagedCache は `recordVersion` を単調増加させる | version check |
| 6 | consent 撤回時は record を保持し metadata 更新 | delete 禁止 |

---

## 8. Acceptance Tests

| # | Input | Expected | Notes |
|---|---|---|---|
| 1 | AppendOnly transcript record 追加 | 成功 | |
| 2 | AppendOnly record に PUT | 拒否 (PolicyFailure) | |
| 3 | ManagedCache embedding 追加 | 成功 + version 1 | |
| 4 | ManagedCache embedding 上書き | 成功 + version 2 | |
| 5 | version-pinned read (version 1) | version 1 の内容 | |
| 6 | 存在しない Observation からの derivedFrom | ValidationFailure | |
| 7 | consent 撤回 metadata 更新 | record 保持 + `retractedAt` 設定 | |
| 8 | by-observation query | 関連 supplemental 全件 | |

---

## 9. Module Interface

### Provides

- Supplemental record CRUD API
- Version-pinned read API
- by-observation query API
- consent metadata query

### Requires

- M01 Domain Kernel: SupplementalRecord 型、mutability enum
- M03 Observation Lake: Observation 存在確認
