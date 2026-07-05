# Spec Delta: domain-kernel

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-18

## Dependencies

- M01 Domain Kernel — 既存 `openspec/specs/domain-kernel.md`(System Laws L1〜L8、Closed Algebras、Observation 型)は中核を **不変** とする
- 正典: [Domain algebra](../../../../../docs/architecture/domain-algebra.md) §7.1 System Laws、[Sharding design](../../../../../docs/architecture/sharding.md) §1.1 System Laws 対照表

> 本 delta は L1〜L7 を **変更しない**。L8 Idempotency Law の precision を「同一 key の再送は二重化しない」から「**完全(決定的)冪等** — per-leaf exact index による正規判定、silent drop なし」へ強化する。`Observation.idempotencyKey` の optional → 必須・高エントロピー・解決可能への格上げも併せて規定する。意味論は破らず、precision のみを上げる(`sharding_refactor.md` R7「中核方針は妥当 = 上書きでなく精緻化」)。

---

## MODIFIED Requirements

### Requirement: L8 Idempotency Law (Exact / Deterministic)

同一 `identity_key` の再送は **二重化されてはならない (SHALL NOT)**。判定は **per-leaf exact index**(`identity_key` UNIQUE B-tree)を正規(authoritative)とし、**完全(決定的)** で **なければならない (SHALL)**。確率的(Bloom + ε 取りこぼし)を冪等性の構成要素として用いてはならない (SHALL NOT)。

- `IngestResult = Ingested(id) | Duplicate(existing_id) | Rejected(...) | Quarantined(...)` の同型を維持し、**silent drop を許してはならない (SHALL NOT)**。
- 衝突時(同 `identity_key` で `canonical_json` 相違 = sha256 衝突)の比較対象は **hash 入力になった canonical タプル**(stored `canonical_json`)のみ。full observation(reactions / 編集 wrapper / ingestion メタ)を比較してはならない (SHALL NOT)。
- Bloom 等の確率的構造は **負パス最適化(β)としてのみ** 後付け可 (MAY)。正パスの判定根拠にはしない (SHALL NOT)。

詳細な実装契約は `observation-lake` delta の SHARD-04 を正規参照とする。

#### Scenario: 確率的 ε による silent drop が起きない

- **WHEN** Bloom filter で false positive が発生し「重複」と判定されかける
- **THEN** 正規判定は SQLite `identity_key` UNIQUE 違反のみが下し、Bloom 結果は最終判定に影響しない
- **AND** silent drop は発生しない

#### Scenario: reactions 変化が偽 `Conflict` を起こさない

- **WHEN** 同 `identity_key` で reactions のみ差分の Observation を再投入する
- **THEN** stored `canonical_json` と incoming canonical タプルが exact 一致するため `Duplicate(existing_id)` が返る
- **AND** `Quarantined`(`Conflict`)にならない

---

## ADDED Requirements

### Requirement: SHARD-DK-01 Observation idempotencyKey is Mandatory and Resolvable

`Observation.idempotencyKey` は **必須(NOT NULL)・高エントロピー・解決可能** で **なければならない (SHALL)**。optional / free-form の運用は廃止する。

- 構成: `identity_key = source : object_id : H(canonical_content)`(`observation-lake` delta SHARD-01)。
- adapter は `idempotencyKey` 文字列を直接組み立ててはならない (SHALL NOT)。`(object_id, canonical タプル)` を宣言し、core が H して組み立てる(`adapter-policy` delta SHARD-ADAPT-01)。
- `idempotencyKey` は **`Observation` の型上で必須フィールド** となり、`per-leaf SQLite の identity_key column の denormalize` として `observation_json` 内にも保持される(`observation-lake` delta SHARD-06)。

```text
Observation =
  { id              : ObservationId
  , schema          : SchemaRef
  , observer        : ObserverRef
  , sourceSystem    : SourceSystemRef
  , authorityModel  : AuthorityModel
  , captureModel    : CaptureModel
  , subject         : EntityRef?
  , target          : EntityRef?
  , payload         : Json
  , attachments     : [BlobRef]
  , published       : Timestamp
  , recordedAt      : Timestamp
  , consent         : ConsentRef?
  , idempotencyKey  : IdempotencyKey    -- ★ optional → 必須 (SHALL)
  , meta            : Json
  }
```

#### Scenario: optional な idempotencyKey は型レベルで拒否される

- **WHEN** adapter が `idempotencyKey` を欠いた Observation を生成する
- **THEN** 型エラー / validation で reject され、ingest は実行されない
- **AND** `idempotencyKey` の付与は adapter contract(`adapter-policy` delta SHARD-ADAPT-01 / SHARD-ADAPT-02)で保証される

---

## MODIFIED Behaviors (parent spec 参照)

既存 `openspec/specs/domain-kernel.md` の以下の節は、本 delta の要件で表現が変わる(他 Law は不変):

- **§4.1 Canonical Observation**: `idempotencyKey : IdempotencyKey?` → `idempotencyKey : IdempotencyKey`(SHARD-DK-01)。
- **§5 System Laws (L8 Idempotency Law)**: 「同一 idempotency key の再送は二重化しない」→ 「per-leaf exact index による完全(決定的)冪等、silent drop なし、衝突判定は canonical タプル exact compare のみ」(本 delta の L8 MODIFIED)。
- **§Failure Routing(参考表)**: `data duplication` の正規判定経路を per-leaf SQLite `identity_key` UNIQUE に集約。確率的判定からの遷移を不要にする。
