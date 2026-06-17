# Spec Delta: adapter-policy

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-17

## Dependencies

- M01 Domain Kernel — 型・law の正規参照
- M02 Registry — Source Contract / Schema Registry
- M03 Observation Lake — identity_key 構成(本 change の `observation-lake` delta SHARD-01 / SHARD-03)
- M09 Adapter Policy — 既存 `openspec/specs/adapter-policy.md` の中核(adapter 責務分離 / configuration / heartbeat / retry)は **不変**
- 正典: [sharding_refactor.md](../../../../sharding_refactor.md) §2 D1 / D3 / D3b / D12.4

> 本 delta は M09 の adapter 責務分離(OCR / 名寄せ / Projection materialization を adapter で行わない)を **変更しない**。`to_observations` の出力契約を「free-form `idempotencyKey` を直接返す」から「(object_id, canonical タプル) を宣言する」に置き換え、core が H して `identity_key` を組み立てる責務分担を明文化する。

---

## ADDED Requirements

### Requirement: SHARD-ADAPT-01 Canonical Tuple Declaration Contract

adapter は、各 Observation について **`(object_id, canonical タプル)` を宣言** **しなければならない (SHALL)**。`identity_key = source : object_id : H(canonical_content)` の構成において:

- **adapter の責務**: source 固有の `object_id` 抽出規則、および `canonical_content` を構成する固定 serialization(canonical タプル実体)の生成。
- **core の責務**: 渡された canonical タプルに対し固定 serialization → sha256(H)で `identity_key` を組み立て、`canonical_json` column に stored canonical タプル実体を保存。

adapter が直接 `idempotencyKey` 文字列を組み立てて返してはならない (SHALL NOT)(現行 `to_observations` 出力 [src/adapter/traits.rs](../../../../src/adapter/traits.rs)、[src/adapter/slack/mapper.rs:149](../../../../src/adapter/slack/mapper.rs) の content-hash 非含有契約は廃止される)。

#### canonical タプル境界(SHARD-03 と整合)

| 項目 | 扱い |
| --- | --- |
| `sender` | include |
| `body` | include(transport ノイズのみ正規化、ユーザ可視空白は保つ) |
| `event_time` | include(RFC3339 UTC 固定精度) |
| 添付の sha256 | include |
| `parent_id` / thread / 構造アンカー | **content に入れず object_id 側で表現** |
| reactions / 編集 wrapper / ingestion meta | **exclude**(独立変化 side-state / wrapper メタ / メタは別 Observation 列か別経路) |

#### 正規化規則(transport ノイズのみ)

- NFC(Unicode 正規化)
- 改行 CRLF → LF
- JSON canonical(キー順序固定、空白除去)
- timestamp 表記統一(RFC3339 UTC、固定精度)

#### Scenario: adapter は object_id + canonical タプルを宣言する

- **WHEN** Slack adapter が message を Observation に変換する
- **THEN** `object_id = "channel:<C-id>:ts:<ts>"` と canonical タプル `(sender, body, event_time, [attachment_sha256...])` が core に渡される
- **AND** core が H を計算して `identity_key` を組み立てる
- **AND** adapter は `identity_key` 文字列を組み立てない

#### Scenario: reactions 変化で identity_key が変わらない

- **WHEN** 同一メッセージに新しい reaction が付き、adapter が再度 Observation を生成する
- **THEN** canonical タプルは変化せず、生成される `identity_key` は変わらない
- **AND** core は `Duplicate(existing_id)` を返す

#### Scenario: body 編集で identity_key が変わる

- **WHEN** 同一メッセージの body が編集される
- **THEN** canonical タプルが変化し、新 `identity_key` が組み立てられる
- **AND** core は新 Observation を `Ingested` として保存する(SHARD-01)

#### Scenario: 添付の差し替えで identity_key が変わる

- **WHEN** 同一メッセージで添付ファイルが差し替えられる(sha256 変化)
- **THEN** canonical タプル内の添付 sha256 が変化し、新 `identity_key` が組み立てられる

### Requirement: SHARD-ADAPT-02 Source-Specific Object ID Extraction

adapter は **source 固有の `object_id` 抽出規則** を **宣言的に定義** **しなければならない (SHALL)**。同一 source の同一論理エンティティに対し、再取り込み / 再 export を経ても **同じ `object_id` が再現** され **なければならない (SHALL)**(これが冪等性の前提)。

| Source | `object_id` |
| --- | --- |
| Slack | `channel:ts`(message)、`channel:ts:thread:<thread-ts>`(thread 構造アンカー) |
| claude.ai message | `uuid`(存在時)。欠落時は `conversation_uuid` + `parent_message_uuid` チェーン上の位置から **決定的に導出**(具体アルゴリズムは Phase 2 実装時に固定し property test で再現性を保証) |
| Google Slides revision | `presentation_id : revision_id` |
| generic source | adapter が宣言(Source Contract に登録) |

#### Scenario: 同 zip を再 import して同じ derived id を得る

- **WHEN** claude.ai 同一 zip を 2 回 import する
- **THEN** uuid 欠落の message も同じ derived `object_id` で 2 回とも生成される
- **AND** 2 回目は core の dedup で `Duplicate` になる

#### Scenario: source contract に object_id 抽出規則が登録される

- **WHEN** 新規 adapter `lethe-adapter-rss` を追加する
- **THEN** Source Contract に `object_id` 抽出規則(item.guid 等)が宣言的に登録される
- **AND** core のコードは変更されない

### Requirement: SHARD-ADAPT-03 Adapter Conformance for Canonical Stability

adapter は **canonical stability conformance test** に合格 **しなければならない (SHALL)**:

1. **無変更再取り込み test**: 同 source の同一 entity を 2 回 ingest して `identity_key` が同一であること。
2. **編集 test**: body 編集で `identity_key` が変わり、両版が distinct な Observation として保存されること。
3. **side-state 不変 test**: reactions / 編集 wrapper / ingestion meta(`recordedAt` / crawler cursor / export run id)の変化が `identity_key` を変えないこと。
4. **正規化 test**: transport ノイズ(CRLF / LF、Unicode 非正規化形)の差異で `identity_key` が変わらないこと。
5. **ユーザ可視空白 保持 test**: body 内のユーザ可視空白(意味のある複数スペース等)を畳まないこと(false-merge 回避)。

#### Scenario: 共通 conformance suite が全 adapter に適用される

- **WHEN** Slack / Google Slides / claude.ai 各 adapter が conformance suite を実行する
- **THEN** すべての adapter が 5 つの test を通過する
- **AND** 不通過は adapter 側の bug として extract / canonical 規則を修正する

---

## MODIFIED Behaviors (parent spec 参照)

既存 `openspec/specs/adapter-policy.md` の以下の節は、本 delta の要件で表現が変わる:

- **§2.1 Adapter がやること**: 「Observation envelope に変換」から「(object_id, canonical タプル) を宣言して Observation envelope を組み立てる(`identity_key` 文字列は core が計算)」へ(SHARD-ADAPT-01)。
- **§3 Common Adapter Contract**: adapter configuration に `object_id_extraction` / `canonical_tuple_schema` の宣言フィールドを追加(SHARD-ADAPT-02)。
- **§adapter の `to_observations` 出力**: free-form `idempotencyKey` から構造化された `(object_id, canonical タプル)` ペアに(SHARD-ADAPT-01)。
- **§adapter conformance test**: canonical stability の 5 test を必須に追加(SHARD-ADAPT-03)。
