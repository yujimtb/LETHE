# Spec Delta: platform-generalization

**Change:** generalize-platform
**Version:** 0.1 (draft)
**Date:** 2026-06-13

## Dependencies

- M01 Domain Kernel — 型・law の正規参照(本 spec は law を変更しない)
- M02 Registry — EntityType / Schema / Observer / Source Contract
- M09 Adapter Policy — 共通 adapter contract(本 spec が具体化する)
- M08 Governance — capability / consent の正規参照

---

## ADDED Requirements

### Requirement: GEN-01 Domain Vocabulary Isolation

コア(domain kernel, lake, registry, projection engine, API serving)は、特定ドメインの Entity Type・Schema・ルート名を **コンパイル時に知ってはならない (SHALL NOT)**。基盤 Entity Type(`et:person` 等)はシードデータとしてのみ供給され、`person_page` 相当の機能はコア外の Projection 実装として提供 **しなければならない (SHALL)**。

#### Scenario: 寮ドメインを含まないビルド

- **WHEN** workspace から `lethe-projection-person`(切り出し後の
  person_page crate)を除外してビルドする
- **THEN** `lethe-core` / `lethe-lake` / `lethe-api` はビルド・テストとも
  成功する
- **AND** `grep -ri "person\|dormitory\|寮"` がコア crate のソースに
  ドメイン参照を検出しない(コメント・テストフィクスチャを除く)

#### Scenario: 型非依存 API ルート

- **WHEN** クライアントが `GET /api/projections/{projection_id}/records`
  を呼ぶ
- **THEN** 任意の Projection の materialized view が Projection Catalog の
  出力契約に従って返る
- **AND** `GET /api/persons/*` は deprecation header 付き alias として
  1 リリースの間のみ応答する

### Requirement: GEN-02 Layered Workspace Structure

実装は Cargo workspace として、Functional Core / Imperative Shell のレイヤごとに crate を分割 **しなければならない (SHALL)**。依存方向は DAG として CI で強制 **しなければならない (SHALL)**。

| Crate | 役割 | 禁止依存 |
| --- | --- | --- |
| `lethe-core` | M01 型・law・pure functions | tokio, reqwest, rusqlite, axum |
| `lethe-policy` | M08 判定ロジック(IO なし) | 同上 |
| `lethe-storage-api` | Effect Ports(trait のみ) | 具象 DB / HTTP |
| `lethe-storage-sqlite` 等 | port の具象実装 | adapter crate |
| `lethe-adapter-*` | source adapter(Slack 等) | 他 adapter |
| `lethe-runtime` | scheduler / queue / health | — |
| `lethe-selfhost` | 参照バイナリ(配線のみ) | — |

> 実装注: 上表の crate 名・境界は提案値である。Phase 0 着手時に現行
> `src/` のモジュール間依存を実測し、レイヤ DAG を確定すること
> (`design.md` の「Crate 境界の確定手順」を参照)。

#### Scenario: 依存方向違反の検出

- **WHEN** `lethe-core` に `tokio` への依存を追加した PR を出す
- **THEN** CI の依存検査(`cargo deny` または同等)が fail する

### Requirement: GEN-03 Pluggable Adapter Contract

read-side source adapter は M09 Adapter Policy を実装する `SourceAdapter` trait として提供 **しなければならない (SHALL)**。adapter は authority model / capture model / 対応 schema / credential 要求 / rate limit / cursor 形式を **宣言的メタデータ** として公開 **しなければならない (SHALL)**。新規 source の追加は「trait 実装 + Registry への Source Contract 登録」のみで完結 **しなければならない (SHALL)**。コアコードの変更を要してはならない。M07 Write-Back は Post-MVP とし、この contract に含めない。

#### Scenario: 新規 source の追加

- **WHEN** 開発者が新しい crate `lethe-adapter-rss` で trait を実装し、
  Source Contract を Registry に登録する
- **THEN** selfhost バイナリは設定でその adapter を有効化でき、
  sync は既存 adapter と同じ runtime 経路(retry / cursor / 失敗隔離)を通る
- **AND** `lethe-core` / `lethe-lake` / `lethe-api` に差分がない

#### Scenario: contract conformance test

- **WHEN** adapter crate が共通 conformance test suite
  (idempotency / cursor 再開 / schema 適合)を実行する
- **THEN** すべての adapter が同一のテストに合格する

### Requirement: GEN-04 Storage Effect Ports

永続化は `ObservationStore` / `BlobStore` / `SupplementalStore` / `ProjectionMaterializer` の trait(Effect Ports)経由でのみ行われ **なければならない (SHALL)**。SQLite + ローカル blob は参照実装のひとつに **降格 (SHALL)** し、blob 参照は sha256 content-addressing を保存先非依存の契約として固定 **しなければならない (SHALL)**。

#### Scenario: storage 実装の差し替え

- **WHEN** selfhost を `storage = "sqlite"` から将来の
  `storage = "postgres"` に切り替える
- **THEN** domain / adapter / API のコードは変更不要であり、
  共通の storage conformance suite が両実装で通過する

#### Scenario: blob の保存先非依存

- **WHEN** Observation の attachment を sha256 で参照する
- **THEN** 参照はファイルパス・URL 形式を含まず、blob backend の交換後も
  Replay Law を満たす

### Requirement: GEN-05 Runtime Schema Validation and Evolution

Observation payload は ingest 時に Schema Registry の JSON Schema で検証 **しなければならない (SHALL)**。Schema は SemVer 管理され、後方非互換な変更(必須フィールド追加等)は major bump なしに登録 **できてはならない (SHALL NOT)**。型階層(is-a)解決は Registry のデータ駆動で行い、Projection は親型フィルタでサブタイプを取得 **できなければならない (SHALL)**。

#### Scenario: 非互換 schema 登録の拒否

- **WHEN** `schema:room-entry` v1.0.0 に required フィールドを追加した
  v1.1.0 を登録しようとする
- **THEN** Registry は互換性検査で拒否し、v2.0.0 としての登録を要求する

#### Scenario: 旧 version Observation の読み取り

- **WHEN** schema v2 の登録後に v1 で記録された Observation を読む
- **THEN** デシリアライズは成功し、schemaVersion により v1 として解釈される

### Requirement: GEN-06 Derivation Provider Abstraction

AI 抽出(現行 Gemini)を含む supplemental derivation は `DerivationProvider` trait として抽象化 **しなければならない (SHALL)**。derivation 結果には生成元 Observation、provider 名、model、version、confidence を lineage として必ず記録 **しなければならない (SHALL)**。provider の出力は構造化スキーマで検証され、検証失敗は supplemental として保存 **されてはならない (SHALL NOT)**。

#### Scenario: provider 交換と replay

- **WHEN** provider を `gemini-2.5-flash` から別モデルへ切り替える
- **THEN** 既存 supplemental record は provider/version 付きで保持され、
  pinned replay は cache された derivation を使用して同一結果を返す

#### Scenario: 抽出結果へのインジェクション耐性

- **WHEN** スライド本文に「これまでの指示を無視して…」等の指示文が含まれる
- **THEN** provider 出力はスキーマ検証を通った構造化フィールドのみが
  supplemental に保存され、自由文の指示は実行系に到達しない

### Requirement: GEN-07 Generalized Identity Resolution

identity 照合は単一の `Email` property 前提ではなく、複数 identity claim(email / slack_id / 外部 ID / 任意 key)に対する解決戦略として一般化 **しなければならない (SHALL)**。解決戦略は Registry で構成可能で **なければならない (SHALL)**。confidence と候補(`resolution_candidates`)の扱いは M12 の既存契約に従う。

#### Scenario: email を持たない source の名寄せ

- **WHEN** email を含まない source(センサー ID のみ)の Observation を
  取り込む
- **THEN** identity resolution は構成された claim 種別で照合を試み、
  未解決の場合は candidate として保留する(エラーにしない)

### Requirement: GEN-08 Structured Multi-Source Configuration

設定は構造化 config ファイル(+ 環境変数 override)へ移行 **しなければならない (SHALL)**。source インスタンスは配列として任意個構成 **できなければならない (SHALL)**。secret は config 本体に平文で要求 **してはならない (SHALL NOT)**(env / secret store 参照)。

#### Scenario: 複数 Slack workspace の構成

- **WHEN** config に Slack source を 2 インスタンス(別 token、別 channel
  集合)定義する
- **THEN** 両インスタンスが独立した observer / cursor で sync され、
  片方の失敗が他方を停止させない
