# google-docs-adapter Specification

## Purpose
TBD - created by archiving change workspace-search-support. Update Purpose after archive.
## Requirements
### Requirement: GDOCS-01 Source Contract
Google Docs adapter は M09 Adapter Policy に従い、以下の Source Contract で動作 SHALL する。observer_id は `obs:gdocs-crawler`、source_system は `sys:google-docs`、schema は `schema:workspace-object-snapshot`、authority_model は `source-authoritative`、capture_model は `snapshot`。

#### Scenario: adapter 登録
- **WHEN** Google Docs adapter が初期化される
- **THEN** M02 Registry に observer `obs:gdocs-crawler` と source system `sys:google-docs` が登録される

### Requirement: GDOCS-02 workspace-object-snapshot schema の利用
Google Docs adapter は M11 Google Slides Adapter で定義された `schema:workspace-object-snapshot` を横展開して使用 SHALL する。artifact フィールドの `service` は `"docs"`、`objectType` は `"document"` とする。

#### Scenario: Docs の Observation 生成
- **WHEN** Google Docs のドキュメントが取得される
- **THEN** `schema:workspace-object-snapshot` に従い、`artifact.service: "docs"`, `artifact.objectType: "document"`, `artifact.sourceObjectId: <document_id>`, `artifact.canonicalUri: <docs_url>` を含む Observation が生成される

### Requirement: GDOCS-03 本文・見出し・リンク・メタデータの取得
Google Docs adapter は Google Docs API を使用して本文、見出し、リンク、メタデータを取得 SHALL する。

#### Scenario: ドキュメント構造の取得
- **WHEN** Google Docs API からドキュメントが取得される
- **THEN** 本文テキスト、見出し構造、リンク、タイトル、lastModifiedTime などのメタデータが Observation の payload に含まれる

### Requirement: GDOCS-04 見出しセクション単位の chunk 生成
Docs の本文は見出しセクション単位で chunk に分割して native content に保持 SHALL する。これにより Corpus Projection がセクション単位のレコードを生成できる。

#### Scenario: 見出しによる分割
- **WHEN** ドキュメントに H1, H2 等の見出しが含まれる
- **THEN** 各見出しセクションが独立した chunk として native content に保持される

#### Scenario: 見出しなしドキュメント
- **WHEN** ドキュメントに見出しが含まれない
- **THEN** ドキュメント全体が 1 つの chunk として保持される

### Requirement: GDOCS-05 revision ベース差分取り込み
Google Docs adapter は revision 情報を使用し、変更がある場合のみ新しい Observation を生成 SHALL する。

#### Scenario: 未変更ドキュメントのスキップ
- **WHEN** 前回クロール以降にドキュメントの revision が変わっていない
- **THEN** 新しい Observation は生成されない

#### Scenario: 変更済みドキュメントの再取り込み
- **WHEN** 前回クロール以降にドキュメントの revision が変わっている
- **THEN** 新しい Observation が生成される (既存は append-only で保持)

### Requirement: GDOCS-06 idempotency key
Google Docs adapter は M09 Adapter Policy の idempotency 契約に従い、`source : object_id : H(canonical_content)` の形式で identity_key を生成 SHALL する。

#### Scenario: 同一内容の再取り込みで重複しない
- **WHEN** 同一 revision のドキュメントが再度取り込まれる
- **THEN** identity_key が一致し、`IngestResult::Duplicate` が返され、新規 Observation は生成されない

