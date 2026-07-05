# google-sheets-adapter Specification

## Purpose
TBD - created by archiving change workspace-search-support. Update Purpose after archive.
## Requirements
### Requirement: GSHEETS-01 Source Contract
Google Sheets adapter は M09 Adapter Policy に従い、observer_id `obs:gsheets-crawler`、source_system `sys:google-sheets`、schema `schema:workspace-object-snapshot`、authority_model `source-authoritative`、capture_model `snapshot` で動作 SHALL する。

#### Scenario: adapter 登録
- **WHEN** Google Sheets adapter が初期化される
- **THEN** M02 Registry に observer `obs:gsheets-crawler` と source system `sys:google-sheets` が登録される

### Requirement: GSHEETS-02 workspace-object-snapshot schema の利用
Google Sheets adapter は `schema:workspace-object-snapshot` を横展開して使用 SHALL する。artifact フィールドの `service` は `"sheets"`、`objectType` は `"spreadsheet"` とする。

#### Scenario: Sheets の Observation 生成
- **WHEN** Google Sheets のスプレッドシートが取得される
- **THEN** `schema:workspace-object-snapshot` に従い、`artifact.service: "sheets"`, `artifact.objectType: "spreadsheet"` を含む Observation が生成される

### Requirement: GSHEETS-03 行単位の内容取得
Google Sheets adapter は Google Sheets API を使用して行単位の内容、ヘッダ文脈、メタデータを取得 SHALL する。

#### Scenario: シートデータの取得
- **WHEN** Google Sheets API からスプレッドシートが取得される
- **THEN** 各シートの行データ、ヘッダ行 (1行目) の文脈、シート名、スプレッドシートのメタデータが native content に含まれる

### Requirement: GSHEETS-04 行単位のレコード生成支援
Sheets の native content は行単位での検索が可能な構造で保持 SHALL する。各行はヘッダ文脈 (列名) と関連付けられる。

#### Scenario: ヘッダ付き行データ
- **WHEN** スプレッドシートにヘッダ行がある
- **THEN** 各データ行はヘッダ行の列名と関連付けられた形で保持される

### Requirement: GSHEETS-05 revision ベース差分取り込み
Google Sheets adapter は revision 情報を使用し、変更がある場合のみ新しい Observation を生成 SHALL する。

#### Scenario: 未変更シートのスキップ
- **WHEN** 前回クロール以降にスプレッドシートの revision が変わっていない
- **THEN** 新しい Observation は生成されない

### Requirement: GSHEETS-06 idempotency key
Google Sheets adapter は M09 Adapter Policy の idempotency 契約に従い identity_key を生成 SHALL する。

#### Scenario: 同一内容の再取り込みで重複しない
- **WHEN** 同一 revision のスプレッドシートが再度取り込まれる
- **THEN** identity_key が一致し、新規 Observation は生成されない

