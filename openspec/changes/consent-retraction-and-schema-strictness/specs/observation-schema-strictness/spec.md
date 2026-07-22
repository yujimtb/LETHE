## ADDED Requirements

### Requirement: SSV-01 宣言フィールドの検証と宣言外フィールドの受理・保存

各 observation schema の `payload_schema` は、projection が読む宣言フィールドについて required・型・format を実データ契約として定義 SHALL する。取り込み時、(a) 宣言済み必須フィールドの欠落または型違反は item エラー SHALL とし当該 item を canonical append SHALL NOT する(fold が安全に読めないため)。(b) 宣言外の余剰フィールドは受理・保存 SHALL し、拒否も隔離も SHALL NOT する。projection は宣言フィールドのみを読 SHALL む(IM-03 の fold 宣言と整合)。(c) 余剰フィールドの発生は計測(`import_timing` 等)で可観測化 SHALL する。これは append-only 原則(データは受けて失わない)と契約明示性(読む側が契約を宣言する)の両立であり、厳格性の置き場所を取り込みゲートから projection 契約へ移す。

#### Scenario: 宣言必須フィールドの欠落・型違反は item エラー
- **WHEN** 宣言済み必須 field(例: Slack `user_id`)を欠く、または型が違反する payload が取り込まれる
- **THEN** 当該 item は item エラーとなり canonical append されない
- **AND** fold が安全に読めない Observation を canonical 化しない

#### Scenario: 宣言外の余剰フィールドは受理・保存する
- **WHEN** schema が宣言していない余剰フィールドを含む payload が取り込まれる
- **THEN** LETHE は当該 payload を拒否も隔離もせず受理し余剰フィールドごと保存する
- **AND** projection は宣言フィールドのみを読み余剰フィールドを解釈しない

#### Scenario: 余剰フィールドの発生を計測で可観測化する
- **WHEN** 宣言外の余剰フィールドを含む Observation が受理される
- **THEN** 余剰の発生は `import_timing` 等の計測で可観測化される

### Requirement: SSV-02 version-gated 厳格化と既存データ非再検証

strict schema は新 version として登録 SHALL し、過去 Observation はその書込時 schemaVersion のまま保持 SHALL し再検証 SHALL NOT する(A-1 append-only)。厳格検証は新 strict version で取り込まれる新規 Observation にのみ適用 SHALL する。version bump は registry の version 規則に従 SHALL う。

#### Scenario: 過去データを遡って再検証しない
- **WHEN** 既存 schema に strict な新 version を登録する
- **THEN** 過去 Observation は書込時 schemaVersion のまま保持され再検証されない
- **AND** canonical Observation は遡って書き換えられない

#### Scenario: 新 version 取り込みのみ厳格検証
- **WHEN** strict 新 version で新規 Observation が取り込まれる
- **THEN** その Observation は新 strict schema で検証される

### Requirement: SSV-03 通信 metadata 等の必須 field 契約

欠落が quarantine または下流 projection 不整合になる field(channel kind・source instance・external ID・sender/thread metadata 等)は strict schema の required に明示 SHALL する。base JSON Schema で必須化されていなかった暗黙契約を明示契約へ引き上げ SHALL る(A-6)。

#### Scenario: 通信 metadata 欠落を明示契約で検出
- **WHEN** channel kind / external ID / sender metadata を欠く通信 Observation が取り込まれる
- **THEN** 宣言必須制約により当該 item は item エラーとなり canonical append されない
