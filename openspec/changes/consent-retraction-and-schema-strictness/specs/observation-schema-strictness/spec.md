## ADDED Requirements

### Requirement: SSV-01 schema/version ごとの strict payload 検証

各 observation schema の `payload_schema` は required fields・型・format・`additionalProperties` 方針・source contract を実データ契約として定義 SHALL し、取り込み時に supplemental kind と同水準で厳格検証 SHALL する。空疎な `{"type":"object"}` のみの schema で payload 本体を素通しして SHALL NOT ならない。schema に適合しない payload は canonical append 前に止め SHALL る(A-6 契約明示性)。

#### Scenario: 必須 field 欠落を append 前に止める
- **WHEN** required field(例: Slack `user_id`)を欠く payload が取り込まれる
- **THEN** strict validation が append 前に不適合を検出する
- **AND** 不正 Observation は canonical 化されず下流 projection に防御分岐を強いない

#### Scenario: strict schema が supplemental と同水準
- **WHEN** observation payload が検証される
- **THEN** required・型・format・additionalProperties 方針が supplemental kind と同水準で強制される
- **AND** `{"type":"object"}` のみの素通しは行われない

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
- **THEN** strict schema の required 制約が append 前に不適合を検出する
