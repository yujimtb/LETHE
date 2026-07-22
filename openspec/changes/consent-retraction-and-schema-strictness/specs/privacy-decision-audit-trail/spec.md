## ADDED Requirements

### Requirement: PDA-01 consent/retraction/blob 判定の監査証跡内容

consent gate 判定・retraction 遮蔽・blob 認可判定は、actor・対象 subject / consent scope・decision(allow / deny / quarantine / shield)・適用 rule・timestamp を含む監査証跡を生成 SHALL する(A-9 consent 境界の追跡可能性、governance の auditable decisions)。

#### Scenario: consent gate 判定が監査証跡を残す
- **WHEN** append 前 consent gate が判定する
- **THEN** actor・subject/scope・decision・適用 rule・timestamp を含む監査証跡が生成される

#### Scenario: retraction 遮蔽が監査証跡を残す
- **WHEN** retraction による projection 遮蔽が行われる
- **THEN** 対象・consent scope・decision・timestamp を含む監査証跡が生成される

#### Scenario: blob 認可判定が監査証跡を残す
- **WHEN** blob 認可判定が行われる
- **THEN** actor・対象 blob・consent scope・decision・timestamp を含む監査証跡が生成される

### Requirement: PDA-02 durability 機構は append-commit ADC へ委譲

監査証跡の durable 化・fail-closed・in-memory mirror 廃止は append-commit-and-lock-split の audit-durability-contract(ADC-01/02/03)の責務 SHALL とし、本 change はそれに依存 SHALL し重複定義 SHALL NOT する。本 change は監査証跡の**内容**を、ADC は durable に記録する**機構**を定義する。

#### Scenario: durability を重複定義しない
- **WHEN** privacy 判定の監査証跡が durable に記録される
- **THEN** durable 化・fail-closed・mirror 廃止は append-commit-and-lock-split の ADC に従う
- **AND** 本 change は監査証跡の内容のみを定義し durability 機構を重複定義しない
