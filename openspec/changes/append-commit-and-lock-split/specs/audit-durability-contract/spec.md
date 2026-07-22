## ADDED Requirements

### Requirement: ADC-01 mandatory audit は commit 境界内で fail-closed

保護操作(認証・write・filter 判定)の mandatory audit record は canonical commit 境界(CAB-01)内で durable append SHALL し、その永続化に失敗した場合は保護操作も失敗 SHALL する。lock 取得失敗・serialization 失敗・DB 失敗をログだけで握り潰して保護操作を成功させる fail-open は廃止 SHALL する。

#### Scenario: audit 永続化失敗で保護操作も失敗する
- **WHEN** mandatory audit record の durable append が lock / serialization / DB 失敗で永続化できない
- **THEN** 対応する保護操作も失敗する
- **AND** audit なしで保護操作を成功させない

#### Scenario: fail-open を廃止する
- **WHEN** audit 永続化が失敗する
- **THEN** LETHE は失敗をログだけで握り潰して成功応答を返さない

### Requirement: ADC-02 同期必須と遅延許容の区分

audit は同期必須(mandatory durable)部分と遅延許容部分に区分 SHALL する。同期必須部分は commit 境界内で durable でなければ SHALL ならず、遅延許容部分は CAB-02 の append-seq consumer として応答後に実行してよい SHALL。

#### Scenario: 同期必須部分は commit 境界内
- **WHEN** 保護操作の mandatory audit が記録される
- **THEN** その同期必須部分は canonical commit 境界内で durable に永続化される

#### Scenario: 遅延許容部分は consumer で実行
- **WHEN** 遅延許容の監査記録が対象になる
- **THEN** その部分は append-seq consumer として応答後に実行され commit 境界をブロックしない

### Requirement: ADC-03 無制限 in-memory audit mirror の廃止

再起動で消える無制限 `Vec` の in-memory audit mirror は廃止 SHALL する。audit 読みは永続台帳の page query で供給 SHALL し、全履歴を in-memory に保持 SHALL NOT する。

#### Scenario: audit 読みは永続台帳の page query
- **WHEN** audit 履歴が読まれる
- **THEN** LETHE は永続台帳を page query して供給する
- **AND** 全履歴を無制限 in-memory `Vec` に保持しない
