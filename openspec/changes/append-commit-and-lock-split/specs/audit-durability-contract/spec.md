## ADDED Requirements

### Requirement: ADC-01 監査イベントの durable enqueue は commit 境界内・同期・fail-closed

保護操作(認証・write・filter 判定)の監査イベントは、canonical commit 境界(CAB-01)内で同期に durable enqueue(canonical 台帳への登録)SHALL され、その enqueue の永続化に失敗した場合は保護操作も失敗 SHALL する。保護操作の成功は、その監査イベントが canonical 台帳に載っていることと等価で SHALL ある。lock 取得失敗・serialization 失敗・DB 失敗をログだけで握り潰して保護操作を成功させる fail-open は廃止 SHALL する。

#### Scenario: enqueue 失敗で保護操作も失敗する
- **WHEN** 監査イベントの durable enqueue が lock / serialization / DB 失敗で永続化できない
- **THEN** 対応する保護操作も失敗する
- **AND** 監査イベントが台帳に載らないまま保護操作を成功させない

#### Scenario: 保護操作成功は台帳登録と等価
- **WHEN** 保護操作が成功応答を返す
- **THEN** その監査イベントは canonical 台帳に durable に登録されている

#### Scenario: fail-open を廃止する
- **WHEN** 監査イベントの durable enqueue が失敗する
- **THEN** LETHE は失敗をログだけで握り潰して成功応答を返さない

### Requirement: ADC-02 遅延許容は書き出し・整形に限る

監査の遅延許容部分は、canonical 台帳に登録済みの監査イベントの**書き出し・整形**(projection 化・可読レンダリング・集計)に限 SHALL る。監査イベントの durable enqueue 自体を遅延許容部分へ回すことは SHALL NOT ある。遅延許容部分は CAB-02 の append-seq consumer として応答後に実行してよく、その consumer 遅延(想定 数秒〜数十秒)は audit / projection health で可視化 SHALL する。

#### Scenario: enqueue は遅延させない
- **WHEN** 監査イベントが記録される
- **THEN** その durable enqueue は commit 境界内で同期に行われ遅延許容部分へ回されない

#### Scenario: 書き出し・整形は consumer で遅延可
- **WHEN** 台帳登録済み監査イベントの書き出し・整形が対象になる
- **THEN** その部分は append-seq consumer として応答後に実行され commit 境界をブロックしない
- **AND** その consumer 遅延は audit / projection health で可視化される

### Requirement: ADC-03 無制限 in-memory audit mirror の廃止

再起動で消える無制限 `Vec` の in-memory audit mirror は廃止 SHALL する。audit 読みは永続台帳の page query で供給 SHALL し、全履歴を in-memory に保持 SHALL NOT する。

#### Scenario: audit 読みは永続台帳の page query
- **WHEN** audit 履歴が読まれる
- **THEN** LETHE は永続台帳を page query して供給する
- **AND** 全履歴を無制限 in-memory `Vec` に保持しない
