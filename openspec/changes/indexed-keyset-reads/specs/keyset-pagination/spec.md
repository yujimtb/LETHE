## ADDED Requirements

### Requirement: KSP-01 keyset cursor による O(返却件数) ページング

ページング API(person 一覧・ClaimQueue・CardQueue・Corpus records)は persisted sort key + keyset cursor で、返却件数 k に対して O(k) SHALL である。全集合の collect / clone / sort、offset 比例の slice、深い offset page の先頭 skip を SHALL NOT する。

#### Scenario: person 一覧が keyset cursor
- **WHEN** person 一覧を limit 付きで読む
- **THEN** persisted sort key + keyset cursor で返却件数に対して O(k) で返す
- **AND** 全 person を collect・sort しない

#### Scenario: ClaimQueue / CardQueue が keyset cursor
- **WHEN** ClaimQueue または CardQueue を limit 付きで読む
- **THEN** 複合 index + keyset cursor で該当 page を O(k) で返す
- **AND** 全集合を filter・clone してから offset slice しない

#### Scenario: 深い Corpus page が返却件数に比例
- **WHEN** Corpus records の深い page を読む
- **THEN** keyset cursor で返却件数に比例して返し、先頭から offset 件を skip しない

### Requirement: KSP-02 無制限全件応答 API への cursor 必須化

従来 pagination なしで全件返した API(person detail・messages・slides・timeline・ReplySLO 読み)は keyset cursor を必須 SHALL とし、無制限全件応答を SHALL NOT する。person detail は当該人物の全履歴を一括で返さず cursor page で返 SHALL す。この cursor 必須化は挙動変更であり、KSP-03 の API バージョニング方式で移行 SHALL する。reply-SLO 読みの projection データモデルは communication-projection の責務であり、本 requirement はその読みへ keyset cursor を課す。

#### Scenario: person messages が cursor 必須
- **WHEN** person messages / slides / timeline を読む
- **THEN** LETHE は keyset cursor page で返し無制限全件応答をしない

#### Scenario: ReplySLO 読みが cursor 必須
- **WHEN** reply-SLO 読みを行う
- **THEN** LETHE は keyset cursor page で返す

### Requirement: KSP-03 cursor 形式の API 間統一と API バージョニング移行

cursor は API 横断で単一の不透明 keyset cursor 抽象として統一 SHALL し、OEL 数値 / Claim-Card offset / grep opaque / persons offset の混在を解消 SHALL する。クライアントが cursor を共通抽象として扱えることを契約 SHALL する。統一と KSP-02 の cursor 必須化は、契約フェーズ(ingestion-api-contract)と同じ **API バージョニング方式**で移行 SHALL する — 新 API version で統一 keyset cursor を提供し、旧 version は凍結して将来廃止 SHALL する。既存 client を無通知で壊す破壊的変更を旧 version へ加えることは SHALL NOT ある。

#### Scenario: 統一 cursor をクライアントが共通抽象で扱える
- **WHEN** クライアントが複数 API のページングを単一 cursor 抽象で扱う
- **THEN** 新 version の各 API は同一の不透明 keyset cursor 抽象で page を返す
- **AND** API ごとに異なる cursor 形式(数値 / offset / opaque の混在)を強制しない

#### Scenario: API バージョニングによる移行
- **WHEN** 統一 cursor と cursor 必須化を導入する
- **THEN** 新 API version で提供し旧 version を凍結して将来廃止する
- **AND** 旧 version へ既存 client を無通知で壊す破壊的変更を加えない
