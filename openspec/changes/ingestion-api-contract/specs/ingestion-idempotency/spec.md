## ADDED Requirements

### Requirement: IDEM-01 サーバ側 canonical identity の導出・検証

取り込みは冪等 identity をサーバ側で決定 SHALL する。canonical identity は `source:object_id:H(canonical_json)` 型で、`object_id`(source-native な不変オブジェクト ID)と `canonical_json`(正規化済み canonical tuple)から導出 SHALL される。サーバは client 提供の入力(`object_id` と canonical tuple、または事前導出済み identity)から identity を導出または厳密再検証 SHALL し、client が送ってきた不透明な `idempotency_key` を検証なしにそのまま UNIQUE 判定へ通すことは SHALL NOT ある。client 提供 identity が同一入力からサーバ導出値と一致しない場合は `validation` として rejected SHALL する。

#### Scenario: 同一実体の再送は duplicate

- **WHEN** 同一の `object_id` と同一の canonical tuple を持つ draft が再送される
- **THEN** サーバは両者から同一 canonical identity を導出する
- **AND** 再送は同一の `observation_id` を指す `duplicate` として返る

#### Scenario: client 提供 identity の厳密検証

- **WHEN** client が `idempotency_key` を提供し、かつ `object_id` と canonical tuple を提供する
- **THEN** サーバは入力から identity を再導出し、提供値と一致しなければ `validation` として rejected を返す

### Requirement: IDEM-02 canonical collision セマンティクス

同一 canonical identity(同一 `object_id`)で canonical 内容(`canonical_json` の hash)が異なる draft は、collision として扱 SHALL い、`duplicate` と機械判別可能に返 SHALL す。collision は既存 Observation を上書き SHALL NOT し(Append-Only Law)、**`outcome = quarantined` として既存 ID を示す ticket 付きで per-item 結果に返 SHALL す**。collision を即時 `rejected` として捨てず、また `duplicate` と同一 outcome へ混同することは SHALL NOT ある(理由: 同一 object の内容不一致は上流の非決定・訂正・client バグの兆候であり、ticket で追跡可能にして後処理・人手判断へ回す)。

#### Scenario: 同一 identity・異なる内容は collision

- **WHEN** 既存 Observation と同一 `object_id` だが canonical 内容が異なる draft が append される
- **THEN** その item は collision として、既存 ID を伴う `quarantined` と ticket を返す
- **AND** 既存 Observation は上書きされない

#### Scenario: collision は duplicate と判別可能

- **WHEN** client が collision と duplicate の結果を受け取る
- **THEN** 両者は異なる `outcome` / `error_code` で機械判別できる

### Requirement: IDEM-03 client 時計非依存の identity

canonical identity は client 時計(`event_time` / `published`)の揺れによって別実体化 SHALL NOT する。同一 source object の retry が retry 時刻で `event_time` を作り直しても、canonical identity と `observation_id` は不変で SHALL ある。canonical tuple の構成、および identity のルーティング(partition 配置)は可変な時刻に依存 SHALL NOT する。

冪等の一意性判定は**グローバルに一意**で SHALL あり、可変時刻由来のルーティング(partition / leaf 配置)に依存 SHALL NOT する。同一 canonical identity が `published` の違いで別 partition / 別 leaf に落ち、per-leaf のスコープしか持たない一意制約をすり抜けて重複 append されることは SHALL NOT ある(判断基準: 一意性はデータ量・partition 数・source 数に対してスケールしなければならない)。その帰結として retry で `event_time` が変わっても duplicate に収束 SHALL する。

#### Scenario: event_time を変えた retry は duplicate

- **WHEN** 同一 source object の draft が、retry のたびに異なる `event_time` で再送される
- **THEN** canonical identity は不変で、2 回目以降は同一 `observation_id` の `duplicate` を返す
- **AND** 時計揺れで新規 append が繰り返される(別実体化する)ことはない

#### Scenario: 一意性はグローバルで partition に依存しない

- **WHEN** 同一 canonical identity の draft が、`published` の違いで別 partition / 別 leaf にルーティングされ得る状況で取り込まれる
- **THEN** 冪等判定(既存検出)は per-leaf スコープの制約をすり抜けず、グローバルに同一実体を検出する
- **AND** 同一実体は単一の duplicate 判定へ収束し重複 append されない
