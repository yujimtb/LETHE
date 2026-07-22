## ADDED Requirements

### Requirement: IRC-01 per-item 取り込み結果

取り込み API は、request 内の各 draft につき 1 件の per-item 結果を入力順で返 SHALL す。各結果は client 相関キー(`client_ref`)、`outcome`(`ingested` | `duplicate` | `quarantined` | `rejected` のいずれか)、および `outcome` に応じた `observation_id`(`ingested`)/ `existing_id`(`duplicate`)/ `ticket`(`quarantined`)/ `reason` + `error_code`(`rejected`)を含 SHALL む。件数のみの応答(`ImportReport { ingested, duplicates, quarantined }` だけを返す形)は正式契約として廃止 SHALL する。集計件数は per-item 結果の付随サマリとしては保持してよい。client が ID を回収するために corpus grep や event-id lookup を行う必要が SHALL NOT ある。

#### Scenario: ingested item は observation_id を返す

- **WHEN** 新規の有効な draft が durable append される
- **THEN** その item の結果は `outcome = ingested` と割り当てられた `observation_id` を含む
- **AND** 結果は request 内の draft と同じ順序・同じ `client_ref` で対応づく

#### Scenario: duplicate item は既存 ID を返す

- **WHEN** 既に append 済みの実体と同一 canonical identity の draft が再送される
- **THEN** その item の結果は `outcome = duplicate` と既存の `existing_id` を含む
- **AND** client は ID 回収のために corpus grep を行う必要がない

#### Scenario: 件数のみ応答の廃止

- **WHEN** 取り込み API が応答を返す
- **THEN** 応答は per-item 結果配列を含み、集計件数だけを返さない

### Requirement: IRC-02 partial success

batch 内の一部 item が `quarantined` または `rejected` になっても、有効 item は durable append され、各 item の結果は per-item 結果として返 SHALL る。1 件の item 失敗を理由に request 全体を 400 等の単一エラーで停止し、有効 item の append を行わないことは SHALL NOT ある。

#### Scenario: 一部 quarantine でも有効 item は append される

- **WHEN** 10 件の draft のうち 1 件が未来時刻 quarantine 対象で、残り 9 件が有効である
- **THEN** 有効な 9 件は append され `outcome = ingested`(または既存なら `duplicate`)を返す
- **AND** quarantine 対象の 1 件は `outcome = quarantined` と ticket を返す
- **AND** request 全体は単一の 400 で abort しない

#### Scenario: 単一失敗で全体を落とさない

- **WHEN** batch 内の任意の 1 件が rejected になる
- **THEN** LETHE は残りの有効 item の append を継続する
- **AND** 全 item の per-item 結果を返す

### Requirement: IRC-03 構造化 error 分類

item 単位の失敗は、`transient`(再送可能)/ `validation`(再送不可)/ `quarantine`(ticket 付き)のいずれかに分類 SHALL され、機械可読な `error_code` で client が判別可能で SHALL ある。partial success(IRC-02)応答は request 全体としては HTTP `200 OK` を返 SHALL し、item 別の成否は per-item `outcome` + `error_code` で判別 SHALL する。item 別失敗のために HTTP status を分岐(例 一部失敗で 207 / 4xx)することは SHALL NOT ある(判断基準: HTTP ツールチェーン互換性はクライアント種別を問わずスケールする)。未来時刻(`published > recordedAt + clock skew 10 分`)は request 全体の 400 ではなく、当該 item の `quarantined` として ticket を返 SHALL す。`validation` は再送しても同一結果になる恒久失敗で SHALL あり、`transient` は同一入力の再送で成功し得る一時失敗で SHALL ある。

`error_code` 語彙は client が実装で依存する凍結された(frozen)taxonomy で SHALL あり、API バージョン(IRC-06)に紐づけて管理 SHALL する。既存バージョン内で `error_code` の意味を変更・削除することは SHALL NOT し、新しい `error_code` の追加はドキュメント化された既知集合の拡張として行 SHALL う。

#### Scenario: 未来時刻は item 別 quarantine

- **WHEN** ある draft の `published` が `recordedAt + 10 分` を超える
- **THEN** その item は `outcome = quarantined` と clock-skew を示す `error_code` + ticket を返す
- **AND** request 全体は 400 で停止せず HTTP 200 で per-item 結果を返す

#### Scenario: partial success は HTTP 200 で item 別に判別する

- **WHEN** batch 内に成功 item と失敗 item が混在する
- **THEN** request 全体は HTTP 200 を返し、各 item の成否は `outcome` + `error_code` で判別できる
- **AND** 一部失敗を理由に 207 / 4xx へ HTTP status を分岐しない

#### Scenario: error_code は凍結 taxonomy

- **WHEN** client が特定 API バージョンの `error_code` に依存して分岐処理を実装する
- **THEN** そのバージョン内で当該 `error_code` の意味は変更・削除されない

#### Scenario: validation 失敗は再送不可として分類される

- **WHEN** schema 検証や必須フィールド欠落で draft が拒否される
- **THEN** その item は `outcome = rejected`、分類 `validation`、機械可読な `error_code` を返す
- **AND** client は同一入力の単純再送では成功しないと判別できる

#### Scenario: transient 失敗は再送可能として分類される

- **WHEN** 一時的な事由(ロック競合・下流一時不能など)で item が失敗する
- **THEN** その item は分類 `transient` と再送可能を示す `error_code` を返す
- **AND** client は同一入力の再送で成功し得ると判別できる

### Requirement: IRC-04 ACK セマンティクス

取り込み応答は、応答コード・item outcome と canonical 台帳状態の対応を明示的な契約として定義 SHALL する。`ingested` / `duplicate` を返した item は canonical Observation ledger に durable append 済みで SHALL あり、その事実は後続の派生処理(projection materialization・検索 index catch-up・audit・非 canonical 補助状態)の成否に依存 SHALL NOT しない。派生処理の失敗を理由に、append 済み item の outcome を失敗へ反転させることは SHALL NOT ある。

**スコープ境界:** 本 requirement は ACK セマンティクスの**宣言と応答形状**までを規定する。派生処理を append commit 境界から分離する実装(canonical append + 最小 audit/outbox を一つの commit 境界にし、projection/index を非同期 consumer にする)は communication-projection / 性能フェーズの責務であり、本 change の対象外である。

#### Scenario: append 成功は派生処理失敗で覆らない

- **WHEN** ある item が canonical ledger へ durable append され、その後の projection materialization または検索 index catch-up が失敗する
- **THEN** その item の per-item 結果は `ingested`(ID 付き)のままである
- **AND** 派生処理の失敗は projection health / 運用シグナルで surface し、取り込み応答の outcome を反転しない

#### Scenario: 応答コードと台帳状態の対応

- **WHEN** 取り込み応答が返る
- **THEN** `ingested` / `duplicate` の item は canonical ledger に存在し、`rejected` の item は存在しない
- **AND** `quarantined` の item は canonical ledger に append されず ticket で追跡される

### Requirement: IRC-05 暗黙契約の開示

client が知らないと事故につながる暗黙契約(監査 C 章)は、API 応答(エラーメッセージ)と文書に機械可読・人間可読な形で明示 SHALL する。少なくとも、draft 件数上限・request body 上限(現行 128MiB)・1 payload 上限(personal 設定 1MiB)・page limit 上限(personal 500)・冪等 identity の構成要素(`source_instance_id` を含むこと)を、超過・違反時に対応する `error_code` と閾値を含むエラーで返 SHALL す。閾値をコードに直書きしたまま client へ非開示にすることは SHALL NOT ある。

#### Scenario: body/payload 上限超過は閾値を返す

- **WHEN** request body または単一 payload が設定上限を超える
- **THEN** LETHE は機械可読な `error_code` と超過した実値・設定上限値を含むエラーを返す

#### Scenario: identity 構成要素の開示

- **WHEN** 取り込み API 契約が文書化される
- **THEN** 文書は冪等 identity が `source_instance_id` を含むこと、および retry で固定すべき入力(object id・canonical tuple・idempotency key)を明示する

#### Scenario: page limit 上限の開示

- **WHEN** client が設定上限を超える page limit を指定する
- **THEN** LETHE は機械可読な `error_code` と適用される上限値を含むエラーを返す

### Requirement: IRC-06 API バージョニングによる後方互換と移行

取り込み API の**挙動変更**(partial success の HTTP 200 化・サーバ権威 identity の厳格検証など、既存 client の前提を壊し得る変更)は、**API バージョニングで導入 SHALL する**。request header による per-request opt-in で挙動を切り替えることは SHALL NOT ある(判断基準: opt-in header は既知 client を列挙する運用に依存しスケールしない。製品境界を厳格に分け、クライアントは Intercom / Nanihold に限らず任意に接続できる前提とする)。バージョンの表現形式(URL パス / メディアタイプ / バージョン header 等)は design が提案し、いずれの形式でも以下を満たす:

- 旧バージョンの意味論は**凍結(frozen)**され、既存の応答形状・HTTP status・outcome 語彙・`error_code` 語彙をバージョン内で変更 SHALL NOT する。
- 新バージョンは初日から厳格な契約(partial success / HTTP 200 / サーバ権威 identity / 厳格 validation)を適用 SHALL し、warn-only の猶予期間を設け SHALL NOT ない。
- 旧バージョンは意味論凍結のまま**非推奨化 → 廃止**の明示的プロセスに載せ SHALL、無通知で挙動を変更 SHALL NOT する。

なお応答への純粋なフィールド追加(既存フィールドを変更しない加算)は、それを解釈しない client を壊さない範囲で同一バージョン内でも導入してよい。

#### Scenario: 挙動変更は新バージョンで導入する

- **WHEN** partial success やサーバ権威 identity 検証など既存 client の前提を壊し得る挙動を導入する
- **THEN** その挙動は新しい API バージョンでのみ有効になる
- **AND** per-request の opt-in header では切り替えない

#### Scenario: 旧バージョンの意味論凍結

- **WHEN** 新バージョンが追加された後に旧バージョンの endpoint が呼ばれる
- **THEN** 旧バージョンは従来の応答形状・HTTP status・outcome / error_code 語彙をそのまま返す
- **AND** 旧バージョンの意味論は無通知で変更されない

#### Scenario: 新バージョンは初日から厳格

- **WHEN** client が新バージョンで取り込みを呼ぶ
- **THEN** partial success・HTTP 200・サーバ権威 identity・厳格 validation が初日から適用される
- **AND** warn-only の猶予期間は存在しない

#### Scenario: 旧バージョンの非推奨化と廃止

- **WHEN** 旧バージョンを廃止する
- **THEN** 非推奨化 → 廃止の明示的プロセスを経る
- **AND** 意味論凍結を保ったまま廃止まで動作する
