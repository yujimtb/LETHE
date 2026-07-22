## ADDED Requirements

### Requirement: CAB-01 canonical commit 境界が応答を確定する

canonical append の成功応答は、(a) canonical Observation ledger への durable append、(b) request 内 per-item Observation ID の確定、(c) 最小限の durable audit と派生駆動用 outbox marker(append-seq high-water)の永続化、を一つの commit 境界として確定 SHALL する。取り込み応答はこの commit 境界の成功をもって確定 SHALL し、projection materialization・検索 index catch-up・遅延許容 audit などの派生処理の完了を待って SHALL NOT ならない。

#### Scenario: commit 境界成功で応答が確定する
- **WHEN** Observation が canonical ledger へ durable append され per-item ID と mandatory audit / outbox marker が同一 commit 境界で永続化される
- **THEN** 取り込み応答は per-item ID を伴って確定する
- **AND** projection materialize・検索 index catch-up・遅延許容 audit の完了を待たない

#### Scenario: 派生処理を応答経路から外す
- **WHEN** 取り込み応答が返る
- **THEN** 応答経路は canonical append + per-item ID + 最小 durable audit/outbox のみを含み、projection 全量再計算・index catch-up・遅延 audit を含まない

### Requirement: CAB-02 派生処理は append-seq consumer として応答後に実行される

projection materialization・検索 index catch-up・遅延許容の監査記録は、canonical commit 境界の外で append-sequence(append-seq / cursor)を消費する consumer として実行 SHALL する。派生処理の失敗は取り込み応答の outcome を反転 SHALL NOT し、projection health / 運用シグナルで surface SHALL する。

#### Scenario: 派生処理は commit 境界外の consumer
- **WHEN** commit 境界が成功し応答が返った後に派生処理が駆動される
- **THEN** projection materialize・検索 index・遅延 audit は append-seq を消費する consumer として実行される

#### Scenario: 派生失敗が ACK を反転しない
- **WHEN** append 成功後の projection materialize または検索 index catch-up が失敗する
- **THEN** その item の取り込み outcome は成功(ID 付き)のままである
- **AND** 派生失敗は projection health / 運用シグナルで surface し応答 outcome を反転しない

### Requirement: CAB-03 consumer は request 成否に依存せず append-seq を追う

派生 consumer は特定の HTTP request の成否や duplicate 判定ではなく canonical ledger の append-seq に対して駆動 SHALL する。したがって request が duplicate を返した場合でも未消費の append-seq が残っていれば consumer は追いつき、派生 materialization を欠落 SHALL NOT する。

#### Scenario: duplicate 応答後も consumer が追いつく
- **WHEN** 再送が duplicate を返し、対応する派生 materialization が未消費の append-seq として残っている
- **THEN** append-seq consumer は当該 append-seq を消費して派生 materialization を実行する
- **AND** duplicate 判定を理由に派生 materialization を欠落させない
