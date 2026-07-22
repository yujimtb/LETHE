## ADDED Requirements

### Requirement: CP-01 通信 projection のデータモデル
LETHE は discord/slack の通信メッセージについて、(チャネル × スレッド)をキーとする専用 projection state を維持 SHALL する。各着信メッセージについて reply-SLO 判定に必要な素データ(`incoming_observation_id`、`channel_id`、`sender_id`、`thread_ref`、`published`、`due_at`、送信済みなら `sent_at`)を保持 SHALL し、送信済み判定は既存の増分 join(reply-draft@1 / send-record@1 supplemental)結果を入力とする SHALL。projection state は canonical Observation と supplemental から決定的に再構築可能な派生 materialization であり ground truth として直接更新して SHALL NOT ならない。

#### Scenario: チャネル × スレッドキーの状態
- **WHEN** 通信メタデータを持つ着信メッセージ Observation が append される
- **THEN** projection は当該メッセージを(`channel_id`, `thread_ref`)キーの下に着信 fact として登録する
- **AND** `sender_id`、`published`、`due_at` を保持する

#### Scenario: 送信済み join の入力
- **WHEN** 着信メッセージに対応する send-record@1 が既存の増分 join で `sent_at` を確定する
- **THEN** projection はその `sent_at` を当該着信 fact に結び付ける
- **AND** send 側の増分 fold を再設計しない

#### Scenario: 決定的再構築
- **WHEN** projection state を canonical Observation と supplemental から再構築する
- **THEN** 同じ入力集合から同じ projection state を得る

### Requirement: CP-02 reply-SLO 判定規則の等価性
communication projection が返す reply-SLO 結果(`rows` / `overdue`、各行の `status` / `latency_seconds` / 並び順)は、任意に固定した評価時刻 T について、現行の全履歴再計算(`ReplySloProjector::project_records` 相当)を同じ T で評価した結果と一致 SHALL する。status 判定規則(`SentOnTime` / `SentLate` / `Overdue` / `Pending`)、latency 計算、`due_at` 昇順・`incoming_observation_id` tie-break の並び、`overdue` の抽出条件(`Overdue` と `SentLate`)は変更して SHALL NOT ならない。

#### Scenario: 全履歴再計算との一致
- **WHEN** 同一の canonical Observation と supplemental 集合に対し、評価時刻 T で communication projection と現行の全履歴再計算をそれぞれ評価する
- **THEN** 両者の `rows`、`overdue`、各行の `status`、`latency_seconds`、並び順が完全に一致する

#### Scenario: status 判定規則の保存
- **WHEN** `sent_at` の有無と `due_at` / T の関係を変化させる
- **THEN** `sent_at <= due_at` で `SentOnTime`、`sent_at > due_at` で `SentLate`、未送信かつ T > `due_at` で `Overdue`、未送信かつ T <= `due_at` で `Pending` を返す

#### Scenario: overdue 抽出と並び
- **WHEN** projection の `overdue` を読む
- **THEN** `Overdue` と `SentLate` の行だけを `due_at` 昇順・`incoming_observation_id` tie-break で返す

### Requirement: CP-03 メッセージ 1 通あたり O(1) の増分 fold と時刻依存判定の読み取り時評価
着信メッセージ 1 通の append に対する projection 更新は既存メッセージ総数に対して O(1) SHALL である。時刻依存の判定(`Overdue` と `Pending` の区別)は materialize 時に固定せず読み取り時の評価時刻に対して算出 SHALL し、時刻の経過だけで再 materialize や全走査を要求して SHALL NOT ならない。

#### Scenario: 1 通 append の O(1) 更新
- **WHEN** 通信メッセージ 1 通が append される
- **THEN** projection は当該メッセージの fact 1 件だけを追加/更新する
- **AND** 他の着信 fact を再走査・再計算しない

#### Scenario: 時刻経過での状態遷移
- **WHEN** 新規 append なしに評価時刻が `due_at` を跨いで進む
- **THEN** 該当行は読み取り時評価で `Pending` から `Overdue` へ遷移する
- **AND** 再 materialize や全観測走査を要しない

#### Scenario: 送信確定での状態遷移
- **WHEN** 未送信の着信に対し send-record@1 が `sent_at` を確定する
- **THEN** 該当行は当該メッセージ分の O(1) 更新で `SentOnTime` または `SentLate` へ遷移する

### Requirement: CP-04 reply-SLO 責務の移管とスナップショット保持
reply-SLO 計算責務だけを communication projection へ移し、その結果 `classify_non_corpus_delta` の `ReplySloRequired` → フルリビルド分岐を廃止 SHALL する。全観測スナップショットからメッセージ系 schema を削除して SHALL NOT ならない。メッセージは既存の増分 fold(`FreshnessOnly` / `SlackMessage`)で引き続きスナップショットに反映 SHALL される。

#### Scenario: ReplySloRequired 分岐の廃止
- **WHEN** reply-SLO 対象の通信メッセージが append される
- **THEN** communication projection の増分 fold で処理し `ReplySloRequired` によるフルリビルドを行わない

#### Scenario: メッセージのスナップショット反映を維持
- **WHEN** 通信メッセージが append される
- **THEN** 当該メッセージは `FreshnessOnly` または `SlackMessage` の増分 fold で引き続きスナップショット(freshness 等)へ反映される
- **AND** メッセージ系 schema をスナップショットから削除しない

### Requirement: CP-05 materialization version 移行と card-queue 責務境界
communication projection の導入は永続 materialization version を更新 SHALL し、version 不一致時に IM-02 の移行フルリビルドで既存 snapshot を canonical Observation と supplemental から自然再構築 SHALL する。card-queue projection は cognition 側の責務であり本 change の対象外であることを明示 SHALL し、その入力(supplemental)と契約を変更して SHALL NOT ならない。

#### Scenario: version 更新による自然再構築
- **WHEN** 永続 materialization version が communication projection 導入前のもので起動する
- **THEN** LETHE は IM-02 の移行フルリビルドで communication projection state を含む snapshot を再構築する
- **AND** 再構築結果は CP-02 の等価性を満たす

#### Scenario: card-queue の不変
- **WHEN** communication projection を導入する
- **THEN** card-queue projection の入力・出力・契約は変更されない
