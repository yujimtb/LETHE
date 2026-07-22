## ADDED Requirements

### Requirement: IM-01 Append-only 基盤に対する増分 fold projection の原則
canonical Observation ledger は append-only であり、そこから導出される全 projection の materialization は増分 fold として定義 SHALL される。新規 Observation の durable append に対する materialize は、append された Observation とそれが触れる projection state だけを更新する増分 fold で応答 SHALL し、全観測を走査するフルリビュルド(`rebuild_materialized_snapshot_paged` 相当)を通常 append の応答経路で実行して SHALL NOT ならない。

#### Scenario: 通常 append は増分 fold で応答する
- **WHEN** 任意の対応 schema の Observation が 1 件以上 durable append される
- **THEN** materialize は append された Observation と影響を受ける projection state だけを更新する
- **AND** materialize の計算量は既存の全観測数に対して O(1) であり全観測を走査しない

#### Scenario: 全観測走査を応答経路から排除する
- **WHEN** 取り込み応答経路の materialize が実行される
- **THEN** 全 canonical Observation の再読取または全 projection の再計算を行わない

### Requirement: IM-02 フルリビルドの許容範囲の限定
全観測走査を伴うフルリビルドは、(a) materialization version の変更に伴う移行、(b) 破損検知からの復旧、(c) 初回ブートストラップ、の 3 ケースに限って許容 SHALL される。これら以外の事由でフルリビルドを開始して SHALL NOT ならない。

#### Scenario: 移行によるフルリビルド
- **WHEN** 永続 materialization の version が実装の要求 version と一致しない
- **THEN** LETHE は canonical Observation からフルリビルドで現行 version の materialization を再構築する

#### Scenario: 復旧によるフルリビルド
- **WHEN** 永続 materialization の破損または整合性違反を検知する
- **THEN** LETHE はフルリビルドで materialization を再構築する

#### Scenario: 初回ブートストラップによるフルリビルド
- **WHEN** 永続 materialization が存在せず canonical Observation が存在する
- **THEN** LETHE はフルリビルドで初回 materialization を構築する

#### Scenario: 通常 append はフルリビルドを開始しない
- **WHEN** 対応 schema の Observation が durable append される
- **THEN** LETHE はフルリビルドを開始せず増分 fold だけで応答する

### Requirement: IM-03 schema fold 網羅性の起動時検証とフォールバック分岐の廃止
取り込みエンジンは schema を registry と照合し未登録 schema を Rejected で拒否するため、レイクに登録済み schema の Observation しか存在しない。したがって projection は「未知 schema」をランタイムで検疫するのではなく、**登録済み全 observation schema に対する fold 挙動(増分 fold / freshness-only / communication のいずれか)を宣言しなければならない SHALL**。LETHE は起動時にこの網羅性(registry の全 observation schema が projection の fold 宣言で被覆されること)を検証 SHALL し、不一致(ドリフト)なら起動を失敗 SHALL する。`classify_non_corpus_delta` の通常 append でフルリビルドへ落とす全フォールバック分岐(`ReplySloRequired` / `UnsupportedSchema` / `EmptyAppend`)は廃止 SHALL する。fold 実行時に宣言外 schema へ遭遇した場合(=起動時検証を素通りしたコード欠陥)は警告ログ + スキップとし、リビルドを実行して SHALL NOT ならない。

#### Scenario: 通信メッセージは communication projection へ
- **WHEN** 通信メタデータ(`communication_channel_id` / `communication_sender_id` / `communication_thread_ref` / `communication/reply_due_at`)を持つ Observation が append される
- **THEN** LETHE は専用 communication projection の増分 fold で reply-SLO 状態を更新する
- **AND** `ReplySloRequired` 事由でフルリビルドを開始しない

#### Scenario: 登録 schema の fold 宣言網羅を起動時に検証する
- **WHEN** LETHE が起動し registry の observation schema 集合と projection の fold 宣言を突き合わせる
- **THEN** 全登録 schema が増分 fold / freshness-only / communication のいずれかで被覆されていれば起動を継続する

#### Scenario: fold 宣言のドリフトで起動失敗する
- **WHEN** registry に登録済みだが projection の fold 宣言に含まれない observation schema が存在する
- **THEN** LETHE は起動を失敗(fail fast)させる
- **AND** ドリフトはデプロイ時検出でありランタイムのフルリビルドや検疫で吸収しない

#### Scenario: fold 時の宣言外 schema 遭遇(コード欠陥)
- **WHEN** 起動時検証を素通りした宣言外 schema の Observation に fold が遭遇する
- **THEN** LETHE は警告ログを出力して当該観測をスキップする
- **AND** フルリビルドを実行しない

#### Scenario: 空 append は no-op
- **WHEN** append 対象の Observation が 0 件である
- **THEN** materialize は何も再構築せず no-op で応答する
- **AND** `EmptyAppend` 事由でフルリビルドを開始しない

### Requirement: IM-04 正当なフルリビルドの背景化・直列化・読み取り一貫性
IM-02 の正当なフルリビルドは HTTP 応答をブロックせず背景で実行 SHALL し、canonical Observation の append 成功をもって取り込み応答を返す。背景リビルドは単一に直列化(single-flight)SHALL し、進行中の projection 読みは直前に公開済みの古い snapshot を返す SHALL であって空結果やエラーへ fallback して SHALL NOT ならない。

#### Scenario: フルリビルド中も append 応答をブロックしない
- **WHEN** 背景フルリビルドの進行中に対応 schema の Observation が append される
- **THEN** 取り込み応答は append 成功で返り、フルリビルド完了を待たない

#### Scenario: 背景リビルドの直列化
- **WHEN** フルリビルドを要する事由が同時に複数発生する
- **THEN** 背景リビルド task は同時に一つだけ実行される

#### Scenario: 進行中の読み取り一貫性
- **WHEN** 背景フルリビルドの進行中に projection 読み(card-queue・reply-SLO・corpus 検索)が実行される
- **THEN** LETHE は直前に公開済みの snapshot を返す
- **AND** 空結果・エラー・部分再構築中の snapshot を返さない

#### Scenario: 公開の atomic 切替
- **WHEN** 背景フルリビルドが完了し整合検証に成功する
- **THEN** LETHE は新 snapshot を atomic に公開し以降の読みへ反映する

### Requirement: IM-05 projection 読みの鮮度契約
projection 読み(card-queue・reply-SLO 読み・corpus 検索)は canonical Observation に対して遅延を持つ派生 materialization であることを契約 SHALL する。増分 fold 済みの通常状態では反映遅延を **5 秒以内** とし、IM-04 の背景リビルド進行中は古い snapshot を返し **60 秒以内** の遅延まで許容することを応答契約として明示 SHALL する。

#### Scenario: 通常時の鮮度
- **WHEN** 対応 schema の Observation が append され増分 fold で反映される
- **THEN** その反映は 5 秒以内に projection 読みに現れる

#### Scenario: 背景リビルド中の鮮度
- **WHEN** 背景フルリビルドの進行中に projection を読む
- **THEN** 直前に公開済みの snapshot を返し、鮮度は 60 秒以内の遅延まで許容する
- **AND** その遅延は契約違反ではない

### Requirement: IM-06 import_timing 計測整合
取り込み応答は既存の `import_timing` ログ(`ledger_append_ms` / `non_corpus_materialize_ms` / `non_corpus_materialize_mode` / `non_corpus_classification` / `full_rebuild_reason` / `search_index_catch_up_ms` / `audit_ms` / `total_ms` / `quarantined`)を維持 SHALL し、新分類での期待ログ値を規定 SHALL する。通信メッセージ append の `non_corpus_materialize_mode` は `incremental` SHALL であり、`full_rebuild_reason` は通常 append で `not_applicable` SHALL である。

#### Scenario: 通信メッセージ append のログ
- **WHEN** 登録チャネルの通信メッセージ 1 通が append される
- **THEN** `non_corpus_materialize_mode` は `incremental`、`full_rebuild_reason` は `not_applicable` を記録する
- **AND** `non_corpus_materialize_ms` は全観測数に依存せず有界である

#### Scenario: 宣言外 schema 遭遇時のログ(コード欠陥時)
- **WHEN** 起動時検証を素通りした宣言外 schema に fold が遭遇しスキップする
- **THEN** `non_corpus_materialize_mode` はフルリビルドを示さず、スキップは警告ログで識別できる

#### Scenario: 背景フルリビルドのログ
- **WHEN** 移行・復旧・ブートストラップの背景フルリビルドが実行される
- **THEN** `non_corpus_materialize_mode` は背景実行を識別でき `full_rebuild_reason` は該当事由を記録する

### Requirement: IM-07 取り込みレイテンシ SLO
登録チャネルの通信メッセージ 1 通の import 応答は append + 増分 fold + search catch-up のみで完了 SHALL し、応答 latency の p95 は 2 秒未満 SHALL である。この SLO は既存 Observation 数に依存せず維持 SHALL される。

#### Scenario: メッセージ 1 通の取り込み p95
- **WHEN** 既存観測数を段階的に増やした instance へ登録チャネルメッセージ 1 通を繰り返し取り込む
- **THEN** import 応答 latency の p95 は各段階で 2 秒未満である
- **AND** 応答経路に全観測走査のフルリビルドを含まない

### Requirement: IM-08 新規 projection の増分 fold 受け入れ条件
LETHE に新規 projection を追加する場合、その projection は増分 fold の定義(append 単位の state 更新規則)を持たなければならない SHALL。増分 fold を定義できない projection は設計上の誤りとみなし、通常 append の応答としてフルリビルドを要求する projection を追加して SHALL NOT ならない。

#### Scenario: 増分 fold 定義を持つ projection の追加
- **WHEN** 新規 projection を substrate へ追加する
- **THEN** その projection は append 単位で state を更新する増分 fold 規則を伴う

#### Scenario: 増分 fold を定義できない projection の拒否
- **WHEN** 通常 append の応答として全観測走査を必要とする projection が提案される
- **THEN** その設計は受け入れ条件を満たさず substrate へ追加しない
