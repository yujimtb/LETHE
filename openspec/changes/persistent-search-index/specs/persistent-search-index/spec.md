## ADDED Requirements

### Requirement: PSI-01 永続 Corpus 検索 materialization
LETHE は Corpus Projection の検索用 materialization をスキーマ版付きオンディスク index として永続化 SHALL する。有効な index がある起動では全 Corpus record を再生成 SHALL NOT し、初回または index スキーマ版不一致の場合だけ canonical Observation から再構築 SHALL する。

#### Scenario: 有効な index を開く再起動
- **WHEN** 現行スキーマ版で正常に commit 済みの index が存在して LETHE が起動する
- **THEN** LETHE はその index を開いて検索可能にする
- **AND** 全 Observation の再ロードまたは全 index 再構築を行わない

#### Scenario: 初回構築
- **WHEN** canonical Observation は存在するが index directory が存在しない
- **THEN** LETHE は Observation を有限サイズの page で読み、Corpus filter を適用して index を構築する
- **AND** 構築完了を atomic に公開するまで検索を index unavailable エラーにする

#### Scenario: スキーマ変更
- **WHEN** 保存 index のスキーマ版が実装の要求版と異なる
- **THEN** LETHE は保存 index を現行 index として開かない
- **AND** canonical Observation から現行版 index を再構築する

### Requirement: PSI-02 増分かつ冪等な更新
LETHE は canonical Observation の durable append に成功したとき、その Observation から露出可能な Corpus record だけを index へ差分反映 SHALL する。更新は record_id 単位の upsert とし、同一 Observation または同一 record を再処理しても検索可能 entry を重複させて SHALL NOT ならない。

#### Scenario: 新規 Observation の差分追加
- **WHEN** 新規 Observation の durable append が成功して一つ以上の Corpus record が生成される
- **THEN** LETHE は生成された record だけを同一 index commit に追加する
- **AND** 既存の全 Corpus record を再構築しない

#### Scenario: duplicate-only 再投入
- **WHEN** durable store が Observation を Duplicate と判定する
- **THEN** index の document 数と検索結果集合は変化しない

#### Scenario: record の再処理
- **WHEN** crash recovery または watermark catch-up が同じ record_id を再処理する
- **THEN** LETHE は旧 entry を置換し、その record_id の検索可能 entry を一件だけ保持する

#### Scenario: durable append 後の index commit 失敗
- **WHEN** canonical Observation の append 後に index commit が失敗する
- **THEN** LETHE は index を unavailable にして明示エラーを返す
- **AND** canonical Observation を削除または変更せず、再構築または watermark catch-up で回復する

### Requirement: PSI-03 検索 v2 契約の等価性
永続 index を使う検索は既存 `grep-api` の検索 v2 契約と同一の match 集合、順序、pagination、表示内容を返さなければならない SHALL。index は候補絞り込みにだけ使い、NFKC 正規化後の原文に対する既存 regex 意味論を最終判定に使用 SHALL する。

#### Scenario: 複合語 AND と全角空白
- **WHEN** 半角空白、全角空白、または tab で区切った複数 term を検索する
- **THEN** term の順序と距離にかかわらず全 term に一致する record だけを返す

#### Scenario: filter と order
- **WHEN** from、to、order、source types、channels、containers の任意の組合せを指定する
- **THEN** 永続 index 検索は既存検索 v2 と同じ inclusive range、date asc / desc と record_id tie-break、filter 結果を返す

#### Scenario: snippet と match range
- **WHEN** 一致が長い本文の中央または末尾にある
- **THEN** snippet は最初の hit を含む最大 240 文字の窓と必要な省略記号を返す
- **AND** matched_ranges は検索対象テキスト内 UTF-8 byte offset で最大 20 件を返す

#### Scenario: cursor pagination
- **WHEN** limit を超える結果を next_cursor で走破する
- **THEN** date asc / desc の双方で record の重複と欠落なく取得できる
- **AND** cursor 発行後にそれより新しい record が追加されても既取得 record を再返却しない

#### Scenario: HTTP と MCP の wire contract
- **WHEN** 既存 HTTP または MCP client が検索 v2 を呼び出す
- **THEN** request field、response field、既定値、field 名と型を変更せず応答する

### Requirement: PSI-04 有界メモリ検索
検索 runtime は全 Observation または全 Corpus record をメモリに常駐させて SHALL NOT ならない。index の reader と、現在の候補 page および返却に必要な record だけをメモリへ読み込む SHALL する。

#### Scenario: 66.5 万件相当の検索
- **WHEN** 665,000 件の Corpus record を持つ instance で代表検索 workload を実行する
- **THEN** LETHE process の peak RSS は 2.5 GiB 以下である

#### Scenario: 検索要求
- **WHEN** limit 20 の検索を実行する
- **THEN** request ごとに全 Observation、全 Corpus record、または全件 n-gram postings を heap 上へ再構築しない

#### Scenario: 再構築
- **WHEN** canonical Observation から index を再構築する
- **THEN** Observation を設定された有限 page 単位で処理し、全件を同時に保持しない

### Requirement: PSI-05 破損検知と単一バックグラウンド再構築
LETHE は index open、reader reload、query、document decode、commit の破損エラーを検知した場合、index 状態を unavailable / rebuilding に遷移させ、単一のバックグラウンド再構築を開始 SHALL する。再構築中の検索は明示エラーとし、空結果、全件 scan、旧 index へ fallback して SHALL NOT ならない。

#### Scenario: 起動時破損
- **WHEN** index metadata または segment が破損して open / validation に失敗する
- **THEN** LETHE は readiness を failed にし、バックグラウンド再構築を一度だけ開始する
- **AND** service process 自体は状態確認と回復完了を提供できる

#### Scenario: 稼働中破損
- **WHEN** 検索または incremental commit が index 破損を検知する
- **THEN** その要求を index unavailable エラーにする
- **AND** 同時要求が複数あっても再構築 task は一つだけ実行する

#### Scenario: 再構築完了
- **WHEN** 一時 directory での全量構築、commit、整合検証が成功する
- **THEN** LETHE は index directory を atomic に切り替えて reader を公開する
- **AND** watermark、document 数、schema version を検証後に readiness を healthy にする

#### Scenario: 再構築失敗
- **WHEN** バックグラウンド再構築が失敗する
- **THEN** LETHE は unavailable 状態と診断可能な error detail を保持する
- **AND** 検索を再開しない

### Requirement: PSI-06 再現可能な watermark と整合性
index は反映済み canonical append position、schema version、Corpus projector version を永続 metadata として保持 SHALL する。検索応答の `projection_watermark` は公開済み index commit に対応し、同じ commit では同じ入力と query に対して同じ結果を再現 SHALL する。

#### Scenario: crash 後の catch-up
- **WHEN** durable append の後かつ index commit の前に process が停止する
- **THEN** 次回起動は保存 watermark より後の Observation だけを page 読取して catch-up する
- **AND** upsert により重複 entry を作らない

#### Scenario: commit 境界
- **WHEN** incremental batch の index commit が成功する
- **THEN** その batch の最終 append position と document 変更が同じ公開世代として検索へ現れる

### Requirement: PSI-07 性能ゲートの実証
実装は repository 外の一時 directory と 4 GiB memory limit を使い、10k、50k、100k、500k の合成 Corpus で検索 workload を測定 SHALL する。合成 Corpus は複数年と複数 channel に分布させ、直近1年相当の日付 range、channel/source、filter 付き複合語 AND を実効クエリ群とし、絞り込み不能な全体検索群と分離 SHALL する。各段階で各群を warm-up 後 20 回以上、並列 2、limit 20 で実行し、群別の wall-clock p95、mean、max、warm-up failure、peak RSS、OOM / swap 状態を記録 SHALL する。

#### Scenario: 段階性能測定
- **WHEN** 各 dataset size の測定を実行する
- **THEN** 再現可能なコマンド、データ生成条件、query mode、filter selectivity、hardware、回数と群別実測表を結果報告へ記録する

#### Scenario: 実効検索 SLO
- **WHEN** 各段階の日付 range、channel/source、filter 付き複合語 AND の実効クエリ群を測定する
- **THEN** 実効クエリ群の検索 p95 は 2 秒以下である
- **AND** warm-up と計測 request は失敗しない
- **AND** OOM kill は 0、swap 使用は 0、peak RSS は 2.5 GiB 以下である

#### Scenario: 絞り込み不能な全体検索
- **WHEN** filter で候補を減らせない全体検索群を測定する
- **THEN** warm-up と計測 request は失敗せず p95、mean、max を参考値として記録する
- **AND** 全体検索群の p95 を実効検索 SLO の合否判定に使用しない
