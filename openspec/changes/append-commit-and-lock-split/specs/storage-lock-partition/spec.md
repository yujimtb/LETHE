## ADDED Requirements

### Requirement: SLP-01 単一 mutex の 3 lane 分割

AppCore・primary persistence・OEL・history projection を直列化する単一 `Mutex` は、canonical 書き込み lane・派生消費者 lane・読み取り lane の 3 系統へ分割 SHALL する。読み取り lane は書き込み lane の critical section へ直列化 SHALL NOT される。

#### Scenario: 読み取りが書き込み critical section に直列化されない
- **WHEN** canonical 書き込み lane が長い import を保持している間に読み取り(既知 ID 読み・cursor page・別 projection 読み)が来る
- **THEN** 読み取りは書き込み lane の critical section を待たずに直前公開の snapshot から応答する

### Requirement: SLP-02 並行読み取りの非ブロック

独立した読み取り(既知 ID 読み・cursor page・別 projection 読み・blob 取得)は相互にブロック SHALL NOT する。単一の長い import / history query / blob I/O が独立読み取りを停止 SHALL NOT する。

#### Scenario: 2 並行読み取りが両方応答する
- **WHEN** 監査系の読み取り 2 件が同時に実行される
- **THEN** 両者は相互にブロックせずそれぞれ応答する
- **AND** どちらも他方または進行中の書き込みによってハングしない

#### Scenario: 長い書き込み中も独立読み取りが進む
- **WHEN** 長時間の import / history query / blob I/O が進行中に独立読み取りが来る
- **THEN** 独立読み取りはその書き込み・I/O の完了を待たずに応答する

### Requirement: SLP-03 I/O 中に排他ロックを保持しない

canonical 書き込み lane の排他ロックは短時間 SHALL とし、blob I/O・page 走査・network 待ちの間に AppCore lock を保持 SHALL NOT する。読み取り lane は immutable snapshot を `Arc` で公開して lock なしで参照 SHALL する。read connection pool の分離は SQLite と PostgreSQL の両バックエンドを同一実装スコープで対象 SHALL とし、いずれのバックエンドでも読み取り connection を書き込みと分離 SHALL する。

#### Scenario: I/O 待ち中にロックを手放す
- **WHEN** 書き込み処理が blob I/O・page 走査・network 待ちに入る
- **THEN** その待ち時間中 AppCore の排他ロックを保持しない

#### Scenario: snapshot は lock なしで読める
- **WHEN** 読み取りが immutable snapshot を参照する
- **THEN** 読み取りは書き込みロックを取得せず `Arc` snapshot から応答する

#### Scenario: 両バックエンドで read pool を分離する
- **WHEN** SQLite バックエンドまたは PostgreSQL バックエンドで読み取りが実行される
- **THEN** いずれのバックエンドでも読み取り connection は書き込みと分離された pool から供給される

#### Scenario: stale 公開と consumer の全置換を同一 lane で直列化する
- **WHEN** bulk import の開始／deferred append または派生 consumer の失敗が非 corpus projection を stale として公開する
- **THEN** その stale 更新は派生 consumer の全置換 writeback と同じ `derived_projection_lane` を保持して実行される
- **AND** stale フラグが consumer の snapshot 全置換によって取りこぼされない
