## 1. commit 境界と派生分離

- [ ] 1.1 [Spec Designer] `commit-ack-boundary` CAB-01 の commit 境界(canonical append + per-item ID + 最小 durable audit/outbox marker)を確定し、ingestion-api-contract IRC-04 の ACK 宣言との対応表を文書化する(参照: `mod.rs:5478-5556`)。受入: 境界に含む / 含まないの区分が一意に定まり、IRC-04 の宣言と矛盾しないことを確認する。
- [ ] 1.2 [Implementer] CAB-01 に従い取り込み応答を commit 境界成功で確定し、projection materialize・検索 index catch-up・遅延 audit を応答経路から外す(`service_support.rs::materialize_after_observation_append`)。受入: 応答経路が commit 境界のみを含み、派生処理完了を待たないテストが通る。
- [ ] 1.3 [Implementer] CAB-02/CAB-03 に従い派生処理を append-seq(cursor / high-water)consumer として駆動し、request 成否・duplicate 判定に依存させない。派生失敗は canonical 台帳の専用エラーイベント + projection health で surface する(旧 Q4 確定)。受入: 派生失敗が outcome を反転せず台帳エラーイベント + health で surface し、duplicate 応答後も未消費 append-seq を consumer が追いつくテストが通る。

## 2. lock lane 分割

- [ ] 2.1 [Implementer] SLP-01/SLP-03 に従い単一 mutex を canonical 書き込み / 派生消費者 / 読み取りの 3 lane へ分割し、immutable snapshot を `Arc` で公開、SQLite read connection pool・PostgreSQL connection pool を書き込みと分離する(参照: `mod.rs:1320/5447`、`postgres/lib.rs:19`、`server.rs:399`)。受入: 読み取りが書き込み critical section へ直列化されないテストが通る。
- [ ] 2.2 [Reviewer] SLP-02 に従い独立読み取りの非ブロックを実測する。受入: 監査系読み取り 2 並行が両方応答し、長時間 import / blob I/O 進行中も独立読み取りが進むことを確認する(B-02 の 2 並行ハング再現が解消)。
- [ ] 2.3 [Implementer] SLP-03 に従い blob I/O・page 走査・network 待ちの間 AppCore lock を保持しないよう書き込み lane を縮小する。受入: I/O 待ち中にロックを保持しないことのテストが通る。

## 3. 書き込み側 O(差分) 化

- [ ] 3.1 [Implementer] BDW-01 に従い境界確認を保存済み count / high-water scalar 化し `observation_stats()` の O(N) `COUNT(*)` を通常 append 経路から除去する(`service_support.rs:245`、`persistence/mod.rs:138/1988`、`search-index/index.rs:669`)。受入: 1 件 append で全件 count しないテストが通る。
- [ ] 3.2 [Implementer] BDW-02 に従い partition tree を起動時 immutable snapshot 化し、partition event 追加時のみ差分適用・atomic 交換にする(`persistence/mod.rs:190/1142/1827`)。受入: 通常 append / OEL append のルーティングが O(tree depth) で `partition_log` 全体を replay しないテストが通る。
- [ ] 3.3 [Implementer] BDW-03 に従い manifest を scalar metadata + per-row state へ分割して変更 row だけ transactional upsert し、ClaimQueue / Decision を keyed reducer + 逆 index へ分解する(`mod.rs:685/969/2426`、`persistence/mod.rs:1283`)。受入: 1 件 write が manifest 全体を書き直さず、ClaimQueue 更新が affected record に比例するテストが通る。
- [ ] 3.4 [Implementer] BDW-04 に従い lineage digest / count を affected 分の増分更新した保存済み scalar で供給する(`service_support.rs:813`)。受入: write 時に全 supplemental ID を collect・sort・hash しないテストが通る(読み取り pagination は indexed-keyset-reads の対象)。

## 4. audit durability

- [ ] 4.1 [Implementer] ADC-01 に従い監査イベントの durable enqueue を commit 境界内・同期・fail-closed にし、enqueue 永続化失敗時に保護操作も失敗させる(`mod.rs:5388` の fail-open 廃止)。保護操作成功=監査イベントが台帳に載っている保証。受入: enqueue 失敗で保護操作も失敗するテストが通る。
- [ ] 4.2 [Spec Designer] ADC-02 に従い commit 境界内で同期 enqueue を必須とする保護操作の具体リスト(design Q2 の細部)を確定する。方式(commit 境界内・同期・fail-closed)は確定済みで、残るのは対象操作の粒度のみ。受入: どの保護操作が同期 enqueue 必須かが一意に定まる。
- [ ] 4.3 [Implementer] ADC-03 に従い無制限 in-memory audit mirror(`audit.rs:18`)を廃止し audit 読みを永続台帳の page query に置換する。受入: 全履歴を in-memory に保持せず page query で供給するテストが通る。

## 5. 検証と回帰

- [ ] 5.1 [Reviewer] 取り込み応答 latency が既存 Observation 数に依存せず有界であること(応答経路に全量再計算を含まないこと)を実測する。受入: 段階的にデータ量を増やした instance で応答経路が O(全観測数) 要因を含まない。
- [ ] 5.2 [Reviewer] workspace 全テスト、cargo fmt、clippy を実行し、observation-lake / operational-event-ledger の append 契約、communication-projection の materialize 経路、ingestion-api-contract の応答形状に回帰がないことを確認する。受入: 全コマンド成功、既存テスト全緑、両 change とのマージ順序衝突がない。
