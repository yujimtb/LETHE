## 1. OEL 索引付き keyset 検索

- [ ] 1.1 [Spec Designer] `operational-event-indexed-query` OIQ-01 の filter query 契約(correlation/causation/event_type/stream × keyset cursor)を確定し、`OperationalEventStore` trait(`crates/storage/api/src/lib.rs:128-166`)への追加メソッド署名を文書化する。受入: 各 filter が `after_cursor` + `limit` を伴い O(log N + k) を満たす契約が定まる(design Q4 のオーナー確定を反映)。
- [ ] 1.2 [Implementer] OIQ-02 に従い SQLite の correlation_id / causation_id / event_type を列・複合 index 化し(`schema.rs:28`)、canonical archive / replay 契約を変えずに索引を派生付与する。受入: 既知 correlation/causation/type の検索が索引で解決し監査 trace が有界になるテストが通る。
- [ ] 1.3 [Implementer] OIQ-01 に従い HTTP surface(`server.rs:58`)へ索引付き keyset filter endpoint を追加し cursor 0 全走査 + クライアント側 filter を廃止する。受入: correlation 指定検索が O(log N + k) で返り全 page 走査しないテストが通る。

## 2. keyset ページング

- [ ] 2.1 [Implementer] `keyset-pagination` KSP-01 に従い person 一覧・ClaimQueue・CardQueue・Corpus records に persisted sort key + keyset cursor を導入し、全集合 collect/clone/sort・offset slice・先頭 skip を廃止する(`projection_api.rs:150/660/901`、`read.rs:658`)。受入: 各 API が返却件数に対して O(k)、母集団増で page latency が悪化しないテストが通る。
- [ ] 2.2 [Implementer] KSP-02 に従い person detail / messages / slides / timeline / ReplySLO 読みへ keyset cursor を必須化する(`projection_api.rs:122/196`)。reply-SLO 読みは communication-projection の projection データモデルの上に cursor を課す。受入: 無制限全件応答が cursor page へ置換されるテストが通る。
- [ ] 2.3 [Spec Designer] KSP-03 / design Q1/Q3 に従い cursor 形式の統一到達点(単一 opaque keyset cursor、旧形式の非破壊併存か将来撤去か)と無制限応答 cursor 必須化の移行方式(opt-in / version / 既定上限)を確定する。受入: 4 系統 cursor の統一方針と移行段階が確定する。
- [ ] 2.4 [Implementer] KSP-03 に従い統一 opaque keyset cursor を非破壊追加し、クライアントが共通抽象で扱えるようにする。受入: 既存 cursor client を壊さず統一 cursor が各 API で機能するテストが通る。

## 3. blob 認可の O(1) 化

- [ ] 3.1 [Implementer] `blob-authorization-index` BAI-01 に従い projection materialization に可視 blob 参照表を持たせ、既知 BlobRef の認可を O(1)〜O(log N) にする(`service_support.rs:389`)。受入: 既知 BlobRef 認可が全 person / slide 走査せず可視表引きで判定するテストが通る。
- [ ] 3.2 [Implementer] BAI-02 に従い可視表を materialization と同一 commit で consent delta と同時に upsert/delete し、canonical + consent から決定的に再構築可能にする。受入: 可視表が同一 commit で更新され再構築が決定的になるテストが通る(維持経路は append-commit-and-lock-split の consumer に整合)。

## 4. 検索の cost class 分離

- [ ] 4.1 [Spec Designer] `search-cost-class` SCC-01/SCC-02 と design Q2 に従い exact/metadata 専用経路の契約と任意 regex の cost class 形態(非同期 job / 必須 filter / 明示 opt-in)を確定する。受入: exact 経路と regex cost class の境界と既定形態が定まる。
- [ ] 4.2 [Implementer] SCC-01 に従い exact metadata / object-id 検索の専用索引経路を追加し ID 回収を grep 500ms 依存から外す(`search.rs:178`、`grep.rs:12`)。受入: exact 検索が O(postings + candidate) で ID 回収でき grep timeout に依存しないテストが通る。
- [ ] 4.3 [Implementer] SCC-02 に従い literal 抽出不能 regex を通常 SLO から分離した cost class として扱い、persistent-search-index の索引実装を変えずに契約を積層する。受入: 任意 regex が通常 exact 検索の SLO と分離されるテストが通る。

## 5. sync 状態復元

- [ ] 5.1 [Implementer] `sync-state-restore` SSR-01/SSR-02 に従い AppCore 生成時の default リセットを廃止し永続 `sync_metrics` を起動時に厳密ロード、欠損・不整合を明示する(`mod.rs:1084`、`schema.rs:150`、`service_support.rs:8`)。受入: 再起動後に health が実 sync 状態を返し欠損時に明示するテストが通る。

## 6. 検証と回帰

- [ ] 6.1 [Reviewer] KSP-01/KSP-02/OIQ-01 の計算量を実測する。受入: 母集団を段階的に増やした instance で各読みの latency が返却件数に依存し全集合に依存しないことを確認する。
- [ ] 6.2 [Reviewer] workspace 全テスト、cargo fmt、clippy を実行し、operational-event-ledger の append 契約、persistent-search-index の索引契約、person-page / grep-api の意味論、communication-projection の reply-SLO 読みに回帰がないことを確認する。受入: 全コマンド成功、既存テスト全緑。
