## Context

取り込み `POST /api/import/observation-drafts` は同一リクエスト・同一 critical section で ledger append → 非 corpus materialize → 検索 index catch-up → audit を同期実行する(`apps/selfhost/src/self_host/app/mod.rs:5478-5556`、`service_support.rs::materialize_after_observation_append`)。応答時間は `O(B + COUNT(N) + projection Δ + manifest 全量 S + index catch-up Δ + fsync)`。監査 D 章の総評は「全量から整合性を再証明してから応答する設計癖」を根因とし、51 秒フルリビルド事故はその最大の表面化とする。

AppCore・primary persistence・OEL・history projection は単一 `Mutex`(`mod.rs:1320`)で、通常 import は bulk-operation lock と AppCore lock をリクエスト全体で保持する(`mod.rs:5447`)。`spawn_blocking`(`server.rs:399`)は async worker を保護するだけで storage 並行性を増やさない。PostgreSQL も内部で単一 `Client` を mutex 化する(`postgres/lib.rs:19`)。

canonical Observation ledger は append-only の正本で、projection は破棄・再生可能な派生 materialization である(Append-Only / Replay / Effect Isolation / No Direct Mutation Law)。

## Goals / Non-Goals

**Goals:**

- canonical append + per-item ID + 最小 durable audit/outbox を一つの commit 境界にし、その成功で応答を確定する。
- projection / index / 遅延 audit を append-seq consumer へ分離し、派生失敗が ACK を反転しないことを実装で保証する。
- 単一 mutex を canonical 書き込み / 派生消費者 / 読み取りの 3 lane へ分割し、読み取りの相互ブロックと書き込みへの直列化を解消する。
- 境界確認・partition tree・manifest・lineage digest を O(差分)または保存済み scalar にする。
- audit を fail-closed にし、同期必須 / 遅延許容を区分し、in-memory mirror を廃止する。

**Non-Goals:**

- 取り込み wire 契約(ingestion-api-contract)、materialize 増分化(communication-projection)、読み取り keyset 化(indexed-keyset-reads)。
- consent / retraction / privacy。

## Decisions

### D1: commit 境界を canonical append + per-item ID + 最小 durable outbox に限定する(CAB-01)

成功応答が依存してよいのは、(a) canonical Observation の durable append、(b) request 内 per-item Observation ID の確定、(c) mandatory audit と派生駆動用 outbox marker(append-seq high-water)の同一トランザクション永続化、の 3 つに限る。projection materialize・検索 index catch-up・遅延許容 audit はこの境界の外へ出す。B-01 の「append 済みなのに timeout で未保存判断」は、境界成功で即応答することで解消する。

### D2: 派生処理は append-seq consumer として応答後に駆動する(CAB-02 / CAB-03)

派生 consumer は特定の HTTP request の成否ではなく canonical ledger の append-seq(cursor / high-water)に対して駆動する。これにより B-01 の破綻(再送が Duplicate になると `request_appended_observations` が空で materialization を再実行しない)を構造的に解消する — request が Duplicate を返しても未消費の append-seq が残っていれば consumer は追いつく。派生失敗は応答 outcome を反転せず projection health / 運用シグナルで surface する。

**communication-projection との境界:** materialize の増分化・classify 分岐・背景リビルドの state machine は communication-projection の責務。本 change はその materialize を「commit 境界の外で append-seq を消費する consumer」として駆動する土台(append-seq outbox と consumer runtime)のみを提供する。両者は同じ materialize orchestration を触るため実装時にマージ順序を調整する(下記 Dependencies)。

**ingestion-api-contract との境界:** IRC-04 は ACK セマンティクス(応答↔台帳、派生失敗が outcome を覆さない)を宣言と応答形状として定義する。本 change はその宣言を成立させる commit 境界と consumer 分離の実装的裏付けを提供する。

### D3: 単一 mutex を 3 lane へ分割する(SLP-01/02/03)

- **canonical 書き込み lane:** 短時間の writer lock。append トランザクションと outbox marker のみを保持し、blob I/O・page 走査・network 待ちの間は保持しない。
- **派生消費者 lane:** append-seq consumer 用。書き込み lane と別ロック/別 connection。
- **読み取り lane:** immutable snapshot を `Arc` で公開し lock なしで参照。SQLite は read connection pool、PostgreSQL は connection pool を書き込みと分離する。

`spawn_blocking` は並行化とみなさない。B-02 の実測(監査系 2 並行で両方ハング)は、独立読み取りが同一 mutex に直列化されない設計で解消する。

### D4: 境界確認を保存済み scalar で O(1) にする(BDW-01)

増分 materialize / OEL / 検索 catch-up の境界確認(差分の有無・high-water)は、transaction 内で単調更新する保存済み count / high-water 行、または `MAX(PK)` と保存済み count を読む。通常 catch-up で `COUNT(*)` の全件整合性検証をしない。`observation_stats()`(`service_support.rs:245`、`persistence/mod.rs:138/1988`)の O(N) を除去する。

### D5: partition tree を immutable snapshot + 差分適用にする(BDW-02)

`load_partition_tree()` + `PartitionTree::from_events` の全 `partition_log` replay(`persistence/mod.rs:190/1142/1827`)を append ごとに行わない。起動時に tree を再生して immutable snapshot 化し、partition control event 追加時のみ差分適用して atomic 交換する。通常 append と OEL append の route は O(tree depth)。

### D6: manifest / ClaimQueue を per-row 分割し lineage digest を保存する(BDW-03/04)

全 write での manifest 全体 JSON 上書き(`mod.rs:969`、`persistence/mod.rs:1283`)を廃止し、manifest を scalar metadata と個別 row state へ分割して変更 row だけ transactional upsert する。ClaimQueue / Decision(`mod.rs:685/2426`)は keyed reducer と逆 index へ分解し affected record に対する O(Δ log S) で更新する。lineage digest / count(`service_support.rs:813`)は全 supplemental ID を毎回 collect・sort・hash せず、affected 分だけ増分更新した保存済み scalar を供給する。**読み取り経路の lineage pagination は indexed-keyset-reads の責務**で、本 change は write 時の digest 計算のみを扱う。

### D7: audit を fail-closed にし区分する(ADC-01/02/03)

mandatory audit は commit 境界(D1)内で durable append し、その失敗時は保護操作も失敗させる(`mod.rs:5388` の fail-open 廃止)。audit を同期必須(mandatory durable)部分と遅延許容部分に区分し、遅延許容部分は append-seq consumer(D2)として応答後に実行する。無制限 `Vec` の InMemoryAuditLog(`audit.rs:18`)を廃止し、audit 読みは永続台帳の page query で供給する。

## Risks / Trade-offs

- **[派生 consumer の遅延で読みが stale]** → 鮮度は communication-projection IM-05 の鮮度契約に載せる(通常 5 秒 / 背景リビルド 60 秒)。本 change は consumer 駆動の土台を提供し鮮度値は comm-projection に委ねる。
- **[audit fail-closed で可用性低下]** → 同期必須部分を最小(commit 境界内の 1 durable append)に絞り、遅延許容部分を consumer へ逃がす。DB 障害時に保護操作が監査なしで成功する現状(B-12)より正しい。
- **[3 lane 分割が既存 lock 前提のコードに波及]** → snapshot 公開と read pool を先に入れ、書き込み lane の縮小は段階導入。
- **[outbox と materialize のマージ衝突]** → communication-projection と `mod.rs` 取り込み経路を共有するため実装順を調整(Dependencies)。

## Dependencies / スコープ重複の回避

- **ingestion-api-contract:** IRC-04 の ACK 宣言の実装的裏付け。wire 応答形状は触らない。
- **communication-projection:** 本 change の append-seq consumer 土台の上に materialize 増分化・背景リビルド state machine が載る。classify 分岐・reply-SLO projection には触れない。
- **indexed-keyset-reads:** 読み取り lane(SLP)の並行性は本 change が提供し、その lane 上の keyset/index query は indexed-keyset-reads が定義する。lineage は write digest(本 change)と read pagination(indexed-keyset-reads)で責務分割。
- **observation-lake / operational-event-ledger:** append 契約を変更せず内部 commit 境界・計算量を規定する。

## Open Questions(オーナー確定が必要)

1. **Q1 outbox 実体:** 派生駆動を (a) canonical ledger の append-seq を直接消費する(専用 outbox テーブルなし)か、(b) 明示的 outbox テーブルへ marker を書くか。(a) は二重書き込みを避けるが consumer の再開位置管理が要る。
2. **Q2 audit の同期必須境界:** mandatory とする audit の範囲(全保護操作 / write 系のみ / 認可 deny のみ)をどこで引くか。広いほど durability が強いが commit 境界のコストが増える。
3. **Q3 lane 分割の storage 対象:** read connection pool を SQLite / PostgreSQL 双方で入れるか、personal(SQLite)を先行させるか。
4. **Q4 派生 consumer の failure surface:** projection health の露出先(既存 health endpoint 拡張 / 新規運用シグナル)。
