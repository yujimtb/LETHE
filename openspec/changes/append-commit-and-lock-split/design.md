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
- **読み取り lane:** immutable snapshot を `Arc` で公開し lock なしで参照。SQLite の read connection pool と PostgreSQL の connection pool を**同一実装スコープでまとめて**書き込みと分離する(両バックエンドの差は小さく段階分けしない。旧 Q3 確定)。

`spawn_blocking` は並行化とみなさない。B-02 の実測(監査系 2 並行で両方ハング)は、独立読み取りが同一 mutex に直列化されない設計で解消する。

### D4: 境界確認を保存済み scalar で O(1) にする(BDW-01)

増分 materialize / OEL / 検索 catch-up の境界確認(差分の有無・high-water)は、transaction 内で単調更新する保存済み count / high-water 行、または `MAX(PK)` と保存済み count を読む。通常 catch-up で `COUNT(*)` の全件整合性検証をしない。`observation_stats()`(`service_support.rs:245`、`persistence/mod.rs:138/1988`)の O(N) を除去する。

### D5: partition tree を immutable snapshot + 差分適用にする(BDW-02)

`load_partition_tree()` + `PartitionTree::from_events` の全 `partition_log` replay(`persistence/mod.rs:190/1142/1827`)を append ごとに行わない。起動時に tree を再生して immutable snapshot 化し、partition control event 追加時のみ差分適用して atomic 交換する。通常 append と OEL append の route は O(tree depth)。

### D6: manifest / ClaimQueue を per-row 分割し lineage digest を保存する(BDW-03/04)

全 write での manifest 全体 JSON 上書き(`mod.rs:969`、`persistence/mod.rs:1283`)を廃止し、manifest を scalar metadata と個別 row state へ分割して変更 row だけ transactional upsert する。ClaimQueue / Decision(`mod.rs:685/2426`)は keyed reducer と逆 index へ分解し affected record に対する O(Δ log S) で更新する。lineage digest / count(`service_support.rs:813`)は全 supplemental ID を毎回 collect・sort・hash せず、affected 分だけ増分更新した保存済み scalar を供給する。**読み取り経路の lineage pagination は indexed-keyset-reads の責務**で、本 change は write 時の digest 計算のみを扱う。

### D7: 監査イベントの durable enqueue を commit 境界内・同期・fail-closed にする(ADC-01/02/03)

オーナー懸念(「遅延許容だと実装の漏れで監査ができない可能性」)への設計回答として、**遅延を許すのは監査記録の書き出し・整形(projection 化・可読レンダリング・集計)のみ**とし、**監査イベントの durable な登録(enqueue)は canonical commit 境界(D1)内で同期・fail-closed** に行う。すなわち本体保護操作の成功は「監査イベントが canonical 台帳に載っていること」と等価であり、enqueue の永続化に失敗したら保護操作も失敗させる(`mod.rs:5388` の fail-open 廃止)。台帳に載った監査イベントの後続の書き出し・整形は append-seq consumer(D2)として応答後に実行してよく、その consumer 遅延(想定 数秒〜数十秒)は audit / projection health で可視化する。無制限 `Vec` の InMemoryAuditLog(`audit.rs:18`)を廃止し、audit 読みは永続台帳の page query で供給する。

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

## 確定事項(オーナー決定 2026-07-23)

1. **outbox = append-seq 直接消費(旧 Q1 確定):** 派生駆動は canonical ledger の append-seq(cursor / high-water)を直接消費し、専用 outbox テーブルは設けない。append-only 原則(canonical 台帳が唯一の順序源)からの導出で、二重書き込みと二台帳同期を避ける。consumer は自身の再開位置(消費済み cursor)のみを保持する。
2. **派生失敗 = 台帳の専用エラーイベント + health(旧 Q4 確定):** 派生 consumer の失敗は lake authoritative に従い canonical 台帳へ専用のエラーイベントとして記録し、加えて projection health で可視化する。取り込み応答の outcome は反転しない。
3. **audit の durable enqueue は commit 境界内・同期・fail-closed(D7):** 遅延を許すのは監査記録の書き出し・整形のみ。監査イベントの durable enqueue は commit 境界内で同期・fail-closed とし、保護操作成功=監査イベントが台帳に載っていることの保証とする。consumer 遅延の想定は数秒〜数十秒で health 可視化する。
4. **read pool 分割は SQLite / PostgreSQL をまとめて同一実装スコープ(旧 Q3 確定):** 両バックエンドの差は小さいため適用順の段階分けをせず、read connection pool(SQLite)と connection pool(PostgreSQL)の書き込みからの分離を一つの実装スコープで行う。

## Open Questions(オーナー確定が必要)

1. **Q2 同期必須 audit の操作リスト(細部):** commit 境界内で同期 enqueue を必須とする保護操作の具体リスト(全保護操作 / write 系のみ / 認可 deny を含むか)。durability を広げるほど commit 境界コストが増える。方式(commit 境界内・同期・fail-closed)は確定済みで、残るのは対象操作の粒度のみ。
