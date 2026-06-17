# Spec Delta: runtime

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-17

## Dependencies

- M01 Domain Kernel — 型・law(Effect Isolation Law / Replay Law の正規参照)
- M03 Observation Lake — per-leaf SQLite / `append_seq` / rehome primitive(本 change の `observation-lake` delta SHARD-05〜07)
- M09 Adapter Policy — adapter が source / container を canonical タプル経由で宣言(本 change の `adapter-policy` delta SHARD-ADAPT-01)
- M15 Runtime — 既存 `openspec/specs/runtime.md` の中核(topology / sandbox / health / 参照技術選定)は **不変**
- 正典: [sharding_refactor.md](../../../../sharding_refactor.md) §2 D4〜D8 / D11

> 本 delta は M15 の topology / sandbox / health 概念を **変更しない**。物理 Lake の分割(routing key / Patricia trie / split protocol / partition log / failover / logical→physical resolver)を runtime の責務として ADDED で規定する。

---

## ADDED Requirements

### Requirement: SHARD-RT-01 Routing Key Contract

`routing_key` は次の固定軸順で構成 **しなければならない (SHALL)**:

```
routing_key = coarse(published month)
            : coarse(published year)
            : source
            : container(workspace, channel)
            : fine(published)
```

- 時刻成分はすべて `published`(event time)で **なければならない (SHALL)**。`recordedAt`(ingestion time)を含めてはならない (SHALL NOT)。理由: 再 export を別月/年に処理すると `recordedAt` が変わり別 leaf に route → 偽「新規」→ 重複挿入(不変条件2)。
- 上位 routing に暗号 hash を **使ってはならない (SHALL NOT)**。最下位 balance も hash でなく `fine(published)` で **なければならない (SHALL)**(projection 可視 / leaf 内時系列順序 / 負荷を単調 spread)。
- 軸順は **month:year**(year:month でない)で **なければならない (SHALL)**。理由: 寮 Slack の支配的読みは「年またぎ同月の串刺し検索」 → month を最上位 split 軸にして「全年の同月」を co-locate。
- placement 原則: **routing 軸 + プリミティブ属性のみ** で prune (SHALL)。person / project / 意味軸 / 解決を要する派生軸(person, subject, project)は placement にも leaf-prune にも入れてはならない (SHALL NOT)(不変条件5)。
- `routing_keyspec` は per-workspace で **version pin**、変更は in-place 不可、blue/green migration(SHARD-09)で **しなければならない (SHALL)**。

#### Scenario: 寮 Slack の同月串刺し読みが co-locate される

- **WHEN** 「2024-04 と 2025-04 と 2026-04 のメッセージ」を検索する
- **THEN** month 軸が先頭のため 3 つの年は同月配下の bounded な部分木で touch される
- **AND** 在籍 ≤ 2 年なら year 次元は最大 2 値で `(month, year)` bucket 数は bounded

#### Scenario: hash 軸が現れない

- **WHEN** routing key 計算 module を grep する
- **THEN** SimHash / hash bucket / random hash は使われない
- **AND** balance は `fine(published)` の単調 spread のみ

### Requirement: SHARD-RT-02 Patricia Trie with Lazy Capacity-Driven Split

leaf 構造は **Patricia trie**(固定ビット trie ではない)として保持 **しなければならない (SHALL)**。split は **容量駆動 (capacity-driven)** であり、content 分布に駆動されてはならない (SHALL NOT)。

- **lazy split**: 容量到達まで leaf を浅いまま太らせる。割る時は leaf の中身ごと全再配置(rehome mode (a))して次の discriminating bit で子へ。
- route は **常に現在 tree の純関数** で **なければならない (SHALL)**(不変条件1)。タイミング・履歴・ingestion 時刻に依存してはならない (SHALL NOT)。
- 「上から埋める / 新データだけ下層へ」方式は **禁止 (SHALL NOT)**(home がタイミング依存になり dedup が割れる)。
- `tree(L)` は partition log の `initialize` + `split_commit` の fold で **一意復元** 可能で **なければならない (SHALL)**。

#### Scenario: 同 message が再取り込みで同じ leaf に着地する

- **WHEN** 同一 `identity_key` の Observation が時間差で 2 回 ingest される
- **THEN** 両方とも現在 tree が示す同じ leaf に route される
- **AND** 着地 leaf の `identity_key` UNIQUE が 2 回目を `Duplicate` で弾く

#### Scenario: split は容量で起き、content 分布で起きない

- **WHEN** ある leaf の容量上限に達する
- **THEN** 次の discriminating bit で split が走る
- **AND** content 偏り(特定 user / channel への偏在)を理由とした split は起きない

### Requirement: SHARD-RT-03 Atomic Split Cutover Protocol

split は **2 フェーズ + write barrier** で原子的に cutover **しなければならない (SHALL)**:

1. **`split_prepare`**: 親の snapshot から子 leaf を build。route はまだ親を指す(read / route は一貫して親を見る)。
2. **catch-up**: prepare 後に親へ入った新規 ingest の差分を子へ rehome(mode (a))。残差が小さくなったら親の該当 key レンジに **write barrier(短い freeze)** を張り最終差分を catch-up。
3. **`split_commit`**: route を子へ **原子的に切替**(log の `event_seq` で確定)、`bit_index` を必須記録、親を retire、freeze 解除。

availability 重視は snapshot + catch-up で freeze 最小化、単純さ重視は全レンジ freeze。両者とも安全(SHALL)。`split_prepare` だけで `split_commit` されなかった split(crash 等)は `tree(L)` に影響しない (SHALL)。

#### Scenario: split 中の新規 ingest が移送漏れしない

- **WHEN** split 進行中に親へ新規 ingest が走る
- **THEN** catch-up + 最終 barrier 中差分で必ず子へ rehome され、`split_commit` 前に取り残されない
- **AND** `split_commit` 後の route は子を指す

#### Scenario: prepare だけで commit なしは tree に現れない

- **WHEN** `split_prepare` 直後に crash し、`split_commit` が記録されない
- **THEN** 再起動後の `tree(L)` 復元(`initialize` + `split_commit` の fold)に当該 split は現れない
- **AND** route は親を指したまま、子 leaf 候補は破棄して構わない

### Requirement: SHARD-RT-04 Partition Rule Log Schema

partition log は次のイベントを **持たなければならない (SHALL)**: `initialize` / `split_prepare` / `split_commit` / `failover` / `recover`。

- **全イベントが control-plane 単調 `event_seq`** + optional control-plane timestamp を **持たなければならない (SHALL)**。`published` 由来の `at` を含めてはならない (SHALL NOT)(split / failover は event-time を持たない control-plane event、R10)。
- `initialize` は `routing_keyspec` と `identity_keyspec` の **両 keyspec を pin** **しなければならない (SHALL)**(軸順 / encoding / version、canonical_content 規則 / object_id rule / normalization / version)。両 keyspec は **不変**、変更は blue/green migration(SHARD-09)。
- `split_commit` は `bit_index` を **必須** で記録 **しなければならない (SHALL)**(Patricia は深さ ≠ ビット位置なので、どのビットで分岐したか記録しないと `tree(L)` が一意復元できない)。`reason="capacity"` も記録する。
- leaf id は **不透明** な `lake:<uuid>` 形式で **なければならない (SHALL)**(SHALL NOT 構造的解釈)。path / 責任ビット範囲は log から計算する。
- `failover` / `recover` の `event_seq` は failover window の境界として **使われる (SHALL)**(R5)。
- 単一インスタンス(leaf = root のみ、ディレクトリ分割不要)で開始 **してよい (MAY)** が、両 keyspec は **完全形で確定必須** (SHALL)。分割は後付け可、keyspec は後付け不可。

#### Scenario: `initialize` の不変性が DB で強制される

- **WHEN** `initialize` 後に `routing_keyspec` または `identity_keyspec` を編集しようとする
- **THEN** partition log の制約 / DB 制約で拒否される
- **AND** 変更は SHARD-09 blue/green migration を要求する

#### Scenario: partition log replay で tree が一意復元される

- **WHEN** partition log の `initialize` + 全 `split_commit` を `event_seq` 順に fold する
- **THEN** 現在 `tree(L)` が一意に再構成される
- **AND** `split_prepare` だけのイベントは tree に影響しない

### Requirement: SHARD-RT-05 AP + Reconcile Failover with Dedicated Spool

leaf 喪失中も ingest を停めず、L の key レンジを **専用 failover spool** で受け **しなければならない (SHALL)**。近傍 leaf 直書きで受けてはならない (SHALL NOT)(近傍 leaf の exact index を汚す)。

spool の物理形(SQLite テーブル):

```sql
CREATE TABLE spool_<failover-id> (
    spool_seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- 排出順を固定 (R15)
    id TEXT NOT NULL,
    identity_key TEXT NOT NULL,                   -- drain で必要 (rehome mode a 前提、R13)
    canonical_json TEXT NOT NULL,                 -- 同上 (衝突比較に必要)
    published TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    observation_json TEXT NOT NULL
);
```

- spool は **append-only**。spool 内で `identity_key` UNIQUE / leaf `append_seq` の単調性は **持たない** (spool 自身は dedup しない)。
- drain は `ORDER BY spool_seq` で 1 件ずつ実施 **しなければならない (SHALL)**(R15)。
- drain は **rehome mode (a)**(stored `identity_key` 信頼)で着地 leaf に内部 append **しなければならない (SHALL)**。fresh ingest 経路は **通してはならない (SHALL NOT)**(R1、不変条件10)。
- 着地 leaf の `identity_key` UNIQUE が `Duplicate` を決定的に弾く。最終状態に dup は残らない (SHALL)。
- エピソード境界は partition log の `failover` / `recover` の control-plane `event_seq` から取る(R5)。
- durability(replica からの L 物理復元)は本 spec の範囲外(別レイヤー据え置き)。

#### Scenario: transient dup が最終状態に残らない

- **WHEN** failover 中に同一 `identity_key` の Observation が spool に 2 回入る
- **THEN** drain 時に着地 leaf で 1 回だけ `Ingested`、2 回目は `Duplicate`
- **AND** 最終状態の leaf には 1 つしか残らない

#### Scenario: drain は spool_seq 順で再現可能

- **WHEN** drain を replay する
- **THEN** spool 行は `spool_seq` 昇順で着地 leaf に rehome される
- **AND** failover window 境界は partition log の `failover` / `recover` の `event_seq` から決定される

#### Scenario: drain 中に L が split 済みでも現 home へ着地

- **WHEN** failover 中に元 L が split されていた場合
- **THEN** drain 時の re-route は **現 tree** に対して行われ、各 spool 行は現在の home leaf に着地する
- **AND** 旧 L の path を参照しない

### Requirement: SHARD-RT-06 Logical-to-Physical Leaf Resolution

`candidate_leaves(filter, log)` を **提供しなければならない (SHALL)**:

- filter の routing 軸制約 + 現在 tree + split log から prune する(常に最新 tree から計算、staleness なし)。
- **published time-window** は **`(month, year)` bucket 集合に展開** して各 bucket の部分木 union を prune **しなければならない (SHALL)**(R3)。連続時間範囲を lexicographic range として扱ってはならない (SHALL NOT)(month 軸が先頭のため range にならない)。
- source / container(equality)は安定 encode で prune する。
- `fine` 軸 = 無制約のクエリは配下全 leaf を touch する。
- 在籍 ≤ 2 年で year 次元 ≤ 2 値なので bucket 集合は bounded。

#### Scenario: time-window の bucket 展開

- **WHEN** `published in [2024-04-01, 2026-05-31]` を filter で渡す
- **THEN** `(month, year)` bucket 集合 `{(04,2024), (05,2024), ..., (05,2026)}` に展開され、各 bucket 配下の部分木 union が candidate_leaves として返る
- **AND** lexicographic range として扱われない

#### Scenario: staleness なしの prune

- **WHEN** split 直後に candidate_leaves を呼ぶ
- **THEN** 結果は最新 partition log fold 後の tree に基づく
- **AND** 古い tree が露呈することはない

### Requirement: SHARD-RT-07 Streaming K-Way Merge Read Primitive

論理 read primitive は **touch leaf 群の streaming k-way merge by `(published, recorded_at, id)`** で **なければならない (SHALL)**(Law S8)。これが唯一の論理 read。

- resolver は **Imperative Shell** に配置 **しなければならない (SHALL)**(Effect Isolation Law)。Kernel は論理 lake のみを見る (SHALL)。
- propagation 検出の `append_seq` cursor とは **明確に区別** **しなければならない (SHALL)**: read 側 = `(published, recorded_at, id)` の merge-sort、propagation 検出 = leaf-local `append_seq`(`dag-propagation` delta SHARD-PROP-04)。

#### Scenario: read 結果は単一 leaf 時と等価

- **WHEN** 多 leaf 環境で全鑑読みを実行する
- **THEN** 結果は単一 leaf 時の `(published, recorded_at, id)` 順序と等価
- **AND** touch leaf 数や split 履歴に依存しない

### Requirement: SHARD-RT-08 Index Policy (No Implicit Fallback)

placement に **解決済みエンティティを使ってはならない (SHALL NOT)**。次は placement にも leaf-prune にも入れてはならない: `person`、`subject`、`project`。これらを query する際は対応する Projection の出力ストアを読む。

secondary index は **投入時ゼロ**、`supplemental-store` の supplemental record(`Mutability::ManagedCache`、`derived_from` は lake frontier)として **後付け可能** (MAY)。ただし:

- index は **加速器** であり **正しさの依存先ではない** (SHALL)。正しさは base read(placement-prune + read 時 filter)が担保する。
- index が stale な場合の挙動を **暗黙 full-scan fallback にしてはならない (SHALL NOT)**(R6)。呼び出し側が宣言する **明示 read mode**(base / full-scan を opt-in)または **stale-with-marker** または **fail-fast** に倒す。
- upstream 改訂時の rebuild は projection 作者の責務 (SHALL)。

#### Scenario: 暗黙 full-scan が起きない

- **WHEN** secondary index が stale な状態で呼び出される
- **THEN** 設定された明示 read mode(base / fail-fast / stale-with-marker)に従って応答する
- **AND** 呼び出し側が知らないうちに full-scan に落ちることはない

#### Scenario: 解決済み person で leaf prune しない

- **WHEN** 「person P の最近の発言を見せろ」というクエリが来る
- **THEN** base read 経路は person 解決を行わず、person projection 出力ストアが query 面になる
- **AND** placement / leaf-prune 段では `person` 軸を見ない

---

## MODIFIED Behaviors (parent spec 参照)

既存 `openspec/specs/runtime.md` の以下の節は、本 delta の要件で内容が追加 / 変わる:

- **§2 Reference Topology**: 「Observation Lake」を **「論理 Lake = 物理 leaf 群 + 現 tree + partition log」** として再解釈(SHARD-RT-01〜04)。
- **§Reference Tech Mapping**: per-leaf SQLite + partition log SQLite + failover spool SQLite の 3 種を runtime 構成として追加(SHARD-RT-04 / SHARD-RT-05)。
- **§Health and heartbeat**: leaf 喪失検出と failover 起動を control-plane の `failover` / `recover` event 発行と接続(SHARD-RT-05)。
- **§Effect Isolation surface**: resolver(`candidate_leaves` + streaming k-way merge)を Imperative Shell の責務として明文化(SHARD-RT-06 / SHARD-RT-07)。
- **§Sandbox / Draft Workspace**: tree / partition log の sandbox 反映は本 change の範囲外(blue/green migration による正系反映のみ)。
