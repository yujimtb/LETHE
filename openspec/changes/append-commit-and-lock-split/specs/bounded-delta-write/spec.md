## ADDED Requirements

### Requirement: BDW-01 境界確認は保存済み scalar で O(1)

増分 materialize・OEL append・検索 index catch-up の境界確認(差分の有無・high-water)は、transaction 内で単調更新する保存済み count / high-water scalar、または `MAX(PK)` と保存済み count を O(1) で読 SHALL む。通常 append の応答経路で `COUNT(*)` 相当の全件走査を実行 SHALL NOT する。

#### Scenario: 1 件 append で全件 count しない
- **WHEN** 対応 schema の Observation が 1 件 append され境界確認が行われる
- **THEN** 境界確認は保存済み count / high-water scalar を O(1) で読む
- **AND** 全件 `COUNT(*)` を通常 append 経路で実行しない

### Requirement: BDW-02 partition tree は immutable snapshot と差分適用

Observation append と OEL append は、毎回 `partition_log` 全体を replay して partition tree を再生 SHALL NOT する。partition tree は起動時に再生して immutable snapshot 化 SHALL し、partition control event の追加時のみ差分適用して atomic 交換 SHALL する。通常 append のルーティングは partition control event 総数 P に依存せず tree depth に対して O(depth) SHALL である。

#### Scenario: append ごとに partition_log 全体を replay しない
- **WHEN** 通常 Observation append または OEL append のルーティングが行われる
- **THEN** ルーティングは immutable な partition tree snapshot を O(tree depth) で辿る
- **AND** `partition_log` 全体の replay を行わない

#### Scenario: partition event 追加時のみ差分適用
- **WHEN** partition control event(split / failover)が追加される
- **THEN** LETHE は snapshot へ差分適用して新 snapshot を atomic に交換する

### Requirement: BDW-03 manifest / ClaimQueue は per-row 分割

全 write で answer log / ClaimQueue / CardQueue を含む manifest 全体を JSON 直列化して上書き SHALL NOT する。manifest は scalar metadata と個別 row state へ分割 SHALL し、変更 row だけを transactional upsert SHALL する。ClaimQueue / Decision は keyed reducer と逆 index へ分解 SHALL し、affected record に対して O(Δ log S) で更新 SHALL する。

#### Scenario: 1 件の write が manifest 全体を書き直さない
- **WHEN** 1 件の decision / claim / card write が発生する
- **THEN** LETHE は変更のあった row だけを transactional upsert する
- **AND** answer log / ClaimQueue / CardQueue を含む manifest 全体を JSON 直列化して上書きしない

#### Scenario: ClaimQueue 更新が affected record に比例する
- **WHEN** ClaimQueue 影響 kind の supplemental が append される
- **THEN** 更新は affected record に対する O(Δ log S) で行われ全 supplemental を project しない

### Requirement: BDW-04 lineage digest は保存済み scalar で供給

lineage digest / count は write のたびに全 supplemental ID を collect・sort・hash して再計算 SHALL NOT する。affected 分だけ増分更新した保存済み digest / count を供給 SHALL する。読み取り経路の lineage pagination は本 requirement の対象外(indexed-keyset-reads の責務)であり、本 requirement は write 時の digest 計算に限る。

#### Scenario: write 時の lineage 再計算を全走査しない
- **WHEN** supplemental が append され lineage digest が更新される
- **THEN** LETHE は affected 分だけ増分更新した保存済み digest / count を供給する
- **AND** 全 supplemental ID を collect・sort・hash して毎回再計算しない
