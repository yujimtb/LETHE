# Spec Delta: dag-propagation

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-17

## Dependencies

- M01 Domain Kernel — 型・law の正規参照
- M03 Observation Lake — `append_seq` schema(本 change の `observation-lake` delta SHARD-06 で追加)
- M05 Projection Engine — projection spec / lifecycle
- M06 DAG Propagation — 既存 `openspec/specs/dag-propagation.md` の中核(watermark 概念 / incremental apply / retry semantics)は **不変**
- 正典: [Sharding design](../../../../../docs/architecture/sharding.md) §2 D10

> 本 delta は M06 の incremental propagation 概念・watermark protocol(read → apply → commit、失敗時は据え置き)・retry 意味論を **変更しない**。watermark の「ひとつの cursor」を「per-(projection × leaf) cursor の集合」に分解し、cursor の物理表現を `append_seq` に変更し、適用契約(可換 + 冪等)を明文化する。

---

## ADDED Requirements

### Requirement: SHARD-PROP-01 Per-(Projection × Leaf) Watermark

watermark は **per-(projection P × leaf L) の cursor** として保持 **しなければならない (SHALL)**。グローバル単一 cursor(Vec への `usize` position など)で表現してはならない (SHALL NOT)。

理由: projection ごとに消費速度が異なり、失敗時は該当 watermark だけ据え置く必要があるため(P 軸が必要)、各 leaf は physical append 順に独立した cursor を持つため(L 軸が必要)。

永続化は partition log と同じ control-plane に行い、in-memory のみの cursor を authoritative にしてはならない (SHALL NOT)。

#### Scenario: 1 projection の失敗が他 projection を停めない

- **WHEN** projection P1 が leaf L1 で失敗し、watermark を据え置く
- **THEN** P2 / L2、P2 / L1、P1 / L2 の watermark は独立に進む
- **AND** P1 / L1 のみが retry 対象

#### Scenario: per-leaf cursor が独立に進む

- **WHEN** leaf L1 / L2 がそれぞれ 100 / 1000 件の Observation を持つ
- **THEN** P / L1 cursor と P / L2 cursor はそれぞれの leaf の `append_seq` で独立に管理される

### Requirement: SHARD-PROP-02 `append_seq` as the Sole Cursor Field

watermark cursor は leaf-local `append_seq`(`observations.append_seq INTEGER PRIMARY KEY AUTOINCREMENT`、SHARD-06)で **なければならない (SHALL)**。`recorded_at` を cursor に使ってはならない (SHALL NOT)。`published` を cursor に使ってはならない (SHALL NOT)。`id`(UUIDv7)は監査 / tie-break 用に併記してよいが、cursor の単調性根拠にはしない。

理由: rehome(D8 drain / split 再配置 / blue-green)は元の古い `recorded_at` / `published` を保持して挿入する。`(recorded_at, id)` cursor では rehome 後の挿入が frontier 下に沈み silent loss(不変条件8、R2)。`append_seq` は rehome append でも必ず新採番される。

#### Scenario: rehome で frontier 下に沈まない

- **WHEN** 古い `recorded_at` を持つ Observation を rehome で着地 leaf に内部 append し、watermark frontier がすでに進んでいる
- **THEN** 着地 leaf の `append_seq` が新採番されて frontier を超え、`WHERE append_seq > ?` で必ず拾われる
- **AND** silent loss は発生しない

#### Scenario: backfill で published が過去でも検出される

- **WHEN** backfill で過去 `published` の Observation が新規に append される
- **THEN** `append_seq` は physical append 順に振られるので watermark で必ず拾われる
- **AND** projection apply は可換 + 冪等(SHARD-PROP-03)なので適用順序非依存

### Requirement: SHARD-PROP-03 Commutative + Idempotent Projection Contract

projection の incremental apply は **可換 (commutative) かつ 冪等 (idempotent)** な fold に限定 **しなければならない (SHALL)**。順序依存 fold は core がサポートしない (SHALL NOT)。

理由:
- 可換性: per-leaf delta を到着順に流せる(D10.2)。`published` / `recordedAt` はデータとして参照可能だが apply 順序には依存させない。
- 冪等性: 配送が at-least-once(apply → watermark commit の順、crash で再配送)なので、二重 apply で結果が変わらないこと(D10.5)。count++ のような単純可換 fold は二重計上するため、distinct observation_id の集合濃度のような **集合ベースの冪等表現** に組み直さなければならない (SHALL)。

順序依存な導出(各時点スナップショット等)は projection 作者が D11 の published 順ストリームを読んで内部実装する。core はその状態を持たない。

#### Scenario: 二重 apply で結果が変わらない

- **WHEN** apply 成功後に watermark commit 前で crash し、retry で同じ delta が再 apply される
- **THEN** projection の出力は 1 回 apply と同じ
- **AND** count 系は distinct observation_id 集合濃度として実装される

#### Scenario: 順序依存 fold は core で受理されない

- **WHEN** projection spec が「直前 Observation の値に依存する fold」を宣言する
- **THEN** projection registration が validation で reject される
- **AND** 作者は read 側の published 順ストリームで自前実装する

### Requirement: SHARD-PROP-04 Detection / Apply Separation

propagation の **検出と適用は分離** **しなければならない (SHALL)**。

- **検出順**: per-leaf の `append_seq` 昇順(`WHERE append_seq > ?`)。leaf 内で取りこぼさない(不変条件8)。
- **適用順**: 可換 fold(SHARD-PROP-03)なので per-leaf delta を到着順に流せる。merge-sort は不要。
- **merge-sort の所在**: `(published, recorded_at, id)` の merge-sort は **read 側**(`runtime` delta SHARD-RT-07)専用であり、propagation には現れない。

#### Scenario: 検出は per-leaf tail のみ読む

- **WHEN** P / L cursor を進める
- **THEN** L の `WHERE append_seq > cursor ORDER BY append_seq` のみが読まれる
- **AND** 他 leaf を巻き込まない

### Requirement: SHARD-PROP-05 Split Re-delivery Contract

split 直後の propagation は **baseline で全量再配送を許容** する (SHALL):

- rehome は子 leaf に新 `append_seq` を振るので、per-(projection × leaf) watermark では split 後に子 leaf の全 Observation が「新規 tail」に見える。
- correctness は SHARD-PROP-03 の冪等 apply で保たれる(set ベース fold は再 add が no-op)。
- コスト bound: leaf 容量 × projection 数 ×(稀な)split 回数。split は容量駆動で稀(D5、インスタンス増加 ≪ データ増加)。

profiling で split 再配送が重いと判明した場合に限り、**β: watermark frontier 子移管**(rehome を親 `append_seq` 昇順で行い、各 projection の親 watermark `W_P` を超えない最大の子 `append_seq` を (projection × 子leaf) watermark に seed)を後付け **してもよい (MAY)**。β は split を projection watermark 集合に結合させるので baseline では採らない。

#### Scenario: split 後の全量再配送で結果が壊れない

- **WHEN** split 直後に子 leaf 全 Observation が全 projection へ再配送される
- **THEN** SHARD-PROP-03 の冪等 apply により出力は変わらない
- **AND** β 未発動の場合のコストは leaf 容量に線形

#### Scenario: β は明示有効化

- **WHEN** β(frontier 子移管)を有効化する
- **THEN** 設定 / partition log に β が記録され、split 時の rehome 順序が `親 append_seq` 昇順で実行される
- **AND** β 無効時の挙動は baseline 全量再配送のまま

### Requirement: SHARD-PROP-06 Watermark Storage and Notification

watermark は SQLite テーブルとして per-(projection × leaf) で永続化 **しなければならない (SHALL)**。当面の通知は per-leaf poll を許容する (MAY)。後に leaf append の event 発行へ移行 **してもよい (MAY)**。

```sql
CREATE TABLE projection_leaf_watermarks (
    projection_id TEXT NOT NULL,
    leaf_id TEXT NOT NULL,
    last_append_seq INTEGER NOT NULL,
    last_observation_id TEXT,            -- 監査 / tie-break 用
    supplemental_version_pins TEXT,
    last_build_at TEXT NOT NULL,
    last_build_status TEXT NOT NULL,
    PRIMARY KEY (projection_id, leaf_id)
);
```

#### Scenario: leaf 追加で watermark スキーマ変更不要

- **WHEN** 新しい leaf L3 が split で生まれる
- **THEN** `projection_leaf_watermarks` に新行が現れ、既存行は影響を受けない
- **AND** スキーマ migration は不要

---

## MODIFIED Behaviors (parent spec 参照)

既存 `openspec/specs/dag-propagation.md` の以下の節は、本 delta の要件で表現が変わる(意味論は維持):

- **§3.1 Watermark Schema**: `lastProcessedRecordedAt` / `lastProcessedId` 単一 cursor から **per-(projection × leaf) の `last_append_seq` cursor** へ(SHARD-PROP-01、SHARD-PROP-02、SHARD-PROP-06)。
- **§3.2 Watermark Update Protocol**: 「Lake から watermark 以降の Observation を取得」を **「P / L 毎に `WHERE append_seq > ?` で leaf tail を取得」** に置換(SHARD-PROP-04)。apply → commit の順序、失敗時の据え置きは維持。
- **§3.3 Storage**: `projection_watermarks` テーブルを `projection_leaf_watermarks`(複合 PK)へ(SHARD-PROP-06)。
- **§2.1 第一優先: Incremental Propagation**: incremental は per-leaf に分解されるが優先順位は維持。projection 作者の責務として「可換 + 冪等」を契約化(SHARD-PROP-03)。
- **§2.2 第二優先: Scheduled Rebuild**: 維持。split 後の baseline 全量再配送(SHARD-PROP-05)は scheduled rebuild とは別経路で incremental の枠内で起きる。
