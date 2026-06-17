# Design: sharding-refactor

**Change:** sharding-refactor
**Version:** 0.1 (draft)
**Date:** 2026-06-17

本書は `proposal.md`(WHY)と `specs/*/spec.md`(WHAT)を受けて、実装上の判断・前提・未確定事項(HOW / リスク)を記録する。normative な要件は spec 側にあり、本書はそれを覆さない。意味論の正典は [domain_algebra.md](../../../domain_algebra.md) / [plan.md](../../../plan.md) / [runtime_reference_architecture.md](../../../runtime_reference_architecture.md)、本 change の sharding 観点上の正典は [sharding_refactor.md](../../../sharding_refactor.md)。

---

## Context

- 本 change は **System Laws と既存モジュール契約(M01–M15)を変えず、Idempotency / Replay を強化** するアーキテクチャ refactor である。
- 横断不変条件は [sharding_refactor.md §1](../../../sharding_refactor.md) に正典がある(不変条件1〜10)。各 D 決定の根拠とコード接地点(`file:line`)も同文書に記載。
- 旧記述(draft の確率的冪等性 / SimHash routing / 固定ビット trie / conversation-level idempotencyKey)は本 change で上書きされる(sharding_refactor.md §4)。

---

## Key Decisions

### D1. 二軸分離: identity ↔ routing

- **identity**(dedup の判定軸): `source : object_id : H(canonical_content)`、per-message、per-leaf SQLite UNIQUE が正規判定。
- **routing**(placement 軸): `coarse(month) : coarse(year) : source : container : fine(published)`、Patricia trie + lazy split で容量駆動に分割。
- **両者は dedup に関して完全分離**。routing key の一意性に dedup は依存しない(不変条件7)。

### D2. 全ての時刻軸成分は `published`、cursor は `append_seq`

- routing の時刻成分はすべて event-time(`published`)。`recordedAt` は使わない(不変条件2)。
- watermark / propagation 検出 cursor は leaf-local の **physical append 順** に単調な `append_seq`(SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` = rowid alias)(不変条件8、R8)。
- これにより rehome(古い `recorded_at` を保持)で watermark frontier 下に沈む silent loss を塞ぐ。

### D3. exact dedup を per-leaf SQLite UNIQUE で完全(決定的)に

- 確率的(Bloom + ε 取りこぼし)を撤回。`identity_key TEXT NOT NULL UNIQUE` の B-tree が正規判定。
- 衝突時の比較対象は **hash 入力になった canonical タプル**(stored `canonical_json`)のみ。full observation(reactions / 編集 wrapper / ingestion メタ)とは比較しない(R4)。
- Bloom は冪等性の構成要素ではない。profiling で IO bound と判明したら β(負パス最適化)として後付け可能(D9.5)。

### D4. SQLite-authoritative per leaf

- 現行の「in-memory `LakeStore` が authoritative・SQLite は補償ロールバックで同期」は多 leaf で RAM に収まらず破綻するため、per-leaf SQLite を authoritative に降格 reverse(D9.2)。
- in-memory `LakeStore` は非永続キャッシュへ。dedup 判定は SQLite UNIQUE に一本化、補償ロールバック不要(INSERT が原子的)。

### D5. rehome primitive(stored Observation の再 route)を単一 migration primitive に

- backfill / split 再配置 / failover drain / blue/green keyspec 変更は **すべて rehome 一つに還元** される(不変条件10)。
- rehome は stored `id` / `published` / `recorded_at` / `consent` を保持して現 tree へ再 route し、着地 leaf に内部 append する。新採番されるのは着地 leaf の `append_seq` だけ。
- **fresh ingest(adapter → 新 id + 新 recorded_at)とは別レール**(R1)。混同が silent loss の元。
- 2 モード: (a) stored `identity_key` 信頼(keyspec 不変) / (b) canonical 再計算(keyspec 変更、blue/green)。mode (b) は `identity_key` column / `canonical_json` column / `observation_json.idempotency_key` の **3 箇所**を新 keyspec で同時に再シリアライズ(R14)。

### D6. Patricia trie + lazy split + atomic cutover

- 固定ビット trie ではない Patricia trie を選ぶ(split bit を `split_commit` event に記録)。
- 容量駆動 lazy split。割る時は leaf の中身ごと全再配置(= rehome mode (a) を子へ)。
- split は 2 フェーズ:
  1. `split_prepare`(子 build、route はまだ親を指す)
  2. catch-up(差分 rehome) → write barrier(短い freeze) → 最終差分 catch-up
  3. `split_commit`(route を子へ原子的に切替、`bit_index` 記録、親 retire)
- 「上から埋める / 新データだけ下層へ」は不採用(home がタイミング依存になり dedup が割れる、D5)。

### D7. partition log は時刻ではなく control-plane `event_seq` で順序付け

- `initialize` / `split_prepare` / `split_commit` / `failover` / `recover` の全 partition event が単調 `event_seq` + optional control-plane timestamp を持つ。
- `published` 由来の `at` は廃止(R10)。split / failover は event-time を持たない control-plane event。
- failover window 境界も `published` でなく control-plane sequence(R5)。`published` は backfill で過去化 → 物理障害窓を再現不能だから。

### D8. AP + reconcile failover + 専用 spool

- leaf 喪失中も ingest を止めず、L レンジ宛は **専用 failover spool**(近傍 leaf 直書きでなく)へ。
- spool 行は observations 行と同形(`identity_key` / `canonical_json` / `id` / `published` / `recorded_at` / `observation_json`)+ 排出順固定の `spool_seq INTEGER PRIMARY KEY AUTOINCREMENT`(R13、R15)。
- 復旧時は `ORDER BY spool_seq` で 1 件ずつ rehome mode (a)。着地 leaf の `identity_key` UNIQUE が `Duplicate` を決定的に弾く。
- transient dup は発生しうるが、排出時に決定的に除去される。最終状態に dup は残らない。

### D9. watermark を per-(projection × leaf) cursor に分解

- watermark は「projection P が leaf L をどこまで消費したか」。projection ごとに消費速度が違い失敗時は該当 watermark だけ据え置くので per (projection, leaf) が必要(D10.1)。
- cursor は leaf-local `append_seq`(rehome append も必ず新採番される、frontier 通過後の挿入が下に沈まない)。
- 検出と適用を分離: 検出は `append_seq` 順、適用は可換 + 冪等 fold なので順序非依存に流せる。
- core は可換 + 冪等 fold のみサポート(D10.3、不変条件9)。順序依存導出は projection 作者の責務。

### D10. split 後の propagation は baseline で全量再配送

- rehome は子 leaf に新 `append_seq` を振るので、per-(projection × leaf) watermark では split 後に子 leaf 全 Observation が「新規 tail」に見え、全 projection へ leaf 全量が再配送される(R11)。
- correctness は冪等 apply で保たれる(set ベース fold は再 add が no-op)。
- baseline でこれを許容(split は容量駆動で稀)。コストが顕在化したら β(watermark frontier 子移管)を後付け。

### D11. resolver は Imperative Shell、Kernel は論理 lake のみ

- `candidate_leaves(filter, log)`: filter の routing 軸制約 + 現在 tree から prune。
- published time-window は `(month, year)` bucket 集合に展開して部分木 union(month:year 軸順では連続時間範囲は lexicographic range にならない、R3)。在籍 ≤2 年で year 次元 ≤ 2 値なので bucket 集合は bounded。
- 読みは touch leaf 群の streaming k-way merge by `(published, recorded_at, id)`(Law S8、D11.2)。
- **解決済みエンティティ(person / subject / project)は placement にも leaf-prune にも使わない**(D11.4)。それらを query する場合は対応 projection の出力ストアを読む。
- secondary index は ManagedCache として後付け可能、ただし暗黙 full-scan fallback は禁止(明示 read mode / fail-fast、R6)。

### D12. 既存ストアは disposable → re-crawl

- 中身は dev/MVP データで保存価値が低い。旧→新 shim は作らず破棄して source から再取得(D12.2)。
- 寮 Slack 本体の初回取り込みは migration でなく通常 ingest。

---

## 未確定事項 / 実装着手時に確定する前提

> これらは sharding_refactor.md と現コードの間に解像度の差があるため、Phase 0 / Phase 1 着手の最初のステップで現物と突き合わせて確定する。

### U1. 現行コードとの差分マッピング(Phase 0 / Phase 1 の前提)

`specs/observation-lake/spec.md` SHARD-04 の `IngestResult` 同型性主張は、現行 [src/lake/ingestion.rs:228](../../../src/lake/ingestion.rs) を実測して確認すること。差分があれば spec の Scenario を現コードに合わせて調整(意味論は変えない)。

1. `IngestResult` の variant 名 / payload を 1 対 1 で確認。
2. `same_idempotent_observation` の比較範囲([src/lake/store.rs:151](../../../src/lake/store.rs))を実コードで確認。canonical 除外群(reactions / 編集 wrapper / ingestion meta)が現実装でも実際に差分を起こすか property test で確認。
3. SQLite schema migration の前後で既存 `idempotency_key` 列の denormalize 形式を確認(`observation_json.idempotency_key` との 1 対 1 関係)。

`Gate P0`(partition log `initialize` の永続 + 両 keyspec の不変性 DB 強制)を満たすまで Phase 1 へ進まない。

### U2. canonical_content の正規化規則の細部

`specs/adapter-policy/spec.md` SHARD-ADAPT-01 の canonical 正規化規則(NFC / CRLF→LF / JSON canonical / timestamp 表記統一)は **transport ノイズのみ** 正規化する。**body 内のユーザ可視空白は畳まない**(D3b、迷ったら false-split を選び false-merge を絶対回避)。Phase 1 の Slack adapter 着手時に edge case(改行末空白、unicode space、絵文字 ZWJ 列など)の扱いを property test で固定する。

### U3. claude.ai message uuid 欠落時の決定的導出

`specs/adapter-policy/spec.md` SHARD-ADAPT-02 の「`conversation_uuid` + `parent_message_uuid` チェーン上の位置から決定的に導出」は、Phase 2 で claude.ai zip importer 着手時に具体アルゴリズムを固定する(チェーン位置のエンコード方法、roots が複数ある場合の扱い、欠落と存在の混在をどう扱うか)。同じ zip を再 import して同じ derived id が再現できることを property test で保証。

### U4. β(watermark frontier 子移管)の発動条件

SHARD-PROP-05 / R11 で baseline は全量再配送を許容する。β(frontier 子移管)を発動するかは Phase 4 完了後の profiling で決める。Open Question 1 を参照。

### U5. blue/green migration の catch-up 戦略

SHARD-07 mode (b) の iterative catch-up(bulk → 差分反復 → 短い freeze → cutover)と完全 freeze のどちらを採るかは Phase 6 着手時の運用要件(許容ダウンタイム / 移行データ量)で決める。両者とも rehome primitive 上で表現可能。

---

## Open Questions (→ `adr_backlog.md` 候補)

1. **split 再配送コストの実測と β 発動条件**: per-leaf 容量上限と split 頻度を実測してから β(frontier 子移管)を発動する基準を作る。
2. **leaf 物理 durability**: replica からの L 復元(D8 で「別レイヤー据え置き」)を、SQLite replication / blob storage / WAL archiving のどれで実現するか。
3. **container 軸の粒度**: routing key の `container` を `workspace` 単位にするか `channel` 単位にするか。寮 Slack では workspace 単位が現実的だが、外部 workspace 追加時の境界。
4. **CDC / Merkle content-model**(Phase 7)を着手する条件: 「内部編集される大型文書」の Observation 容量がどのスレッショルドを超えたら revisioned snapshot から CDC へ移行するか。

---

## Mapping to sharding_refactor.md Decisions

| D 決定 | 本 change の Requirement | 本 design の Key Decision |
| --- | --- | --- |
| D0 スコープ | proposal.md Rollout(Phase 3 で分割実装) | — |
| D1 観測粒度 per-message | SHARD-02 | D1 |
| D2 dedup 意味論 exact | SHARD-04 | D3 |
| D3 identity_key | SHARD-01 | D1 |
| D3b canonical_content 境界 | SHARD-03 / SHARD-ADAPT-01 | D1, U2 |
| D4 routing_key | SHARD-RT-01 | D1 |
| D5/D6 trie + split | SHARD-RT-02 / SHARD-RT-03 | D6 |
| D7 partition log schema | SHARD-RT-04 | D7 |
| D8 failover AP + reconcile | SHARD-RT-05 | D8 |
| D9 exact index / spool / recovery | SHARD-04 / SHARD-05 / SHARD-06 / SHARD-RT-05 | D3, D4 |
| D10 watermark | SHARD-PROP-01〜05 | D9, D10 |
| D11 logical→physical 解決 | SHARD-RT-06〜08 | D11 |
| D12 migration path | SHARD-07 / SHARD-08 / SHARD-09 | D5, D12 |
| R1〜R15 改訂ログ | proposal.md "OVERRIDE" + 各 spec Scenario | sharding_refactor.md §6 |
