## オーナー確定済みの Q

以下は owner 承認済みであり、本 change の実装・運用の正とする。Q1〜Q3 は全て「確定」している。

| Q | 判断点 | 確定内容 | 理由 |
|---|---|---|---|
| Q1（確定） | cutover の原子的単位を何にするか | v1/v2 で同じ値を維持する `source_instance_id` を unit とし、その値を共有する producer は一括移行する。切替時の rename は禁止する | v2 identity の先頭成分そのものであり、既知 client の列挙なしに任意数へ拡張できる |
| Q2（確定） | 同一 unit の v1 admission をどう fence するか | unit/API バージョンに束縛した credential generation を handler 前に検証する。stale v1 は既存の認可失敗として拒否する | stale process・遅延 retry を handler 到達前に停止し、凍結 v1 の応答・error・identity 判定を変更しない |
| Q3（確定） | 最初の v2 `outcome=ingested` 後の rollback を一般機能として許すか | `ingested=0`（duplicate/rejected/quarantined のみ）の間は pre-commit rollback を許可する。最初の v2 `ingested` 後は `v2_committed` を記録し、一般 rollback は不許可として forward-fix にする | v1 は v2 key を参照しないため、任意 retry を含む安全な切戻しを一般 client に証明できない |

## Context

### 現行コードで起きている名前空間不連続

`ingestion-api-contract` は v1 の意味論凍結と v2 の strict identity を別 endpoint に分離したが、両 endpoint の identity を接続していない。

1. v1 は `namespace_draft` で client 提供 key を `{source_instance_id}:{client_key}` にするだけである (`apps/selfhost/src/self_host/app/service_support.rs:857-879`)。
2. v2 は `meta.object_id` と `meta.canonical_json` から `{source_instance_id}:{object_id}:sha256(canonical_json)` を導出し、client key との一致を要求する (`apps/selfhost/src/self_host/app/mod.rs:181-242`)。
3. `ObservationPreparer` は identity を再解釈せず、受け取った `idempotency_key` を Observation へ移す (`crates/engine/src/lake/ingestion.rs:204,235-253`)。
4. SQLite は `observation_identity_registry.identity_key` の完全一致で既存行を検索する。見つからなければ同じ transaction で registry と Observation を新規 insert する (`crates/storage/sqlite/src/persistence/mod.rs:2131-2233`)。canonical JSON の比較は**同じ key が見つかった後**にだけ行われる。
5. schema v8 起動 migration は既存 Observation の保存済み `identity_key` を registry へ登録する (`crates/storage/sqlite/src/persistence/schema.rs:511-526`)。以後の v1/v2 append も同じ registry へ入るが、別名 key 間の alias は作らない。

adapter が通常生成する client key は `source:object_id:sha256(canonical_json)` (`crates/adapters/api/src/idempotency.rs:43-45`) である。したがって典型例では次の二つが同じ論理 item に対して併存する。

```text
v1 stored key = source_instance_id:source:object_id:sha256(canonical_json)
v2 stored key = source_instance_id:object_id:sha256(canonical_json)
```

v1 は opaque client key を許す凍結契約なので、両者が常に異なるとは限らない。しかし一致を保証する契約もない。key が異なる場合、v1→v2 は既存 v1 row を取りこぼして二つ目を `ingested` し、v2→v1 の rollback も既存 v2 row を取りこぼす。これは canonical collision ではない。registry lookup 自体が miss するため、同じ canonical 内容の別 Observation が正規に append される。

`docs/development/personal-lake-ingestion.md:113-148` は endpoint 契約と v2 retry 固定値を説明するが、cross-version alias、drain/fence、rollback 条件を定義していない。既存 client は shared import client の固定 v1 path (`apps/selfhost/src/self_host/import_client.rs:92-110`) を使用するため、単なる URL 変更では安全に移行できない。

### 制約

- M03 `openspec/specs/observation-lake.md` の Append-Only / IngestResult を維持する。
- v1 の応答形式、request-level error、identity 判定を変更しない。
- canonical ledger は append-only であり、橋渡しのために既存 Observation を更新・削除しない。
- append-only 入力に対する全量 rebuild を通常運用へ導入しない。必要な一回限りの過去処理も bounded batch + durable watermark で再開可能にする。
- client は既知二製品に限らず、任意数の producer/source instance が独立に移行する。

## Goals / Non-Goals

**Goals:**

- v1 由来 Observation を v2 canonical identity で解決し、切替後の retry を既存 ID の `duplicate` へ収束させる。
- system 全体では v1/v2 client が混在しても、同じ cutover unit では二つの protocol を同時に受理しない。
- 各 unit が他 unit を止めずに bootstrap、検証、drain、activate できる状態機械を定める。
- cutover の race、crash recovery、rollback 可否を機械的な gate として検証可能にする。

**Non-Goals:**

- v1 handler の応答、error mapping、prepare、identity 判定の変更。
- canonical ledger に既に存在する cross-version/legacy 重複の削除・統合。
- client 実装、本番 credential 操作、本番データ接続。
- v2 identity formula や per-item response taxonomy の変更。
- bridge projection 以外の projection を rebuild すること。

## Decisions

### D1: v2 identity alias は canonical ledger の増分 projection とする

bridge は canonical Observation を `append_seq` 順に読み、各 eligible row の `meta.source_instance`、`meta.object_id`、`meta.canonical_json` から現行 v2 formula で identity を導出する。派生状態の概念モデルは次の三つである。

- `identity_bridge_candidates(v2_identity_key, observation_id, append_seq, canonical_json_sha256)`: 同じ v2 key に到達した候補を失わず保持する。
- `identity_bridge_gaps(append_seq, source_instance_id?, reason)`: 必須 identity 原料が欠落・不正な row を保持する。
- `identity_bridge_watermark(last_append_seq)`: 処理済み canonical high-water。

1 batch の candidates/gaps 追加と watermark 前進を同じ transaction で commit する。失敗時は watermark を進めず、再試行は `(append_seq, v2_identity_key, observation_id)` の一意性で冪等とする。startup ごとの全 scan や全 rebuild は禁止する。初回 bootstrap は `append_seq=0` から bounded batch で過去を一度だけ処理し、停止点から再開する。その後は watermark より後ろだけを処理する。

同じ v2 key に複数の既存 Observation がある場合、resolver は最小 `append_seq` を deterministic winner として ACK 対象にし、candidate multiplicity を anomaly として公開する。canonical row は変更しない。同一 key なのに canonical JSON の exact compare が一致しなければ hash collision/corruption として activation を block する。

**却下案:** schema migration/startup で全 Observation を毎回 scan する案はデータ量に比例して停止時間が増え、append-only + 増分 projection 原則に反する。既存 row の `identity_key` を v2 key へ書換える案は Append-Only と v1 再送の冪等性を破る。

### D2: v2 resolver だけが bridge alias を参照する

v2 append boundary は通常の exact registry 判定より前に bridge candidate を v2 key で検索する。

- candidate があり canonical JSON が exact match: 最小 `append_seq` の `observation_id` を `duplicate.existing_id` として返し、新規 append しない。
- candidate があり exact mismatch: `canonical_collision` として quarantine し、新規 append しない。
- candidate がない: 現行 v2 の global registry/append を実行する。

v1 は bridge を検索せず、現行の `namespace_draft` と registry 判定だけを使い続ける。これにより v1 identity 判定は凍結したままになる。新規 v2 append は現行 registry が並行 v2 request を直列化し、bridge projection は後から同じ row を増分反映する。

**却下案:** v1/v2 共通 lookup へ alias を差し込む案は、v1 が従来なら `ingested` した key を `duplicate` に変え、製品境界を破る。

### D3: `source_instance_id` ごとの single-protocol admission とする

Q1/Q2 の推奨案では `source_instance_id` を cutover unit とする。値は v1 と v2 の間で不変であり、一つの値を複数 producer が共有している場合は全 producer を同時に drain する。client 名の列挙や system-wide flag は使わない。

durable cutover event log は unit ごとに次の単調な phase を記録し、current state はその fold とする。

```text
v1_active -> draining -> v2_active -> v2_committed
                 \-> v1_active       \-> v1_active (v2 ingested が 0 の時だけ)
```

- `v1_active`: v1 credential のみ admission。projection は追随する。
- `draining`: 新規 v1 admission を閉じ、既に authorization 済みの request と append transaction の終了を待つ。v2 はまだ閉じる。
- `v2_active`: fence 以前の alias coverage を確認後、v2 credential のみ admission。v1 は既存 authorization failure で handler 前に拒否する。
- `v2_committed`: 最初の v2 `ingested` を記録した不可逆 operational phase。通常 rollback を認めない。

`draining` への transition、in-flight v1 の完了待ち、ledger high-water の `fence_append_seq` 記録は同じ admission barrier の下で行う。したがって fence 後に v1 row が commit する race はない。他 unit は独自 phase にあり、v1/v2 の system-wide 混在を妨げない。

authorization は credential の unit/version binding と cutover generation を検証する。失効済み v1 credential は既存の認可失敗として拒否され、v1 handler の response/error/identity contract は変更しない。共有 write token がある unit は credential 分離を cutover 前提とする。

### D4: activation は fence coverage と zero-gap を fail-closed gate にする

`draining` 完了時に `fence_append_seq` を固定し、次の全条件を満たすまで v2 を開かない。

1. bridge watermark が `fence_append_seq` 以上である。
2. 対象 unit の fence 以下の eligible row に未解決 gap がない。
3. alias candidate の canonical exact-compare 異常がない。
4. client の v2 生成 fixture が `source_instance_id`、`object_id`、canonical JSON、identity key を retry 間で固定する。
5. 対象 unit の既知 v1 sample を v2 resolver で dry-run すると既存 ID に解決し、ledger count が増えない。

gap を無視する bounded time window や「見つからなければ append」の silent fallback は設けない。過去 row に原料がなければ、canonical row を書換えず、source-native evidence から決定論的な alias input を補う append-only mapping を用意する。それも不可能なら unit の cutover を block する。

### D5: rollback 境界は最初の v2 append で分ける

`draining` 中、または `v2_active` で v2 `ingested` がまだ 0 件なら、v2 admission を閉じて in-flight を drain し、v1 credential を再発行して `v1_active` へ戻せる。`duplicate` / `rejected` / `quarantined` だけなら新しい canonical v2 row はないため同じ扱いとする。bridge 派生行は削除せず、次回 cutover に再利用する。

最初の v2 `ingested` 後は、v1 がその row の v2 key を認識しないため一般的 rollback は unsafe である。Q3 の推奨案では transition を `v2_committed` として forward-fix のみ許す。例外を将来設ける場合も、「v2 で ACK した object を v1 が決して再送しない」ことを source-native monotonic cursor で証明できる別 spec が必要であり、client 固有の手順を本 bridge の fallback にしない。

### D6: verification は ledger delta と state invariant を観測する

各 unit は次を machine-readable に公開する。

- phase、credential generation、fence append sequence、first v2 append sequence。
- bridge watermark/lag、candidate count、gap count、multiplicity/collision count。
- bridge hit により v1 Observation へ解決した v2 duplicate count。
- fence 後に拒否した stale v1 admission count。

テストは少なくとも、v1→v2 duplicate、v2 の新規 append、同一 unit の dual-admission 拒否、別 unit の v1/v2 並行、fence 中の in-flight race、projection crash/resume、missing meta gate、pre-append rollback、post-append rollback refusal、凍結 v1 contract regression を固定する。データ規模試験は bootstrap が bounded batch で再開し、steady-state work が `O(new observations)` であることを確認する。

## Migration Plan

### Bridge deployment (client traffic を変えない)

1. bridge candidate/gap/watermark と cutover event log、read-only verifier を追加する。v1/v2 admission は現状のままにし、client を切り替えない。
2. bridge bootstrap を bounded batch で開始する。停止・再起動を挟んでも watermark から再開し、通常 ingestion を止めない。
3. unit ごとの ownership、`source_instance_id` 不変性、credential 専有を監査する。共有 credential は client 列挙ではなく unit binding へ分離する。

### Per-unit cutover

1. client の v2 payload/identity fixture と retry fixture を staging/local で検証する。
2. 対象 unit の producer を停止し送信 queue/in-flight retry を drain する。
3. admission barrier 下で v1 を閉じ、accepted request の完了後に `fence_append_seq` を記録する。
4. bridge を fence まで catch up し、zero-gap/exact-match gate と dry-run を通す。
5. v2 credential を有効化し、既知 v1 sample の v2 retry が同じ `existing_id` の `duplicate`、ledger delta 0 になることを確認する。
6. 新規 item の `ingested` を確認した時点で `v2_committed` を記録する。その後は forward-fix とする。
7. 他 unit は任意の時点で同じ手順を独立実行する。全 client 同時切替を要求しない。

### Rollback

- bridge deployment 自体: projection runner/admission control を停止しても canonical ledger と v1 contract は変わらない。派生行は保持する。
- per-unit fence 後、v2 `ingested` 前: v2 を drain/close して v1 credential を再発行し、`v1_active` へ戻す。
- v2 `ingested` 後: 自動 rollback は拒否する。v2 を維持して forward-fix し、v1 を再開しない。

## Risks / Trade-offs

- **[legacy row に identity 原料がない]** → gap として記録し、決定論的 mapping が用意されるまで unit activation を block する。
- **[同一 `source_instance_id` を複数 producer が共有]** → unit ownership audit で検出し、全 producer を一括 drain する。切替時 rename で回避しない。
- **[projection lag 中に v2 を開く]** → fence coverage を durable activation precondition にし、manual override/silent fallback を禁止する。
- **[stale v1 process が切替後に再送]** → unit/version-bound credential generation で handler 前に拒否し、拒否数を監視する。
- **[bridge index が canonical state と乖離]** → canonical ledger を正本とし、batch commit と watermark を原子的にする。派生 state の障害で canonical row を変更しない。
- **[最小 append_seq winner が既存重複を隠す]** → candidate multiplicity を保持・公開する。winner 選択は ACK の決定性だけに使い、既存重複の修復とはみなさない。
- **[post-v2 rollback の運用自由度低下]** → 最初の v2 append 前に canary/dry-run を完了し、それ以降は forward-fix とする。

## Open Questions

冒頭 Q1-Q3 は owner により確定済みである。実装はこの確定内容に従い、CUT-02 の v1 不変条件、CUT-03 の single-protocol admission、CUT-05 の fail-closed rollback を同時に満たす。
