# 結論

対象コミット `2531de3672fac82e3b07446e42ef04ebe607140c` を `git show` で読み取り専用解析した。テストは実行していない。

完全な主要経路として、N を増やしても安全に O(N) 外挿できる経路はない。見かけ上 O(B) の append consumer にも、AppCore deep clone、Supplemental 全量再投影、manifest 全量シリアライズ、検索 index 全体件数検証が入る。

以下では追加記号を用いる。

- `D`: payload/canonical JSON の総バイト数
- `V`: registry に保存された schema version 数
- `L_p`: PartitionTree のイベント・ノード数
- `G`: AppCore の deep-copy 対象サイズ
- `A`: answer-log・auxiliary projection のサイズ
- `Q`: corpus index のレコード数
- `T`: corpus index の本文・token 総量
- `q_k`: privacy key `k` に紐づく Observation 数
- `E`: identity replay event 数
- `J`: 1 Observation あたり privacy key 数。実装上最大 7

SQLite の indexed lookup は通常の B-tree モデルで `O(log N)` とした。`HashMap` は通常時の期待計算量 `O(1)` とし、理論上の全衝突ケースは別途悪化し得る。

## 1. 単発 import v1/v2

| 区分 | 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|---|
| 全 duplicate | `O(R log R + D + R(V + L_p + log N) + R)` | `ObservationImportContext::from_drafts` が BTreeSet を構築: `apps/selfhost/src/self_host/app/mod.rs:729-755`。draft 準備: `:1846-1863`, `crates/engine/src/lake/ingestion.rs:90-307`。schema version は Vec 後方走査: `crates/registry/src/registry/store.rs:164-173`。identity registry hit/miss と fallback query: `crates/storage/sqlite/src/persistence/mod.rs:2802-2869`。監査: `apps/selfhost/src/self_host/app/mod.rs:1865-1891, 6619-6634, 6921-6936`。 | `R log R`、`R·V`、`R·L_p`、SQLite の `R log N` が線形でない。duplicate では insert/privacy-key追加は発生しない。v1/v2 の per-item分類は `O(R)`: `:6659-6718`, `:6969-7043`。 |
| 全新規 | duplicate の式 + `O(R(log N + J log(NJ) + D_i))` | 新規時は identity registry、observations、privacy reverse index を insert: `crates/storage/sqlite/src/persistence/mod.rs:2872-2918`。v2 bridge の per-item 処理: `crates/storage/sqlite/src/persistence/cutover.rs:550-596, 668-692`。 | B-tree insert による `R log N`。`J` が固定でも物理的には O(R log N)。監査と分類は別途 `O(R)`。 |

v1 は 512 件単位で durable append するが、総量の漸近次数は変わらない。v2 は `derive_v2_identity` で canonical JSON を再 parse するため、canonical JSON 長も `D` に含める必要がある: `apps/selfhost/src/self_host/app/mod.rs:197-242, 6790-6936`。

全 duplicate の場合、`request_appended_observations` が空になるため、通常の non-corpus materialize は起動しない: `apps/selfhost/src/self_host/app/mod.rs:6720-6734, 7045-7064`。

ここでの式は import API の返却までである。返却後の append consumer/search catch-up を含める場合は、2、4 のコストを加算する必要がある。

## 2. append consumer の増分 fold 1ページ

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(B·F + A_Δ log A_Δ + S_c log S_c + G + Z_manifest + Σ q_k)` | 最大ページは `min(pending, 16_384)`: `apps/selfhost/src/self_host/app/service_support.rs:1161-1195`。ページ clone と AppCore clone: `:1197-1216`。fold/materialize: `:510-572`, `apps/selfhost/src/self_host/app/mod.rs:2922-3263`。identity/consent の BTree 操作: `:2196-2321`。 | `G` はページ外の全 resident state。`Z_manifest` は answer-log・communication projection 等の全量 serialize。`S_c log S_c` は Supplemental cognition 再投影。したがって一般には O(B) ではない。 |

主な超線形成分は次の通り。

1. `AppCore` をページごとに clone

   `let mut core = (*self.core_snapshot()).clone()` が毎ページ実行される: `service_support.rs:1216`。さらに publish 時に `Arc::new(core.clone())`: `:989-999, 1234-1238`。

2. Supplemental の全量再投影

   各ページで以下が実行される: `apps/selfhost/src/self_host/app/mod.rs:3221-3236`。

   - cognition: `crates/projections/cognition/src/lib.rs:210-260, 289-335`
   - cognition 内で records の clone + sort: `:220-228`
   - card queue projection: `crates/projections/cognition/src/lib.rs:432-490, 526-548`

   よって Supplemental が増えると、ページごとに概ね `O(S_c log S_c)` が追加される。claim queue dirty 時は `claim_queue()` も全量処理する: `apps/selfhost/src/self_host/app/mod.rs:1118-1129`。

3. manifest の全量 serialize

   `core.manifest_value()` は answer-log、communication projection、freshness、queue 等を含む: `apps/selfhost/src/self_host/app/mod.rs:1418-1445`。ページごとの commit: `service_support.rs:546-551`。

   したがって `Z_manifest = O(A + C + queue + freshness)` で、A や C が N に比例する場合はページ単位でも O(N)。

4. privacy key の再 materialize

   consent decision があるページでは、その key に属する Observation を SQLite から全ページ取得する: `apps/selfhost/src/self_host/app/mod.rs:2835-2871`。これは 8 の式が追加される。

5. 影響を受けた opt-out person の全 projection item 読み出し

   `person_message_items` は page API ではなく owner 全件取得を呼ぶ: `apps/selfhost/src/self_host/app/mod.rs:3133-3169`, `:1006-1035`; SQLite 側: `crates/storage/sqlite/src/persistence/mod.rs:1759-1791`。

6. identity の Observation ID 重複検査

   node ごとの Observation ID Vec を `.iter().any(...)` で走査する: `apps/selfhost/src/self_host/app/mod.rs:2293-2297`。巨大 component では 1 Observation あたり O(component 内 Observation 数) になり得る。

したがって、通常の新規 Slack message だけでも、ページの直接 fold 部分は概ね O(B) だが、運用経路全体は次のようになる。

`O(B + S_c log S_c + G + Z_manifest)`

`G` と `Z_manifest` が N 依存なので、N に対する線形外挿は不可。

## 3. `publish_core_snapshot()`

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(G)` | `AppCore` は `#[derive(Clone)]`: `apps/selfhost/src/self_host/app/mod.rs:1390-1415`。publish は `core.clone()` を実行: `apps/selfhost/src/self_host/app/service_support.rs:989-999`。 | 償却されない。snapshot publication ごとに deep clone。 |

`G` に含まれる主なものは以下。

- Registry、Catalog、BlobStore
- Supplemental Store/cache
- `CompactProjectionState`
- `person_consents`, `person_components`
- communication projection
- answer-log、freshness、claim/card/resume/plan state
- identity event、person message、slide、reply SLO projection

一方、AppCore は SQLite の canonical Observation 全件を直接保持する構造ではない。そのため「raw Observation 全件の clone」ではないが、derived projection が N に比例して残る場合、その分は O(N) で clone される。

append consumer では 1ページにつき少なくとも、

- materialize 前の clone
- publish 時の clone

の 2 回相当が発生する。

## 4. 検索 index catch-up

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| 1ページ: `O(B + X log X + T_X + L_s log L_s + Count(Q,M) + Commit_selected)` | catch-up のページ処理: `crates/search-index/src/index.rs:827-918`。privacy key の全件取得: `:875-888`。candidate は BTreeMap: `:889-898`。delta 適用、linked sheet sort、commit: `:930-1025`。 | `X = B + Σq_k`。通常ページでも `validate_record_count()` が全 index 件数を検証: `:1027-1035`, `:779-789`。ページ外の index 全体項が毎回入る。 |
| 単発 commit の merge | `O(I_selected + T_selected)`、上限は index 全体規模 | explicit な merge 呼出しは存在しない。Tantivy 自動 merge policy に依存。単発 commit に大規模 merge が重なると latency は線形以上に跳ねる。 |
| 持続 ingest | merge だけなら通常は償却 `O(T log N)` の見込み。ただしコード全体は `Σ Count(Q_i,M_i)` もある | `AllQuery + Count` の実装が全 doc を数える場合、ページ数 `N/B` に対して最悪 `O(N²/B)`。segment metadata だけで数えられる場合でも `ΣM_i` が残る。 |

重要なのは、Tantivy merge の償却計算量と、v15.1 の catch-up 1回の最悪時間は別という点である。

- merge policy の償却だけを見るなら、geometric merge を仮定して document/text が O(log N) 回程度 merge され、総量は概ね `O(T log N)`。
- しかし `apply_delta_locked()` の最後で毎回 `validate_record_count()` を呼ぶ。
- `record_count()` は `AllQuery` と `Count` を使う: `crates/search-index/src/index.rs:779-789`。
- したがって、持続 ingest の総計を O(N log N) と断定できない。少なくとも全 segment、実装によっては全 document 数え上げをページごとに実行する。

また `rebuild_from_store()` もページごとに `apply_delta()` するため、同じ件数検証が入る: `crates/search-index/src/index.rs:266-370`。

## 5. ブート、復元成功時

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(W + H log H + Q_proj log Q_proj + E log P + P log P + S log S + L_p² + I_index + M)` | bootstrap 全体: `apps/selfhost/src/self_host/app/mod.rs:5957-6098`。History rebuild: `crates/history/src/lib.rs:1088-1147`。SQLite open/WAL/schema/tree: `crates/storage/sqlite/src/persistence/mod.rs:118-143, 1322-1341`。manifest restore: `apps/selfhost/src/self_host/app/mod.rs:5675-5875`。AppCore 構築: `:1466-1600`。 | `H log H` は履歴 sort。identity/person の BTreeMap 挿入は `log` 項。PartitionTree は最悪 O(L_p²)。index open/checksum は index サイズ・M 依存。 |

詳細:

- History は operational event をページ読みするが、最後に全 entries を sort: `crates/history/src/lib.rs:1106-1145`。よって `O(H log H)`。
- `current_materialized_snapshot()` は identity replay event を全件読み、順次 `compact_state.apply_replay_event()` する: `apps/selfhost/src/self_host/app/mod.rs:5756-5786`。
- person component は `BTreeMap::insert` で復元: `:5788-5811`。一括構築 API はないため O(P log P)。
- IdentityState 内部にも `components`、identifier bucket 等の BTreeMap/BTreeSet がある: `crates/engine/src/identity/state.rs:83-92, 111-342`。
- Supplemental cache は records clone/sort: `apps/selfhost/src/self_host/app/mod.rs:1051-1130`。さらに cognition/card projection を検証するため S を再走査する。
- PartitionTree は単純な木だが、各 split で `contains_current_leaf`、`contains_retired_parent`、`replace_leaf_with_split` が木全体を再帰探索する: `crates/runtime/src/runtime/partition.rs:598-621, 679-714, 780-823`。split が L_p 件あると最悪 `O(L_p²)`。
- SQLite operational event store の `COUNT(*)` も実行される: `crates/storage/sqlite/src/persistence/mod.rs:2215-2264`。
- WAL 回復は SQLite 内部処理であり、コード上の入力規模は WAL bytes `W`。最悪 I/O 量を `O(W)` と見積もる。
- manifest が完全かつ high-water と一致する場合、canonical Observation 全 N 件を raw replay はしない。`load_observations()` はこの成功経路では呼ばれない。
- Tantivy index は `open_current`、checksum、reader/writer open、record count 検証を行う: `crates/search-index/src/index.rs:372-379, 715-751`。ここは index bytes と M に依存する。

## 6. ブート、フル再構築時

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| non-corpus: `O(N + A log A + P log P + S log S + Σq_k + projection writes)` | ページ helper: `apps/selfhost/src/self_host/app/mod.rs:4785-4855`。第1 pass: `:4972-5043`。第2 pass: `:5096-5207`。answer-log sort: `:5035-5040`。 | canonical lake は少なくとも2回走査。answer-log sort は O(A log A)。privacy key 再 materialize は q_k 依存。 |
| search full rebuild: `O(N + Σ_i Count(Q_i,M_i) + Σ_i Commit_i + merge)` | `crates/search-index/src/index.rs:266-370`。linked sheet pass と index build pass の2回走査。 | `validate_record_count` が各ページで入るため、O(N log N) を保証できない。最悪 O(N²/B) 相当まであり得る。 |

non-corpus rebuild は staging manifest を使い、通常 append のように毎ページ AppCore 全量 clone する構造ではない。そのため、直接の Observation fold 部分だけならページ走査として O(N) に近い。

ただし以下が追加される。

- answer-log 全件 sort: `O(A log A)`
- identity resolution の sort/BTree 構築: `O(P log P)`
- Supplemental の claim/card/cognition 構築: `O(S log S)`
- privacy consent による reverse-index page 読み
- page ごとの projection item DB commit
- 検索 full rebuild の per-page 全 index 件数検証と Tantivy commit/merge

したがってフル再構築全体は O(N) ではない。特に検索側の `Count` が全 index document を走査する実装なら、page size B に対して `O(N²/B)` の項が出る。

## 7. bulk session

| 区分 | 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|---|
| begin | `O(1)`、ただし projection stale なら即時 error | `apps/selfhost/src/self_host/app/bulk_import.rs:192-225`、freshness 検査: `service_support.rs:313-329` | begin 自体は全量 rebuild しない。 |
| append | 通常は import cost + `O(1)` trigger。最初の append または consent を含む場合 `+O(B + G)` | `bulk_import.rs:348-371`。consent capture、stale mark、snapshot publish。 | `publish_core_snapshot_for_import` が AppCore clone を行う。 |
| end、target == base | `O(1)` | `bulk_import.rs:259-263, 285-299` | no-op branch。 |
| end、Deferred/CatchingUp | `O(search catch-up + non-corpus rebuild)` | `bulk_import.rs:264-268, 301-329` | search catch-up は4の式。target がまだ materialized でなければ full rebuild。 |
| search index rebuild が必要 | 呼び出し側の待ち時間はエラー返却まででも、最終作業量は full search rebuild | `apps/selfhost/src/self_host/app/search_index.rs:225-247` | `requires_rebuild()` なら background rebuild を開始して unavailable を返す。CPU総量とHTTP latencyが乖離する。 |

bulk session の end は「まとめて append する」処理ではなく、すでに deferred された canonical append に対する catch-up と publication である。したがって最悪時には、

`import total + search catch-up + full non-corpus rebuild + full search rebuild`

まで膨らむ。

## 8. re-consent による遮蔽解除 / retraction

### privacy key 1件

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(q_k · (SQLite page read + fold) + q_k² log q_k / B_r)` | privacy key page 読み: `crates/storage/sqlite/src/persistence/mod.rs:823-855`。再 materialize: `apps/selfhost/src/self_host/app/mod.rs:2835-2893`。communication fold: `crates/projections/cognition/src/lib.rs:748-807, 827-859`。 | `B_r` は re-consent page size。page 読みでメモリは抑えられるが、時間は q_k に比例。累積 delta をページごとに sort するため最悪二次的。 |

`merge_reply_slo_delta()` は累積 rows をページごとに sort する: `apps/selfhost/src/self_host/app/mod.rs:2873-2893`。したがって、単に `O(q_k)` と見積もるのは不十分である。

同一 consent decision が subject と identifier の両方を持つ場合は複数 key が対象になり、総量は `Σ_k` である。

### retraction

- `source_object_id` retraction は communication state の `source_object_ids` 全体を scan: `crates/projections/cognition/src/lib.rs:780-787`。最悪 O(C)。
- `observation_id` retraction の `forget_observation()` は全 privacy-key bucket を確認: `:990-999`。概ね O(K log C)。
- corpus projection の retraction は record 全体を scan: `crates/projections/corpus/src/lib.rs:613-715`。最悪 O(Q)。
- したがって retraction は対象 ID が一意でも、source-object retraction では O(C) または O(Q) になり得る。

page 分割はピークメモリを `O(B_r + q_k の累積ID集合)` に抑えるだけで、時間計算量を線形化しない。

## 9. regex search job / exact search

### regex search job

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(Q_pattern + X + Σ_{i∈X}(ℓ_i + e_i) + E log E)` | query compile: `crates/api/src/api/grep.rs:245-278, 539-552`。候補取得: `crates/search-index/src/search.rs:50-151, 167-210`。match ranges: `crates/api/src/api/grep.rs:578-640, 651-674`。 | literal ngram が取れない regex は `AllQuery` になり、X が全 corpus Q になり得る。matches は20件に truncate する前に全件 collect する。 |

- `X`: Tantivy から取得した検証候補数
- `ℓ_i`: record i の本文長
- `e_i`: record i 内の match range 数
- `E = Σe_i`

`find_iter()` / `match_indices()` で全 match range を Vec に集めた後、`match_record()` で最大20件に切り詰める: `grep.rs:622-674`。そのため、本文が長く同一 pattern が大量に出る場合、結果件数制限は走査コストを制限しない。

regex に必要な literal ngram がない場合、候補 query は `AllQuery`: `crates/search-index/src/search.rs:167-210`。一致なしの最悪ケースでは全 Q 件を確認する。

非同期 job は計算量を変えない。30秒 timeout は fail-fast の制限であり、O(N) 保証ではない: `crates/search-index/src/search.rs:40-47`。job 登録・worker 処理: `apps/selfhost/src/self_host/app/projection_api.rs:793-923`。

### exact search

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(M + X + output_bytes)` | exact page: `crates/search-index/src/read.rs:198-280`。keyset query: `:807-835`。 | index が効いても common term では X が Q まで膨らむ。keyset は offset の悪化を避けるが、worst-case の全候補走査は避けない。 |

exact search は regex より通常は高速だが、最悪計算量は O(1) ではない。検索語が全 record に一致する場合や filter が弱い場合、segment 横断の posting traversal と record load が発生する。

## 10. v13 migration、streaming 化後

| 最悪計算量 | 根拠 | 超線形成分・償却 |
|---|---|---|
| `O(N·(D_i + J log(NJ)))` | page size 512: `crates/storage/sqlite/src/persistence/schema.rs:5`。migration 本体: `:693-759`。privacy key insert: `:733-750`。 | Observation の読み出しは streaming だが、privacy reverse index の B-tree insert により物理計算量は O(N log N)。 |
| Rust heap peak | `O(512·D_max + 512·J)` | 全 Observation の Vec は作らない。SQLite page cache/transaction WAL は別。 |
| disk/WAL 増分 | `O(NJ)` bytes 程度 | 1 transaction のため、Rust heap と WAL/disk 使用量は別に評価する必要がある。 |

この migration は、

1. `append_seq > cursor ORDER BY append_seq LIMIT 512`
2. page 内 Observation を deserialize
3. `observation_privacy_keys()` を計算
4. privacy key ごとに `INSERT OR IGNORE`

という streaming 構造である。

従って「Rust 側の一時メモリ」は O(512) 型だが、「migration 総時間」は privacy reverse index の B-tree 更新を含むため、厳密には O(N log N)。他の古い schema migration も未適用なら、追加の Observation 全量 pass が加算される: `schema.rs:301-400, 1034-1117`。

# 線形外挿を使ってよい経路

完全な主要 endpoint としては、該当なし。

条件を固定した部分経路なら、次だけを O(N) または O(B) と扱える。

- payload サイズ、V、PartitionTree 高さ、SQLite index サイズを固定した単発 draft 準備・分類部分
- consent/retraction がなく、`G`、`S`、manifest サイズを固定した append page の直接 Observation fold
- v13 migration の page scan・deserialize 部分だけ
- full rebuild の canonical Observation 二回走査部分だけ

いずれも endpoint 全体の計算量ではない。

# 線形外挿を使ってはいけない経路

- 単発 import v1/v2 全体
- append consumer の増分 fold 全体
- `publish_core_snapshot()`
- 検索 index catch-up / commit
- 検索 index の持続 ingest 総量
- ブート復元成功
- ブートフル再構築
- bulk session end
- re-consent 遮蔽解除
- retraction
- regex search job
- exact search
- v13 privacy reverse-index migration 全体

特に「append 1ページだから O(B)」「Tantivy merge は償却 O(N log N) だから毎回線形」「streaming migration だから O(N)」という外挿は、v15.1 のコードにはそのまま適用できない。
