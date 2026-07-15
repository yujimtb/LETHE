# LETHE 並列修正ルーティング・統合計画

## 1. 目的と適用範囲

本書は、基点 `a00e14a32dc031acf20213aa0291d6ab94c854c5` から並列に実装する次の5レーンを、統合ブランチ `fix/lethe-linearization` へ安全かつ再現可能に統合するための事前ルールである。

| レーン | ブランチ | 主責務 |
|---|---|---|
| 1 | `fix/supplemental-incremental` | supplemental / ClaimQueue / cognition / CardQueue の増分化 |
| 3 | `fix/replyslo-index` | ReplySLO join index と observation/supplemental delta |
| 2 | `fix/slack-thread-catalog` | Slack thread catalog / discovery high-water / active queue |
| a | `fix/bulk-defer-noncorpus` | 明示 bulk session と非corpus Deferred lifecycle |
| b | `fix/identity-dsu` | stable identity/fact ID、affected component 再投影、DSU |

本書が扱うのは、編集境界、共有契約、マージ順、衝突時の意味的裁定、テストと統合手順である。このレーンではアプリケーション実装を変更しない。

調査時点（2026-07-15）では5ブランチの tip はすべて基点 `a00e14a` と同一であり、実差分はまだ存在しない。したがって以下の重複マップは、各 prompt、設計資料、および `a00e14a` の実コードから確定した「予測編集範囲」である。統合開始時には必ず実際の lane tip からマップを再生成する。

参照資料:

- `../codex/lane-{1,2,3,a,b}-*.prompt.md`
- `../codex/lethe-complexity-audit.md`
- `../codex/lethe-identity-oquadratic-solutions.md`

## 2. 現行の共有経路

### 2.1 Observation append

```text
HTTP import / sync
  -> canonical append
  -> materialize_after_observation_append
       -> compact_incremental_delta
            identity/person + freshness + cognition + ReplySLO
       -> または refresh_materialized_snapshot
            -> rebuild_materialized_snapshot_paged
  -> persist manifest + projection items
  -> AppCore::install_materialized

HTTP bulk import のみ、その後に corpus catch_up_after_append
```

問題は、`MaterializedProjectionSnapshot` が identity/person、cognition、ReplySLO 等を一つの manifest と lifecycle に束ねている点である。ある projection の局所変更が、同じ outer coordinator、persist、stale 判定、テスト fixture に波及する。

### 2.2 Supplemental write

```text
write_supplemental
  -> resident SupplementalStore を更新
  -> materialized_snapshot_after_supplemental_delta
       -> supplemental 全件 list/fingerprint
       -> ClaimQueue / resume / plan / CardQueue を全再生
       -> send-record の ReplySLO row を再計算
       -> frontend profile を再計算
  -> supplemental + manifest + item delta を同一 transaction で commit
  -> AppCore::install_materialized
```

lane 1 と lane 3 はこの一つの transaction plan を共有しなければならない。別々の commit や別々の supplemental 全走査を追加してはならない。

### 2.3 Slack thread discovery

```text
sync_all
  -> channel ごとに known_thread_roots
       -> canonical Observation 全ページ走査
  -> history delta から root を追加
  -> 全 root に sync_thread_replies
       -> thread ごとの oldest cursor 以降を remote poll
```

lane 2 の主経路は materialization と論理的には独立だが、`AppService`、SQLite schema/storage port、`app/tests.rs` を共有する。また、lane a の active bulk session 中に sync append を許すかという lifecycle 上の干渉がある。

## 3. レーン別 file:function 編集予測

記号は次のとおり。

- **主**: レーンが意味論を所有し、実装変更が必要になる領域
- **接**: 呼び出し接着、state field、manifest、テストのため変更が見込まれる領域
- **参**: 原則 read-only。ここへアルゴリズムを複製しない

### 3.1 lane 1 — supplemental incremental

| file | function/type | 区分 | 理由 |
|---|---|---:|---|
| `crates/projections/cognition/src/lib.rs` | `CognitionStateProjector::{resume_snapshot, plan_state}` | 主 | ClaimQueue の一回生成・共有、project/kind 単位 reducer |
| 同上 | `CardQueueProjector::project_records`, `expires_at` | 主 | expiry index、draft/card keyed state、全 card × records 走査除去 |
| `crates/projections/claim-queue/src/lib.rs` | `ClaimQueueProjector::project_records` と新しい state/reducer API | 主 | 同一 write 内で一回だけ確定する中間結果 |
| `crates/engine/src/supplemental/store.rs` | `SupplementalStore::{list, by_kind, upsert_*}` 周辺 | 接 | 全 sort/list を通常 write から外すための keyed access。不要なら変更しない |
| `apps/selfhost/src/self_host/app/supplemental_write.rs` | `AppService::write_supplemental` | 主 | `SupplementalDelta` 作成、projection delta と atomic commit |
| `apps/selfhost/src/self_host/app/mod.rs` | `ProjectionSnapshot`, `MaterializedProjectionSnapshot`, `AppCore` の cognition state field | 接 | reducer state/cache の永続・resident 保持 |
| 同上 | `ProjectionSnapshot::build_with_state` | 接 | full replay oracle を新 reducer から生成 |
| 同上 | `MaterializedProjectionSnapshot::compact_incremental_delta` | 接 | Observation append 時の不要な全 cognition 再生を除去 |
| 同上 | `rebuild_materialized_snapshot_paged` の cognition assembly | 接 | full replay と incremental の同一 reducer 化 |
| 同上 | `materialized_snapshot_after_supplemental_delta` | 主 | kind router と単一 atomic delta plan |
| `apps/selfhost/src/self_host/app/tests.rs`、cognition/claim-queue tests | differential/property/performance counter tests | 主 | full replay 同値、ClaimQueue 一回、expiry lookup の上界 |

lane 1 は cognition file の ReplySLO section（`ReplySloProjector` 以下）のアルゴリズムを所有しない。共有 import、共通 utility、test fixture に必要な最小変更だけを許す。

### 3.2 lane 3 — ReplySLO index

| file | function/type | 区分 | 理由 |
|---|---|---:|---|
| `crates/projections/cognition/src/lib.rs` | `ReplySloProjector::project_records` | 主 | `draft -> observation` / `observation -> earliest sent` join index |
| 同上 | 新しい `ReplySloJoinIndex` / reducer API | 主 | rebuild 全ページ共有と supplemental delta の局所更新 |
| `apps/selfhost/src/self_host/app/mod.rs` | `compact_incremental_delta` の ReplySLO hook | 主 | appended Observation だけを index と join |
| 同上 | `canonical_reply_slo_row`, `reply_slo_*`, `detach_projection_items`, `validate_pending_projection_item_commit` | 主 | row key/count/item delta invariant |
| 同上 | `rebuild_materialized_snapshot_paged` の ReplySLO section | 主 | page loop の外で index を一度だけ構築 |
| 同上 | `materialized_snapshot_after_supplemental_delta` の reply-draft/send-record hook | 主 | 影響 Observation row の upsert/delete |
| `apps/selfhost/src/self_host/app/projection_api.rs` | `persisted_reply_slo`, `reply_slo_response` | 接 | index/row count または clock-derived status の整合 |
| `apps/selfhost/src/self_host/app/tests.rs`、cognition tests | differential/late-arrival/multiple-send tests | 主 | earliest sent、未送信、順序、page-size 非依存 |

lane 3 は独自の supplemental dispatcher を作らず、lane 1 が所有する一つの supplemental delta plan に ReplySLO handler を登録する。`core.supplemental.list()` を lane 3 の通常 update 内で再導入してはならない。

### 3.3 lane 2 — Slack thread catalog

| file | function/type | 区分 | 理由 |
|---|---|---:|---|
| `apps/selfhost/src/self_host/app/sync.rs` | `AppService::sync_all` の Slack channel/root loop | 主 | tail discovery と active/due roots のみを poll |
| `apps/selfhost/src/self_host/app/service_support.rs` | `ingest_slack_message` | 接 | root append と catalog upsert の原子境界 |
| 同上 | `sync_thread_replies` | 主 | active state、cursor、next poll の更新 |
| 同上 | `known_thread_roots` | 主 | 全 Observation scan を廃止し catalog query へ置換 |
| `apps/selfhost/src/self_host/app/sync_support.rs` | `thread_root_ts`, `thread_cursor_key`, `known_thread_roots_from_observations` | 主 | source-aware catalog key、明示 backfill/tail extraction |
| `crates/storage/api/src/lib.rs` | thread catalog port/type | 主 | `(source_instance, channel_id, thread_ts)` keyed durable API |
| `crates/storage/sqlite/src/persistence/{schema.rs,mod.rs}` | catalog/high-water/active queue table と transaction | 主 | idempotent upsert、atomic cutover、due query |
| `apps/selfhost/src/self_host/app/tests.rs`、storage tests | root/reply reorder、backfill境界、duplicate tests | 主 | 取りこぼし・重複・remote call 数を検証 |

lane 2 は `refresh_materialized_snapshot`、cognition、identity/person の実装を変更しない。Slack message が結果としてそれらを起動しても、lane 2 の責務は discovery catalog までである。

### 3.4 lane a — bulk Deferred lifecycle

| file | function/type | 区分 | 理由 |
|---|---|---:|---|
| `apps/selfhost/src/self_host/server.rs` | `build_router`, import begin/append/finalize handler, `ApiError` mapping | 主 | 明示 session API と 503/409 契約 |
| `apps/selfhost/src/self_host/import_client.rs` | import session client API | 主 | importer が begin/finalize を必ず明示 |
| `apps/tools/lethe-import-*/src/main.rs` | import orchestration | 接 | session token を全 batch で伝播し finally ではなく明示 finalize |
| `apps/selfhost/src/self_host/app/mod.rs` | bulk state type、`AppCore`/`AppService` state | 主 | durable lifecycle の resident mirror |
| 同上 | `ingest_observation_drafts` | 主 | append + corpus catch-up、非corpus materialize の defer |
| `apps/selfhost/src/self_host/app/service_support.rs` | `ensure_projection_fresh` | 主 | Deferred/CatchingUp/Failed 中の非corpus read を 503 |
| 同上 | `refresh_materialized_snapshot`, `materialize_after_observation_append`, `persist_materialized_snapshot` | 主 | lifecycle outer coordinator。finalize 時だけ rebuild 1回 |
| 同上 | `health`, `deep_health` | 接 | session id/state/lag/error を公開し、Ready と偽らない |
| `crates/storage/api/src/lib.rs` | bulk session port/type | 主 | begin/append progress/finalize/recovery の durable contract |
| `crates/storage/sqlite/src/persistence/{schema.rs,mod.rs}` | bulk session table/transaction | 主 | crash 後に Deferred を失わない |
| `apps/selfhost/src/self_host/app/tests.rs`、server/import/E2E tests | session state machine tests | 主 | duplicate、resume、finalize retry、read 503、rebuild 1回 |

lane a は `rebuild_materialized_snapshot_paged` の内部アルゴリズムを所有しない。lane 1/3/b が提供する同じ reference rebuild を、固定 high-water で一度呼び、成功時だけ Ready へ遷移させる。

### 3.5 lane b — identity/person affected component + DSU

| file | function/type | 区分 | 理由 |
|---|---|---:|---|
| `crates/engine/src/identity/types.rs` | stable node/component/person/identifier types | 主 | positional `pc:{index}` / `resolved-N` の廃止 |
| `crates/engine/src/identity/projector.rs` | `extract_candidates`, `cross_source_match`, `resolve` | 主 | normalized bucket、append-only DSU、同一 reducer replay |
| `crates/projections/person/src/person_page/types.rs` | fact/provenance/stable row ID types | 主 | person ordinal key の廃止 |
| `crates/projections/person/src/person_page/projector.rs` | `project`, `collect_related`, `observation_belongs_to`, profile/activity helpers | 主 | `project_component` と affected component 再投影 |
| `apps/selfhost/src/self_host/app/mod.rs` | `CompactProjectionState` と全 impl | 主 | node/claim/component/consent index へ置換 |
| 同上 | `compact_incremental_delta` の identity/person hook | 主 | topology change を通常 component delta として処理 |
| 同上 | `increment_person_page_for_slack` と message/slide ID/item helpers | 主 | Slack 専用 gate と ordinal key の廃止 |
| 同上 | `rebuild_materialized_snapshot_paged` の identity/person pass | 主 | empty state から同じ reducer を page replayする oracle |
| 同上 | `MaterializedProjectionSnapshot::validate`, format version | 主 | local invariant、count/watermark、破壊的 materialization rebuild |
| `apps/selfhost/src/self_host/app/projection_api.rs` | person row read/order/detail helpers | 接 | stable fact key と indirect owner join |
| `crates/storage/api/src/lib.rs`、SQLite persistence | component/node/fact index と atomic delta commit | 条件付き主 | Bまで実装する場合。Cのみなら既存 projection item owner APIを優先 |
| engine/person/selfhost/storage/E2E tests | oracle/property/crash tests | 主 | global renumber、late bridge、consent、ordering、unrelated component不変 |

lane b は cognition/ReplySLO の reducer を再実装しない。`rebuild_materialized_snapshot_paged` や `MaterializedProjectionSnapshot` を大きく書き換える際も、先に統合済みの lane 1/3 の state、row count、hook を保持する。

## 4. 衝突ホットスポット

以下は `a00e14a` での主要anchorである。行番号はlane実装後にずれるため、統合時はfunction名で追跡する。

| file | `a00e14a` line | anchor |
|---|---:|---|
| `app/mod.rs` | 188-338 | `ProjectionSnapshot`, `MaterializedProjectionSnapshot::{matches,validate}` |
| 同上 | 387-570 | `AppCore::{install_materialized,mark_non_corpus_materializations_stale,activate_non_corpus_projections}` |
| 同上 | 657-742 | `ProjectionSnapshot::build_with_state` |
| 同上 | 1095-1263 | `MaterializedProjectionSnapshot::compact_incremental_delta` |
| 同上 | 1406-1600 | `increment_person_page_for_slack` |
| 同上 | 2560-2800 | `rebuild_materialized_snapshot_paged` |
| 同上 | 2803-2961 | `materialized_snapshot_after_supplemental_delta` |
| 同上 | 3257-3345 | `AppService::ingest_observation_drafts` |
| `app/service_support.rs` | 187-290 | freshness gate、refresh/materialize/persist lifecycle |
| 同上 | 385-515 | Slack ingest、thread reply、known roots |
| `app/supplemental_write.rs` | 34-165 | `AppService::write_supplemental` |
| `app/sync.rs` | 15-98 | Slack channel history/root/reply loop |
| `cognition/src/lib.rs` | 203-317 | resume/plan と重複ClaimQueue build |
| 同上 | 403-465, 770-775 | CardQueue とexpiry線形lookup |
| 同上 | 518-604 | ReplySLO join stateの呼出内構築 |

### 4.1 物理・意味重複マトリクス

| hotspot | 1 | 3 | 2 | a | b | 危険度 | 最終所有者 |
|---|:---:|:---:|:---:|:---:|:---:|---|---|
| `mod.rs`: snapshot/manifest/AppCore fields | ● | ● |  | ● | ● | Critical | 統合担当 |
| `ProjectionSnapshot::build_with_state` | ● | ● |  |  | ● | High | 統合担当。各 reducer helper は各 lane |
| `compact_incremental_delta` | ● | ● |  | △ | ● | Critical | outer は統合担当、1/3/b の hook を合成 |
| `rebuild_materialized_snapshot_paged` | ● | ● |  | call | ● | Critical | outer/staging は統合担当、内部 reducer は1/3/b |
| `materialized_snapshot_after_supplemental_delta` | ● | ● |  | △ | △ | Critical | lane 1 dispatcher、lane 3/b handler |
| `service_support.rs`: materialize/refresh/persist/read gate | △ | △ | 別関数 | ● | △ | High | lane a lifecycle |
| `service_support.rs`: Slack ingest/thread functions |  |  | ● | △ |  | Medium | lane 2。aはsession guardだけ |
| `cognition/src/lib.rs` | ● | ● |  |  |  | High | 1=非Reply、3=ReplySLO |
| `storage/api::StoragePorts` supertrait | △ | △ | ● | ● | ● | Critical | 統合担当 |
| SQLite `schema.rs` / `persistence/mod.rs` / tests | △ | △ | ● | ● | ● | Critical | 統合担当がlane別moduleを接続 |
| `app/tests.rs` shared fixtures | ● | ● | ● | ● | ● | High | 既存fixtureの意味を保持し、lane別test moduleへ分離 |
| shared OpenSpec/design docs | ● | ● | ● | ● | ● | Medium | lane別result + 統合担当の最終同期 |

`●` は直接編集、`△` は state/lifecycle 接着による編集可能性を示す。

### 4.2 最も危険な意味的干渉

1. **lane 1 × lane 3**: 同じ supplemental write が cognition と ReplySLO の両方を変える。別々に manifest を生成すると last-writer-wins で一方を失う。
2. **lane 1/3/b × lane a**: reducer は即時 apply を前提にしやすいが、bulk 中は非corpus全体を apply してはならない。lane a は reducerを削除せず呼出タイミングだけを包む必要がある。
3. **lane 3 × lane b**: ReplySLO row は `proj:person-page` の projection item tableを共有するが、ownerは `__reply_slo__` であり person component merge と独立でなければならない。
4. **lane 2 × lane a**: active bulk session 中に session外 sync が canonical high-water を進めると finalize target が曖昧になる。
5. **lane a × lane b**: lane b の format/version変更中に active bulk session を暗黙再開すると、旧 reducerのbaseと新 reducerのtargetが混在する。
6. **lane 2/a/b × storage**: 各 lane が `StoragePorts` と一枚の `schema.rs` に独自拡張を加える。コンパイル衝突だけでなく、transaction境界の不一致が起きる。

## 5. 編集ゾーンと事前ルール

### 5.1 ゾーン所有

| zone | owner | 許可される変更 | 他 lane のルール |
|---|---|---|---|
| S: supplemental/cognition | lane 1 | kind router、ClaimQueue共有、resume/plan/CardQueue reducer | lane 3はReplySLO handlerだけを接続。a/bはstateを保持する |
| R: ReplySLO | lane 3 | join index、row reducer、observation/send delta | lane 1 dispatcherを再実装しない。bはowner/keyを変更しない |
| T: Slack discovery | lane 2 | catalog/high-water/active queue/remote poll | aはactive session guardだけ。materialization最適化を混ぜない |
| L: lifecycle | lane a | begin/append/finalize、Deferred/Ready、read gate、outer refresh | 1/3/bのreducer内部へ入らない |
| I: identity/person | lane b | stable ID、component reducer、person facts/rows | cognition/ReplySLOを再投影しない |
| X: outer coordinator/storage aggregation | 統合担当 | manifest shape、outer function、`StoragePorts`集約、schema init | 各 lane は専用helper/type/moduleを提供し、Xの全面置換を避ける |

### 5.2 materialize 経路の共通部

最終形では、outer coordinator と projection-specific reducer を論理的に分ける。実際の型名は各lane差分に合わせてよいが、責務は次からずらさない。

```text
ObservationDelta
  -> identity/person reducer        [b]
  -> freshness reducer              [既存]
  -> ReplySLO observation reducer   [3]
  -> combined ProjectionItemCommit  [X]

SupplementalDelta { old?, new }
  -> one kind/anchor router         [1]
       -> Claim/cognition reducer   [1]
       -> CardQueue reducer         [1]
       -> ReplySLO join reducer     [3]
       -> frontend-profile handler  [b/既存]
  -> one combined manifest/item plan [X]

BulkLifecycle
  -> Ready の通常時だけ上記deltaをpublish
  -> Deferred中はcanonical append + corpus catch-upのみ
  -> finalizeで同じfull replay oracleを固定high-waterに1回 [a]
```

`compact_incremental_delta` と `rebuild_materialized_snapshot_paged` が一つの長い関数のまま衝突した場合、統合担当は lane のどれかを丸ごと採用せず、上の責務単位へ helper を抽出して合成する。これは互換layerではなく、最終コードの責務分離である。

### 5.3 lane 1 と lane 3 の共有方針

- cognition と ReplySLO を一つの巨大 reducer に統合しない。
- 共有するのは **一つの `SupplementalDelta`、決定的 replay order、atomic commit plan** である。
- ClaimQueue snapshot は lane 1 が一度だけ確定し、resume/plan/CardQueueへ渡す。lane 3はClaimQueueを再計算しない。
- ReplySLO join index は lane 3 が所有し、lane 1 の generic stateに不透明なsubstateとして保持してよい。
- full replay は各substateを空から同じ順序でfoldする。incremental専用とfull専用の二重アルゴリズムを作らない。
- clock依存の expiry/age/overdue 比較は同一 `built_at/now` を渡してoracle比較する。wall clock差による偽差分を許さない。

### 5.4 storage の分離

各 lane が一つの `persistence/mod.rs` にSQLを直書きして肥大化させない。実差分が許せば次の専用moduleへ寄せ、`schema.rs` と `StoragePorts` の接続だけを統合担当が行う。

```text
persistence/
  thread_catalog.rs       [2]
  bulk_session.rs         [a]
  identity_state.rs       [b, Bを統合する場合]
  supplemental_state.rs   [1/3, 専用永続stateが必要な場合]
```

最終 `StoragePorts` は必要なtyped portを明示的に要求する。未実装backendに成功を装うdefault method、`get_state()` JSONへのsilent fallback、missing rowを空state扱いする処理は禁止する。schema/format不一致は明示エラーとし、derived stateは指定されたfull rebuildで再生成する。

### 5.5 テストと文書の編集分離

- 新規 unit test は可能な限り各 crate または lane専用 test moduleへ置く。`app/tests.rs` の既存fixtureをlaneごとに別解釈で変更しない。
- 共通fixtureを変える必要がある場合、入力生成と期待値生成を分け、期待値はreference full replayから作る。
- 各 lane は一意な result/complexity 記録を持つ。共有specの同一段落を複数laneで編集した場合、統合担当が最終実装に合わせて一本化する。
- 実装・テスト完了後は、各 lane の前後計算量、failure/recovery契約、追加API/formatを関連OpenSpec文書へ反映する。

## 6. 統合後も破ってはならない不変条件

### 6.1 correctness oracle

同一 canonical prefix、同一 current supplemental set、同一 `built_at` に対して次を満たす。

```text
canonicalize(incremental logical output)
  == canonicalize(full replay logical output)
```

比較対象は identity partition/person ID、consent、profiles/slides/messages/activities、ClaimQueue、resume/plan/CardQueue、ReplySLO rows、row key/order/count、watermark、lineage fingerprintである。

### 6.2 atomicity と high-water

- supplemental record、影響する reducer state、projection item delta、manifest は一つのtransactionでcommitする。
- thread root observation と catalog upsertは一つのtransaction、または同等に取りこぼし不能な明示recovery protocolにする。keyは必ず `(source_instance, channel_id, thread_ts)` とする。
- bulk session のstate/base/targetはdurableにする。crash後にcanonical statsがsession記録より進んでいれば、その差を無視せずDeferred/Failedとして明示する。
- identity/component state、person row delta、manifestはold/newどちらか一方だけが見えるようにする。
- manifestをDBへcommitしてからresident `AppCore` を交換する。逆順は禁止する。

### 6.3 fail-fast

- 通常writeからfull replayへsilent fallbackしない。full replayはbootstrap、明示rebuild、bulk finalize、test oracleに限定する。
- unknown supplemental kind、unsupported non-monotone identity change、schema/format mismatch、active session競合は明示エラーにする。
- active bulk session中の非corpus readは503とする。stale payload、空配列、stale flag付き200への暗黙切替は禁止する。
- active bulk session中の supplemental write と session外 canonical append/sync は、最終実装が同じsession transactionへ明示参加させない限り拒否する。暗黙queueやtarget high-waterの推測は禁止する。
- corpus read/index catch-upは非corpus lifecycleから独立し、Deferred中も通常どおり成功可能でなければならない。
- 明示sessionを使わない既存の単一import requestは、即時materializeする通常経路として維持する。sessionはopt-inであり、既存endpointを別名aliasへ迂回させない。

### 6.4 format と後方互換

- laneごとの中間materialization formatを互換対応しない。最終統合shapeに対して単一の新format versionを定め、旧derived materializationを破棄してcanonical dataから再構築する。
- person ID/fact IDのalias、旧ordinal keyのread fallback、旧thread discovery scan fallbackを残さない。
- lane bを含むversionへdeployする前にbulk sessionが `Ready` であることを要求する。旧versionのactive sessionを新versionで暗黙resumeしない。

## 7. マージ順

採用順は次のとおり。

```text
1 -> 3 -> 2 -> a -> b(C) -> b(B, 完成時のみ)
```

### 7.1 lane 1 を最初にする理由

supplemental writeのkind router、共有ClaimQueue、増分state/atomic delta planが lane 3 の supplemental側ReplySLO更新の土台になる。lane 3を先に入れると、lane 1が後から同じ全走査/commit pathを再度置換しやすい。

### 7.2 lane 3 を次にする理由

lane 1の一つの supplemental delta planへReplySLO handlerを接続し、同時にfull rebuild page loopへ一回だけjoin indexを導入できる。ここでH1/H3を同じoracle testに固定する。

### 7.3 lane 2 を三番目にする理由

アルゴリズムは前二つとほぼ独立している。先にstorage catalogを確定し、その後lane aがglobal lifecycle/session guardを追加すると、syncとbulkの競合ルールを一箇所で裁定できる。

### 7.4 lane a を四番目にする理由

Deferred lifecycleは、それ以前に統合した通常delta/rebuildを包むべきであり、それらを旧base版へ戻してはならない。lane a統合時には「session中は呼ばない」「finalizeで統合済みreference rebuildを一回呼ぶ」という呼出制御だけを採用する。

### 7.5 lane b を最後にする理由

lane bはID、row key、manifest、validation、person rebuildを最も広く破壊的に変更する。先に1/3/aのprojection state/lifecycleを固定し、bの大きい`mod.rs`差分からidentity/person部分だけを移植する方が、bが古いcognition/ReplySLO/full fallbackを復活させる事故を検出しやすい。

lane bは最低完成点Cと本命Bのcommit境界を明示する。Cだけが完了している場合はC tipだけを統合する。Bは、stable fact/DSU/storage/migration/oracleが一式完成している場合だけ別段階で統合し、半完成stateや互換shimは入れない。

## 8. laneごとの衝突解消方針

### 8.1 lane 1 統合

1. cognition/ClaimQueue/CardQueueの新reducerを採用する。
2. `materialized_snapshot_after_supplemental_delta` は単一dispatcher/planとして残す。
3. ReplySLO sectionをlane 1が変更していた場合、共有contextに必要な変更以外は保留し、lane 3で裁定する。
4. 通常writeに残った全`SupplementalStore::list()`、全`project_records()`を洗い出し、full replay/lineage以外は理由を要求する。

### 8.2 lane 3 統合

1. lane 1のdispatcher、ClaimQueue共有、cognition stateをoursとして保持する。
2. lane 3のReplySLO join index/reducerだけをそのdispatcherとmanifestへ接続する。
3. `rebuild_materialized_snapshot_paged` はpage loop外でjoin indexを一度構築し、各pageはimmutable indexを参照する。
4. `__reply_slo__` owner、stable observation由来item key、count invariantを保持する。
5. send-record/reply-draftがcognitionとReplySLOの双方へ一回のatomic commitで反映されるcross-testを追加する。

### 8.3 lane 2 統合

1. `known_thread_roots`のcanonical全走査を削除し、明示backfillとtail discoveryに置換する。
2. `source_instance`を欠いたkey/cursorを採用しない。
3. backfill high-water固定後にlive tailをreplayし、境界のroot/reply到着順逆転をtestする。
4. inactive rootを毎poll全件remote callする実装はactive queue要件未達として差し戻す。
5. lane 1/3のmaterialization変更を`service_support.rs`の別hunkから誤って落とさない。

### 8.4 lane a 統合

1. lane a版の古い`refresh_materialized_snapshot`/`rebuild_materialized_snapshot_paged`本文を丸ごと採用しない。
2. 統合済みreducerをReady時の通常pathとして保持し、Deferred中だけ呼出を抑止する。
3. finalizeは固定target high-waterに対し統合済みfull replayをちょうど一回実行し、atomic publish成功後だけReadyへ遷移する。
4. begin/finalize忘れ、process crash、duplicate append、finalize retry、index catch-up失敗を明示state/errorで処理する。
5. lane 2 syncをsession外writerとして拒否するか、session transactionへ明示参加させる。曖昧な併用は認めない。

### 8.5 lane b 統合

1. lane b差分のouter snapshot全置換ではなく、identity/person state、row key、component reducer部分を統合済みouterへ移植する。
2. lane 1のcognition state、lane 3のReplySLO index/count/owner、lane aのbulk lifecycle、lane 2のcatalog storage portを保持する。
3. `FullRebuildRequired`を通常topology changeの逃げ道として復活させない。未対応changeはcomponent rebuild requestまたは明示errorにする。
4. final format versionを統合shapeに一度だけ上げ、branch固有の中間version互換を残さない。
5. unrelated component ID/rows不変、late bridge、consent、historical order、ReplySLO owner不変をcross-testする。

## 9. 意味的衝突の裁定ルール

同じ行を両laneが変更した場合、採用順は「新しい方」「変更量が大きい方」では決めない。次の優先順位で裁定する。

1. 本書の共有不変条件と各lane promptの完了条件
2. reference full replayとの論理同値
3. atomicity/high-water/fail-fast契約
4. 目標計算量を満たすことを示すcounter/benchmark
5. lane固有unit test
6. 実装の局所的な簡潔さ

具体的には次を不採用とする。

- lane 1/3のどちらか一方のmanifestだけを採り、もう一方のstateを落とす解決
- conflictを消すための`ours`/`theirs`ファイル丸ごと選択
- normal writeから`refresh_materialized_snapshot()`へ戻す解決
- thread catalog miss時に`known_thread_roots_from_observations`全履歴scanへ戻るfallback
- missing incremental cache時に空stateを返すfallback
- lane bの旧person ID/row ID aliasまたは二重read
- bulk Deferred中に非corpus stale dataを200で返す解決

## 10. 統合実行手順

### 10.1 前提確認とtip固定

統合は本worktreeだけで実行する。他lane worktreeは読み取り参照に留め、branch tipのcommitをmergeする。

PowerShell例:

```powershell
$Base = 'a00e14a32dc031acf20213aa0291d6ab94c854c5'
$RoutingPlanTip = (git rev-parse fix/parallel-routing).Trim()
$Lane1 = (git rev-parse fix/supplemental-incremental).Trim()
$Lane3 = (git rev-parse fix/replyslo-index).Trim()
$Lane2 = (git rev-parse fix/slack-thread-catalog).Trim()
$LaneA = (git rev-parse fix/bulk-defer-noncorpus).Trim()
$LaneB = (git rev-parse fix/identity-dsu).Trim()

if (git status --porcelain) { throw 'integration worktree is not clean' }
foreach ($tip in @($RoutingPlanTip, $Lane1, $Lane3, $Lane2, $LaneA, $LaneB)) {
    if ((git merge-base $Base $tip).Trim() -ne $Base) {
        throw "lane tip $tip is not based on $Base"
    }
}
git show-ref --verify --quiet refs/heads/fix/lethe-linearization
if ($LASTEXITCODE -eq 0) { throw 'fix/lethe-linearization already exists' }
git switch --create fix/lethe-linearization $RoutingPlanTip
```

`fix/parallel-routing` には本書だけが基点以降へ追加されていることを確認する。lane tip hash、lane内commit一覧、各laneのtest結果を `openspec/parallel-routing-integration-log.md` に記録してからmergeする。

### 10.2 実差分マップの再生成

各laneについて次を保存し、予測zone外の変更はmerge前に理由を確認する。

```powershell
git diff --name-status $Base..$Lane1
git diff --stat $Base..$Lane1
git diff --function-context $Base..$Lane1 -- apps/selfhost/src/self_host/app/mod.rs
```

同じ手順を `$Lane3`, `$Lane2`, `$LaneA`, `$LaneB` に行う。特に次を確認する。

- lane 1がReplySLO algorithmを独自変更していないか
- lane 3がgeneric supplemental dispatcherを複製していないか
- lane 2がmaterializationを変更していないか
- lane aがrebuild内部を古い版で置換していないか
- lane bがcognition/ReplySLO/bulk/catalog stateを削除していないか

### 10.3 一laneずつno-commit merge

各tipを必ず一つずつmergeし、衝突解消・targeted test・workspace testを終えてから次へ進む。

```powershell
git merge --no-ff --no-commit $Lane1
# 衝突解消、文書同期、テスト
git commit -m 'merge: integrate supplemental incremental lane'

git merge --no-ff --no-commit $Lane3
# 衝突解消、cross-test、テスト
git commit -m 'merge: integrate ReplySLO index lane'

git merge --no-ff --no-commit $Lane2
# 衝突解消、catalog tests、テスト
git commit -m 'merge: integrate Slack thread catalog lane'

git merge --no-ff --no-commit $LaneA
# 衝突解消、bulk lifecycle tests、テスト
git commit -m 'merge: integrate bulk Deferred lane'

git merge --no-ff --no-commit $LaneB
# Cまたは完成済みBまで。衝突解消、oracle/cross-test、テスト
git commit -m 'merge: integrate identity component lane'
```

lane bがC/Bの段階commitを持つ場合、`$LaneB_C` を先にmergeしてworkspace greenを確認し、B完成tipは別mergeとする。

### 10.4 conflictの読み方

conflict fileはstageごとに読む。

```powershell
git status --short
git show :1:apps/selfhost/src/self_host/app/mod.rs  # merge base
git show :2:apps/selfhost/src/self_host/app/mod.rs  # 現在までの統合結果
git show :3:apps/selfhost/src/self_host/app/mod.rs  # 今回のlane
```

ファイル単位の`--ours`/`--theirs`選択は禁止する。関数単位でbase→laneの意図を読み、既統合hook/stateを保持した最終形を`apply_patch`で作る。conflict解消後は次を実行する。

```powershell
rg -n '^(<{7}|>{7})' .
git diff --check
git diff --cached --check
```

### 10.5 lane別targeted test

最低限、各merge直後に次を実行する。実laneが追加した専用testも必ず加える。

| merge後 | targeted test |
|---|---|
| lane 1 | `cargo test -p lethe-projection-claim-queue --lib`; `cargo test -p lethe-projection-cognition --lib`; `cargo test -p lethe-selfhost --lib` |
| lane 3 | `cargo test -p lethe-projection-cognition --lib`; `cargo test -p lethe-selfhost --lib` |
| lane 2 | `cargo test -p lethe-storage-api --lib`; `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib` |
| lane a | `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib`; `cargo test -p lethe-e2e --test self_host_api` |
| lane b | `cargo test -p lethe-engine --lib`; `cargo test -p lethe-projection-person --lib`; `cargo test -p lethe-storage-sqlite --lib`; `cargo test -p lethe-selfhost --lib` |

targeted test後、**各merge commitの前に** `cargo test --workspace` を実行する。これにより、失敗を直前laneへ帰属できる。最終tipではさらに次を実行する。

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check $Base..HEAD
```

失敗した状態で次laneへ進まない。testを弱める、ignoreする、fixtureを都合よく縮める解決は禁止する。

## 11. 必須cross-lane回帰テスト

各laneの単体oracleに加え、統合branchで次を持つ。

| 組合せ | シナリオ | 必須assertion |
|---|---|---|
| 1 × 3 | incoming Observation → reply-draft → approval/send | cognitionとReplySLOが一atomic commitでfull replayと一致。supplemental全走査/ClaimQueue再構築回数が規定内 |
| 1 × a | active bulk中にcognition supplemental write/read | stale 200や暗黙queueなし。明示503/競合。finalize後full replay一致 |
| 3 × a | ReplySLO対象Observationを複数batch import | session中非corpus 503、finalize rebuild 1回、join index build 1回、逐次経路と同一 |
| 2 × a | active bulk中にSlack sync開始 | sessionへ明示参加しない構成ではfail-fastし、canonical/session/catalog high-waterが進まない |
| 2 × b | root/replyのSlack ingestionがidentity componentも更新 | catalogの重複/欠落なし、person deltaは一回、unrelated component不変 |
| 3 × b | identity late bridgeでperson component merge | `__reply_slo__` rows/item key/countがperson owner変更の影響を受けない |
| a × b | bulk内でlate bridgeを含む複数batch | finalize 1回、global renumberなし、逐次referenceと同じlogical output |
| all | commit/publish直前のfault injectionとrestart | oldまたはnewの整合stateだけが見え、silent fallbackしない |

性能testはwall timeだけでなく、少なくとも次をcounterで検証する。

- `supplementals_scanned`
- `claim_queue_rebuild_count`
- `reply_slo_join_index_build_count`
- `reply_slo_rows_touched`
- `observation_pages_read`
- `non_corpus_rebuild_count`
- `thread_roots_discovered`
- `thread_roots_polled`
- `remote_reply_calls`
- `identity_components_touched`
- `person_rows_inserted/updated/deleted`

## 12. 最終静的監査

最終test後、次の呼出を列挙し、許可されたfull replay/read lineage以外に全量処理が残っていないことを人手で分類する。

```powershell
rg -n 'refresh_materialized_snapshot|rebuild_materialized_snapshot_paged' apps/selfhost/src/self_host/app
rg -n 'supplemental\.list\(\)|load_supplementals|project_records' apps/selfhost/src crates/projections
rg -n 'known_thread_roots|observation_page' apps/selfhost/src/self_host/app
rg -n 'FullRebuildRequired|fallback|unwrap_or_default|unwrap_or_else' apps/selfhost/src crates
```

`unwrap_or_else` 等は全て禁止という意味ではない。missing durable state、format mismatch、active session競合を暗黙正常値へ変換していないかを確認する。通常writeからのfull rebuild、catalog missからの全履歴scan、旧ID aliasが見つかった場合は完了扱いにしない。

## 13. 統合ログと完了判定

`openspec/parallel-routing-integration-log.md` には次を残す。

```text
base hash
routing plan hash
各laneの固定tip hashと統合対象commit
各merge commit hash
実際に衝突した file:function
各衝突の意味的裁定と保持したinvariant
lane別targeted test結果
各段階の cargo test --workspace 結果
最終 fmt/clippy/test 結果
cross-lane counter/complexity結果
最終materialization format/versionとmigration手順
未統合commit（特にlane b B段階）と理由
```

完了条件は次の全てである。

1. 5レーン、または明示的にCまでとしたlane bの対象commitが、固定hashで順序どおりmergeされている。
2. conflict marker、zone外の未説明変更、silent fallback、互換aliasがない。
3. incrementalとfull replayのlogical outputがcross-lane testで一致する。
4. bulk Deferred、thread catalog、supplemental/ReplySLO、identity/personのatomicity/high-water契約が同時に成立する。
5. 各merge段階と最終tipで `cargo test --workspace` がgreenである。
6. 最終tipでfmt/clippyがgreenである。
7. 関連OpenSpec/運用/complexity文書とintegration logが実装に一致している。
