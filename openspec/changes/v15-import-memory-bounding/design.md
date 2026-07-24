## Context

`AppService` は canonical append と派生 materialization を同一プロセスで扱う。現在は import request が `core_snapshot` を読み、consent snapshot を個別 publish した後に append consumer を起動するため、同じ append が request と consumer の二つの境界を通る。さらに `CommunicationProjectionState` は `facts` に加えて非 serde の `BTreeMap<String, Observation>` を保持し、再 consent 用の reverse index と本文が二重に resident になる。SQLite v13 migration も observations を一括 `Vec` 化している。

変更は import API、selfhost config/service、検索 job、cognition projection、SQLite schema migration にまたがる。v1 の permissive/request-level error、v2 の per-item result、append-only canonical semantics、SQLite DDL を維持する必要がある。`openspec/specs/observation-lake.md`、`openspec/specs/api-serving.md`、`openspec/specs/platform-robustness.md` の契約を参照する。

## Goals / Non-Goals

**Goals:**

- import の同時数と request drafts 数を設定値で bounded にし、上限超過は処理開始前に reject する。
- consent と append consumer を同一 materialization 境界へ寄せ、publish 回数を計測し、search catch-up を watermark 後の単一起動にする。
- terminal search job を古い順に設定上限まで保持する。
- communication state を fact/scalar/reverse-index のみで保持し、re-consent は SQLite reverse-index の ID を page 単位で再読込する。
- non-corpus manifest を v11 にし、version 判定を full serde より前に行う。v10 は silent fallback せず canonical rebuild へ送る。
- v13 privacy reverse-index backfill を同一 transaction 内の cursor page 処理に変更する。

**Non-Goals:**

- AppCore の書込/読取分離、`core.clone()` の全面撤去、lock-free snapshot の導入。
- SQLite の新規 DDL、schema migration number の追加、checkpoint table の追加。
- v1/v2 の結果分類・identity・per-item semantics の変更。

## Decisions

### D1: import admission は service 内の non-blocking permit

`ResourceLimits` に `max_concurrent_imports`、`max_import_drafts`、`max_search_job_records` を必須設定として追加する。`AppService` は共有 `AtomicUsize` を CAS し、満杯なら `ImportConcurrencyLimit` を返す。permit は RAII で解除し、既存の `bulk_import_operation` mutex は canonical append の直列化として残す。これにより HTTP route と直接 service caller の両方が同じ契約を通る。

満杯の HTTP status は 429、error code は `import_concurrency_limit`、`details` に `maximum` を含める。draft 上限は 413 を新設せず、v2 の超過 items を既存 per-item result の `rejected`/`draft_count_exceeded` として返し、`details` に `actual`/`maximum` を入れる。v2 は先頭の設定上限内だけを準備・appendし、超過 item の検査や副作用を行わずに fail-fast 分類する。v1 は凍結された request-level bad request envelope のまま上限超過を返す。

Alternatives: blocking semaphore は待ち行列そのものが memory pressure を保持するため不採用。route だけの admission は直接 caller と将来の非 HTTP import を保護しないため不採用。

### D2: publish の単一境界と watermark 後 catch-up

通常 request は durable append 後に consent snapshot を単独 publish せず、append consumer が page を読み、consent fold と projection materialization を同じ `core` に適用して一回 publish する。bulk session では request が consent decision と stale marker を同じ lock/publish 境界で反映し、各 request は最大一回とする。検索 catch-up は各 durable append request の watermark 確定後に既存 single-flight で高々一度起動し、`end_bulk_import_session` では session target watermark 確定後に最終 catch-up を同期実行する。これにより既存 bulk session の corpus search 可視性を維持しつつ、observation ごとの起動増殖を防ぐ。通常 consumer も tail watermark 到達後に既存 single-flight を一回起動する。

`publish_core_snapshot` は共有 atomic counter を increment し、test-only accessor で測定する。counter は instrumentation であり、materialization の正は canonical storage と projection materialization のままとする。これは B3 の AppCore deep clone 廃止ではなく、clone の publication 回数と同時生存数を bounded にする段階 A の措置である。

Alternatives: consent publish と consumer publish を残して counter だけ抑える方法は同一入力を二境界で複製し続けるため不採用。全 request を同期 materialize する方法は latency と request-held memory を増やすため不採用。

### D3: search job は terminal oldest-first eviction

`SearchJobRecord` に monotonic insertion sequence を持たせ、完了/失敗 record が `max_search_job_records` を超えたら terminal record のうち最古から削除する。running/queued record は eviction 対象にしない。上限が terminal record だけで埋まる場合は新しい job を登録する前に eviction し、queue が満杯なら従来の immediate error を返す。削除済み job の status は既存 `NotFound` から 404 とする。

Alternatives: TTL は wall-clock と cleanup cadence の揺らぎを持つため、決定的な件数上限を採用する。

### D4: communication projection は body-free state + explicit repull

`observations` map を削除し、以下だけを resident とする。

- materialized `CommunicationFact` と observation → thread key
- observation → subject/source object の scalar map
- privacy key → observation ID reverse index
- retraction と最新 consent の index

通常の communication observation は scalar index を記録してから可視なら fact を materialize する。opt-out は fact を外すが re-consent 用 scalar/reverse index を保持する。retraction は fact と scalar/reverse index を `forget_observation` で除去し、re-consent で復活しない。re-consent は `observations_for_privacy_key` の stored observations を取得し、`rematerialize_observations` に page/bounded batch で渡す。serde 後の state に canonical body は存在しない。

`fold_observations` は consent decision の index 更新と existing fact の除去までを担当し、本文の再読込は storage-aware caller の責務とする。full rebuild の入力 slice を持つ test/helper は observation ID→位置の scalar map から対象だけを一時的に取り出し、state に保持しない。selfhost の paged rebuild は各 page で同じ explicit repull boundary を使う。

manifest format は `NON_CORPUS_MATERIALIZATION_VERSION = 11` とする。`current_materialized_snapshot` は JSON object と `format_version` scalar だけを先に確認し、10 以下は `serde(deny_unknown_fields)` 型へ deserialize せず `None` を返して canonical/page rebuild へ送る。11 より新しい値は error。旧 manifest の field 名を受理する compatibility layer や silent fallback は追加しない。

Alternatives: body を `Arc<Observation>` にして共有する方法も AppCore snapshot clone と同時生存を残すため不採用。consent ごとの全 corpus scan は O(corpus) であり reverse index page pull を採用する。

### D5: v13 backfill は一 transaction 内 cursor pages

`apply_reconsent_privacy_index_migration` は index/table を作った同じ transaction 内で `append_seq > cursor ORDER BY append_seq LIMIT 512` を繰り返し、各 page の JSON だけを decode して index rows を insert する。page を処理したら cursor を最後の append sequence に進め、全件 `Vec` は作らない。schema migration ledger の insert と commit は従来どおり最終 transaction commit に含めるため、途中 failure は transaction rollback、再起動時は migration 未記録として安全に再実行される。checkpoint table は作らない。

`init_schema` の base DDL には新しい index/column を追加しない。既存 v13 table/index と schema version はそのまま維持する。migration semantics は privacy key の集合と append sequence を変えない。

### D6: memory acceptance harness

`tests` または `scripts` に Linux 前提の harness を置く。N 件の synthetic Observation を生成し、idle RSS/VmHWM、bulk import 後の peak RSS/VmHWM、publish counter を記録し、`peak - idle <= constant + O(batch payload)` を機械判定する。N と batch/request 件数は引数化する。CI では小さい N と publish-count assertion を実行し、実 corpus の RSS 判定は Linux container job で行う。秘密・本番 endpoint は使用しない。

### D7: boot migration outcome と manifest restore outcome を分離する

v15.1 の boot 判定は `SqlitePersistence::open` が schema migration を実際に適用したかを返さず、`had_persisted_manifest` だけで background rebuild reason を選んでいた。そのため現行 manifest の restore が watermark/fingerprint/queue index 等で拒否された場合も、manifest が存在するというだけで `full_rebuild_reason="migration"` になった。さらに `current_materialized_snapshot` は legacy version、version 欠落・非数値、watermark/fingerprint 不一致をすべて `Ok(None)` に畳み、運用 log から復元失敗の根因を消していた。

`SqlitePersistence` は open 中に legacy column migration または未記録 schema migration を実行したかを immutable flag として保持する。boot は最初に開いた canonical connection の flag だけを今回の migration outcome として使い、後続 read-pool connection の open を判定に混ぜない。既存 manifest を持つ boot でこの flag が true の場合だけ migration reason の full rebuild を強制する。manifest がなく初期 bootstrap する場合は bootstrap reason とする。

manifest loader は `Restored(snapshot)` と `RebuildRequired(reason)` を区別する。legacy format（v15/v15.1 が受理してきた version 欠落・非数値の実 legacy shape を含む）、canonical watermark 不一致、supplemental fingerprint 不一致は具体値を含む reason を warning log に出して recovery rebuild へ送る。JSON object でない manifest、future version、現行 version の serde/invariant failure は安全な旧形式と断定できないため fail-fast する。現行 schema、現行 manifest、整合する keyed items の二回目 boot は restore のみで background rebuild を起動しない。

### D8: background rebuild の writer lock slicing と fixed high-water tail handoff

v15.1 の `run_background_materialized_rebuild` は `derived_projection_lane` を取った後、単一の `persistence_lock()` guard を `rebuild_materialized_snapshot_paged` の二巡、privacy repull、全 staging commit、target publish が終わるまで保持していた。通常 import は admission/cutover 検査で同じ writer mutex を最初に取得するため、durable append の計測開始前に rebuild 全時間を待った。これが総時間 460〜960 秒に対して `ledger_append_ms` が数十 ms のままになる未計測区間である。

background rebuild 専用 storage adapter は各 `observation_page`、privacy page、supplemental anchor read、staging commit、count 検証、最終 atomic publish ごとに writer mutex を取得・解放する。canonical 二巡は開始時の `(count, max_append_seq)` を固定し、通常 page と privacy reverse-index page の双方で `append_seq <= target.max_append_seq` だけを fold する。append-only 順序により、後から追加された row を final page が余分に返しても target より後を切り落とせる。

`derived_projection_lane` は base build と target install の間保持し、append consumer や supplemental publish が staging result と競合しないようにする。通常 import はこの lane を取得せず、page 間に canonical append と per-item response を完了して consumer を single-flight 起動できる。base rebuild 完了時に、開始後の append を理由として全件 rebuild を先頭からやり直さない。base snapshot とその high-water を install し、append-consumer cursor を base high-water に保存して lane を解放する。待機中 consumer が tail を append sequence 順に増分適用する。これにより固定 snapshot の整合性を保ちつつ、継続 import が full rebuild を永久に再試行させる v8 型 starvation を防ぐ。

page read/staging commit と最終 publish の lock wait/hold を log し、canonical page は page number・rows・総経過を記録する。import は bulk operation mutex 待ち、writer persistence mutex 待ち、Tokio `spawn_blocking` queue 待ちを別 field に加算し、ledger operation 自体の時間から mutex 待ちを除外する。v15.2 時点の最終 publish は item copy/delete を一 transaction に残していたが、568k fixture でこの区間だけが約890秒になったため、D10 の generation head 切替へ置換する。

### D9: bulk-session admission と long-running projection operation を分離する

568k fixture では writer lock slicing 後も単発 v1 import が600秒で timeout した。直接原因は `sync_all` が `bulk_import_operation` を先に取得し、その guard を保持したまま `derived_projection_lane` を取得していたことである。background rebuild は base build/install の全期間 derived lane を保持するため、polling sync は bulk mutex を占有したまま rebuild 完了を待ち、後続 v1/v2 import は durable append より前の bulk mutex 取得で convoy した。import timing log は handler 終了時にだけ emit されるため、client timeout まで guard を取得できない request では timing 自体が残らなかった。

空 source 構成にも増幅要因があった。`latest_workspace_slide_observations` は Google Slides runtime source が空でも canonical observation pages を終端まで走査していた。`sync_metrics.latency_ms` は derived lane 取得後から計測されるため、実測の約44.7秒はこの不要な568k row scan と整合する。

source sync と supplemental write は、derived projection laneを取得した後にnon-bulk projection operation mutexを取得して互いを直列化する。そのguard下で`bulk_import_operation`を短時間だけ取得し、persisted bulk sessionがinactiveであることを確認して直ちにbulk mutexを解放する。derived lane待機中はどちらのoperation mutexも保持しない。operation guardは実処理の間保持するため、bulk session beginはbulk mutex下でoperation mutexを`try_lock`し、実行中なら待たずconflictを返す。通常importはnon-bulk operation mutexを取得しない。

bulk session end も persisted phase を `CatchingUp` に遷移する短い区間だけ bulk mutex を保持し、search catch-up、background rebuild の起動・完了待ちはロック外で行う。最終 Ready 遷移時に mutex を再取得し、session id/phase/target watermark を persisted state から再検証する。同時 end が先に Ready へ進めた場合は persisted Ready report を返す。CatchingUp 中の append は既存 conflict 契約で fail-fast するため、ロック外待機によって session target が増えることはない。

cutover admission は import ごとに writer mutex を取得するが単一 SQL admission check の区間だけであり、background rebuild の page writer hold とだけ競合する。通常 v1/v2 import は bulk session id がない場合 derived lane を取得せず append consumer を非同期起動する。bulk session append の最初の stale publish/consent publish だけは既存契約上 derived lane を必要とする。polling task は一回の `spawn_blocking(sync_all)` を await してから sleep するため sync task は累積せず、background rebuild 自体も Tokio pool ではなく専用 `std::thread` で動く。したがって実測の600秒停止は cutover admission、通常 import の derived lane、spawn-blocking pool 枯渇ではなく、sync→bulk mutex→derived lane の convoy で説明できる。

### D10: projection generation head による O(1) atomic publish

v15.2 の `publish_projection_items_from_staging` は writer mutex と SQLite transaction を保持したまま、live item/visible-blob rows の全削除、staging item/visible-blob rows の `INSERT ... SELECT`、staging rows の全削除を行っていた。projection item 主キー、owner/sort index、visible-blob 主キーと検索 indexを、item数 N に比例して削除・挿入する O(N) publish である。568k fixture の `max_persistence_lock_hold_ms=889858` は page commit ではなくこの最終 transaction に一致する。

SQLite schema v15 は `projection_materialization_heads(logical projection_id -> storage_projection_id)` と `retired_projection_materializations` を migration だけで追加する。base DDL には v15 object を入れない。v14 upgrade は既存の論理 projection ID をそのまま最初の物理世代として head に backfill し、manifest のない orphan item/blob row があれば推測せず migration を rollback して fail-fast する。新規/replace staging は UUID v7 を含む一意な物理世代へ書き、通常 delta/read/count/blob visibility は毎回 logical head を解決してその世代だけを操作する。

最終 publish transaction は staging item count を物理世代上で検証した後、target manifest、staging head の消費、target head 1行の切替、旧 target 世代の retirement 登録、staging manifest の削除だけを commit する。item/visible-blob rows をcopy/deleteしないため、writer hold は N に比例しない。論理 target の reader は transaction commit 前には旧 head、commit 後には新 headだけを見る。head とmanifest/retirementは同一transactionなので部分世代は公開されず、同じ固定 staging IDを再利用しても物理世代IDは衝突しない。

旧世代は公開経路から外れた後、専用 single-flight worker が128 rowsずつ短い writer transaction で item とvisible-blob参照を削除する。各pageのwait/hold/削除件数をlogし、page間に1msの譲歩を置いてimportが割り込めるようにする。retirement rowは全row削除後にだけ消すため、任意pageの前後でcrashしても次bootのworkerが再開できる。publish直後のcrashでも旧世代はdurable queueに残り、新headの公開状態は変わらない。

レビューで、公開readerが `active_storage_projection_id()` でheadを読んだ後、別のautocommit文でitem/blob rowを読む実装になっていることが判明した。二文は別SQLite snapshotなので、その間にA→Bのhead切替とAのcleanupが進むと、Aを解決済みのreaderが削除後のAを読み、空・部分結果またはblob visibilityの偽陰性を返せる。これは単一文でlogical IDを参照していたv15以前からの整合性回帰である。

key 1件、owner全件、複数owner page、blob visibility、owner count、total countの6読取は、logical head・manifest存在・physical rowを一つのCTE/JOIN文で解決する。SQLiteの一文snapshotにより、publish/cleanupと並行しても各API呼出しは旧世代または新世代の完全な一方だけを見る。head/manifestの片側欠落は同じsnapshot内で検出してschema invariant errorとし、silent fallbackしない。`commit_projection_items(Replace)` が既存世代をretireする場合も、commit成功後に同じsingle-flight cleanup drainを要求する。

### D11: rebuild進行中のsync cycleをskipし、lane waiterをnon-bulk operationから外す

v15.2.1は`bulk_import_operation`の長期保持を解消したが、`sync_all`は`non_bulk_projection_operation`を取得してから`derived_projection_lane`を待っていた。background rebuildは二巡とinstallの全期間derived laneを保持するため、通常importは進めても、bulk session beginの`try_lock(non_bulk_projection_operation)`だけはrebuild全期間`bulk_import_non_bulk_projection_active`になった。568k schema v15移行bootで数十分409が続いた直接原因はこの残存lock順である。

scheduled syncはoperation lock取得前に`non_corpus_rebuild_in_flight`をAcquire loadする。trueなら`sync_skip_reason="background_non_corpus_rebuild"`をinfo logへ出し、source fetch、cursor、health last sync、persisted sync stateを変更せず成功reportを返す。polling taskは一cycleをawaitしてから次intervalを待つため、queueを増殖させず次回scheduleで自然に追いつく。false確認直後にrebuildが開始するraceでも、最終lock順をderived lane→non-bulk operation→bulk-session handshakeへ統一したため、syncはlaneを待ってもnon-bulk admissionを占有しない。

supplemental writeはユーザー操作なのでskipしない。derived laneを先に待ち、取得後にnon-bulk operationとbulk-session handshakeを行う。待機中にbulk sessionが始まればlane取得後のhandshakeで明示conflictとなり、inactiveなら従来どおりsupplemental append・projection delta・auditを一transactionでcommitする。この順序はsyncとsupplementalの両方で同一なのでlock inversionを作らない。

migration/recovery bootは公開catalogをstaleにしてからrebuildするが、bulk session begin自体はsession metadataとauditだけを永続化し、derived projectionを変更しない。このため`non_corpus_rebuild_in_flight=true`の間に限って`ProjectionStale`をbeginの拒否理由から除外する。in-flightでないstale catalog、active session、実行中non-bulk operationは従来どおりfail-fastする。bulk appendやsupplemental publishがderived laneを必要とする契約は変更しない。

## Risks / Trade-offs

- **[consumer の遅延中は通常 request の snapshot が stale]** → canonical append が成功した時点で response を返す既存非同期契約を維持し、consumer single-flight と health/error state を使う。
- **[v1 の request-level reject が既存 client に見える]** → permissive payload/result semantics は変えず、draft 上限だけを明示 error として既存 bad-request 形式で返す。
- **[re-consent repull の DB read が増える]** → privacy reverse index で対象 ID を絞り、page 単位に制限する。全 corpus の本文は保持しない。
- **[manifest v10 の起動時 rebuild latency]** → format pre-guard で明確に rebuild に送り、rebuild は既存 page/cursor 経路を利用する。破損した旧型を部分的に読み込まない。
- **[deep clone 自体は残る]** → B3 は次期候補として明示し、v15 は admission、publish 回数、resident body、migration page を bounded にする。
- **[rebuild 中の target manifest は base high-water のまま一時的に canonical tail より遅れる]** → live catalog は rebuild 中 stale のままにし、base install 後は append consumer が persisted cursor から追う。crash 時も次回 boot の watermark 不一致が理由付き recovery rebuild になる。
- **[旧 projection 世代の回収が publish 後まで残る]** → live head から外した世代だけを durable retirement queue に登録し、128 row pageで回収する。回収失敗は明示 error logに残し、次bootまたは次publishのsingle-flight workerで再開する。
- **[実行中sync/supplementalとbulk session beginは排他]** → derived lane取得後の短いadmissionから処理完了までだけnon-bulk operationを保持する。rebuildによるlane待機やsync skip中はbulk beginを拒否せず、実処理と競合した場合だけ明示的に再試行する。

## Migration Plan

1. 設定ファイルへ新しい limits を必須追加し、既定運用値を `max_concurrent_imports=2`、既存 sync limit と分離した `max_import_drafts`、検索 job 保持上限とする。
2. SQLite schema v15 migration で generation head/retirement tableを追加し、v14 projection rowsを同一IDの初期世代としてbackfillする。v15 objectはbase DDLへ追加しない。
3. v1/v2 import の admission、publish boundary、search eviction を有効化する。
4. rollback は code/config を同時に戻す。SQLite v15 migrationとmanifest v11は旧binaryへの同一DB rollbackをサポートしない。
5. boot は schema migration applied flag、manifest restore outcome、background rebuild reason を別々に記録する。v15 migrationが今回適用されたbootだけmigration reasonで一度rebuildする。

## Open Questions

なし。AppCore の clone 廃止は次期 change の候補として記録済みであり、本 change の実装判断をブロックしない。

## Verification (2026-07-23)

- `cargo fmt --all -- --check`、`cargo check --workspace`、`cargo test --workspace --quiet` が成功した。workspace の集計は `711 passed / 0 failed / 3 ignored`（実行 714 件）。
- publish counter の実測は、現行経路で `1000件/1 request = 1`、`25件×40 request 直列 = 36`。非同期 consumer のスケジューリングにより後者は 36〜37 の範囲で変動するが、上限 43 を満たす。
- before は旧 `refresh_capture_consent_snapshot` の単独 publish 呼出しを一時的に復元して同じテストを実行し、`1000件/1 request = 2`、`25件×40 request 直列 = 78` を測定した。測定用の一時変更は取り除き、最終コードには残していない。
- SQLite の `CURRENT_SCHEMA_VERSION` は 14 のまま。v13 backfill は page/cursor 化したが、既存 DDL、migration ledger、schema semantics は変更していない。`v13_privacy_backfill_rolls_back_and_retries_from_first_page` で transaction rollback と先頭 page からの再実行も検証した。
- 残課題は B3（AppCore の書込/読取分離と deep clone 全廃）であり、本 change では扱わない。RSS harness の実 corpus 判定は Linux container 実行、通常 CI は小規模 corpus と publish counter を使う。

## v15.2 Verification (2026-07-24)

- 根本原因 A は、boot が schema migration の今回適用結果を持たず、`had_persisted_manifest` をそのまま `full_rebuild_reason="migration"` に変換していたこと、および manifest restore の legacy/watermark/fingerprint rejection を `Ok(None)` に畳んで理由を失っていたことである。
- `current_schema_and_manifest_restore_without_second_boot_rebuild` は実 Observation を append/materialize して同じ SQLite を再 open し、二回目 boot の rebuild counter `0` と persisted projection restore を検証した。`schema_migration_applied_on_restart_forces_migration_rebuild` は v14 ledger を未適用状態へ戻した既存 DB を再 open し、今回 migration 適用後の rebuild counter `1`、reason `migration` を検証した。legacy manifest の既存 recovery 挙動も維持した。
- 根本原因 B は、background rebuild が二巡全体を単一 writer `persistence_lock` guard 内で実行し、import が admission 検査で durable append timing より前に同じ mutex を待っていたことである。read page は read pool、staging write は page commit、target publish は atomic transaction 単位へ分割した。
- `background_rebuild_allows_bounded_v2_import_and_hands_off_tail` は 40 行、page size 1、page 間 delay の background rebuild 中に単発 v2 import を実行し、5 秒未満の response、`ingested` per-item 結果、base 二巡ちょうど 80 page（全件 retry なし）、consumer 収束後 41 行の canonical/core/manifest watermark 一致を検証した。
- `cargo fmt --all -- --check` と `cargo test --workspace` は成功した。workspace 集計は `721 passed / 0 failed / 3 ignored`、selfhost unit は `106 passed / 0 failed / 0 ignored` である。
- SQLite DDL と `CURRENT_SCHEMA_VERSION=14`、non-corpus manifest shape と `NON_CORPUS_MATERIALIZATION_VERSION=11` は変更していない。`deploy/personal-lake/compose.yaml` の既存 `MALLOC_ARENA_MAX=1` を維持し、関連運用設計へ 16 GiB container の arena 制約を記録した。
- 残課題は、提供された 568k fixture/16 GiB Linux container をこの worktree では再実行していないため、実規模の boot restore 所要時間、最終 atomic staging publish の最大 lock hold、VmHWM は同 fixture で再計測することである。ローカル synthetic test は correctness と 5 秒 response bound を担当する。

## v15.2 sync convoy follow-up Verification (2026-07-24)

- `background_rebuild_and_source_sync_do_not_block_bounded_v1_import` は4行、page size 1、page delay 1秒の background rebuild と、Slack/Google runtime source が空の `sync_all` を同時に開始する。sync が non-bulk projection operation を保持して derived lane 待ちへ入った状態で、bulk session begin が1秒以内に明示 conflict、単発 v1 import が5秒以内に `ingested` を返し、rebuild/sync/append consumer 収束後の canonical/core count 5を検証した。
- `empty_google_source_latest_workspace_lookup_does_not_read_canonical_storage` は writer persistence mutex を意図的に保持した状態でも lookup が1秒以内に空集合を返すことを検証し、空 Google source の canonical page scan を禁止した。
- `cargo fmt --all -- --check` と `cargo test --workspace` は成功した。workspace 集計は `723 passed / 0 failed / 3 ignored`、selfhost unit は `108 passed / 0 failed / 0 ignored` である。
- SQLite DDL、`CURRENT_SCHEMA_VERSION=14`、non-corpus manifest shape/version、v1/v2/429/per-item 契約は変更していない。提供された568k fixture/16 GiB image はこの worktree では再実行していないため、実環境の v1 response、空-source sync latency、最終 lock hold は再計測対象として残る。

## v15.2.2 final publish Verification (2026-07-24)

- 根本原因は、v15.2の最終transactionがlive全削除、stagingからのitem/blob全件copy、staging全削除を行い、主キー・owner/sort・visible-blob各indexをN件比例で更新していたことである。568k実測の`max_persistence_lock_hold_ms=889858`はこのO(N)区間であり、page lock最大959msとは別である。
- SQLite schemaは15へ進めた。v15 objectはmigrationにだけ定義し、`schema_v15_upgrades_true_v14_projection_shape_and_backfills_heads`で真のv14 item/blob rowを初期headへbackfillすることを検証した。manifestのないorphanはmigration transactionをrollbackしてfail-fastする。
- `projection_generation_publish_is_constant_size_and_cleanup_resumes_after_reopen`はlive/staging各5,000件をpublishし、SQLite変更row数32未満、2秒未満、1件publishと同一変更row数を検証した。再open後も新target 5,000件を公開したまま、旧5,000件を128件以下のpageで全回収できる。
- `large_background_rebuild_final_publish_allows_bounded_v1_import`は2,000 Slack Observationから4,000件以上のprojection itemをstagingし、final publish直前に単発v1 importが2秒以内で`ingested`を返すこと、rebuildの`max_persistence_lock_hold_ms < 2000`を検証した。
- `cargo fmt --all -- --check`と`cargo test --workspace --quiet`は成功した。workspace集計は`726 passed / 0 failed / 3 ignored`、selfhost unitは`109 passed / 0 failed / 0 ignored`、SQLite storage unitは`69 passed / 0 failed / 0 ignored`である。non-corpus manifest shape/versionとv1/v2/429/per-item契約は変更していない。
- 提供された568k fixture/16 GiB imageは本worktreeでは再実行していない。次の受入は同fixtureで`background non-corpus generation head published`のitem count/lock holdとcleanup page holdを採取することであり、旧890秒窓の実規模再測定を残す。

## v15.2.2 generation read snapshot review Verification (2026-07-24)

- 根本原因は、6本の公開readがlogical headを一文目で解決し、item/blob/countを別のautocommit文で読んでいたことである。Aを解決した直後にpublishがheadをBへ切り替え、cleanupがA rowを削除すると、二文目は新しいSQLite snapshotで削除途中のAを読み、空・部分結果またはblob visibilityの偽陰性を返せた。
- 6本すべてをlogical head・manifest存在・physical rowを同時に読む単一CTE/JOIN文へ変更した。遅延transactionのfallbackは使わない。`BackgroundRebuildStorage::commit_projection_items(Replace)` の成功後にもcleanup single-flightを要求し、再利用されたstaging世代のretirementをpublish待ちにしない。
- `projection_generation_reads_stay_complete_during_publish_and_cleanup` は64回のhead publish、1 row単位のretired cleanup、2接続のreaderを並行させる。各readerはkey、owner、複数owner page、blob visibility、owner count、total countを反復し、各呼出しが完全な16件またはtrueを返し、owner/page内で世代が混在しないことを検証する。
- `cargo fmt --all -- --check` と `cargo test --workspace --quiet` は成功した。workspace集計は`727 passed / 0 failed / 3 ignored`、selfhost unitは`109 passed / 0 failed / 0 ignored`、SQLite storage unitは`70 passed / 0 failed / 0 ignored`である。

## v15.2.3 sync/rebuild non-bulk convoy Verification (2026-07-24)

- 根本原因は、v15.2.1でbulk mutexを短いhandshakeへ分離した後も、`sync_all`がnon-bulk operationを先に取得してからbackground rebuild所有のderived laneを待っていたことである。通常importは進行できたが、bulk beginのnon-bulk `try_lock`だけがrebuild全期間409になった。
- `background_rebuild_skips_source_sync_without_blocking_bulk_begin_and_next_sync_runs`は4件、page size 1、page delay 100msのstale projection rebuild中にsyncが1秒以内でskipし、`sync_skip_reason="background_non_corpus_rebuild"`をlogすること、last sync/persisted stateを進めないこと、bulk begin/endが成功することを検証した。rebuild完了後の次回syncはhealthとpersisted sync stateを更新する。
- `supplemental_write_waits_for_derived_lane_without_holding_non_bulk_admission`はderived laneを保持してsupplemental writeを待たせ、その間もnon-bulk `try_lock`とbulk begin/endが成功することを検証した。lane解放後は通常のvalidationへ進む。
- `cargo fmt --all -- --check`と`cargo test --workspace --quiet`は成功した。workspace集計は`728 passed / 0 failed / 3 ignored`、selfhost unitは`110 passed / 0 failed / 0 ignored`、SQLite storage unitは`70 passed / 0 failed / 0 ignored`である。SQLite DDL、manifest shape/version、v1/v2/429/per-item契約は変更していない。

## v15.1 P0 follow-up: dup-only no-op と import 入口の snapshot 世代

v15 の B3 non-goal のうち、sol 監査で import request ごとの deep clone が直接のメモリ根因と確定した範囲だけを後続修正する。v1/v2 import は `core_snapshot()` が返す `Arc<AppCore>` を `&AppCore` として読む。draft preparation は read-only であり、AppCore 全体を clone する必要がない。append 後の mutable state は既存の `core_lock()` と derived lane の短い区間だけで更新し、全体 clone を読み取り経路へ戻さない。

副作用のゲートは audit event の有無ではなく、durable append の成功結果である `!request_appended_observations.is_empty()` に固定する。したがって duplicate、canonical collision、validation-only request は audit を記録しても stale 化、append consumer、publish、search catch-up、bulk target 更新を行わない。監査と materialization の経路を分離することで、監査要件を維持したまま dup-only の世代を不変にする。

bulk session の begin は session state の永続化と監査だけを行い、stale 化・publish をしない。最初の実 append で stale state を一度 publish し、通常 observation の後続 append は session end または background rebuild の install+publish 境界へ寄せる。例外は新しい consent decision を後続 request の capture resolver が読む必要がある場合で、その request だけ live compact state を更新して publish する。この例外は cross-request consent ordering を維持するために必要な最小境界であり、通常の duplicate/Slack/freshness append では発生しない。search index catch-up は既存の bulk 中可視性契約を維持するため実 append ごとに single-flight 起動するが、dup-only では起動しない。

bulk end は `target_append_seq == base_append_seq` を no-op session として、rebuild・clone・publish・search catch-up なしで Ready を永続化する。実 append がある場合も background rebuild が install+publish 済みなら end 側は再 clone・再 publishしない。SQLite DDL、manifest/projection shape、v1/v2/429/per-item 契約は変更しない。

import timing には `app_core_clone_ms`（この経路では常時 0）と `publish_clone_ms` を追加し、publish は旧 snapshot の `Arc::strong_count` を debug log に出す。`scripts/import_memory_harness.py` は対象 selfhost PID の `/proc/<pid>/status` を seed 収束後・dup-only 連投後に直接読み、`ingested=0/duplicates=batch`、後半傾き、最終差分を判定する。
