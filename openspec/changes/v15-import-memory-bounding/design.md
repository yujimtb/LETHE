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

## Risks / Trade-offs

- **[consumer の遅延中は通常 request の snapshot が stale]** → canonical append が成功した時点で response を返す既存非同期契約を維持し、consumer single-flight と health/error state を使う。
- **[v1 の request-level reject が既存 client に見える]** → permissive payload/result semantics は変えず、draft 上限だけを明示 error として既存 bad-request 形式で返す。
- **[re-consent repull の DB read が増える]** → privacy reverse index で対象 ID を絞り、page 単位に制限する。全 corpus の本文は保持しない。
- **[manifest v10 の起動時 rebuild latency]** → format pre-guard で明確に rebuild に送り、rebuild は既存 page/cursor 経路を利用する。破損した旧型を部分的に読み込まない。
- **[deep clone 自体は残る]** → B3 は次期候補として明示し、v15 は admission、publish 回数、resident body、migration page を bounded にする。

## Migration Plan

1. 設定ファイルへ新しい limits を必須追加し、既定運用値を `max_concurrent_imports=2`、既存 sync limit と分離した `max_import_drafts`、検索 job 保持上限とする。
2. binary 起動時に SQLite schema を変更せず、persisted non-corpus manifest v10 以下を pre-deserialize guard で検出して page rebuild する。
3. v1/v2 import の admission、publish boundary、search eviction を有効化する。
4. rollback は code/config を同時に戻す。SQLite v13 migration は transaction が commit 済みなら再実行不要で、manifest v11 は旧 binary が newer format として fail-fast するため、旧 binary への rollback は同じ DB でサポートしない。

## Open Questions

なし。AppCore の clone 廃止は次期 change の候補として記録済みであり、本 change の実装判断をブロックしない。

## Verification (2026-07-23)

- `cargo fmt --all -- --check`、`cargo check --workspace`、`cargo test --workspace --quiet` が成功した。workspace の集計は `711 passed / 0 failed / 3 ignored`（実行 714 件）。
- publish counter の実測は、現行経路で `1000件/1 request = 1`、`25件×40 request 直列 = 36`。非同期 consumer のスケジューリングにより後者は 36〜37 の範囲で変動するが、上限 43 を満たす。
- before は旧 `refresh_capture_consent_snapshot` の単独 publish 呼出しを一時的に復元して同じテストを実行し、`1000件/1 request = 2`、`25件×40 request 直列 = 78` を測定した。測定用の一時変更は取り除き、最終コードには残していない。
- SQLite の `CURRENT_SCHEMA_VERSION` は 14 のまま。v13 backfill は page/cursor 化したが、既存 DDL、migration ledger、schema semantics は変更していない。`v13_privacy_backfill_rolls_back_and_retries_from_first_page` で transaction rollback と先頭 page からの再実行も検証した。
- 残課題は B3（AppCore の書込/読取分離と deep clone 全廃）であり、本 change では扱わない。RSS harness の実 corpus 判定は Linux container 実行、通常 CI は小規模 corpus と publish counter を使う。

## v15.1 P0 follow-up: dup-only no-op と import 入口の snapshot 世代

v15 の B3 non-goal のうち、sol 監査で import request ごとの deep clone が直接のメモリ根因と確定した範囲だけを後続修正する。v1/v2 import は `core_snapshot()` が返す `Arc<AppCore>` を `&AppCore` として読む。draft preparation は read-only であり、AppCore 全体を clone する必要がない。append 後の mutable state は既存の `core_lock()` と derived lane の短い区間だけで更新し、全体 clone を読み取り経路へ戻さない。

副作用のゲートは audit event の有無ではなく、durable append の成功結果である `!request_appended_observations.is_empty()` に固定する。したがって duplicate、canonical collision、validation-only request は audit を記録しても stale 化、append consumer、publish、search catch-up、bulk target 更新を行わない。監査と materialization の経路を分離することで、監査要件を維持したまま dup-only の世代を不変にする。

bulk session の begin は session state の永続化と監査だけを行い、stale 化・publish をしない。最初の実 append で stale state を一度 publish し、通常 observation の後続 append は session end または background rebuild の install+publish 境界へ寄せる。例外は新しい consent decision を後続 request の capture resolver が読む必要がある場合で、その request だけ live compact state を更新して publish する。この例外は cross-request consent ordering を維持するために必要な最小境界であり、通常の duplicate/Slack/freshness append では発生しない。search index catch-up は既存の bulk 中可視性契約を維持するため実 append ごとに single-flight 起動するが、dup-only では起動しない。

bulk end は `target_append_seq == base_append_seq` を no-op session として、rebuild・clone・publish・search catch-up なしで Ready を永続化する。実 append がある場合も background rebuild が install+publish 済みなら end 側は再 clone・再 publishしない。SQLite DDL、manifest/projection shape、v1/v2/429/per-item 契約は変更しない。

import timing には `app_core_clone_ms`（この経路では常時 0）と `publish_clone_ms` を追加し、publish は旧 snapshot の `Arc::strong_count` を debug log に出す。`scripts/import_memory_harness.py` は対象 selfhost PID の `/proc/<pid>/status` を seed 収束後・dup-only 連投後に直接読み、`ingested=0/duplicates=batch`、後半傾き、最終差分を判定する。
