# Personal History Ingestion

## 目的

NaniholdのInterface Pilotが、Claude Code、Codex、Intercom、旧Nanihold、
既存LETHEの履歴を再読解できるよう、Personal DataSpaceへ原文と時系列索引を
保存します。履歴はOperational Event Ledger上のfirst-class Observationであり、
原文はcontent-addressed blobです。会社DataSpaceへの生会話の複製は行いません。

## 同一性とdry-run

履歴レコードの同一性は
`source_instance_id + source_session_id + source_message_id`で決まり、raw bytesの
SHA-256を併記します。

- 同じ同一性かつ同じraw digestは再取込時にduplicateになります。
- 同じ同一性でraw digestが違う場合はsource identity collisionとして停止します。
- 本文だけが同じ別messageは別レコードです。「はい」のような短文を本文比較で
  dedupしません。
- Claude CodeとCodexでは、native message IDだけでなくnative transcript内の
  immutable occurrence locatorも`source_message_id`へ含めます。同じnative message
  IDを再生した別行を誤って衝突させず、元のIDは`native_message_id` metadataとして
  保持します。同じoccurrence locatorのrawが変わった場合は引き続き停止します。
- direct native sourceと既存LETHE sourceは、
  `upstream_source_kind + upstream_source_instance_id + upstream_session_id +
  upstream_message_id`を共通provenance identityとして保存します。異なるsourceに
  同じidentityが現れた件数をmanifestへ含め、1件でもあれば`ready_for_import=false`
  としてimportを停止します。raw表現が違う同一messageを本文digestで統合しません。
- sourceごとにcutover cursorとPersonal ownerを必須にします。
- ownershipが`unresolved`のsourceが1件でもあればimportを開始しません。

`inventory_history`はsource件数、unique件数、raw bytes、cutover cursor、
ownershipとcanonical manifest digestを計算します。`import_history`はdry-runで得た
manifest digestの明示指定を要求し、再scan結果が違えば停止します。成功時は全source
cursorとmanifest digestを持つ`HistoryImportReceipt`をEvent Ledgerへ追加します。

raw blobの書込みと各bounded message batchのappendを完了した後、最後にreceiptを
appendします。各batchは原子的で、receiptが存在しないimportは未完了です。途中失敗後は
同じmanifestでidempotentに再実行し、全messageとreceiptが揃うまでactivationの証拠に
しません。CAS上の未参照blobやmessage eventだけは成功の証拠ではありません。

## Native history CLI

`lethe-import-history`はarchiveの再配置を要求せず、Claude Codeの
`.claude/projects`とCodexの`.codex`を直接読みます。Codexは`sessions`と
`archived_sessions`を同じsource snapshotへ含めます。tree cursorは対象JSONLの
相対path、byte length、file digestから作ります。

dry-runは本文やrawを表示しません。

```powershell
cargo run -p lethe-import-history -- `
  --mode=dry-run `
  --inventory-id=inventory:2026-07-20 `
  --data-space-id=space:personal `
  --owner-id=owner:personal `
  --captured-at=2026-07-20T12:00:00+09:00 `
  --spool-database=D:\secure-temp\history-dry-run.sqlite3 `
  --max-source-record-bytes=134217728 `
  --max-resident-batch-records=128 `
  --max-handoff-session-entries=100000 `
  --claude-root=C:\Users\owner\.claude\projects `
  --claude-source-instance=claude-code-personal `
  --codex-root=C:\Users\owner\.codex `
  --codex-source-instance=codex-personal
```

## Seven-source activation cutover freeze

Interface activation uses exactly seven sources: native `claude_code`, Lake-derived
`claude_ai`, native `codex`, final-drained `intercom`, Lake residual `lethe`,
`nanihold_legacy`, and `system_snapshot`. Set
`--require-activation-source-set=true` for both dry-run and execute. This gate
requires each kind exactly once; a partial inventory or an eighth source stops
before an activation handoff can be emitted.

Before the final dry-run, stop the Claude Code and Codex writers and copy only
their native JSONL archives to a new, empty cutover directory. The importer
must read those copies for both phases; it must not read the live home
directories after the cutover point. The following command is intentionally
non-overwriting. `robocopy` exit codes 0 through 7 are success.

```powershell
$cutover = 'D:\secure-cutover\interface-activation-<utc-timestamp>'
$claudeSnapshot = Join-Path $cutover 'history\native\claude\projects'
$codexSnapshot = Join-Path $cutover 'history\native\codex'
if (Test-Path -LiteralPath $cutover) { throw "cutover path already exists: $cutover" }
New-Item -ItemType Directory -Path $cutover -ErrorAction Stop | Out-Null

robocopy C:\Users\mitob\.claude\projects $claudeSnapshot *.jsonl /E /COPY:DAT /DCOPY:DAT /XJ /R:1 /W:1
if ($LASTEXITCODE -gt 7) { throw "Claude archive freeze failed: $LASTEXITCODE" }
robocopy C:\Users\mitob\.codex $codexSnapshot *.jsonl /E /COPY:DAT /DCOPY:DAT /XJ /R:1 /W:1
if ($LASTEXITCODE -gt 7) { throw "Codex archive freeze failed: $LASTEXITCODE" }
```

Stop the Personal Lake writer, copy its complete `data/` directory with a
SHA-256 manifest, and use that copied `lethe.sqlite3` and `blobs/` as the
LETHE source. The source Lake key is supplied only by its environment-variable
name. Neither the command line, the spool, nor the handoff prints the key.
The source copy must contain a single consistent SQLite generation; copying a
live database or mixing the current database with an older blob directory is
invalid.

The legacy JSONL and report are a frozen pair: retain the report's source
manifest digest and do not regenerate over existing output. Capture a new
secret-free snapshot only after the native and Lake freezes, then convert it
to JSONL. Each producer rejects pre-existing output paths.

```powershell
# Run from the Nanihold repository's approved container environment.
python -m tools.capture_system_snapshot `
  --spec <absolute-cutover>\system-snapshot-spec.json `
  --output <absolute-cutover>\system-snapshot.json
python -m tools.history_source_export system-snapshot `
  --snapshot <absolute-cutover>\system-snapshot.json `
  --output <absolute-cutover>\history\system-snapshot.jsonl `
  --report <absolute-cutover>\history\system-snapshot-report.json
```

Only after Intercom stops intake, drains its outbox, and emits its verified
export may it be converted. `--require-cutover-ready` rejects an export whose
manifest, digest, count, or drain state is not final.

```powershell
python -m tools.history_source_export intercom `
  --export-dir <absolute-cutover>\intercom-final `
  --require-cutover-ready `
  --output <absolute-cutover>\history\intercom.jsonl `
  --report <absolute-cutover>\history\intercom-report.json
```

With all four JSONL files and the three frozen native/Lake inputs present, run
the final dry-run once. `captured-at`, all paths, source instance IDs, and
limits must be copied verbatim into execute. Use explicit residual mappings
only for source systems actually represented in the frozen Lake; unsupported
or unmapped represented systems stop the scan.

```powershell
cargo run -p lethe-import-history -- `
  --mode=dry-run `
  --inventory-id=inventory:interface-activation-<utc-timestamp> `
  --data-space-id=<assigned-personal-data-space> `
  --owner-id=<assigned-owner> `
  --captured-at=<fixed-rfc3339-cutover-time> `
  --spool-database=<absolute-cutover>\spool\history-dry-run.sqlite3 `
  --max-source-record-bytes=134217728 `
  --max-resident-batch-records=128 `
  --max-handoff-session-entries=100000 `
  --require-activation-source-set=true `
  --claude-root=<absolute-cutover>\history\native\claude\projects `
  --claude-source-instance=claude-code-personal `
  --codex-root=<absolute-cutover>\history\native\codex `
  --codex-source-instance=codex-personal `
  --history-jsonl=intercom:intercom-personal:<absolute-cutover>\history\intercom.jsonl `
  --history-jsonl=nanihold_legacy:nanihold-legacy-personal:<absolute-cutover>\history\nanihold-legacy.jsonl `
  --history-jsonl=system_snapshot:nanihold-system-current:<absolute-cutover>\history\system-snapshot.jsonl `
  --lethe-source-backend=sqlite `
  --lethe-claude-ai-source-instance=claude-ai-personal `
  --lethe-residual-source-instance=lethe-personal `
  --lethe-direct-coding-source-policy=exclude `
  --lethe-source-database=<absolute-cutover>\lethe-data\lethe.sqlite3 `
  --lethe-source-blob-dir=<absolute-cutover>\lethe-data\blobs `
  --lethe-source-key-env=LETHE_STORAGE_ENCRYPTION_KEY `
  --lethe-source-routing-key-order=year_month_source_container_published `
  --lethe-source-page-size=512 `
  --lethe-upstream-instance=sys:chatgpt=<stable-chatgpt-instance-if-present> `
  --lethe-upstream-instance=sys:slack=<stable-slack-instance-if-present> `
  --lethe-upstream-instance=sys:discord=discord-primary
```

Omit a residual mapping only when that source system is absent from the frozen
Lake. Do not pass mappings for `sys:claude`, `sys:claude-ai`,
`sys:claude-code`, or `sys:codex`: their partitions are explicit and such
mappings are rejected. Execute is the same command with a fresh, non-existing
spool path. Copy the dry-run command verbatim, make only these changes, and
append the listed destination options; changing any source option, timestamp,
or limit is a manifest drift and fails before receipt creation.

| dry-run argument | execute replacement / addition |
|---|---|
| `--mode=dry-run` | `--mode=execute` |
| `--spool-database=<...>\\history-dry-run.sqlite3` | a new path ending `history-execute.sqlite3` |
| additional | `--expected-manifest-digest=<64-hex-dry-run-manifest-digest>` |
| additional | `--max-blob-bytes=10485760` |
| additional | `--backend=sqlite` |
| additional | `--sqlite-database=<destination-personal-lake>\\personal-operational.sqlite3` |
| additional | `--sqlite-blob-dir=<destination-personal-lake>\\personal-operational-blobs` |
| additional | `--sqlite-key-env=LETHE_OPERATIONAL_STORAGE_ENCRYPTION_KEY` |

The dry-run output contains `activation_handoff`; execute returns the matching
`HistoryImportReceipt`. The handoff and receipt must be retained together. The
dry-run output must have
`ready_for_import=true`, `cross_source_overlap_identities=0`, seven sources,
and only resolved Personal ownership. Execute uses a new spool path and the
exact dry-run `manifest_digest`; it rescans every frozen input and fails on
any manifest drift before appending blobs, messages, or the
`HistoryImportReceipt`. Nanihold accepts activation only when its submitted
handoff and LETHE's receipt have the same manifest digest, source counts, and
cutover cursors.

取込はJSONLを1 recordずつ読み、rawとcanonical順序を明示した新規SQLite spoolへ
保存します。source file digestもincremental SHA-256で計算し、source tree全体や
全recordをメモリに保持しません。executeは最大
`--max-resident-batch-records`件のrawだけを読み、明示的なbatch blob APIでCASへ
保存した後、同じ件数のmessage Eventをbatch appendします。importerは単件blob APIへ
fallbackしません。SQLiteはbatch内のcontent-addressed fileを先に確定し、blob索引を
1 transactionでcommitします。同じdigestはbatch内で1 file writeへまとめます。
file writer数は`available_parallelism`、8、unique digest数の最小値を上限とする
bounded並列処理で、すべてのwriterがjoinして成功した後だけblob索引transactionを
開始します。writerのerrorまたはpanic時は索引を確定せず停止します。先に成功した
writerのfileだけがorphanとして残り得ますが、同じdigestの再実行は冪等です。
PostgreSQLもbatch全体を1 transactionで登録します。
batch内のどれか1件でも`--max-blob-bytes`を超える場合はfile/DBを書き始める前に
batch全体を拒否し、返すBlobRefは入力順を保持します。

したがってexecute時に同時保持するraw record数とevent request数は
`--max-resident-batch-records`を超えません。spoolにはPersonal履歴のrawが含まれるため、
保護された場所を指定し、検証後に運用手順に従って破棄します。

中断時に同じexecute spoolを再利用してはならない。凍結済みsource、固定
`captured-at`、同じ`--expected-manifest-digest`を使い、別の新規spool pathで
execute全体を再実行します。すでにcommit済みのblobはdigestで、message Eventは
idempotency keyでDuplicateとして照合され、未完batch以降だけが実質的に追加されます。
SQLiteでfile確定後かつblob索引commit前に停止した場合は、索引されないorphan fileが
残り得ます。これはreceiptでも取込成功でもなく、再実行時に同じdigestのfileとして
再利用され、通常のorphan blob GCでも回収可能です。blob索引commit後かつmessage
Event commit前の停止では、索引済みだが未参照のblobが残りますが、同じbatchの
再実行はblobとEventの双方で冪等です。全message batchと最後の
`HistoryImportReceipt`が揃うまでNaniholdへactivation handoffを渡しません。

Intercom、旧Nanihold、既存LETHE Projection、current snapshotなどproducerが生成した
厳密な`HistoryRawRecord` JSONLは、次を繰り返して同じmanifest、dedup、receipt経路へ
投入します。未知のsource kind、不正なJSONL、identity collisionは停止します。
`record_kind`はmessage、decision、commitment、work item、preference、current stateに
加えて、`memory_id`と`node_id`を持つfirst-class `node_memory`を含みます。旧Naniholdの
Node memoryをcurrent stateやpreferenceへ意味変換しません。

```text
--history-jsonl=<source_kind>:<source_instance_id>:<path>
```

既存LETHE Personal Lakeには専用のSQLite observation source adapterを使います。
`sys:claude-ai`、`sys:chatgpt`、`sys:claude-code`、`sys:codex`、message schemaの
`sys:slack`と`sys:discord`を、固定した`append_seq` watermarkまでbounded paging
します。LETHE observation IDをsource-native identity、serialized Observationをraw
blobとし、upstream native message IDはmetadataに保持します。
`sys:lethe-history`とhistory schemaは自己取込ループを防ぐため対象外です。

```text
--lethe-source-backend=sqlite
--lethe-claude-ai-source-instance=<stable-id>
--lethe-residual-source-instance=<stable-id>
--lethe-direct-coding-source-policy=exclude
--lethe-source-database=<existing-personal-lake.sqlite3>
--lethe-source-blob-dir=<existing-blob-directory>
--lethe-source-key-env=<32-byte-hex-key-environment-variable>
--lethe-source-routing-key-order=<explicit-routing-keyspec-order>
--lethe-source-page-size=<positive-record-limit>
--lethe-upstream-instance=sys:claude-ai=<stable-source-instance>
--lethe-upstream-instance=sys:chatgpt=<stable-source-instance>
--lethe-upstream-instance=sys:claude-code=<same-instance-as-direct-source>
--lethe-upstream-instance=sys:codex=<same-instance-as-direct-source>
--lethe-upstream-instance=sys:slack=<stable-source-instance>
--lethe-upstream-instance=sys:discord=<stable-source-instance>
```

既存Personal Lakeは単一のhistory sourceとしては取り込まない。`sys:claude`と
`sys:claude-ai`は必ず`claude_ai` partitionへ入り、CLIで指定した
`--lethe-claude-ai-source-instance`をprovenance instanceとして使う。
それ以外の対応済み会話Observationは`lethe` partitionへ入り、
`--lethe-residual-source-instance`を使う。両instanceは異なる値を必須とする。

`sys:claude-code`と`sys:codex`は既存Lake partitionから常に除外する。これらは
Claude Code/Codex native archiveの直接取込が正本であり、Lake側にも入れると
cross-source native identity overlapになるためである。`--lethe-direct-coding-source-policy=exclude`
を明示しなければ開始せず、これら四つのsource system (`sys:claude`,
`sys:claude-ai`, `sys:claude-code`, `sys:codex`) に対する
`--lethe-upstream-instance`も受理しない。instanceを推測したり、重複時に片方を
黙って採用したりしない。

したがって7-source activation handoffでは、Lake adapterが
`claude_ai:<claude-ai-instance>` と `lethe:<residual-instance>` の二sourceを出し、
native Claude Code、native Codex、Intercom、旧Nanihold、current system snapshotと
合計7 sourceになる。

現在のPersonal Lake正本はSQLiteなので、source backendは`sqlite`だけを受理します。
database、blob directory、key環境変数、routing key order、page sizeが欠けた場合は
開始前に停止し、
remote endpointや別backendへfallbackしません。別VMからの取込は整合したbackup
snapshotを明示的にmountしてdry-runします。

対象Observationのsource systemに対応するupstream instance mappingがなければ、
provenanceを推測せず停止します。direct Claude Code/Codexと同時にinventoryする場合は
同じinstance IDを指定し、cross-source overlapを明示的に検出します。空本文の
Observationはconversation Projectionの対象外として数えません。

## Activation handoff

dry-runとexecuteは同一の`schema:history-activation-handoff` version `1.0.0`を
出力します。handoffはinventory/DataSpace/manifest digest、全7 source kindの
`source_id=<kind>:<instance>`、解決済みownership/owner、record count、raw bytes、
`digest_sha256`、cutover cursor、およびsessionごとのref・source ID・期間・message
countだけを含みます。全message refや本文は含みません。
session entry数は`--max-handoff-session-entries`で上限を必須指定し、超過時はtruncateせず
停止します。NaniholdはこのhandoffとLedger上の`HistoryImportReceipt`が同じ
manifest digest、count、cursorを指すことをactivation前に検査します。

LETHEは同じmanifestから`session_index_ref`、`open_commitments_ref`、
`current_state_ref`を決定論的に生成・検証します。この3つは
`history-projection:<projection>:sha256:<digest>`形式で、Naniholdは内容を別正本として
再生成せず受領してgateします。

正式fixtureは
`crates/history/tests/fixtures/history_activation_handoff.json`です。

## Validation spool cleanup

dry-run spoolはraw Personal履歴を含む一時機密データで、Git ignoredであっても保持物では
ありません。検証完了後、操作者は絶対pathが指定workspaceの`target`または承認済み
保護temp directory内であることを確認し、spool本体と同名の`-wal`/`-shm`だけを
明示削除します。wildcardや親directoryのrecursive deleteは使いません。削除権限が
得られない場合はstageせず、未完cleanupとしてhandoffに記録します。

executeでは同じ引数に加えて、直前の`--expected-manifest-digest`、
`--max-blob-bytes`、SQLiteまたはPostgreSQLの明示backend設定が必要です。
backend、secret環境変数、digestの欠落時に別backendへfallbackしません。

production importはwriter停止、backup、最終delta inventory、所有先解決を終えてから
実行します。CLIの存在はproduction importの自動承認を意味しません。

既存Personal LakeへOperational Ledgerを初めて追加する場合は、再起動前に
`scripts/add_operational_ledger_key.ps1 -EnvFile <absolute-path>`を一度だけ実行する。
この鍵は`LETHE_STORAGE_ENCRYPTION_KEY`と別の32-byte hex値でなければならず、既存値の
再利用や表示は行わない。新規環境は`scripts/new_personal_lake_env.ps1`が両方の鍵を
別々に生成する。

Nanihold runtimeは履歴検索とOperational Event appendの両方を行うため、Personal
Lakeでは`LETHE_NANIHOLD_TOKEN`を専用に発行し、
`read:operational`、`write:operational`、`read:history`、`write:history`だけを
付与する。read-only tokenとwrite-only tokenをruntime側で暗黙に切り替えず、
既存tokenのscopeも拡張しない。既存`.env`への初回追加は
`scripts/add_nanihold_token.ps1 -EnvFile <absolute-path>`を一度だけ実行し、
値を標準出力やGitへ出さない。

## Nanihold HistoryReader

Nanihold向けread portは`POST /api/history/query`だけです。個別GETや全blob走査APIは
ありません。scopeは`read:history`です。

```json
{
  "data_space_id": "space:personal",
  "operation": "search",
  "argument": {"query": "Interface Pilot"},
  "page_cursor": null,
  "max_result_bytes": 65536
}
```

operationとargumentは次の閉じた組です。

| operation | argument |
|---|---|
| `list_sessions` | `{}` |
| `list_open_commitments` | `{}` |
| `get_current_state` | `{}`（本文を含まないcursor付き索引） |
| `get_current_state` | `{"state_key":"..."}`（指定した1件の本文） |
| `read_timeline` | `{"session_id":"history-session:..."}` |
| `read_raw` | `{"message_id":"event:history-message:..."}` |
| `search` | `{"query":"..."}` |
| `resolve_reference` | `{"reference_id":"..."}` |

応答は`result_json`、`next_cursor`、`source_cursor`を返します。`result_json`の
canonical JSON bytesが`max_result_bytes`を超えない最大pageだけを返し、継続が
あればopaque `next_cursor`を必ず返します。1 recordだけで上限を超える場合も暗黙に
truncateせず`HistoryResultTooLarge`で停止します。

cursorにはoperation、Projection watermark、offsetが含まれます。履歴追加後に古い
cursorを使うと`HistoryCursorStale` (HTTP 422)になります。クライアントは明示的に
初回から再開するか判断し、LETHEはsilent restartしません。DataSpace不一致、
argument/cursor不正も422です。

`read_raw`の`result_json`にはblob ref、SHA-256、raw byte数、base64本文が入り、
同じsize上限を受けます。すべてのread結果はEvent Ledgerの
`source_cursor = operational:<watermark>`に固定されます。

## Projection

履歴ProjectionはEvent Ledgerだけから決定論的に再構築します。session一覧、
session timeline、本文検索、event/source/blob reference解決に加え、履歴Eventから
決定論的に得られるopen commitmentとcurrent stateを提供します。後者二つは
再オリエンテーションの根拠用read modelであり、activation後の可変な運用正本を
LETHEへ新設するものではありません。運用上のcommitmentとcurrent stateの正本は
引き続きNanihold側に置きます。

履歴queryのcontinuation cursorはOperational Ledger全体のwatermarkではなく、
履歴indexを最後に変更したhistory event cursorを`query_revision`として固定します。
Naniholdが同じLedgerへ監査Eventをappendしても`source_cursor`は最新位置へ進みますが、
履歴indexが不変なら既発行cursorは失効しません。新しいhistory messageによって
indexが変わった場合だけ旧cursorを拒否します。これにより、ページ間の監査appendで
走査が自己破壊することなく、監査上の最新watermarkも失いません。

`get_current_state`の無引数queryは、`state_key`、根拠Event、時刻、本文byte数、
本文SHA-256だけを返し、本文をpromptへ一括投入しません。索引自体も
`max_result_bytes`とcursorでpage化します。本文が必要な場合だけ、索引に存在する
`state_key`を明示して1件を取得します。未知key、空key、targeted queryへの
continuation cursorはfail-fastし、旧形式の全件本文応答へfallbackしません。

SQLiteとPostgreSQLは同じ`OperationalStoragePorts` conformanceを使います。
PostgreSQL試験は明示的なdisposable schemaとroleがある場合だけ実行します。
