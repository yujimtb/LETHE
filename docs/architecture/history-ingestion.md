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

取込はJSONLを1 recordずつ読み、rawとcanonical順序を明示した新規SQLite spoolへ
保存します。source file digestもincremental SHA-256で計算し、source tree全体や
全recordをメモリに保持しません。execute時に同時保持するevent request数は
`--max-resident-batch-records`を超えません。spoolにはPersonal履歴のrawが含まれるため、
保護された場所を指定し、検証後に運用手順に従って破棄します。

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
--lethe-source-instance=<stable-id>
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

## Nanihold HistoryReader

Nanihold向けread portは`POST /api/history/query`だけです。個別GETや全blob走査APIは
ありません。scopeは`read:history`です。

```json
{
  "data_space_id": "space:personal",
  "operation": "search",
  "argument": {"query": "Fable"},
  "page_cursor": null,
  "max_result_bytes": 65536
}
```

operationとargumentは次の閉じた組です。

| operation | argument |
|---|---|
| `list_sessions` | `{}` |
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
session timeline、本文検索、event/source/blob reference解決を提供します。
commitmentとcurrent operational stateの正本ProjectionはNanihold側に置き、履歴
read portで別の現在状態を作りません。

SQLiteとPostgreSQLは同じ`OperationalStoragePorts` conformanceを使います。
PostgreSQL試験は明示的なdisposable schemaとroleがある場合だけ実行します。
