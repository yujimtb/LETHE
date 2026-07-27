## Why

外部adapterのprovider pageは、frontierを進める前に全itemが同一commit境界で可視化されなければならない。しかし現行v2取込はitem別partial successを正しく実装しているため、page内の不正itemだけを除外して正常itemをappendし、adapter側の原子的frontier契約を満たせない。またcanonical Observationが参照するsource blobを、外部adapterがLETHE BlobStoreへcontent-addressed admissionするAPIも存在しない。

## What Changes

- `POST /api/v3/import/atomic-observation-pages`を追加し、全draftのJSON/schema/policy/identity/blob参照をappend前に検証する。
- page内に`rejected`、`quarantined`、canonical collision、またはtransient failureが1件でもあれば、Observationとauditを1件もcommitせず、frontierを進めてよいACKを返さない。
- 全itemが有効な場合だけ、PostgreSQL/SQLite共通StoragePortsの単一transactionで`ingested`/`duplicate`結果を入力順にcommitする。
- `PUT /api/v3/import/source-blobs/{sha256}`を追加し、body上限、path digest、実body digestを検証してBlobStoreへ冪等保存する。
- ask_botが送る閉じたsource Observation envelope schema、source system、source別observerをRegistryへ登録する。
- v1/v2 endpointの意味論は変更せず、新規adapterはv3だけを明示選択する。

## Capabilities

### New Capabilities

- `atomic-source-page-ingestion`: 外部adapter pageのall-or-nothing検証、append、ACK、入力順結果を規定する。
- `source-blob-admission`: canonical Observation向けcontent-addressed source blobの認証済み冪等取込を規定する。

### Modified Capabilities

なし。M03 Observation LakeのAppend-Only Law、M09 Adapter Policyのfrontier、既存v2 partial-success契約は変更しない。

## Impact

- 対象: M02 Registry、M03 Observation Lake、M09 Adapter Policy、M14 API Serving。
- 実装: selfhost router/AppService、Registry seed、StoragePorts blob/atomic append経路、API/E2E/backend parity tests。
- System Laws: Append-Only Law、Replay Law、Effect Isolation Law、Explicit Authority Lawを維持する。原子的失敗時にcanonical stateを部分変更しないことでNo Direct Mutation Lawも維持する。
- clientは`write:observations` scope、source instance、positive admission generation、content digestを必須提示する。

## Non-goals

- v1/v2の互換挙動変更、alias、silent fallback。
- provider API取得、adapter frontier永続化、Projection生成。
- private Slack取込、公開read API、blob削除API。
