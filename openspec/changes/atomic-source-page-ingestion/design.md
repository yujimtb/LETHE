## Context

M03はcanonical Observationをappend-only Lakeへ保存し、M09 adapterはprovider pageを全件処理した後だけfrontierを進める。現行`/api/v2/import/observation-drafts`は汎用client向けのper-item partial successであり、このpage原子性とは意図的に異なる。またPostgreSQL/S3 backendは`BlobStore::put_blob`と参照admissionを持つが、外部source adapter用HTTP境界がない。

ask_botはSlack/Google/noteのsource-native payloadを自身の閉じたJSON Schemaで検証し、そのschema IRI、typed native identifier、source position、revision、payload、tombstone、blob digestを一つのsource Observation envelopeとしてLETHEへ送る。

## Goals / Non-Goals

**Goals:**

- 外部adapterの1 provider pageを、validationからObservation/audit commitまでall-or-nothingにする。
- 同一入力の再送を入力順の`ingested`/`duplicate`結果へ収束させる。
- source blobを一般LakeのBlobStoreへdigest検証付きで冪等admissionする。
- SQLite/PostgreSQL双方で同じStoragePorts契約を通す。
- 空のv3-only source unitを、v1 sampleやbridgeを作らず明示的にbootstrapする。
- v1/v2を変更せず、v3を明示選択したclientだけに新契約を適用する。

**Non-Goals:**

- provider取得、cursor永続化、Projection、公開blob read。
- v2 partial successや既存v1/v2 cutover canonical identityの変更。
- blob削除、private Slack、動的Registry管理API。

## Decisions

### 専用v3 endpoint

`POST /api/v3/import/atomic-observation-pages`を新設する。既存v2へ`atomic` flagを追加すると、field欠損時のdefaultと同一endpoint内の二重意味論が生じるため採用しない。requestは`deny_unknown_fields`で、`source_instance_id`と非空`drafts`だけを受け付ける。`bulk_session_id`は受け付けない。

v3はv2 canonical identityとcutover generationを再利用する。`X-LETHE-Admission-Generation`はpositiveかつ必須で、StoragePortsではV2 active generationとしてadmissionする。wire versionとidentity generationを混同しないことをAPI文書に明記する。

### v3-only source unit bootstrap

`POST /api/v3/source-units/{source_instance_id}/bootstrap`を新設する。これはhistorical v1 rowを持つunitのcutoverではなく、空namespaceへ新規v3 producerを登録するための専用操作である。StoragePortsは同一transaction内でstate不在とsource instance付きObservation不在を確認し、`uninitialized -> v2_active` generation 1 transition、v2 credential、metrics rowを作る。既に未appendの同一bootstrap stateだけはexact retryとして同じstateを返し、それ以外のstateまたはObservationがあればconflictにする。v1 credential、fixture、bridge candidate、readiness bypassは作らない。

### 二段階の原子的処理

AppServiceは全draftについてbody/count、`client_ref`一意性、server-derived identity、Registry、policy、時刻、blob参照を先に検証する。1件でもterminal failureならStoragePortsを呼ばず、`atomic_page_rejected` Problemに入力index、`client_ref`、安定error codeだけを返す。

全件prepare後、`CutoverStore::append_observation_page_v2_atomic`を呼ぶ。SQLite/PostgreSQL実装はadmission、blob existence、global identity、Observation、auditを一つのtransactionに含める。結果にcanonical collisionがあればtransactionをrollbackする。成功時の結果語彙は`ingested`と`duplicate`だけで、入力順と件数を厳密検証する。materialization triggerはcommit後に行い、canonical ACKを反転させない。

### source blob admission

`PUT /api/v3/import/source-blobs/{sha256}`は`write:observations`、`X-LETHE-Source-Instance`、positive generationを必須とする。AppServiceは先にcutover admissionを検査し、設定済み`limits.max_blob_bytes`でbodyを制限する。pathは64文字lowercase hexだけを許可し、body SHA-256と不一致なら保存しない。一般StoragePortsの`put_blob`結果が期待`blob:sha256:<digest>`と完全一致した場合だけreceiptを返す。redirect、別store、operational blobへのfallbackはない。

### ask_bot source envelope Registry

`schema:askbot-source-observation` v1はsource Observationの全fieldを閉じたouter payloadとして検証する。`payload`自体はadapter bundleでschema検証済みのnative JSONであり、outer schemaはその任意JSON値を改変せず保持する。Slack、Drive、Docs、Sheets、Slides、Forms、noteごとにobserverを登録し、対応source systemとschema source contractを明示する。source別identityをTSVやmapping tableへ複製しない。

### エラーとログ

validation/policy/collisionはraw payloadを含まないtyped Problem、storage一時障害は503 Problemとする。ログはsource instance、page件数、digest、error codeだけを持つ。body、token、email、Slack IDを追加ログしない。

## Risks / Trade-offs

- [prepare後からcommit前にblobが消える] → transaction内の既存blob reference admissionを再実行し、失敗時は全rollbackする。
- [既存v2より大きなpageがlockを保持する] → 設定済み`max_import_drafts`とbody/payload上限をそのまま強制し、adapter page sizeをclient設定で制限する。
- [動的source schemaをouter Registryが完全検証できない] → outer envelopeを閉じ、inner native payloadはask_bot release-pinned schemaでappend前に検証し、schema IRIとpayload digestをenvelopeへ保存する。
- [blob upload後にpageが失敗してorphanになる] → content addressにより再送を冪等化し、既存二回scan/min-age audited GCだけが削除できる。

## Migration Plan

1. Registry追加とStoragePorts atomic methodをbackend parity test付きで配備する。
2. v3 blob/page APIをHTTP/E2E testで検証する。
3. ask_bot test adapterをv3へ接続し、故障注入時にfrontierとledgerが不変であることを確認する。
4. 空namespaceのsource instanceをv3 bootstrapし、返されたgenerationをSecretとして配備する。
5. v3 adapterを開始する。失敗時はadapterを停止し、v1/v2へfallbackせず、既存canonical stateを保持する。

## Open Questions

なし。endpoint、identity generation、failure semantics、Registry envelopeは本changeで固定する。
