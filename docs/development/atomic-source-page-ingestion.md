# Atomic source page ingestion v3

外部source adapterがproviderの1ページをLETHEへ投入する場合は、専用のv3 APIだけを使います。
`POST /api/v3/import/atomic-observation-pages`は既存v1/v2の別名ではなく、
部分成功を持たない独立契約です。v1/v2へ`atomic` flagを渡す方法、v3失敗時に
v1/v2へ切り替える方法、bulk sessionを併用する方法はありません。

## Source-unit prerequisite

historical v1 Observationを持つunitは、既存のregister、drain、bridge readiness、
activate手順で移行します。新しい空namespaceへv3-only adapterを登録する場合は、
その手順や架空のv1 sampleを使わず、専用bootstrapだけを使います。

```http
POST /api/v3/source-units/<source_instance_id>/bootstrap
Authorization: Bearer <admin:cutover token>
Content-Type: application/json

{"authority":"<operator>","reason":"<audit reason>"}
```

LETHEは同一transaction内でunit state不在と、そのsource instanceを持つcanonical
Observation不在を確認し、`v2_active` generation 1とv2 credentialを作ります。
未appendの同一bootstrap stateへのexact retryだけは同じstateを返します。既存data、
別state、不明field、空authority/reasonは拒否し、v1 credentialやbridge fixtureを
作りません。

v3 page APIとsource blob APIは`V2Active`または`V2Committed`のunitと、そのunitで
現在有効なpositive generationだけを受理します。未登録unit、欠落・0・stale
generationは受理しません。

bootstrapに必要なscopeは`admin:cutover`、書込み両APIは`write:observations`です。
page APIは
`X-LETHE-Admission-Generation`、blob APIはさらに
`X-LETHE-Source-Instance`を必須にします。

## Source blob PUT

```http
PUT /api/v3/import/source-blobs/<64文字のlowercase sha256>
Authorization: Bearer <write:observations token>
X-LETHE-Source-Instance: <stable source instance>
X-LETHE-Admission-Generation: <positive active generation>
Content-Type: application/octet-stream

<exact bytes>
```

pathはlowercase hexadecimalだけを許可します。本文は設定済み
`limits.max_blob_bytes`を上限とし、本文SHA-256がpathと一致した後にだけ一般Lakeの
`BlobStore::put_blob`へ渡します。operational blob store、ローカル一時ファイル、
別endpointへのfallbackはありません。

成功receiptは次の2 fieldだけです。

```json
{
  "blob_ref": "blob:sha256:<sha256>",
  "size_bytes": 1234
}
```

同じbytesの再PUTは同じreceiptへ収束します。PUT成功だけではObservationを作りません。
後続のatomic page transactionが全attachmentの存在とdigestを再検証します。

## Atomic page POST

requestのtop-level fieldは`source_instance_id`と`drafts`だけです。不明field、
空白source instance、空pageを拒否します。全draftはnon-blankかつpage内uniqueな
`client_ref`を持つ必要があります。

```http
POST /api/v3/import/atomic-observation-pages
Authorization: Bearer <write:observations token>
X-LETHE-Admission-Generation: <positive active generation>
Content-Type: application/json
```

```json
{
  "source_instance_id": "askbot-slack-main",
  "drafts": [
    {
      "schema": "schema:askbot-source-observation",
      "schema_version": "1.0.0",
      "observer": "obs:askbot-slack-adapter",
      "source_system": "sys:slack",
      "authority_model": "lake_authoritative",
      "capture_model": "event",
      "subject": "https://askbot.hlab.college/subjects/slack-message-1",
      "target": null,
      "payload": {
        "subject": "https://askbot.hlab.college/subjects/slack-message-1",
        "observation": "https://askbot.hlab.college/observations/slack-message-1",
        "conflict_set": "https://askbot.hlab.college/conflicts/slack-message-1",
        "schema": "https://askbot.hlab.college/schemas/slack/v1",
        "native_identifier": {
          "source": "https://askbot.hlab.college/sources/slack",
          "schema": "https://askbot.hlab.college/native-identifiers/slack/v1",
          "parts": {
            "channel_id": "C01ABC",
            "ts": "1234.5678"
          }
        },
        "source_position": "1234.5678",
        "revision": "revision:1234.5678",
        "observed_at": "2026-07-27T00:00:00Z",
        "payload": {
          "channel_id": "C01ABC",
          "ts": "1234.5678",
          "text": "source-native text"
        },
        "tombstone": false,
        "blob_digests": [],
        "payload_sha256": "<64文字のlowercase sha256>"
      },
      "attachments": [],
      "published": "2026-07-27T00:00:00Z",
      "idempotency_key": "<server formulaと一致するv2 identity>",
      "client_ref": "slack-message-1",
      "meta": {
        "object_id": "slack-message-1",
        "canonical_json": "<valid JSON string>",
        "source_container": "C01ABC"
      }
    }
  ]
}
```

LETHEは全itemについてrequest limit、client reference、server-derived identity、
Registry source contract、authority、policy、時刻、payload、blob referenceを先に検証します。
1件でも失敗すればStorage appendを呼ばず、Observationもpage監査も0件です。

全件prepare後は、cutover admission、blob reference admission、global canonical identity
resolution、Observation append、page監査append、cutover metricsをSQLiteまたはPostgreSQLの
1 transactionで処理します。canonical collision、監査書込み失敗、backend失敗は
transaction全体をrollbackします。materialization triggerはcommit後だけ実行します。

成功responseの各resultは入力順で、`ingested`または`duplicate`だけです。

```json
{
  "ingested": 1,
  "duplicates": 1,
  "results": [
    {
      "outcome": "ingested",
      "client_ref": "item-1",
      "observation_id": "019..."
    },
    {
      "outcome": "duplicate",
      "client_ref": "item-2",
      "existing_id": "019..."
    }
  ]
}
```

terminal item failureまたはcanonical collisionはHTTP 422です。raw payloadは返しません。

```json
{
  "error": "atomic_page_rejected",
  "details": {
    "failures": [
      {
        "index": 1,
        "client_ref": "item-2",
        "error_code": "canonical_collision",
        "failure_class": "collision"
      }
    ]
  }
}
```

commit前の一時storage failureはHTTP 503、`error=atomic_page_transient`、
`retry_after=1`です。clientは同じpageを同じidentity入力で再送します。LETHEが
成功ACKを返していないpageについてadapter frontierを進めてはいけません。

## ask_bot Registry contracts

outer schemaはclosedな`schema:askbot-source-observation@1.0.0`です。native payloadを
renameせず`payload`内に保持し、TSV、catalog、provider fieldの正規化コピーを作りません。

| Observer | Source system |
| --- | --- |
| `obs:askbot-slack-adapter` | `sys:slack` |
| `obs:askbot-drive-adapter` | `sys:google-drive` |
| `obs:askbot-docs-adapter` | `sys:google-docs` |
| `obs:askbot-sheets-adapter` | `sys:google-sheets` |
| `obs:askbot-slides-adapter` | `sys:google-slides` |
| `obs:askbot-forms-adapter` | `sys:google-forms` |
| `obs:askbot-note-adapter` | `sys:note` |

observer、source system、schemaの組合せが表と一致しないitemはpage全体をappend前に
拒否します。

## Verification

SQLiteの単体試験は成功、完全再送、stale generation、blob欠落、監査失敗、
canonical collisionのledger/audit/metric rollbackを検証します。selfhost HTTP E2Eは
Slack、Google Docs、note envelope、混在page拒否、source blobの認証・path・digest・size、
再起動後のduplicate収束を検証します。

PostgreSQL/S3を含む使い捨て試験は次で実行します。

```powershell
./scripts/test-pre-nas-storage.ps1
```

この試験は同一fixtureをSQLiteとPostgreSQL/S3へ投入し、成功・再送・stale・blob欠落・
監査失敗・collision index・可視ledger delta・page監査delta・cutover metricを正規化比較し、
終了時にCompose container、network、volumeを削除します。
