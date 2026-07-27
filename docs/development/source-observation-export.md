# Source Observation export v3

## 境界

内部のProjection Builderとmigration clientは、正規Lakeを次の1本だけで読みます。

```http
GET /api/v3/export/source-observations
Authorization: Bearer <dedicated token>
```

必要scopeは`read:source-observations`です。`read:corpus`、`read:persons`、MCP OAuth
scopeからは暗黙に付与されません。tokenは専用principalへ発行し、一般MCP clientへ
渡しません。search、v1、v2、storage table直読の別名やfallbackはありません。

## クエリ

受理するquery fieldは次の3個だけです。

| field | 必須 | 意味 |
| --- | --- | --- |
| `after_append_seq` | 必須 | このappend sequenceより後を走査する |
| `limit` | 必須 | 返すsource Observation件数。1以上`limits.max_page_size`以下 |
| `watermark` | 継続時必須 | 最初のresponseが返した固定high watermark |

未知field、必須field欠落、0または上限超過の`limit`、
`after_append_seq > watermark`、現在のdurable maximumより大きい`watermark`は
明示的な4xx errorです。既定limitやcursor補正はありません。

初回例:

```http
GET /api/v3/export/source-observations?after_append_seq=0&limit=100
```

継続例:

```http
GET /api/v3/export/source-observations?after_append_seq=42100&limit=100&watermark=587475
```

## 応答

成功bodyは次の閉じた形です。

```json
{
  "watermark": 587475,
  "next_after_append_seq": 42100,
  "complete": false,
  "items": [
    {
      "append_seq": 42001,
      "observation": {
        "id": "019f0000-0000-7000-8000-000000000001",
        "schema": "schema:askbot-source-observation",
        "schema_version": "1.0.0",
        "observer": "obs:askbot-slack-adapter",
        "source_system": "sys:slack",
        "authority_model": "lake_authoritative",
        "capture_model": "event",
        "subject": "slack-message:C0123:1710000000.000001",
        "payload": {
          "schema": "askbot-source-slack-message@1.0.0",
          "native_id": "C0123:1710000000.000001"
        },
        "attachments": [],
        "published": "2026-07-27T00:00:00Z",
        "recorded_at": "2026-07-27T00:00:01Z",
        "idempotency_key": "askbot-slack:C0123:1710000000.000001:sha256",
        "meta": {
          "source_instance": "askbot-slack"
        }
      }
    }
  ]
}
```

`observation`は保存された外側Observation全体です。serviceは
`schema:askbot-source-observation@1.0.0`だけを返し、payloadを変換しません。
他schemaの行はappend順を保ったまま走査対象にはなりますが、`items`には入りません。

clientは`items`の件数から完了を推測せず、必ず`complete`を見ます。成功pageを永続化
した後だけ`next_after_append_seq`をcheckpointします。再起動時は最後に確定した
cursorと、初回に取得した同じ`watermark`を送ります。

## 固定watermarkと上限

初回はstorageの`max_append_seq`を`watermark`として固定します。build中に追加された
より大きいappend sequenceはそのbuildへ混入しません。継続要求は同じwatermarkを
明示するため、再起動しても同じ有限集合を再生できます。

1 requestの出力上限は`limits.max_page_size`、無関係schemaを含む走査上限は必須設定
`limits.max_source_export_scan_records`です。出力pageを安全に確定する前に走査上限へ
達した場合、部分成功を返さず
`source_export_scan_bound_exhausted`（503）で失敗します。運用者はコーパス構成を
確認して走査上限を明示的に変更し、同じcursorから再試行します。

## 検証

通常のRust testに加え、次を実行します。

```powershell
cargo test --locked -p lethe-selfhost source_export
cargo test --locked -p lethe-e2e source_observation_export_http_contract
./scripts/test-pre-nas-storage.ps1
```

pre-NAS storage conformanceはSQLiteとPostgreSQLで、各pageのwatermark、continuation、
completion、append sequence、外側Observation JSON、およびrestart後の再生結果が
一致することを検証します。
