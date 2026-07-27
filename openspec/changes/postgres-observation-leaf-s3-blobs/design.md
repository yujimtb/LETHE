## Context

`lethe-storage-api::StoragePorts`はObservation、cutover、Supplemental、Projection、runtime state、Slack thread、watermark、blobを抽象化している。`SqlitePersistence`だけが全portを実装し、既存`PostgresOperationalEventStore`はM18 Operational Event専用である。selfhostの一般`[storage]`もSQLite fieldを直接要求する。

ask_botの試験環境はPostgreSQLとMinIOを使うが、NAS本番配備と本番data/credentialは今回行わない。M01のSystem Laws、M03、M15、M16を維持する。

## Goals / Non-Goals

**Goals:**

- `PostgresPersistence`が同一`StoragePorts`契約とappend-only/replay意味論を実装する。
- blob payloadをS3/MinIO CASへ分離し、PostgreSQLにはbackend非依存`BlobRef`だけを保存する。
- 明示backend設定、versioned migration、deep health、共通conformanceでfail-fastする。
- Docker試験環境でPostgreSQL/MinIOを実際に起動して検証する。

**Non-Goals:**

- SQLiteからの自動migration、dual write/read、fallback。
- 既存`PostgresOperationalEventStore`のdata model統合。
- NAS本番配備、本番credential、本番Observationの利用。

## Decisions

### General LakeとOperational Ledgerを分離する

`PostgresPersistence`を新規追加し、既存Operational Storeを変更しない。両者は異なるschemaとmigration ledgerを使う。既存storeを巨大enumにする案はM03とM18のdata-space pin、cutover、retention意味論を混在させるため採用しない。

### 同期portのままblocking PostgreSQL/S3 adapterを使う

既存portは同期traitでselfhostが`Mutex<Box<dyn StoragePorts>>`を利用している。PostgreSQLは既存`postgres` crate、S3はblocking HTTPとSigV4を使う。adapter内部にasync runtimeを隠す案はtokio runtime内block_onの失敗モードを生むため採用しない。

### PostgreSQL transactionとCAS admission

blobは`blob:sha256:{hex}`をS3 object key `sha256/{hex}`へ先にidempotent PUTし、HEAD/GETでbytesとdigestを検証する。Observation/Supplemental append transactionは全参照blobの存在を検査してからcommitする。DB失敗後のorphanは参照tableとのmark-and-sweep差分だけを削除候補にし、監査eventを記録してからdeleteする。

### Versioned SQL migration

SQLをcrate内の番号付きfileとして埋め込み、schema migration ledgerにversionとSHA-256を保存する。未知version、checksum差異、接続role/schema/data-space不一致は起動エラーとする。起動時DDLの継続実行や暗黙repairは行わない。

### 破壊的な必須tagged config

`[storage] backend = "sqlite" | "postgres"`とし、variant固有fieldを`deny_unknown_fields`で検証する。PostgreSQLはDSN env名、schema、role、data-space、read pool、S3 endpoint/region/bucket/access-key env/secret-key env/path-styleをすべて必須とする。

## Risks / Trade-offs

- [全StoragePortsのSQL範囲が広い] → port単位の短いmigrationと既存共通conformanceを先に通す。
- [S3 SigV4実装誤り] → AWS公開signature fixtureと実MinIO統合試験を両方通す。
- [DB commit後にS3 objectが消える] → deep healthと全参照監査をreadinessに含め、欠損を成功扱いしない。
- [同期clientのcontention] → writer 1接続と設定必須read poolを分離し、120並列gateはNAS cutover時に別途行う。
- [orphan GCの誤削除] →最小age、二回連続unreferenced、監査記録を全て必須にする。

## Migration Plan

1. 空の試験用PostgreSQL schemaとMinIO bucketを作る。
2. migration、S3 fixture、port conformance、backend parityを実行する。
3. selfhostを`backend = "postgres"`でfixture Observationだけに対して起動する。
4. failure injection、restart、deep health、orphan GCを検証する。
5. SQLite経路は明示variantとして回帰試験する。自動切替は行わない。
6. NAS利用可能後に別changeで本番namespace作成・全量再取込・cutoverを行う。

## Open Questions

なし。本番resource値とcredentialは将来のNAS配備設定で必須指定する。
