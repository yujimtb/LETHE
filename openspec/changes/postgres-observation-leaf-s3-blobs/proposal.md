## Why

LETHEのPostgreSQL実装はOperational Event専用で、一般Observation Lake・Supplemental・Projection・runtime stateはSQLiteとローカルblobに固定されている。単一NAS上でstateless query replicaとatomic Projection activationを行うask_botには、全StoragePortsを満たすPostgreSQL Leafと外部CASが必要である。

## What Changes

- 一般Lake向け`PostgresPersistence`を追加し、`StoragePorts`全契約を実装する。
- S3互換CASを別adapterとして追加し、SHA-256 key、存在・digest検証、監査付きorphan GCを実装する。
- selfhostのstorage設定を必須tagged enumへ変更し、PostgreSQLとSQLiteを明示選択する。
- **BREAKING**: 旧`[storage]`の無tag SQLite設定を廃止する。自動fallback、dual write、暗黙migrationは提供しない。
- versioned SQL migration、deep health、SQLite/PostgreSQL共通conformance、MinIO統合試験を追加する。

## Capabilities

### New Capabilities

- `postgres-observation-leaf`: PostgreSQLによる一般Observation Leaf、全StoragePorts、migration、conformanceを規定する。
- `s3-blob-storage`: S3/MinIO content-addressed blobの書込・検証・取得・GCを規定する。

### Modified Capabilities

- `observation-lake`: Observation appendとblob参照のtransaction境界をbackend非依存にする。
- `platform-generalization`: SQLite/PostgreSQLの明示選択と共通port parityを必須化する。
- `runtime`: selfhostの必須tagged storage設定、backend readiness、fallback禁止を追加する。

## Non-goals

- 既存SQLiteデータの自動変換、dual read/write、互換config。
- NASへの本番配備や本番credentialの使用。
- Projection意味論、source adapter、公開APIの変更。

## System Laws

Append-Only Law、Replay Law、Effect Isolation Law、No Direct Mutation Lawを維持する。Filtering-before-ExposureとExplicit Authority Lawのpolicy判断はstorage adapterへ移さない。

## Impact

M03 Observation Lake、M15 Runtime、M16 Platform Generalization、`lethe-storage-api`、`lethe-storage-postgres`、新S3 crate、selfhost配線、設定例、Docker試験環境が対象となる。PostgreSQL、MinIO、S3 client依存が追加される。
