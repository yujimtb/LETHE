# PostgreSQL + S3 general storage

## Scope

This document covers the general Observation Lake selected by `[storage]`. It
does not merge or replace the separately configured Operational Event Ledger.
The two stores have independent data-space pins, schemas, migrations, and
failure domains.

The supported general-storage variants are exactly:

- `backend = "sqlite"` with one database path, blob directory, and encryption
  key environment reference.
- `backend = "postgres"` with a pinned data space, DSN environment reference,
  schema, role, read-pool size, and one S3-compatible content-addressed blob
  store.

The backend tag and every field of its selected variant are required. Mixed,
unknown, legacy untagged, insecure, or incomplete configurations fail before a
storage port is exposed. There is no automatic backend selection, dual read,
dual write, SQLite import, or compatibility fallback.

## PostgreSQL and MinIO configuration

The PostgreSQL DSN and S3 credentials are loaded from the environment variables
named by the configuration. Secret values do not belong in TOML.

```toml
[storage]
backend = "postgres"
data_space_id = "space:college"
dsn_env = "LETHE_GENERAL_POSTGRES_DSN"
schema = "lethe_general"
role = "lethe_general"
read_pool_size = 4

[storage.blobs]
endpoint = "https://minio.storage.example"
region = "us-east-1"
bucket = "lethe-general-blobs"
access_key_env = "LETHE_GENERAL_S3_ACCESS_KEY"
secret_key_env = "LETHE_GENERAL_S3_SECRET_KEY"
path_style = true
tls_policy = "required"
timeout_seconds = 10
max_object_bytes = 10485760
orphan_min_age_seconds = 86400
```

`tls_policy = "test_http"` accepts only `http` endpoints on localhost,
loopback, or the Compose service name `minio`. It is only for the disposable
pre-NAS topology. Certificate verification cannot be disabled.

## Data and migration model

PostgreSQL stores canonical Observation, leaf, Supplemental, Projection,
runtime, audit, Slack-thread, watermark, and cutover state. Blob bytes live
only in the configured bucket. A `BlobRef` is always
`blob:sha256:{lowercase-hex}` and maps to exactly
`sha256/{lowercase-hex}`; it contains no endpoint or bucket name.

Migrations are ordered SQL files embedded in `lethe-storage-postgres`.
`general_schema_migrations` records the exact version and SHA-256 checksum.
Startup:

1. connects using the configured role;
2. applies missing known migrations in order;
3. verifies every recorded checksum and rejects unknown newer versions;
4. pins and verifies the data space, schema, writer, and configured readers;
5. admits the configured S3 bucket.

A checksum mismatch, role/schema/data-space mismatch, unknown version, missing
bucket, or unavailable selected service is a startup error. Operators must
investigate the mismatch; the process does not repair, downgrade, or switch
backends.

Observationの`append_seq`は、全append pathが保持するtransaction-scoped partition
lock下で`MAX(append_seq) + 1`を明示採番します。PostgreSQLのnon-transactional
sequence defaultはmigration 2で削除されています。このため、失敗したatomic pageが
rollbackしてもcursor gapを残さず、同一入力に対するSQLite/PostgreSQLのwatermarkと
continuation位置が一致します。採番とObservation insertは同じtransactionです。

## Blob admission and garbage collection

Blob PUT is content-addressed and idempotent. The PostgreSQL adapter holds the
blob-admission lock from S3 PUT through metadata commit. Observation,
Supplemental, Projection, Slack, and cutover writes take the same lock and
verify every referenced object, length, and digest before commit. Missing or
corrupt references roll back the whole database transaction.

Orphan collection deletes an object only when all of these are true:

1. its S3 `LastModified` age meets `orphan_min_age_seconds`;
2. it is unreferenced in two consecutive scans;
3. a final reference check under the blob-admission lock still finds no use;
4. the delete audit event and metadata removal commit successfully.

An audit failure leaves both metadata and object intact. An S3 delete failure
leaves the candidate available for a later retry.

## Readiness

The deep-health endpoint checks:

- PostgreSQL migration/data-space/role/schema pins;
- the writer and every configured reader;
- structural database invariants and stored domain JSON;
- an S3 PUT/GET/digest/DELETE probe using a unique ephemeral object;
- every visible Observation, Supplemental, and Projection blob reference.

PostgreSQL success with S3 failure is not healthy. Startup cannot publish a
service with an unadmitted selected backend, and a later deep-health failure is
reported as the `storage` dependency with status `failed`.

## Disposable pre-NAS test

Run from the repository root on a Docker host:

```powershell
.\scripts\test-pre-nas-storage.ps1
```

The command accepts only `DeploymentMode=test`. It uses the image digests and
fixture-only credentials in `deploy/pre-nas-storage-test/test.env`, binds
PostgreSQL and MinIO only to `127.0.0.1`, stores their data on tmpfs, and
removes only the `lethe-pre-nas-storage-test` Compose project in `finally`.

It runs migrations; all PostgreSQL ports; MinIO signing, size, timeout, missing,
and corrupt-object cases; transactional failure injection; conservative orphan
GC; concurrent idempotency; SQLite/PostgreSQL normalized parity（source
Observation exportの各page、固定watermark、continuation、restart replayを含む）;
restart and Replay Law; separate SQLite/PostgreSQL selfhost boot; deep
readiness; and selected-S3 failure rejection.

The 2026-07-27 pre-NAS run completed with
`pre_nas_storage_conformance=passed`. It used synthetic fixtures only and did
not access NAS or production credentials.

## Backup and restore

PostgreSQL and the S3 bucket form one logical backup set. A usable restore point
must include both at a mutually consistent writer fence.

1. stop or fence all ingestion, Projection publication, and orphan GC;
2. record the PostgreSQL transaction boundary and bucket inventory;
3. back up the PostgreSQL general schema and versioned S3 objects;
4. restore into a new schema and new bucket, never over the active set;
5. configure a test selfhost explicitly for that schema and bucket;
6. require migration admission and deep health to pass;
7. compare Observation counts/high-water marks, Projection heads, referenced
   blob inventory, and fixture queries before changing the selected backend.

Do not delete or mutate the prior SQLite database, prior PostgreSQL schema, or
prior bucket as part of restore. Production NAS backup and restore rehearsal is
a separate deployment change because NAS is not available in this pre-NAS
implementation.

## Incident response

### S3 unavailable or corrupt

Stop ingestion and Projection publication. Preserve PostgreSQL and bucket
state, inspect the `storage` deep-health detail, and restore the missing exact
digest object from the matching backup set. Never replace an object with bytes
whose digest differs from its key, and never bypass reference admission.

### Migration, role, schema, or data-space mismatch

Keep the service stopped. Compare configuration with
`general_schema_migrations` and `general_storage_pin`. Do not edit the ledger or
pin to make startup pass. Correct the selected target or ship a reviewed
forward migration.

### Orphan collection failure

An audit failure is safe: the object remains. An S3 delete failure may occur
after the audit and metadata commit; retain the audit and candidate state,
repair S3 connectivity, and rerun collection. Do not bulk-delete bucket keys
outside the collector.
