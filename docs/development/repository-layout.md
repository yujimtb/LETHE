# Repository Layout

## Ownership rule

workspace memberは自身の`src/`配下だけをコンパイルします。別ディレクトリのsourceを
`#[path]`や`include!`で共有してはいけません。workspace rootはpackageを持ちません。

## Dependency direction

```text
core
├─ policy
├─ registry
├─ storage-api
├─ engine ── policy, registry, storage-api
├─ runtime
├─ adapter-api
└─ profile-model

api ── core, engine
projection-person ── core, policy, engine, profile-model
adapter-* ── core, adapter-api
derivation-gemini ── core, engine, adapter-api, profile-model
storage-sqlite ── core, runtime
search-index ── api, core, projection-corpus, storage-api
selfhost ── composition root
tools / e2e ── public crate interfaces
```

依存方向は`scripts/check_dependency_layers.ps1`で検証します。

## Module size

大きな実装は責務別submoduleへ分割します。

- selfhost app: bootstrap/auth、sync workflow、Projection API、
  persistence/ingestion support、検索index lifecycle、固定high-waterの二段ページング
  materialization、supplemental atomic delta、media support、slide scoring
- search-index: Tantivy schema/query、`record_id`単位の冪等upsert、canonical tailの
  incremental catch-up、世代のbuild/検証/publish/retire
- SQLite storage: persistence API、schema initialization、tests

## Runtime data

`data/`と`target/`はsourceではありません。credential、SQLite、blob、手動取得資料は
Git管理対象外です。

永続corpus検索indexは必須設定`corpus.index_dir`配下に置きます。標準の配置は
次のとおりです。

```text
<corpus.index_dir>/
├─ CURRENT
└─ generations/
   └─ <UUIDv7>/
      └─ Tantivy index files
```

`CURRENT`は検証済みの公開世代を指します。selfhostは起動時にその世代をdiskから
openし、canonical SQLiteの未反映tailだけをincremental catch-upします。初回、schema
またはcorpus設定fingerprintの不一致、checksum不一致などの破損時だけ新世代を
background rebuildし、検証後に`CURRENT`を原子的に切り替えます。切替前の世代は
in-flight readerが解放されてからretireします。`opening`、`catching_up`、`rebuilding`、
`failed`の間に検索結果を推測して返してはいけません。HTTP/MCPは明示的な検索index
unavailable errorでfail-fastします。

corpus検索はこの永続Tantivy indexとヒットしたstored recordだけを読み、全
Observation/Corpusを常駐させたりrequestごとにtrigram indexを構築したりしません。
非corpus projectionのfull materializationはcanonical SQLiteの固定high-waterを二段で
ページ走査し、message/reply-SLO itemをSQLite staging projectionへ逐次書き込みます。
完成したmanifestと件数を検証した後だけtarget projectionへ単一transactionでpublish
します。supplemental writeはsupplemental append、厳密なprojection item delta、manifest
更新を同じSQLite transactionでcommitします。
