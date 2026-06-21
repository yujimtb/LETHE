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
selfhost ── composition root
tools / e2e ── public crate interfaces
```

依存方向は`scripts/check_dependency_layers.ps1`で検証します。

## Module size

大きな実装は責務別submoduleへ分割します。

- selfhost app: bootstrap/auth、sync workflow、Notion workflow、Projection API、
  persistence/ingestion support、media/write-back support、slide scoring
- Notion adapter: HTTP client、media mapping、page block construction
- SQLite storage: persistence API、schema initialization、tests

## Runtime data

`data/`と`target/`はsourceではありません。credential、SQLite、blob、手動取得資料は
Git管理対象外です。
