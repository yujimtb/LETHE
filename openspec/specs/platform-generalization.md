# M16 Platform Generalization

## Scope

LETHE を特定ドメイン固定の単一 crate から、Cargo workspace と effect port に分割された汎用データ基盤へ一般化する。

## Requirements

- コア層は特定ドメインの Entity Type・Schema・route 名をコンパイル時に必須としてはならない。
- 実装は `lethe-core`, `lethe-policy`, `lethe-storage-api`, `lethe-storage-sqlite`, `lethe-adapter-*`, `lethe-runtime`, `lethe-selfhost`, `lethe-projection-person` の workspace 境界を持つ。
- 依存方向は `scripts/check_dependency_layers.ps1` で検査する。
- storage effect は `ObservationStore`, `BlobStorePort`, `SupplementalStorePort`, `ProjectionMaterializer` trait を通る。
- blob 参照は `blob:sha256:{hex}` の content-addressing とする。
- source adapter は `SourceAdapter` / writeback trait と宣言的 `AdapterConfig` を contract とする。
- derivation provider は `DerivationProvider` trait と lineage metadata を持つ。
- identity claim は email 固定ではなく、`IdentifierType` と `IdentityResolutionStrategy` で構成可能な claim 種別として扱う。
- selfhost config は source instance 配列へ正規化され、secret は `credential_ref` で参照する。

## Verification

- `cargo test --workspace`
- `powershell -ExecutionPolicy Bypass -File scripts/check_dependency_layers.ps1`
