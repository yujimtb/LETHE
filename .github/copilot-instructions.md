# LETHE Project Instructions

## Project

LETHE は、外部 source から Observation を取り込み、append-only Lake、
Projection、Governance を通じて再現可能な知識 view を構築する Rust workspace
です。workspace root は virtual manifest で、実装は `apps/` と `crates/` の各
workspace member が所有します。

## Authoritative documents

- `openspec/specs/_index.md`: module index と依存関係
- `docs/architecture/system-overview.md`: システム全体像
- `docs/architecture/domain-algebra.md`: 型、law、failure model
- `docs/architecture/governance-capability-model.md`: consent、access、capability
- `docs/architecture/runtime-reference.md`: runtime topology
- `docs/architecture/sharding.md`: sharding の確定設計
- `docs/decisions/adr-backlog.md`: 未確定の設計判断
- `docs/development/repository-layout.md`: repository ownership と依存方向

## Required laws

- Append-Only Law
- Replay Law
- Effect Isolation Law
- Explicit Authority Law
- No Direct Mutation Law
- Filtering-before-Exposure Law
- Idempotency Law
- Provenance Completeness Law

## Repository rules

- root `src/` を作らない。各 crate は自身の `src/` だけをコンパイルする。
- `#[path]`、`include!` による crate 外 source の取り込みを行わない。
- 依存方向は `scripts/check_dependency_layers.ps1` に従う。
- 互換 layer、alias、silent fallback を追加しない。安全に継続できない場合は
  明示的な error を返す。
- API は `/api/projections/{projection_id}/records...` を正規 route とし、
  domain 固有 alias を追加しない。
- 実装と test が完了したら、関連する OpenSpec と `docs/` を同じ変更で更新する。

## Verification

```powershell
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
./scripts/check_dependency_layers.ps1
python ./scripts/check_markdown_links.py
python ./scripts/public_release_audit.py
```

エージェントロールの詳細は `docs/development/agents/` を参照してください。
