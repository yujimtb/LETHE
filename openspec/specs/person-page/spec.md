# person-page Specification

## Purpose
TBD - created by archiving change generalize-platform. Update Purpose after archive.
## Requirements
### Requirement: Person Page Placement (Core-External)

Person Page は `lethe-projection-person` crate としてコア外に配置され **なければならない (SHALL)**。コア crate(`lethe-core` / `lethe-lake` / `lethe-api` 等)は本モジュールに依存 **してはならない (SHALL NOT)**。依存方向は `lethe-projection-person → lethe-core` の一方向とする。M13 の Projection 契約・出力テーブル・API 契約そのものは変更しない。

#### Scenario: コア crate から person-page への依存禁止

- **WHEN** CI の依存方向検査(`cargo deny` / custom check)を実行する
- **THEN** `lethe-core` / `lethe-lake` / `lethe-api` から
  `lethe-projection-person` への依存が存在しないことが確認される
- **AND** `lethe-projection-person → lethe-core` の一方向依存のみが
  許容される

#### Scenario: person crate 除外ビルド

- **WHEN** workspace から `lethe-projection-person` を除外してビルドする
- **THEN** コア crate はビルド・テストとも成功し、Person Page 由来の
  API ルートのみが利用不可になる(コアの他機能は影響を受けない)

