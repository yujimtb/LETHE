# Spec Delta: person-page

**Change:** generalize-platform
**Version:** 0.1 (draft)
**Date:** 2026-06-13

## Dependencies

- M01 Domain Kernel — 型・law の正規参照
- M13 Person Page — 既存契約(`openspec/specs/person-page.md`)は不変

> 本 delta は M13 の **契約(Projection spec / output tables / API 契約)を
> 変更しない**。コア外への配置と依存方向のみを規定する placement 要件を
> 追加する。元の提案では「MODIFIED: M13 を降格」と表現されているが、
> 既存 `person-page.md` は `### Requirement:` 形式を持たない narrative spec
> のため、新規の placement 要件として **ADDED** で表現する。

---

## ADDED Requirements

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
