# Track A Handoff: Supplemental Kind Registry

## 実装した内容

- `SupplementalKindSchema` / `SupplementalKindVersion` / `SupplementalKindValidationConfig` / `FieldViolation` / `SupplementalKindError` を追加した。
- `RegistryStore` に supplemental kind schema の登録・取得・一覧・version 履歴・payload 検証 API を追加した。
- version rule は既存 Schema Registry と同じ SemVer 遷移で検証する。
  - optional field 追加は minor 以上で許可。
  - required field 追加・削除、field 削除、既存 field schema 変更は major bump 必須。
  - 同一 kind 全体の最新 SemVer と比較し、major key をまたぐ順序逆転も拒否する。
- `validate_supplemental_payload` を純関数として実装した。
  - JSON Schema meta validation と payload validation を実行する。
  - `iter_errors` で全違反を収集し、`FieldViolation` に違反 field / keyword / message を列挙する。
- 初期 6 kind を `base_supplemental_kind_schemas()` として定義し、selfhost の registry seed で登録した。
  - `claim@1`
  - `decision@1`
  - `parking@1`
  - `verification-result@1`
  - `claim-transition@1`
  - `session-summary@1`
- `verification-result@1` / `claim-transition@1` について、`derivedFrom.supplementals` に少なくとも 1 件の `claim@1` supplemental ID が含まれる検証を追加した。
- `supplemental.reject_unregistered_kinds` 設定を追加し、個人 lake 設定値を `true` にした。
- Track A の tasks を完了状態に更新した。

## 変更ファイル一覧

- `crates/registry/src/registry/supplemental_kind.rs`
- `crates/registry/src/registry/store.rs`
- `crates/registry/src/registry/mod.rs`
- `crates/registry/src/registry/schema.rs`
- `apps/selfhost/src/self_host/registry.rs`
- `apps/selfhost/src/self_host/config.rs`
- `apps/selfhost/src/self_host/app/tests.rs`
- `tests/e2e/tests/self_host_api.rs`
- `config.example.toml`
- `deploy/personal-lake/config.toml`
- `deploy/personal-lake/config.host.toml`
- `scripts/personal_lake_pipeline_smoke.py`
- `README.md`
- `docs/development/personal-lake-ingestion.md`
- `openspec/changes/supplemental-write-and-mcp-read/specs/supplemental-kind-registry/spec.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-a.md`

## 実行したテストと結果

- `cargo fmt --all -- --check`: PASS
- `cargo test -p lethe-registry`: PASS, 19 tests
- `cargo check -p lethe-selfhost`: PASS
- `cargo test -p lethe-selfhost self_host::config`: PASS, 5 tests
- `cargo test -p lethe-selfhost`: PASS, 27 tests

Track A に外部実機でしか確認できない項目はない。

## Track B が呼ぶべき公開 API

型:

- `lethe_registry::registry::SupplementalKindSchema`
- `lethe_registry::registry::SupplementalKindVersion`
- `lethe_registry::registry::SupplementalKindValidationConfig`
- `lethe_registry::registry::FieldViolation`
- `lethe_registry::registry::SupplementalKindError`
- `lethe_registry::registry::SupplementalKindRef`

関数・method:

- `base_supplemental_kind_schemas()`
- `parse_supplemental_kind_ref(kind_ref: &str)`
- `validate_supplemental_payload(schema: &SupplementalKindSchema, payload: &serde_json::Value)`
- `validate_supplemental_record_claim_anchor(record: &SupplementalRecord, lookup)`
- `RegistryStore::register_supplemental_kind_schema(schema)`
- `RegistryStore::get_supplemental_kind_schema(kind, major_version)`
- `RegistryStore::get_supplemental_kind_versions(kind)`
- `RegistryStore::list_supplemental_kind_schemas()`
- `RegistryStore::validate_supplemental_payload_for_kind(config, kind, major_version, payload)`
- `RegistryStore::validate_supplemental_record_kind(config, record, lookup)`

Track B の書き込み path では、Store insert 前に `RegistryStore::validate_supplemental_record_kind` を呼ぶこと。`SupplementalRecord.kind` は `claim@1` のような `kind@major` 形式で渡す。`verification-result@1` / `claim-transition@1` では、lookup closure が既存 supplemental ID からその record kind を返す必要がある。少なくとも 1 件が `claim@1` でなければ `MissingClaimSupplementalAnchor` を返す。

エラー型:

- `KindNotRegistered`
- `UnregisteredKindPolicyDisabled`
- `InvalidKindRef`
- `InvalidJsonSchema`
- `PayloadSchemaViolation`
- `SchemaVersionRuleViolation`
- `MissingClaimSupplementalAnchor`

未登録 kind は `reject_unregistered_kinds = false` でも silent accept しない。false の場合は `UnregisteredKindPolicyDisabled` で fail-fast する。

## 未完了または統合担当への引き継ぎ

- Track A 自身の未完了事項はない。
- Track B は supplemental write handler で `RegistryStore::validate_supplemental_record_kind` を必ず Store insert 前に呼ぶこと。
- Track I は `POST /supplementals` から `claim@1` / `claim-transition@1` / `verification-result@1` を通し、payload 違反・未登録 kind・claim anchor 欠落が HTTP contract に正しく写像されることを E2E で確認すること。
- Track A ではスタブを追加していない。

## 仕様 SHALL と evidence 対応

- SKIND-01: `SupplementalKindSchema` と Registry 登録・取得・version rule を実装。
  - Evidence: `crates/registry/src/registry/supplemental_kind.rs`, `crates/registry/src/registry/store.rs`
  - Tests: `supplemental_kind_same_major_rejects_required_addition`, `supplemental_kind_same_major_rejects_required_removal`, `supplemental_kind_minor_allows_optional_field_addition`, `supplemental_kind_rejects_version_order_regression_across_major_keys`
- SKIND-02: payload JSON Schema 検証と違反 field 列挙を実装。
  - Evidence: `validate_supplemental_payload`, `FieldViolation`
  - Tests: `payload_violation_fields_include_required_type_and_enum`, `payload_violation_fields_include_missing_required_field`, `supplemental_payload_detects_required_type_and_enum_violations`
- SKIND-03: 初期 6 kind schema を登録。
  - Evidence: `base_supplemental_kind_schemas()`, `seed_registry()`
  - Tests: `supplemental_kind_register_and_get_by_kind_and_major_version`, `parking_without_resume_context_is_rejected`
- SKIND-04: `supplemental.reject_unregistered_kinds` を導入し、個人 lake 設定を true にした。
  - Evidence: `apps/selfhost/src/self_host/config.rs`, `deploy/personal-lake/config.toml`, `deploy/personal-lake/config.host.toml`, `config.example.toml`
  - Tests: `unregistered_supplemental_kind_is_rejected`, `cargo test -p lethe-selfhost self_host::config`
- 対象 claim anchor rule: `verification-result@1` / `claim-transition@1` が `derivedFrom.supplementals` 内の `claim@1` を要求する。
  - Evidence: `validate_supplemental_record_claim_anchor`, `RegistryStore::validate_supplemental_record_kind`
  - Tests: `claim_transition_requires_claim_supplemental_anchor`
