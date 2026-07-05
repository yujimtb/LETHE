# Spec Delta: supplemental-kind-registry

**Change:** supplemental-write-and-mcp-read
**Module:** M02 Registry(拡張)
**Scope:** Supplemental の kind ごとの payload スキーマ登録・検証機構と初期 6 種の登録
**Dependencies:** M01 Domain Kernel, M02 Registry, M04 Supplemental Store
**Agent:** Spec Designer(スキーマ定義)→ Implementer(Registry 拡張+検証関数)→ Reviewer(バージョン規則検証)

---

## ADDED Requirements

### Requirement: SKIND-01 Supplemental Kind Schema の登録

Registry は ObservationSchema と並行な `SupplementalKindSchema` を保持 SHALL する。各エントリは kind 名、セマンティックバージョン、JSON Schema(payload の必須・任意フィールド定義)を持つ。バージョン規則は既存 Schema Registry と同一(任意フィールド追加 = minor、必須フィールド追加・削除 = major)。

#### Scenario: kind スキーマの登録と参照
- **WHEN** `claim@1` の kind スキーマが Registry に登録されている
- **THEN** 書き込み検証パスは kind 名 `claim` とバージョン `1` で当該 JSON Schema を取得できる

#### Scenario: バージョン規則違反の拒否
- **WHEN** 既存 `claim@1` に必須フィールドを追加した定義を `claim@1` のまま再登録しようとする
- **THEN** 登録は拒否され、major 昇版(`claim@2`)が要求される

### Requirement: SKIND-02 payload 検証関数

登録済み kind に対する payload の検証は純関数として提供 SHALL する(Functional Core)。検証は JSON Schema 準拠判定であり、失敗時は違反フィールドの列挙を返す。

#### Scenario: 必須フィールド欠落の検出
- **WHEN** verification_mode を欠いた payload を kind `claim` として検証する
- **THEN** 検証は失敗し、欠落フィールド `verification_mode` が特定される

### Requirement: SKIND-03 未登録 kind の扱い(個人 lake 設定)

インスタンス設定 `supplemental.reject_unregistered_kinds`(bool)を導入 SHALL する。true の場合、未登録 kind の supplemental 書き込みは拒否される。個人 lake インスタンスの設定値は true。

#### Scenario: 未登録 kind の拒否
- **WHEN** reject_unregistered_kinds = true のインスタンスに kind `random-note` を書き込む
- **THEN** 書き込みは検証エラーで拒否される

### Requirement: SKIND-04 初期 kind スキーマ 6 種

以下を初期登録 SHALL する(必須/任意は design.md D9 の全文定義に従う):

| kind | 必須フィールド | 任意フィールド |
|------|--------------|--------------|
| `claim@1` | statement, verification_mode(enum: check, generate) | context, source_quote |
| `decision@1` | statement | rationale, alternatives, supersedes |
| `parking@1` | statement, resume_context | — |
| `verification-result@1` | verdict(enum: consistent, inconsistent, inconclusive), reasoning | — |
| `claim-transition@1` | to_state(enum: open, dispatched, verified, refuted, inconclusive, terminated, parked) | reason |
| `session-summary@1` | summary | topics |

#### Scenario: parking の再開文脈必須
- **WHEN** resume_context を欠いた payload を kind `parking` として書き込む
- **THEN** 書き込みは拒否される(終端プロトコル上、再開最小文脈のないパーキングは存在しない)

#### Scenario: verification-result / claim-transition のアンカー種別
- **WHEN** `verification-result@1` または `claim-transition@1` を書き込む
- **THEN** derivedFrom.supplementals に対象 claim の supplemental ID が少なくとも一つ含まれることが検証される

## Invariants(継承)

- Append-Only Law: スキーマ登録は追記(新バージョン追加)であり、既存バージョンの破壊的変更は不可
- Replay Law: 同一 payload×同一スキーマバージョン → 同一検証結果(純関数)

## Failure Modes

- `KindNotRegistered`(未登録 kind、reject 設定時)
- `PayloadSchemaViolation { violations: Vec<FieldViolation> }`
- `SchemaVersionRuleViolation`(登録時のバージョン規則違反)
