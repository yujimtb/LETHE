# google-forms-adapter Specification

## Purpose
TBD - created by archiving change workspace-search-support. Update Purpose after archive.
## Requirements
### Requirement: GFORMS-01 Source Contract
Google Forms adapter は M09 Adapter Policy に従い、observer_id `obs:gforms-crawler`、source_system `sys:google-forms`、authority_model `source-authoritative`、capture_model `snapshot` で動作 SHALL する。

#### Scenario: adapter 登録
- **WHEN** Google Forms adapter が初期化される
- **THEN** M02 Registry に observer `obs:gforms-crawler` と source system `sys:google-forms` が登録される

### Requirement: GFORMS-02 Form 構造の取り込み
Google Forms adapter は Google Forms API を使用して Form の構造情報 (タイトル、説明、設問、URL、締切、対象者の記述) を取り込む SHALL する。schema は `schema:workspace-object-snapshot` を使用し、`artifact.service: "forms"`, `artifact.objectType: "form"` とする。

#### Scenario: Form 構造の Observation 生成
- **WHEN** Google Forms API から Form が取得される
- **THEN** タイトル、説明、設問一覧、Form URL、締切や対象者の記述が Observation に含まれる

### Requirement: GFORMS-03 回答事実の取り込み
Google Forms adapter は誰がいつ回答したかの事実を Observation として Lake に投入 SHALL する。この Observation は `schema:workspace-object-snapshot` 内で `objectType: "form-response-fact"` として区別する。

#### Scenario: 回答事実の記録
- **WHEN** Form に回答が提出される
- **THEN** 回答者の識別子、回答日時が `form-response-fact` Observation として Lake に投入される
- **AND** 個別回答内容はこの Observation に含まれない

### Requirement: GFORMS-04 回答内容の分離取り込み
Google Forms adapter は個別回答内容を別の Observation として Lake に投入 SHALL する。この Observation は `objectType: "form-response-content"` として区別し、Corpus Projection のフィルタ (CORPUS-04) で除外可能にする。

#### Scenario: 回答内容の Observation 生成
- **WHEN** Form の個別回答内容が取得される
- **THEN** `objectType: "form-response-content"` の Observation として Lake に投入される
- **AND** この Observation は CORPUS-04 により Corpus Projection から除外される

#### Scenario: 回答事実と回答内容の分離
- **WHEN** 同一回答に対して事実と内容が取り込まれる
- **THEN** `form-response-fact` と `form-response-content` は別の Observation として生成され、`object_id` の一部で関連付け可能だが、Corpus Projection では独立にフィルタ可能である

### Requirement: GFORMS-05 回答連携 Sheet の識別
Google Forms adapter は Form 回答の連携先 Sheet を識別し、メタデータに記録 SHALL する。これにより Corpus Projection が連携 Sheet を除外できる。

#### Scenario: 連携 Sheet ID の記録
- **WHEN** Form に回答連携先の Sheet が設定されている
- **THEN** その Sheet の ID が Form の Observation メタデータに記録される

### Requirement: GFORMS-06 idempotency key
Google Forms adapter は M09 Adapter Policy の idempotency 契約に従い identity_key を生成 SHALL する。Form 構造、回答事実、回答内容それぞれが独立した identity_key を持つ。

#### Scenario: Form 構造の冪等取り込み
- **WHEN** 同一 revision の Form 構造が再取り込みされる
- **THEN** identity_key が一致し、新規 Observation は生成されない

#### Scenario: 回答事実の冪等取り込み
- **WHEN** 同一回答者・同一 Form の回答事実が再取り込みされる
- **THEN** identity_key が一致し、新規 Observation は生成されない

