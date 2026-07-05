# Spec Delta: drive-file-adapter

**Change:** workspace-search-support
**Module:** (new) drive-file-adapter
**Scope:** Google Drive file source adapter — allowlist フォルダ配下の汎用ファイル
**Dependencies:** M01 Domain Kernel, M02 Registry, M03 Observation Lake, M09 Adapter Policy, M11 Google Slides Adapter (schema 参照)
**Agent:** Spec Designer (capture 仕様) → Implementer (API client + adapter) → Reviewer (snapshot 検証)

---

## ADDED Requirements

### Requirement: GDRIVE-01 Source Contract
Drive file adapter は M09 Adapter Policy に従い、observer_id `obs:gdrive-crawler`、source_system `sys:google-drive`、schema `schema:workspace-object-snapshot`、authority_model `source-authoritative`、capture_model `snapshot` で動作 SHALL する。

#### Scenario: adapter 登録
- **WHEN** Drive file adapter が初期化される
- **THEN** M02 Registry に observer `obs:gdrive-crawler` と source system `sys:google-drive` が登録される

### Requirement: GDRIVE-02 allowlist フォルダの巡回
Drive file adapter は Google Drive API のファイル検索と一覧取得を使用し、指定フォルダ allowlist を起点にファイルを巡回 SHALL する。

#### Scenario: allowlist フォルダ配下の巡回
- **WHEN** クロールが実行される
- **THEN** `allowed_folder_ids` に指定されたフォルダ配下のファイルが再帰的に巡回される

#### Scenario: allowlist 外の無視
- **WHEN** ファイルが allowlist に含まれないフォルダにある
- **THEN** そのファイルは巡回対象にならない

### Requirement: GDRIVE-03 workspace-object-snapshot schema の利用
Drive file adapter は `schema:workspace-object-snapshot` を使用 SHALL する。artifact フィールドの `service` は `"drive"`、`objectType` はファイルの MIME type に基づく値とする。

#### Scenario: Drive file の Observation 生成
- **WHEN** Drive のファイルが取得される
- **THEN** `schema:workspace-object-snapshot` に従い、`artifact.service: "drive"`, `artifact.objectType: <mime-based>`, `artifact.canonicalUri: <drive_url>` を含む Observation が生成される

### Requirement: GDRIVE-04 ファイル本文の取得
Drive file adapter は検索可能なファイルの本文とメタデータを取得 SHALL する。Google Drive API の export 機能を使用してテキスト抽出可能な形式で取得する。

#### Scenario: テキスト抽出可能ファイルの取得
- **WHEN** テキスト抽出可能なファイル (text/plain, application/pdf 等) が見つかる
- **THEN** ファイル本文がテキストとして抽出され、メタデータとともに Observation に含まれる

#### Scenario: テキスト抽出不能ファイル
- **WHEN** テキスト抽出が不可能なファイル (画像、動画等) が見つかる
- **THEN** メタデータのみの Observation が生成される (MVP では OCR は対象外)

### Requirement: GDRIVE-05 revision ベース差分取り込み
Drive file adapter は revision 情報を使用し、変更がある場合のみ新しい Observation を生成 SHALL する。

#### Scenario: 未変更ファイルのスキップ
- **WHEN** 前回クロール以降にファイルの revision が変わっていない
- **THEN** 新しい Observation は生成されない

### Requirement: GDRIVE-06 クロール間隔
Drive file adapter の既定クロール間隔は日次と SHALL する。設定変更で短縮可能とする。

#### Scenario: 日次クロール
- **WHEN** 既定設定でクロールが実行される
- **THEN** 日次の間隔で実行される

#### Scenario: 間隔短縮
- **WHEN** 管理者がクロール間隔を短縮する
- **THEN** 変更後の間隔でクロールが実行される

### Requirement: GDRIVE-07 idempotency key
Drive file adapter は M09 Adapter Policy の idempotency 契約に従い identity_key を生成 SHALL する。

#### Scenario: 同一内容の再取り込みで重複しない
- **WHEN** 同一 revision のファイルが再度取り込まれる
- **THEN** identity_key が一致し、新規 Observation は生成されない

### Requirement: GDRIVE-08 共有レベルの記録
Drive file adapter は各ファイルの共有レベル (domain, anyone-with-link, specific users 等) を Observation のメタデータに記録 SHALL する。これにより Corpus Projection が broad_visibility_threshold のフィルタに使用できる。

#### Scenario: 共有レベルのメタデータ記録
- **WHEN** ファイルの Observation が生成される
- **THEN** ファイルの共有レベルが metadata に含まれる
