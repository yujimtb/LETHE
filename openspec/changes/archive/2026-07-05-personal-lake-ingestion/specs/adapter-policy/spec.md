# Spec Delta: adapter-policy

**Change:** personal-lake-ingestion
**Version:** 0.1 (draft)
**Date:** 2026-07-04

## Dependencies

- M09 Adapter Policy — SHARD-ADAPT-01(canonical tuple 宣言契約)に新規 mapper が準拠する。既存契約は**不変**
- M03 Observation Lake — exact idempotency / identity_key 構成(source : object_id : H(canonical_content))
- 正典: 本 change design.md P4 / P8 / P9 / P10 / P12、proposal.md 境界原理

---

## ADDED Requirements

### Requirement: PING-GH-01 GitHub Object Mapping Contract

`lethe-import-github` の mapper は、dump 済み JSON に対する**純関数**として、以下の object_id 規約で canonical tuple を宣言しなければならない (SHALL)。source は `github` 固定。

| オブジェクト | object_id | canonical content | published |
| --- | --- | --- | --- |
| issue 本文 | `{repo}#issue#{n}` | title + body(U2 正規化) | created_at |
| issue コメント | `{repo}#issue_comment#{id}` | body | created_at |
| PR 本文 | `{repo}#pr#{n}` | title + body | created_at |
| PR review | `{repo}#pr_review#{review_id}` | state + body | submitted_at |
| PR review コメント | `{repo}#pr_review_comment#{id}` | body + anchor(path, line, anchor_sha) | created_at |
| commit | `{repo}#commit#{sha}` | message + author + author_date + changed_files | author_date |
| timeline event | `{repo}#issue_event#{event_key}` | event_type + actor/author raw attribution + 付随フィールド | created_at。GitHub が `committed` timeline event を `sha` / `author.date` で返す場合は author date |

#### Scenario: 再 dump の完全 no-op

- **WHEN** 同一リポジトリを再 dump して再 import する
- **THEN** 編集のないオブジェクトはすべて `Duplicate`、本文編集のあったオブジェクトのみ新 distinct Observation となる (D3b)

#### Scenario: commit の歴史書き換え

- **WHEN** rebase / force-push により sha が変わった commit を再 dump する
- **THEN** 新 sha は新 Observation として append され、旧 sha の Observation は不変に残る(出来事スパインの保存)

### Requirement: PING-GH-02 Diff 非含有

mapper は commit / PR の diff・patch 内容を canonical content にも payload にも含めてはならない (SHALL NOT)。内容参照は sha(content address)で表現する。Projection が内容を要する場合は ADR-002 source-native read で解決する。

#### Scenario: commit payload に diff を含めない

- **WHEN** commit dump に file-level patch または diff 相当フィールドが含まれる
- **THEN** mapper は該当フィールドを canonical content と payload の双方から除外し、sha と変更ファイル一覧のみを Observation に含める

### Requirement: PING-GH-03 無差別取り込み

fetch スクリプトおよび mapper は、オブジェクト型・イベント型・リポジトリのホワイトリスト/選別を行ってはならない (SHALL NOT)。解釈は Projection 層の責務である。

#### Scenario: unknown timeline event の保持

- **WHEN** dump に既知の event_type ではない timeline event が含まれる
- **THEN** mapper は event_type と付随フィールドを保持した Observation を生成し、event_type による除外を行わない

#### Scenario: committed timeline event の native key

- **WHEN** GitHub timeline API が `committed` event を numeric `id` / `created_at` / `actor` なしで返す
- **THEN** mapper は `sha` を `event_key`、`author.date` を `published`、author の raw attribution を canonical content に使い、event を除外しない

### Requirement: PING-CL-01 claude.ai Import 配線

`lethe-import-claude` は実装済み `ClaudeAiImporter::import_zip` を IngestionGate 経由で lake に配線する。新規 mapping ロジックを追加してはならない (SHALL NOT)。

#### Scenario: 実データ失敗の U3 還流

- **WHEN** 実 export データで importer が失敗またはID導出が非決定になる
- **THEN** 該当ケースを U3 検証 issue の property test ケースとして記録してから修正する

### Requirement: PING-AR-01 Source Archive 規律

claude.ai export は private git リポジトリに会話単位ファイルへ展開(純関数)して commit しなければならない (SHALL)。archive repo の読者は ingest CLI のみとし、Projection・API・取り出し口が archive repo を参照してはならない (SHALL NOT)。GitHub dump は scratch とし、アーカイブしてはならない (SHALL NOT)。

#### Scenario: claude archive と GitHub scratch の分離

- **WHEN** claude.ai export と GitHub dump を同じ個人 lake に取り込む
- **THEN** claude.ai export は private source archive に会話単位で保存され、GitHub dump は gitignore された scratch ディレクトリにのみ置かれる
