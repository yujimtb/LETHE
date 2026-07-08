# grep-api Specification

## Purpose
TBD - created by archiving change workspace-search-support. Update Purpose after archive.
## Requirements
### Requirement: GREP-01 正規表現 grep エンドポイント
LETHE は `POST /api/projections/{projection_id}/grep` エンドポイントを提供 SHALL する。このエンドポイントは Access Controlled Corpus Projection のレコードに対して正規表現パターンマッチを実行し、一致するレコードを返す。検索は ranking ではなく match 結果として返す。

#### Scenario: regex パターンによる検索
- **WHEN** `pattern: "落とし物|忘れ物|遺失物"` で grep API が呼び出される
- **THEN** パターンに一致するすべてのレコードが match 結果として返される

#### Scenario: 結果が ranking ではない
- **WHEN** grep API が結果を返す
- **THEN** 結果にスコアやランキングは含まれず、一致したレコードの全件が cursor pagination で取得可能である

### Requirement: GREP-02 NFKC 正規化
Grep API は NFKC 正規化済みテキストに対して検索 SHALL する。原文は引用や表示用に保持する。NFKC は Unicode Standard Annex #15 に定義された互換正規化形式であり、全角半角差を吸収する。

#### Scenario: 全角半角の吸収
- **WHEN** 検索対象に全角 `１２３` が含まれ、パターンが半角 `123` である
- **THEN** NFKC 正規化により一致し、結果に含まれる

#### Scenario: 原文の保持
- **WHEN** NFKC 正規化後のテキストに対してパターンが一致する
- **THEN** snippet には正規化前の原文が含まれる

### Requirement: GREP-03 線形時間正規表現エンジン
Grep API は Rust の regex crate 相当の線形時間で動作する正規表現エンジンを使用 SHALL する。backreference、look-around など ReDoS や非線形評価につながる機能は許可しない。

#### Scenario: OR 表現の許可
- **WHEN** `落とし物|忘れ物|遺失物` のような OR パターンが入力される
- **THEN** パターンは受け付けられ、いずれかに一致するレコードが返される

#### Scenario: backreference の拒否
- **WHEN** backreference を含むパターンが入力される
- **THEN** パターンはコンパイルエラーとして拒否される

### Requirement: GREP-04 Cursor Pagination
Grep API は cursor pagination によりすべての match 結果を取得可能に SHALL する。既定の limit は 100。

#### Scenario: 結果が limit を超える場合のページング
- **WHEN** match 件数が limit (既定 100) を超える
- **THEN** レスポンスに `next_cursor` が含まれ、次ページを取得できる

#### Scenario: 全件走破
- **WHEN** cursor を使って繰り返しリクエストする
- **THEN** `complete: true` になるまですべての match を取得できる

### Requirement: GREP-05 既定表示順
Grep API の既定の表示順は日付降順 (date_desc) と SHALL する。

#### Scenario: 日付降順での返却
- **WHEN** order パラメータを指定せずに検索する
- **THEN** 結果は timestamp の降順で返される

### Requirement: GREP-06 フィルタ
Grep API はソース種別 (types)、日時範囲 (from/to)、チャンネル (channels)、コンテナ (containers) によるフィルタを受け付ける SHALL する。

#### Scenario: ソース種別フィルタ
- **WHEN** `types: ["slack", "doc"]` が指定される
- **THEN** Slack メッセージと Google Docs のレコードのみが検索対象になる

#### Scenario: 日時範囲フィルタ
- **WHEN** `from: "2026-01-01T00:00:00Z"` と `to: "2026-06-30T23:59:59Z"` が指定される
- **THEN** その範囲内のレコードのみが検索対象になる

### Requirement: GREP-07 レスポンス形状
Grep API のレスポンスは以下のフィールドを含む SHALL する: `matches[]` (record_id, source_type, anchor_url, source_title, source_location, timestamp, snippet, matched_ranges, metadata)、`next_cursor`、`complete`、`projection_watermark`。snippet は省略記号を含めて最大 240 文字、`matched_ranges` は 1 レコードあたり最大 20 件に制限する。

#### Scenario: match レスポンスのフィールド
- **WHEN** grep 検索で match が見つかる
- **THEN** 各 match に record_id, source_type, anchor_url, source_title, source_location, timestamp, snippet, matched_ranges が含まれる

#### Scenario: projection_watermark の付与
- **WHEN** grep 検索結果が返される
- **THEN** レスポンスに projection_watermark が含まれ、同じ watermark では同じ入力に対して同じ結果が再現可能である

#### Scenario: snippet はヒット位置を含む
- **WHEN** 一致箇所が長い本文の先頭240文字より後ろにある
- **THEN** snippet は最初のヒット位置を中心にした窓を返し、前後が省略された場合は `...` を付ける

#### Scenario: レコード単位の応答サイズ上限
- **WHEN** 1 レコード内で同一 pattern が多数一致する
- **THEN** snippet は最大 240 文字、`matched_ranges` は最大 20 件に制限される

### Requirement: GREP-08 Trigram Index による高速化
実装上の高速化として trigram index を持ってよい SHALL する。ただし trigram index は候補絞り込みにのみ使用し、最終判定は regex の意味論とする。

#### Scenario: index による match 漏れの禁止
- **WHEN** trigram index で候補絞り込みが行われる
- **THEN** regex による最終判定で一致するすべてのレコードが返され、index によって match が欠落しない

#### Scenario: index なしと同一結果
- **WHEN** 同一パターンで検索する
- **THEN** trigram index の有無にかかわらず同一の match 集合が返される

### Requirement: GREP-09 regex 実行時間上限
Grep API は regex 実行時間に上限を設ける SHALL する。既定は 500ms。

#### Scenario: タイムアウト
- **WHEN** regex の実行が設定されたタイムアウトを超える
- **THEN** 実行が打ち切られ、エラーレスポンスが返される

### Requirement: GREP-10 複合語 AND 検索
Grep API は pattern を半角スペース・全角スペース・タブで複数 term に分割できる SHALL する。複数 term が存在する場合、term の順序や距離に関係なく、全 term に一致するレコードのみを返す。各 term は regex として解釈し、複合語検索中の不正 regex term はリテラル部分一致として扱う。単一 term の pattern は従来通り単一 regex として扱い、不正 regex はエラーとする。

#### Scenario: 複合語 AND
- **WHEN** `pattern: "Nanihold OS ロードマップ"` で grep API が呼び出される
- **THEN** `Nanihold`、`OS`、`ロードマップ` の全てを含むレコードだけが返る

#### Scenario: 全角スペース区切り
- **WHEN** `pattern: "Nanihold　ロードマップ"` で grep API が呼び出される
- **THEN** 半角スペースと同じく複数 term AND として検索される

### Requirement: GREP-11 get_record エンドポイント
LETHE は record_id から露出可能な全文または詳細を取得する API を提供 SHALL する。Corpus Projection に含まれないレコードはアクセス拒否する。

#### Scenario: 露出可能なレコードの取得
- **WHEN** Corpus Projection に含まれる record_id で get_record が呼び出される
- **THEN** レコードの全文と詳細が返される

#### Scenario: 非露出レコードのアクセス拒否
- **WHEN** Corpus Projection に含まれない record_id で get_record が呼び出される
- **THEN** 403 またはレコード不在として拒否される

### Requirement: GREP-12 get_thread エンドポイント
LETHE は Slack thread の文脈を取得する API を提供 SHALL する。Corpus Projection に含まれるメッセージのみ返す。

#### Scenario: スレッド文脈の取得
- **WHEN** parent permalink または thread_ts で get_thread が呼び出される
- **THEN** そのスレッド内の Corpus Projection に含まれるメッセージ一覧が返される

### Requirement: GREP-13 resolve_link エンドポイント
LETHE は Slack permalink、Drive URL、Docs URL などの外部 URL を LETHE record_id に解決する API を提供 SHALL する。

#### Scenario: Slack permalink の解決
- **WHEN** Slack permalink で resolve_link が呼び出される
- **THEN** 対応する Corpus Projection 内の record_id が返される

#### Scenario: Google Drive URL の解決
- **WHEN** Google Docs/Sheets/Forms/Slides/Drive の URL で resolve_link が呼び出される
- **THEN** 対応する Corpus Projection 内の record_id が返される
