# corpus-projection Specification

## Purpose
TBD - created by archiving change workspace-search-support. Update Purpose after archive.
## Requirements
### Requirement: CORPUS-01 共通コーパス Projection
Access Controlled Corpus Projection は M05 Projection Engine の仕組みに従い、Lake の Observation から Bot に露出してよいレコードのみを生成 SHALL する。MVP では共通コーパスとし、質問者ごとの可視範囲分離は行わない。

#### Scenario: Projection 生成
- **WHEN** Projection Engine が Corpus Projection の build を実行する
- **THEN** フィルタルール (CORPUS-02〜CORPUS-06) を満たす Observation のみがコーパスに含まれる
- **AND** フィルタルールを満たさない Observation は Bot に渡らない

#### Scenario: watermark 増分更新
- **WHEN** 新しい Observation が Lake に append される
- **THEN** Corpus Projection は M06 DAG Propagation の watermark 増分更新で差分反映される

### Requirement: CORPUS-02 Slack チャンネルフィルタ
Slack メッセージは以下の条件をすべて満たす場合のみコーパスに露出 SHALL する: `is_public_channel == true` AND `channel_name matches ^\d{3}_` AND `author is not bot` AND `author is not opted_out_person`。

#### Scenario: 命名規則一致チャンネルの露出
- **WHEN** public channel `123_event` のメッセージが Lake に存在する
- **THEN** Corpus Projection に含まれる

#### Scenario: 命名規則不一致チャンネルの除外
- **WHEN** public channel `general` のメッセージが Lake に存在する
- **THEN** Corpus Projection に含まれない

#### Scenario: Bot 投稿の除外
- **WHEN** author が bot のメッセージが Lake に存在する
- **THEN** Corpus Projection に含まれない

#### Scenario: opt-out 人物の投稿除外
- **WHEN** author が opt-out 登録された人物のメッセージが Lake に存在する
- **THEN** Corpus Projection に含まれない

#### Scenario: 個人チャンネルの原則除外
- **WHEN** 個人が立てたチャンネルで opt-in 設定がない
- **THEN** そのチャンネルのメッセージは Corpus Projection に含まれない

#### Scenario: opt-in された個人チャンネルの露出
- **WHEN** 個人チャンネルが `channel_opt_in` リストに含まれている
- **THEN** そのチャンネルのメッセージは Corpus Projection に含まれる

### Requirement: CORPUS-03 Drive ファイルフィルタ
Drive file は以下の条件をすべて満たす場合のみコーパスに露出 SHALL する: `file is under allowed_folder` AND `file sharing level satisfies broad_visibility_threshold` AND `owner/author is not opted_out_person` AND `file is not explicitly excluded`。

#### Scenario: allowlist 配下かつ共有閾値を満たすファイルの露出
- **WHEN** ファイルが allowed_folder 配下にあり、共有レベルが broad_visibility_threshold を満たす
- **THEN** Corpus Projection に含まれる

#### Scenario: allowlist 外のファイルの除外
- **WHEN** ファイルが allowed_folder 配下にない
- **THEN** Corpus Projection に含まれない

#### Scenario: 共有閾値を満たさないファイルの除外
- **WHEN** ファイルが allowed_folder 配下だが共有レベルが broad_visibility_threshold を満たさない
- **THEN** Corpus Projection に含まれない (個人ファイル誤配置に対する二重防御)

#### Scenario: opt-out 人物の Drive ファイル除外
- **WHEN** ファイルの owner/author が opt-out 登録された人物である
- **THEN** Corpus Projection に含まれない

### Requirement: CORPUS-04 Form 回答内容の非露出
Form の個別回答内容はコーパスに露出しない SHALL とする。Form 構造、設問、URL、締切、回答した事実 (誰がいつ) は露出する。

#### Scenario: Form 構造の露出
- **WHEN** Form の Observation (構造、設問、URL) が Lake に存在する
- **THEN** Corpus Projection に含まれる

#### Scenario: 回答事実の露出
- **WHEN** Form に誰がいつ回答したかの Observation が Lake に存在する
- **THEN** Corpus Projection に含まれる

#### Scenario: 個別回答内容の非露出
- **WHEN** Form の個別回答内容の Observation が Lake に存在する
- **THEN** Corpus Projection に含まれない

#### Scenario: Form 回答連携 Sheet の除外
- **WHEN** Form 回答が連携先 Sheet として Lake に存在する
- **THEN** その Sheet は Corpus Projection から明示的に除外される

### Requirement: CORPUS-05 コーパスレコード粒度
コーパスのレコードはソース種別ごとに自然な引用単位で生成 SHALL する。各レコードは anchor URL を持つ。

#### Scenario: Slack メッセージのレコード粒度
- **WHEN** Slack メッセージが Corpus Projection に含まれる
- **THEN** 1メッセージ = 1レコードで生成され、anchor は Slack permalink (channel, ts, thread_ts) を含む

#### Scenario: Docs のレコード粒度
- **WHEN** Google Docs が Corpus Projection に含まれる
- **THEN** 見出しセクション単位の chunk で生成され、anchor は Doc URL (可能なら heading anchor) を含む

#### Scenario: Sheets のレコード粒度
- **WHEN** Google Sheets が Corpus Projection に含まれる
- **THEN** 1行 = 1レコードで生成され、anchor は Sheet URL, sheet name, row number を含む

#### Scenario: Forms のレコード粒度
- **WHEN** Google Forms が Corpus Projection に含まれる
- **THEN** Form 構造 1件 + 提出イベント 1件で生成され、anchor は Form URL を含む

#### Scenario: Slides のレコード粒度
- **WHEN** Google Slides が Corpus Projection に含まれる
- **THEN** slide または text block 単位で生成され、anchor は Slides URL + slide id を含む

### Requirement: CORPUS-06 Filtering-before-Exposure 準拠
Corpus Projection は M08 Governance の Filtering-before-Exposure Law に準拠 SHALL する。Bot がレコードを受け取ってから除外するのではなく、Projection 生成時にフィルタリングする。

#### Scenario: 除外レコードが API 経由で返されない
- **WHEN** Grep API または get_record API が Corpus Projection を検索する
- **THEN** CORPUS-02〜04 で除外されたレコードは結果に含まれない
- **AND** Bot は除外されたレコードの存在を知ることができない

