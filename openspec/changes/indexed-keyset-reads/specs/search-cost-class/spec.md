## ADDED Requirements

### Requirement: SCC-01 exact / metadata 検索の専用索引経路

exact metadata / object-id 検索は、任意 regex 全文書走査とは別の専用 API・index 経路で O(postings + candidate) 提供 SHALL する。exact 検索を全 document scan + regex 判定へ落とすこと、および ID 回収を grep の固定 500ms timeout へ依存させることは SHALL NOT する。

#### Scenario: exact metadata 検索が索引経路
- **WHEN** exact metadata または object-id で検索する
- **THEN** 専用 index 経路で O(postings + candidate) で返す
- **AND** 全 document scan + regex 判定へ落とさない

#### Scenario: ID 回収が exact 経路で成立する
- **WHEN** クライアントが取り込んだ Observation の ID を回収する
- **THEN** exact 検索経路で回収でき、grep の 500ms timeout に依存しない

### Requirement: SCC-02 任意 regex は非同期 search job

safe literal n-gram を抽出できない任意 regex 検索は、通常の同期検索経路から分離した**非同期 search job** として実行 SHALL する。固定 500ms timeout で全 document を同期走査する経路を、通常 exact 検索と同一 SLO で提供 SHALL NOT する。同一プロセス内の低優先同期実行は資源の食い合いで規模破綻するため採 SHALL NOT らず、job キューで隔離 SHALL する。この非同期 job 経路は本 change の初期実装スコープに含む必須経路 SHALL であり、後続フェーズやオプション扱いとして先送り SHALL NOT する。`persistent-search-index` の索引実装・catch-up を再設計 SHALL NOT し、その上に非同期 job 契約を積層 SHALL する。

#### Scenario: 任意 regex は非同期 job で隔離
- **WHEN** literal 抽出不能な任意 regex 検索が要求される
- **THEN** LETHE はそれを非同期 search job として実行し通常の同期 exact 検索 SLO から隔離する
- **AND** 固定 500ms timeout の全 document 同期走査を通常経路で提供しない

#### Scenario: 非同期 job は初期スコープの必須経路
- **WHEN** 本 change の SCC 実装が行われる
- **THEN** 非同期 search job 経路は初期実装スコープの必須経路として実装され、後続フェーズやオプションへ先送りされない

#### Scenario: 索引実装を変えず契約を積層
- **WHEN** 非同期 job 分離を導入する
- **THEN** persistent-search-index の索引実装・catch-up state machine は変更されず、その上に非同期 job 契約が積層される
