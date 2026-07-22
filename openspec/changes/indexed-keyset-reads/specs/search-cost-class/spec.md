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

### Requirement: SCC-02 任意 regex を cost class として分離

safe literal n-gram を抽出できない任意 regex 検索は、通常 SLO から分離した cost class(非同期 search job / 明示的 cost class / 必須 filter のいずれか)として扱 SHALL う。固定 500ms timeout で全 document を走査する経路を、通常 exact 検索と同一 SLO で提供 SHALL NOT する。`persistent-search-index` の索引実装・catch-up を再設計 SHALL NOT し、その上に cost-class 契約を積層 SHALL する。

#### Scenario: 任意 regex は通常 SLO から分離
- **WHEN** literal 抽出不能な任意 regex 検索が要求される
- **THEN** LETHE はそれを cost class(非同期 job / 明示 cost class / 必須 filter)として扱い通常 exact 検索の SLO から分離する

#### Scenario: 索引実装を変えず契約を積層
- **WHEN** cost class 分離を導入する
- **THEN** persistent-search-index の索引実装・catch-up state machine は変更されず、その上に cost-class 契約が積層される
