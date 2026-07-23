## ADDED Requirements

### Requirement: RPS-01 typed retraction と projection 増分遮蔽

`meta.retracts` は target Observation ID / source object ID の typed metadata(逆 index 可能)SHALL であり、subject 文字列の直入れは SHALL NOT する。retraction 記録は corpus / 検索 / 通信 projection へ watermark 増分 fold で反映 SHALL し、対象 record を同一 commit で projection から遮蔽 SHALL する。canonical Lake の Observation は保持 SHALL し物理削除 SHALL NOT する(A-1 append-only、A-3 増分 fold)。

#### Scenario: retract は typed metadata で表現
- **WHEN** source(例: Slack delete)が retraction を発行する
- **THEN** `meta.retracts` は target Observation ID / object ID を typed に保持する
- **AND** subject 文字列を直接入れない

#### Scenario: retraction が projection から対象を遮蔽
- **WHEN** retraction 記録が append される
- **THEN** corpus / 検索 / 通信 projection は対象 record を増分反映で遮蔽する
- **AND** canonical Observation は Lake に保持される

### Requirement: RPS-02 遮蔽の完全性検証

retract 対象は corpus record・検索 index・通信 projection・可視 blob の全公開経路から到達不能 SHALL である。CorpusProjector は consent / retraction filtering を組み込 SHALL み、`read:corpus` scope だけで撤回対象の全文へ到達すること(C-13)を SHALL NOT 許す。完全性検証は毎 commit の増分検証と on-demand の full 検証で行 SHALL い、全観測走査の定期(日次)full 検証はスケールしないため行 SHALL NOT する(A-9 公開時境界、A-2 再構築可能性)。

#### Scenario: 全公開経路から到達不能
- **WHEN** ある Observation が retract されている
- **THEN** corpus・検索・通信 projection・可視 blob のいずれからも対象へ到達できない

#### Scenario: read:corpus だけで撤回対象へ到達しない
- **WHEN** `read:corpus` scope で personal_all_text を検索する
- **THEN** retract 対象 record は結果に含まれない

#### Scenario: 完全性検証は毎 commit 増分 + on-demand full
- **WHEN** 遮蔽の完全性を検証する
- **THEN** 毎 commit の増分検証と on-demand の full 検証で確認する
- **AND** 全観測走査の定期(日次)full 検証は行わない

### Requirement: RPS-03 canonical 保持と決定的再構築

retraction 反映後も canonical Lake は破壊 SHALL NOT し、遮蔽状態は canonical Observation と retraction / consent 記録から決定的に再構築可能な派生 SHALL である(A-1、A-2)。遮蔽 projection を ground truth として直接更新して SHALL NOT ならない。撤回は projection 遮蔽方式で表現 SHALL し、crypto-erasure 等の物理消去方式は採用 SHALL NOT する。consent は retraction と独立した状態として保持 SHALL し、同一 subject / identifier では `(published, recorded_at, observation_id)` の辞書順で最大の consent decision を最新と評価 SHALL する。この評価は非単調 SHALL であり、最新 decision が `unrestricted` の場合は過去の同一キーの record を増分 fold で再 materialize / 再 index SHALL する。un-retract(遮蔽解除)は認め SHALL NOT ず、retraction 済み record は最新 consent が `unrestricted` でも恒久遮蔽 SHALL する。公開再開は新たな明示 consent 記録として別途表現 SHALL する。

#### Scenario: 遮蔽状態の決定的再構築
- **WHEN** 遮蔽状態を canonical Observation と retraction / consent 記録から再構築する
- **THEN** 同じ入力から同じ遮蔽状態を得る

#### Scenario: un-retract を認めない
- **WHEN** 既に retract された対象の遮蔽解除が要求される
- **THEN** LETHE は un-retract を honored せず遮蔽を維持する
- **AND** 公開再開は新たな明示 consent 記録の追記としてのみ表現される

#### Scenario: 最新 consent による増分復帰
- **WHEN** 同一 subject / identifier に `opted_out` の後で `unrestricted` の consent decision が append される
- **THEN** corpus・検索・通信 projection は過去の対象 record をフルリビルドなしに増分で再 materialize / 再 index する
- **AND** capture gate と各 projection は同じ `(published, recorded_at, observation_id)` 順序規則で同じ最新状態に収束する

#### Scenario: 遅着した古い consent decision
- **WHEN** より新しい `published` の decision が既に存在する状態で、より古い `published` の decision が後着する
- **THEN** 後着した古い decision は最新状態を変更しない

#### Scenario: retraction と consent の独立性
- **WHEN** retraction 済み record に `unrestricted` の consent decision が append される
- **THEN** record は corpus・検索・通信 projection のいずれにも復帰しない

### Requirement: RPS-04 通信projectionの永続化境界と再起動後の増分遮蔽

通信projectionは最新consent state(`consent_by_subject` / `consent_by_identifier`)と privacy reverse key を materialization manifest に永続化 SHALL し、canonical Observation 本体を in-memory stateだけに依存して SHALL NOT ならない。manifest format version はこのschema変更時に bump SHALL し、旧versionはfull deserialize前のversion判定で background rebuildへ送 SHALL る。incremental foldは `opted_out` と `unrestricted` の双方で `observations_for_privacy_key` から対象Observationを再pullしてから再materialize SHALL する。再pull前でも persisted observation key/fact の遮蔽除去は SHALL 完了する。

#### Scenario: 旧manifestは起動時deserialize失敗にならず再構築される
- **WHEN** 旧versionの通信projection manifestが旧privacy field名を含んでロードされる
- **THEN** 新schemaへfull deserializeせず background rebuildへ送られる
- **AND** rebuild完了後は新format versionと新privacy fieldで保存される

#### Scenario: 再起動後のopt-outは既存factを遮蔽する
- **WHEN** 通信projectionをserde往復した後に `opted_out` decisionがappendされる
- **THEN** reverse indexから対象Observationを再pullして既存factを増分遮蔽する
- **AND** 同じopted-out subjectの新規Observationも通信projectionへ現れない

#### Scenario: 再起動後のre-consentは対象factを増分復帰する
- **WHEN** serde往復後に `unrestricted` decisionがappendされる
- **THEN** reverse indexから対象Observationを再pullして通信factを増分再materializeする
