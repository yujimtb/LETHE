## ADDED Requirements

### Requirement: CGE-01 append 前 gate が実 consent-decision を評価

append 前 consent gate は subject / channel の実 consent-decision(`schema:consent-decision` supplemental)を評価 SHALL し、`Role::SystemAdmin` / `AccessScope::Internal` / `ConsentStatus::RestrictedCapture` などの定数で評価 SHALL NOT する。最新 consent-decision を正とし SHALL、未登録時の既定は `restricted_capture` SHALL とする。consent 違反・opt-out は明示 quarantine SHALL とする(A-9 capture 時境界、A-10 台帳リプレイ)。

#### Scenario: 定数評価を廃止し実 consent を評価
- **WHEN** Observation が append 前 consent gate を通る
- **THEN** gate は subject/channel の実 consent-decision を解決して評価する
- **AND** SystemAdmin/Internal/RestrictedCapture などの定数では評価しない

#### Scenario: 最新 decision が opted_out なら quarantine
- **WHEN** subject の最新 consent-decision が `opted_out` である
- **THEN** 対象 Observation は明示 quarantine として扱われる

#### Scenario: consent-decision 未登録の既定
- **WHEN** subject に consent-decision が未登録である
- **THEN** 既定 `restricted_capture` として評価される

### Requirement: CGE-02 consent 変更の反映鮮度契約

consent 変更の反映は 2 境界で契約 SHALL する。capture gate は評価時点で解決済みの最新 consent-decision を使用 SHALL する(A-9、correctness 境界)。公開 projection への consent delta 反映は watermark 増分 fold で行 SHALL う(A-3)。projection 反映の許容 staleness bound は運用確定事項(design Q2)とし、gate の「評価時最新」性はそれに依存 SHALL NOT する。

#### Scenario: capture gate は評価時点の最新 consent
- **WHEN** consent-decision が変更された後に Observation が取り込まれる
- **THEN** capture gate は評価時点で解決済みの最新 decision を使う

#### Scenario: 公開 projection への consent 反映は増分
- **WHEN** consent delta が公開 projection に反映される
- **THEN** watermark 増分 fold で差分反映され全量再計算しない
