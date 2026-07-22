## ADDED Requirements

### Requirement: SSR-01 再起動時の永続 sync 状態の厳密復元

AppCore 生成時に `last_sync_at` / error / metrics を default(None / zero)へ戻 SHALL NOT し、永続 `sync_metrics` から起動時に厳密ロード SHALL する。health 応答は canonical 台帳と整合した実 sync 状態を返 SHALL し、再起動直後に「sync 実績なし・metrics ゼロ」の偽初期状態を SHALL NOT 表示する。

#### Scenario: 再起動後に実 sync 状態を返す
- **WHEN** 永続 `sync_metrics` を持つ instance が再起動する
- **THEN** AppCore は起動時に永続 metrics / last-sync を厳密ロードする
- **AND** health は偽の初期値ではなく台帳と整合した実 sync 状態を返す

### Requirement: SSR-02 欠損・不整合の明示

persisted sync metrics が欠損または不整合な場合は明示 SHALL し、偽の初期値で無言に埋めることを SHALL NOT する。

#### Scenario: 欠損時に明示する
- **WHEN** 起動時に永続 sync metrics が欠損または不整合である
- **THEN** LETHE はその欠損・不整合を明示する
- **AND** 偽の初期値(ゼロ metrics / sync 実績なし)で無言に埋めない
