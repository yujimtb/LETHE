## ADDED Requirements

### Requirement: BAI-01 可視 blob 参照表による O(1) blob 認可

既知 BlobRef の参照可否判定は、projection materialization が保持する可視 blob 参照表を介して O(1)〜O(log N) SHALL である。全 `person_components` と slide refs を `any()` 走査して判定すること(O(person 数 + 全 blob ref 数))は SHALL NOT する。

#### Scenario: 既知 BlobRef の認可が索引で O(1)
- **WHEN** 既知 hash の BlobRef の参照可否を判定する
- **THEN** 可視 blob 参照表を引いて O(1)〜O(log N) で判定する
- **AND** 全 person_components と slide refs を走査しない

#### Scenario: 並行画像取得が相互ブロックしない
- **WHEN** 複数の画像 BlobRef 要求が並行して認可判定される
- **THEN** 各判定は可視表引きで完了し相互に長時間ブロックしない

### Requirement: BAI-02 可視 blob 参照表の増分維持と再構築可能性

可視 blob 参照表は projection materialization と同一 commit で(consent delta と同時に)upsert / delete SHALL し、canonical Observation と consent 状態から決定的に再構築可能な派生 materialization SHALL である。可視表を ground truth として直接更新して SHALL NOT ならず、Filtering-before-Exposure Law を保 SHALL つ。

#### Scenario: 可視表は materialization と同一 commit で更新
- **WHEN** projection materialization が blob 可視性を変える(consent delta 含む)
- **THEN** 可視 blob 参照表は同一 commit で upsert / delete される

#### Scenario: 可視表の決定的再構築
- **WHEN** 可視 blob 参照表を canonical Observation と consent 状態から再構築する
- **THEN** 同じ入力から同じ可視表を得る
