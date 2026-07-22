## ADDED Requirements

### Requirement: CBV-01 consent scope 単位の可視性モデルを正として定義

可視性の単位は **consent scope**(人物・artifact・space・group・external partner に適用)SHALL であり、本 change がその可視性モデルの正 SHALL とする。indexed-keyset-reads C2Q5(可視 blob 参照表の粒度: owner / projection / consent scope)は consent scope 単位で解決 SHALL する。可視性は接続 client 個別合意でなく projection と consent scope で強制 SHALL する(A-9 consent 境界、スケーラビリティ原則: 製品境界厳格・client 任意接続前提)。

#### Scenario: 可視性の単位が consent scope
- **WHEN** ある record / blob の可視性を判定する
- **THEN** 判定は consent scope 単位の可視性モデルに従う
- **AND** 接続 client 個別合意には依存しない

#### Scenario: C2Q5 を consent scope 単位で解決
- **WHEN** 可視 blob 参照表の粒度を確定する
- **THEN** 粒度は consent scope 単位で定義される

### Requirement: CBV-02 可視 blob 表を consent scope でキー化し retraction と連動

可視 blob 参照表は consent scope でキー化 SHALL し、consent delta・retraction と同一 commit で upsert / delete SHALL する。表は canonical Observation と consent scope 状態から決定的に再構築可能な派生 SHALL であり、ground truth として直接更新して SHALL NOT ならない(A-9 filtering-before-exposure、A-2 再構築可能性)。表の索引実装・O(1) 認可経路は indexed-keyset-reads(BAI-01/02)の責務とし、本 change はキー(=consent scope)と連動意味論のみを定義 SHALL する。

#### Scenario: consent delta と retraction が可視表に反映
- **WHEN** consent delta または retraction が発生する
- **THEN** 可視 blob 参照表は同一 commit で consent scope キーの下に upsert / delete される

#### Scenario: 可視表の決定的再構築
- **WHEN** 可視 blob 参照表を canonical Observation と consent scope 状態から再構築する
- **THEN** 同じ入力から同じ可視表を得る
