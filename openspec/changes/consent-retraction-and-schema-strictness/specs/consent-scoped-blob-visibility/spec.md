## ADDED Requirements

### Requirement: CBV-01 record/subject 細粒度の可視性モデルを正として定義

可視性モデルは consent scope(人物・artifact・space・group・external partner)を基礎としつつ、**record / subject 細粒度を最初から**備え SHALL、本 change がその可視性モデルの正 SHALL とする。indexed-keyset-reads C2Q5(可視 blob 参照表の粒度)は record / subject 細粒度で解決 SHALL し、scope 単位のみで開始して後から細分化する段階設計は採 SHALL NOT らない。可視性は接続 client 個別合意でなく projection 側で record / subject 細粒度に強制 SHALL する(A-9 consent 境界、スケーラビリティ原則: 製品境界厳格・client 任意接続前提)。

#### Scenario: 可視性が record/subject 細粒度
- **WHEN** ある record / blob の可視性を判定する
- **THEN** 判定は record / subject 細粒度の可視性モデルに従う
- **AND** 接続 client 個別合意には依存しない

#### Scenario: C2Q5 を record/subject 細粒度で解決
- **WHEN** 可視 blob 参照表の粒度を確定する
- **THEN** 粒度は record / subject 細粒度で定義され scope 単位開始→後から細分化はしない

### Requirement: CBV-02 可視 blob 表を record/subject 細粒度でキー化し retraction と連動

可視 blob 参照表は record / subject 細粒度(consent scope 下)でキー化 SHALL し、consent delta・retraction と同一 commit で upsert / delete SHALL する。表は canonical Observation と consent 状態から決定的に再構築可能な派生 SHALL であり、ground truth として直接更新して SHALL NOT ならない(A-9 filtering-before-exposure、A-2 再構築可能性)。表の索引実装・O(1) 認可経路は indexed-keyset-reads(BAI-01/02)の責務とし、本 change はキー(=record/subject 細粒度)と連動意味論のみを定義 SHALL する。

#### Scenario: consent delta と retraction が可視表に反映
- **WHEN** consent delta または retraction が発生する
- **THEN** 可視 blob 参照表は同一 commit で record / subject 細粒度キーの下に upsert / delete される

#### Scenario: 可視表の決定的再構築
- **WHEN** 可視 blob 参照表を canonical Observation と consent 状態から再構築する
- **THEN** 同じ入力から同じ可視表を得る
