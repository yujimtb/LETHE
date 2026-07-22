## ADDED Requirements

### Requirement: OIQ-01 correlation/causation/event_type の索引付き keyset 検索

OEL storage は correlation_id / causation_id / event_type / stream / actor_id による索引付き filter 検索を、keyset cursor(`after_cursor` + `limit`)付きで提供 SHALL する。既知の correlation / causation / event_type / stream / actor_id に対する検索は O(log N + k) SHALL であり、cursor 0 からの全 page 走査 + クライアント側 JSON filter を強制 SHALL NOT する。

#### Scenario: correlation 指定の検索が索引付き keyset
- **WHEN** 既知の correlation_id に一致する operational event を検索する
- **THEN** storage は索引付き keyset cursor 検索で O(log N + k) で該当 event を返す
- **AND** cursor 0 からの全 page 走査とクライアント側 filter を要求しない

#### Scenario: causation / event_type / actor_id の索引付き検索
- **WHEN** 既知の causation_id・event_type・actor_id のいずれかで検索する
- **THEN** storage は索引付き keyset cursor 検索で該当 event を返す

### Requirement: OIQ-02 索引列の最小集合と需要駆動拡張

OEL の初期索引対象列は correlation_id / causation_id / event_type / stream / actor_id に occurred_at(時刻レンジ)を加えた集合 SHALL とし、これらを JSON 内埋め込みだけでなく列・複合 index として持 SHALL つ。監査 trace が台帳量に比例して数分かかること(実測 Nanihold で 3〜6 分)を、索引化によって解消 SHALL する。canonical append は append-only ゆえ索引は後付け可能であり、この初期集合を超える索引列は実需要に駆動されて拡張 SHALL し、投機的に全列を索引化 SHALL NOT する。canonical archive / replay 契約は変更 SHALL NOT する。

#### Scenario: 初期集合の索引で監査 trace が有界
- **WHEN** 既知 correlation / causation / event_type / stream / actor_id / occurred_at レンジで監査 trace を辿る
- **THEN** 応答時間は台帳全量に比例せず返却件数に対して有界である

#### Scenario: 索引の需要駆動拡張
- **WHEN** 初期集合を超える列での検索需要が生じる
- **THEN** append-only 台帳ゆえ索引を後付けで追加でき、投機的に全列を索引化しない

#### Scenario: canonical 契約を変えない
- **WHEN** 索引列を追加する
- **THEN** canonical archive 形式と replay 契約は変更されず索引は派生として付与される
