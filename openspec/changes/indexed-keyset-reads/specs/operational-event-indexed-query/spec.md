## ADDED Requirements

### Requirement: OIQ-01 correlation/causation/event_type の索引付き keyset 検索

OEL storage は correlation_id / causation_id / event_type / stream による索引付き filter 検索を、keyset cursor(`after_cursor` + `limit`)付きで提供 SHALL する。既知の correlation / causation / event_type / stream に対する検索は O(log N + k) SHALL であり、cursor 0 からの全 page 走査 + クライアント側 JSON filter を強制 SHALL NOT する。

#### Scenario: correlation 指定の検索が索引付き keyset
- **WHEN** 既知の correlation_id に一致する operational event を検索する
- **THEN** storage は索引付き keyset cursor 検索で O(log N + k) で該当 event を返す
- **AND** cursor 0 からの全 page 走査とクライアント側 filter を要求しない

#### Scenario: causation / event_type の索引付き検索
- **WHEN** 既知の causation_id または event_type で検索する
- **THEN** storage は索引付き keyset cursor 検索で該当 event を返す

### Requirement: OIQ-02 correlation/causation/event_type の索引列化

OEL の correlation_id / causation_id / event_type は、JSON 内埋め込みだけでなく列・複合 index として持 SHALL つ。監査 trace が台帳量に比例して数分かかること(実測 Nanihold で 3〜6 分)を、索引化によって解消 SHALL する。canonical archive / replay 契約は変更 SHALL NOT する。

#### Scenario: 索引列で監査 trace が有界
- **WHEN** 既知 correlation の監査 trace を索引付き検索で辿る
- **THEN** 応答時間は台帳全量に比例せず返却件数に対して有界である

#### Scenario: canonical 契約を変えない
- **WHEN** 索引列を追加する
- **THEN** canonical archive 形式と replay 契約は変更されず索引は派生として付与される
