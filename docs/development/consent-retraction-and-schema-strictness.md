# consent-retraction-and-schema-strictness 実装記録

この文書は、OpenSpec change `consent-retraction-and-schema-strictness` の実装境界を記録する。canonical Observation は append-only であり、撤回・opt-out は projection の増分 fold によって遮蔽する。

## 契約

- 登録済み observation schema は projection が読む必須フィールドの型・format・source contract を宣言する。
- 宣言外フィールドは拒否・隔離せず、payload とともに保存する。取り込み時の余剰フィールド数は `observation_import_timing` に記録する。
- schema は書込時の version で凍結する。seed registry では旧 v1 の permissive snapshot と strict v2 を併存させ、過去 Observation の再検証を行わない。
- 通信 Observation の channel kind、source instance、external ID、sender、thread metadata は取り込み時に必須とする。

## consent と遮蔽

append 前 gate は subject/channel の最新 consent decision を解決し、未登録は `restricted_capture`、`opted_out` は明示 quarantine とする。capture 時の最新性は projection の鮮度に依存しない。公開側は watermark 差分を処理し、通常の consent 反映契約は 5 秒以内とする。

`meta.retracts` は `observation_id` または `source_object_id` を持つ typed object だけを受理する。Corpus、personal search、communication projection は対象を増分削除するが、canonical Lake の対象 Observation は保持する。un-retract、crypto-erasure、Quarantine 専用領域は導入しない。

可視 blob 参照表は v12 で `subject_key` を追加し、record/item 単位で保存する。v12 の ALTER と subject index は migration 内でのみ作成し、base DDL には新列を参照する index を置かない。migration は再実行可能で、真の v11 operational shape からの upgrade をテストする。

privacy 判定の監査 detail は actor、subject、scope、decision、rule、timestamp を持つ。durability と commit 境界は先行 ADC 実装へ委譲する。

## 検証

通常経路は増分 fold と増分 privacy 検証を使う。全量検証は operator が明示的に `validate_privacy_projections_on_demand` を呼んだ場合だけ実行し、定期日次 scan は行わない。テストは strict v2 の欠落・余剰、最新 consent gate、typed retraction の corpus/search/communication 遮蔽、canonical 保持、on-demand 検証、v12 upgrade を含む。
