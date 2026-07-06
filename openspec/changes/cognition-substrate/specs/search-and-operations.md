# Spec Delta: search-and-operations(cognition-substrate)

## SRCH-01: corpus 検索の現状維持と検証

corpus 検索は regex grep を継続する SHALL。現在修正中の実行予算超過不具合の修正後、broad な一語クエリ(例: `cognition`)が公開 MCP 経由で予算内に応答することを検証し、結果を本 change の tasks に evidence として記録する SHALL。予算超過が継続する場合は FTS materialization(tantivy 第一候補)を別 change として起票する SHALL(本 change では実装しない)。

受け入れ: 実公開面での broad クエリ疎通 evidence。

## SRCH-02: 拡張点の予約

corpus projection の materialization 種別として、将来の FTS / VectorIndex 追加を妨げない契約であることを spec 上明記する SHALL(具体実装は行わない)。

受け入れ: spec レビュー。

## OPS-01: 鮮度閾値と予算の ops 設定化

鮮度閾値(ソース別)・バックフィル夜間予算は instance ops 設定とする SHALL。

受け入れ: 設定変更が再起動なしまたは再起動のみで反映される test。
