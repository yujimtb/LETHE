# Spec Delta: chat-export-ingestion(cognition-substrate)

対象: claude.ai / ChatGPT のブラウザ自動化エクスポート取り込み。既存の claude.ai インポート経路(パイプライン・IngestionGate・identity key 規則)を共用する。

## CEXP-01: エクスポート成果物の archive 配置

ブラウザ自動化エクスポートの成果物は、source archive リポジトリの `chatgpt/`(ChatGPT 分)および既存 claude.ai ディレクトリに、追記のみで配置される SHALL。importer の入力は archive のワーキングコピーのみとする SHALL(エクスポートジョブの一時出力を直接読まない)。

受け入れ: archive に置いた fixture のみが取り込まれ、archive 外のファイルは無視される test。

## CEXP-02: ChatGPT parser

ChatGPT エクスポート形式(conversations.json 系)の parser を実装し、既存の claude.ai 会話写像と同じ canonical 形式に変換する SHALL。不正・未知構造のレコードは skip+quarantine 報告とし、パース全体を失敗させない SHALL。

受け入れ: 実エクスポート fixture のパース test(壊れレコード混入でも完走)。

## CEXP-03: identity key と冪等性

ChatGPT 観測の identity key は `chatgpt:{conversation_id}:{message_id}:H(canonical)` とする SHALL。published はメッセージ timestamp とする SHALL。同一エクスポートの再実行は全件 duplicate となる SHALL。

受け入れ: 再実行 idempotency test(初回 ingested=N、再実行 duplicates=N、quarantined=0)。

## CEXP-04: 日次実行とバックフィルフラグ搬送

取り込み CLI は範囲指定(期間・会話 ID)実行に対応する SHALL。バックフィル実行時に下流へ搬送するための実行メタデータ(backfill フラグ)を取り込み報告に含める SHALL。

受け入れ: 範囲指定で対象外の会話が取り込まれない test。

## CEXP-05: 故障の可観測性

エクスポートジョブと取り込み CLI は、成功・失敗・取り込み件数・quarantine 件数を機械可読な終了報告として出力する SHALL(鮮度 projection の従系である exit code 通知の材料)。

受け入れ: 失敗時に非ゼロ exit code+構造化エラー報告が出る test。
