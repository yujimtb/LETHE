# Documentation

`openspec/`はnormative specification、`docs/`は説明・判断・監査・外部成果物を
保持します。

## Current documents

- `architecture/`: 現行システムの構造と意味論
- `decisions/`: ADR backlogと設計判断
- `development/`: repository運用、エージェント、実装手順
- `audits/`: 実施済み監査と検証報告
- `post/`: 外部で作成されたロードマップ、監査、レビュー
- `archive/`: 現在の正典ではない歴史資料

文書を移動した場合はMarkdown link checkを実行し、READMEと関連OpenSpecを同じ
変更で更新してください。

最新の実装整合性監査は
[`audits/openspec-verification-2026-06-22.md`](audits/openspec-verification-2026-06-22.md)
です。

個人 lake の Claude/ChatGPT/GitHub ingestion 手順は
[`development/personal-lake-ingestion.md`](development/personal-lake-ingestion.md) にあります。

OpenSpec SHALL coverage の CI ハーネス規約は
[`development/review-harness.md`](development/review-harness.md) にあります。
