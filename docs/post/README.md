# External Posts

このディレクトリは、リポジトリ外で作成されたロードマップ、監査、レビュー、
調査報告を受け入れるための領域です。

## Naming

```text
YYYY-MM-DD-<kind>-<short-title>.md
```

`kind`の例:

- `roadmap`
- `audit`
- `review`
- `assessment`

## Required metadata

各文書の冒頭に次を記載してください。

```markdown
# Title

- Date: YYYY-MM-DD
- Author/Tool: ...
- Scope: ...
- Repository revision: <commit SHA>
- Status: Draft | Reviewed | Accepted | Superseded
```

外部文書は自動的に正典にはなりません。採用した要件はOpenSpec changeへ移し、
実装判断は`docs/decisions/`またはchangeの`design.md`へ反映してください。
