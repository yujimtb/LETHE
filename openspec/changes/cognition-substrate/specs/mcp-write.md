# Spec Delta: mcp-write(cognition-substrate)

対象: MCP 面への `write_supplemental` ツール追加。

## MCPW-01: 汎用ツール1本

MCP サーバに `write_supplemental` ツールを1本追加する SHALL。引数は kind(registry 登録済み識別子)・payload・anchors・origin メタデータとし、kind 別の型付きツールは設けない SHALL。

受け入れ: 全登録 kind について書き込み contract test。

## MCPW-02: 検証パスの単一性

MCP write は HTTP API `POST /supplementals` と同一の registry スキーマ検証・Store 不変条件(空アンカー/未解決参照 → エラー、AppendOnly 衝突 → エラー)を通る SHALL。入口による検証差異が存在しない SHALL(実装は共有関数)。

受け入れ: HTTP で 422/409 になる fixture が MCP でも同種のエラーになる対照 test。

## MCPW-03: scope

認可は既存 `write:supplemental` scope の流用とし、新 scope を追加しない SHALL。Auth0 側で当該 scope を持たないトークンの write は拒否される SHALL(read 専用クライアントの分離はトークン発行で行う)。

受け入れ: read-only トークンでの write 拒否 test。

## MCPW-04: 事後処理ワークフローの強制

ツール説明文に「書き込み対象は lake に取り込み済みの観測に限る(会話中のリアルタイム自己書き込みは行わない)」を明記する SHALL。anchors は必須(anchor_required=true の kind)であり、未解決参照は拒否される SHALL — これが live-enrichment の構造的な防止線である。

受け入れ: ツール説明文の spec レビュー。存在しない観測 ID をアンカーに指定した書き込みの拒否 test。

## MCPW-05: 公開クライアント疎通

claude.ai カスタムコネクタおよび ChatGPT custom app から、実公開 endpoint 経由で write_supplemental → claim queue projection への反映、の一連が通る SHALL。

受け入れ: 手動 E2E(change ① H4 と同型の evidence を tasks に記録)。

## SKIND-05(registry 拡張): per-kind アンカーポリシー

Supplemental Kind Registry に `anchor_required`(既定 true)を追加する SHALL。false の kind は origin メタデータ(発生主体・時刻・文脈識別子)を必須とする SHALL。既存 6 kind は anchor_required=true のまま意味不変 SHALL。

受け入れ: anchor_required=false kind の空アンカー受理+origin 欠落拒否 test。既存 kind の後方互換 test。

## SKIND-06(registry 拡張): 新 kind 登録

次の kind を登録する SHALL: reply-draft@1(anchor= 着信観測、必須)/ reply-approval@1(anchor= 対象 draft、必須。payload に承認インターフェース識別子)/ send-record@1(anchor= 対象 draft、必須。payload に自動送信か承認送信かの別と、自動送信時は三条件審査の記録)/ nudge-event@1・eos-state-transition@1・mode-transition@1・briefing-issue@1・briefing-feedback@1(anchor 任意、origin 必須)。

受け入れ: 各 kind のスキーマ検証 test(必須欠落・enum 違反の拒否)。
