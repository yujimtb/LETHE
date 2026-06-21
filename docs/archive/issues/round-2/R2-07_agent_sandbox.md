# Issue R2-7: Agent Sandbox の最小構成

**Labels:** [IMPL]
**Priority:** Medium
**Status:** Approved

---

## 問題

ADR-003 で agent playground の方向性は示されているが、**MVP で必要な最小限の sandbox 構成**が具体化されていない。MVP シナリオでは agent 利用は必須ではないが、MVP+4 に向けた設計は今から考えておく必要がある。

## 影響

- MVP+4 の実装見積もりが立たない
- sandbox なしで Projection 開発を始めると、後から sandbox を導入する際の移行コストが高い

## 提案

### MVP（sandbox なし）

- Projection の開発は Git + CLI + local DuckDB で行う
- spec lint は CLI ツールとして提供
- dry-run build は local 環境で実行

### MVP+4（最小 sandbox）

| Component | 実装 | 目的 |
|---|---|---|
| Spec editor | Web UI with YAML validation | 非技術ユーザーの Projection 作成 |
| Dry-run runner | 隔離された local container | 安全な試行 |
| Build log viewer | Web UI | build 状態の確認 |
| Agent connector | API endpoint for coding agent | agent からの spec 生成・lint |

### sandbox の capability 制限

- Network: default deny（source-native read は明示的に許可した endpoint のみ）
- Storage: ephemeral（build 完了後に破棄）
- CPU/Memory: upper bound 設定
- Duration: timeout 設定

---

## ユーザー回答

これで良い気がします。

---

## 次のアクション

- MVP+4 の設計検討
