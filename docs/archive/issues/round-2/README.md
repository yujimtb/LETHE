# LETHE Open Issues — Round 2

**Date:** 2026-03-10
**Scope:** Round 1 の回答を反映した後に浮上した、次の設計ラウンドで解決すべき論点

---

## 凡例

| ラベル | 意味 |
|---|---|
| **[ARCH]** | アーキテクチャの核に関わる |
| **[IMPL]** | 実装方式に関わる |
| **[GOV]** | ガバナンス・倫理に関わる |
| **[MVP]** | MVP 着手に直結する |
| **Priority: High / Medium / Low** | 実装着手前に解決すべき度合い |

---

## Issue 一覧

### Architecture

| Issue | タイトル | Priority | Status | File |
|---|---|---|---|---|
| R2-1 | Incremental Propagation の Watermark 管理 | High | Approved | [R2-01](R2-01_incremental_propagation_watermark.md) |
| R2-5 | Projection rebuild コスト見積もりと閾値設定 | Medium | Approved | [R2-05](R2-05_rebuild_cost_threshold.md) |

### MVP — Source Adapter

| Issue | タイトル | Priority | Status | File |
|---|---|---|---|---|
| R2-2 | Slack Schema / Adapter 設計 | High | Approved (adapter policy 先行) | [R2-02](R2-02_slack_schema_adapter.md) |
| R2-6 | Google Slides Adapter 実装仕様 | High | Approved (adapter policy 先行) | [R2-06](R2-06_google_slides_adapter.md) |

### MVP — Projection / API

| Issue | タイトル | Priority | Status | File |
|---|---|---|---|---|
| R2-3 | 名寄せ Projection 設計 | High | Approved | [R2-03](R2-03_identity_resolution_projection.md) |
| R2-4 | 個人ページ Projection API 設計 | High | Approved | [R2-04](R2-04_person_page_api.md) |

### Infrastructure / Tooling

| Issue | タイトル | Priority | Status | File |
|---|---|---|---|---|
| R2-7 | Agent Sandbox 最小構成 | Medium | Approved | [R2-07](R2-07_agent_sandbox.md) |
| R2-8 | Projection Catalog Discovery UX | Medium | Approved | [R2-08](R2-08_projection_catalog_ux.md) |

---

## 優先順位サマリ

| Priority | Issue | 次のアクション |
|---|---|---|
| **High** | R2-1 Incremental Propagation watermark 管理 | domain_algebra.md に watermark 仕様を追加 |
| **High** | R2-2 Slack Schema / Adapter 設計 | source adapter policy を先行策定 → plan.md に schema 追加、adapter 実装着手 |
| **High** | R2-3 名寄せ Projection 設計 | MVP 実装の中核。spec + projector の試作 |
| **High** | R2-4 個人ページ Projection API | API 契約を確定し GUI チームと連携 |
| **High** | R2-6 Google Slides Adapter 実装 | source adapter policy を先行策定 → adapter 実装着手 |
| **Medium** | R2-5 Rebuild コスト見積もり閾値 | Projection Spec の拡張定義 |
| **Medium** | R2-7 Agent Sandbox 最小構成 | MVP+4 の設計検討 |
| **Medium** | R2-8 Projection Catalog Discovery UX | Catalog API の仕様策定 |

---

## 担当エージェント・マッピング

各 Issue は以下のエージェントロールによって進行する。詳細は [../agents/](../../../development/agents/) を参照。

| Issue | Spec Designer | Implementer | Reviewer |
|---|---|---|---|
| R2-1 | ✓ spec 策定 | watermark state 実装 | law 違反チェック |
| R2-2 | ✓ schema + contract | Slack API client + adapter | idempotency / authority 検証 |
| R2-3 | ✓ 名寄せ spec | projector SQL/Python | confidence / lineage 検証 |
| R2-4 | ✓ API 契約 | FastAPI endpoint | exposure policy 検証 |
| R2-5 | ✓ 閾値ガイドライン | rebuildEstimate 拡張 | SLA 整合性チェック |
| R2-6 | ✓ capture 仕様 | Google API wrapper | n+1 / rate limit 検証 |
| R2-7 | ✓ sandbox 構成 | container / CLI | capability 制限検証 |
| R2-8 | ✓ Catalog API 仕様 | API + 検索実装 | UX / 再利用性レビュー |
