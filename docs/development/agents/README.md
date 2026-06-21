# LETHE Multi-Agent Development Structure

## Overview

[`../ai-collaboration.md`](../ai-collaboration.md) の方針に基づき、
AI駆動開発を3つのエージェントロールで進行する。
人間は「意味論の所有者」として最終承認を行い、AIエージェントが設計・実装・レビューを並列に担当する。

> **"何が真か"は人間、"どう作るか"はAI**

---

## Agent Roles

### 1. Spec Designer（設計役）

**File:** [spec-designer.md](spec-designer.md)

**責務:**
- interface / invariant / test plan の策定
- schema 設計（JSON Schema / YAML spec）
- source contract の定義
- acceptance test の記述
- ADR 草案の作成

**入力:** 人間からの要件（Goal / Invariants / Non-goals）
**出力:** 仕様文書 + acceptance test 定義

---

### 2. Implementer（実装役）

**File:** [implementer.md](implementer.md)

**責務:**
- Spec Designer の出力をもとにコードを実装
- adapter / projector / API / migration の実装
- テスト（unit / integration）の実装
- CI / lint / typing の整備

**入力:** Spec Designer の仕様 + acceptance test
**出力:** 実装コード + テストコード + API 仕様

---

### 3. Reviewer（レビュー役）

**File:** [reviewer.md](reviewer.md)

**責務:**
- System Law 違反の検出
- 意味論逸脱の発見
- idempotency / authority / lineage の検証
- restricted data の exposure チェック

**入力:** Implementer の成果物  
**出力:** レビュー結果（approve / request-changes + 指摘リスト）

---

## Workflow

```
人間: タスクを小さく切る + Goal / Invariants / Non-goals を定義
  ↓
Spec Designer: interface / invariant / test plan を書く
  ↓
Implementer: コード + テストを書く
  ↓
Reviewer: law 違反を探す
  ↓
人間: merge / reject を決める
```

---

## Task Delegation Template

各タスクをエージェントに渡す際は、以下のテンプレートを使用する。

```markdown
## Task: [タスク名]

### Goal
[このタスクで達成すること]

### Invariants（守るべき law）
- [ ] Append-Only Law
- [ ] Replay Law
- [ ] Effect Isolation Law
- [ ] Explicit Authority Law
- [ ] No Direct Mutation Law
- [ ] Filtering-before-Exposure Law

### Input
[入力となるファイル / spec / データ]

### Non-goals
[明示的にやらないこと]

### Acceptance Tests
[成功条件のリスト]

### Output Format
- 実装コード
- 単体テスト
- integration test
- 変更された API 仕様
- リスク / 未確定点メモ
```

---

## Responsibility Matrix

| 領域 | 人間 | Spec Designer | Implementer | Reviewer |
|---|---|---|---|---|
| コア意味論 | **Owner** | 整理・ADR 草案 | — | law 検証 |
| Governance / consent | **Owner** | policy 草案 | — | exposure チェック |
| Schema 設計 | 境界判断 | **Primary** | — | 整合性チェック |
| Source contract | authority 判断 | **Primary** | — | authority 検証 |
| Adapter 実装 | — | spec 提供 | **Primary** | idempotency 検証 |
| Projector / SQL | acceptance 確認 | spec 提供 | **Primary** | replay 検証 |
| API / Axum | — | contract 定義 | **Primary** | exposure チェック |
| テスト生成 | — | test plan | **Primary** | coverage 確認 |
| CI/CD / lint | — | — | **Primary** | — |
| 仕様レビュー / merge | **Final** | — | — | recommend |

---

## LETHEレビュー・チェックリスト（Reviewer 用）

- [ ] append-only を破っていないか
- [ ] authority model を暗黙にしていないか
- [ ] read mode を混同していないか
- [ ] restricted data が filtering 前に露出していないか
- [ ] lineage が失われていないか
- [ ] idempotency が壊れていないか
- [ ] replay 不能な hidden mutable state が混入していないか
