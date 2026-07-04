# Spec Delta: answer-log-projection

**Change:** workspace-search-support
**Module:** (new) answer-log-projection
**Scope:** Bot 回答の構造化ログを蓄積する Projection + 検索 API
**Dependencies:** M01 Domain Kernel, M03 Observation Lake, M05 Projection Engine, M06 DAG Propagation, M14 API Serving
**Agent:** Spec Designer (Projection spec) → Implementer (Projector + API) → Reviewer (scaffolding 検証)

---

## ADDED Requirements

### Requirement: ALOG-01 回答ログの Lake 投入
Bot の回答は構造化された Observation として LETHE Lake に投入 SHALL する。schema は `schema:bot-answer-log` とし、question, answer, citations, used_queries, asker, ts, model, usage, confidence, unknowns を含む。

#### Scenario: 回答ログの Observation 生成
- **WHEN** Search Bot が質問に対して回答を生成する
- **THEN** 回答の構造化データが `schema:bot-answer-log` の Observation として Lake に投入される

#### Scenario: citations の記録
- **WHEN** 回答に一次ソースへの参照が含まれる
- **THEN** 各 citation の url, record_id, source_type が Observation に含まれる

### Requirement: ALOG-02 Answer Log Projection
Answer Log Projection は M05 Projection Engine の仕組みに従い、Bot 回答ログを scaffolding 検索可能な形で materialization SHALL する。

#### Scenario: Projection 生成
- **WHEN** 新しい回答ログ Observation が Lake に投入される
- **THEN** Answer Log Projection が watermark 増分更新され、検索可能になる

### Requirement: ALOG-03 prior_qa_search API
LETHE は Answer Log Projection に対する検索 API を提供 SHALL する。この API は過去の Bot 回答から関連する回答と citations を返す。

#### Scenario: 過去回答の検索
- **WHEN** prior_qa_search API にクエリが渡される
- **THEN** 過去の Bot 回答ログから関連する回答 (question, answer, citations) が返される

#### Scenario: 結果は一次ソースではないことの明示
- **WHEN** prior_qa_search の結果が返される
- **THEN** レスポンスに `is_primary_source: false` が含まれ、この結果が scaffolding 用であることが示される

### Requirement: ALOG-04 回答ログの Corpus Projection からの除外
Bot 回答ログの Observation は Corpus Projection (CORPUS-02 の bot 除外ルール) と独立に、Answer Log Projection のみに含まれる SHALL する。Corpus Projection の grep 結果には Bot 回答ログは含まれない。

#### Scenario: 回答ログと Corpus の分離
- **WHEN** grep API で Corpus Projection を検索する
- **THEN** Bot 回答ログの Observation は結果に含まれない

#### Scenario: prior_qa_search での回答ログ参照
- **WHEN** prior_qa_search API で Answer Log Projection を検索する
- **THEN** Bot 回答ログが結果に含まれる

### Requirement: ALOG-05 idempotency key
回答ログの Observation は M09 Adapter Policy の idempotency 契約に従い identity_key を生成 SHALL する。同一の質問に対する同一の回答が重複投入されない。

#### Scenario: 回答ログの冪等取り込み
- **WHEN** 同一の回答ログが再投入される
- **THEN** identity_key が一致し、新規 Observation は生成されない
