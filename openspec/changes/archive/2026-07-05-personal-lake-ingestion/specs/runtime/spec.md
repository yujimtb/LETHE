# Spec Delta: runtime

**Change:** personal-lake-ingestion
**Version:** 0.1 (draft)
**Date:** 2026-07-04

## Dependencies

- M15 Runtime — partition initialize / routing keyspec pin
- M03 Observation Lake — append-only partition log
- 正典: design.md P1 / P2 / P3 / P6

---

## ADDED Requirements

### Requirement: PING-RT-01 Personal Routing Keyspec Pin

Personal lake selfhost config は routing order `coarse(year):coarse(month):source:container:fine(published)` を明示し、SQLite partition log の `initialize` event にその routing keyspec を永続しなければならない (SHALL)。既存 DB の partition log と config の routing keyspec が一致しない場合、selfhost は起動を拒否しなければならない (SHALL)。

#### Scenario: personal initialize records year-first keyspec

- **WHEN** personal lake config `key_order = "year_month_source_container_published"` で新規 SQLite DB を開く
- **THEN** partition log の `initialize.routing_keyspec_json` は axes `coarse_year`, `coarse_month`, `source`, `container`, `fine_published` の順序を保持する

#### Scenario: keyspec mismatch fails fast

- **WHEN** month-first で初期化済みの SQLite DB を year-first config で開く
- **THEN** 起動は keyspec mismatch error で失敗し、暗黙 migration や fallback routing を行わない

### Requirement: PING-RT-02 Import-only Instance Config

GitHub / claude.ai one-shot import 専用 selfhost instance は `sources` を空配列として起動できなければならない (SHALL)。Slack / Google Slides source が未設定の場合、対応する credential や derivation secret を要求してはならない (SHALL NOT)。

#### Scenario: empty sources boot for one-shot import

- **WHEN** config に API token、storage、routing、limits を明示し、`sources.slack = []` と `sources.google_slides = []` を設定する
- **THEN** selfhost config validation は成功し、source credential と Gemini API key を要求しない
