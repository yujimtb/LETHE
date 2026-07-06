## Context

review-harness は、OpenSpec change の SHALL 要件と、その要件を検証する evidence の対応を機械的に確認する CI ハーネスである。複数 change を並列実装する際に、要件被覆レビューが手作業に戻ることを防ぐ。

対象は LETHE リポジトリ内の change アーティファクトであり、同じ規約を agent-runtime など関連リポジトリにも展開できる形にする。ハーネスが検証するのは「各 SHALL 要件 ID に judgement と evidence が存在すること」であり、test の十分性や仕様解釈の妥当性はレビューで扱う。

## Goals / Non-Goals

**Goals:**

- spec delta から SHALL 要件 ID を決定的に抽出する。
- test コードの coverage annotation と tasks.md の manual evidence を検出する。
- 要件 ID ごとの coverage matrix を生成し、未被覆 SHALL を失敗として報告する。
- PR ごとに新規要件、新規被覆、被覆喪失の差分を出力する。
- LETHE CI から同じ検証を実行できるようにする。
- agent-runtime へ展開するための規約ドキュメントを整備する。

**Non-Goals:**

- test 内容が要件を十分に検証しているかの品質判定は行わない。
- LLM 判定や非決定的な外部評価は CI に含めない。
- 既存の手動 requirements-coverage.md を互換入力として扱わない。
- 暗黙の fallback や ID 推測による補完は行わない。

## Decisions

### D1. Coverage は対応の存在だけを検証する

CI で保証する範囲は、SHALL 要件 ID ごとに automated test または manual evidence が存在し、matrix 上で judgement が決まることに限定する。test の意味的な十分性まで機械判定すると決定性が落ちるため、そこはレビュー対象として残す。

### D2. Requirement ID は explicit ID として抽出する

spec delta では見出しまたは本文に現れる `RVH-01` のような明示 ID を SHALL 要件の識別子とする。ID 形式が不正、または SHALL 文に ID を対応付けられない場合はエラーにする。推測やファイル名由来の補完は行わない。

### D3. Evidence は automated と manual の 2 種類に限定する

automated evidence は test ソース内の `covers: REQ-ID` annotation で宣言する。manual evidence は tasks.md 内の `manual evidence: REQ-ID` 記録で宣言する。どちらも同じ requirement ID に複数紐づけられるが、存在しない requirement ID を参照した evidence はエラーにする。

### D4. Matrix 生成と verify は同じ CLI で扱う

ハーネスは `generate` と `verify` のような明確なコマンドを持つ。`generate` は coverage matrix と diff report を標準出力またはファイルへ出力し、`verify` は未被覆または規約違反を検出した時点で非 0 終了する。

### D5. PR diff は snapshot 同士の比較にする

PR ごとの被覆差分は、base 側と head 側で生成した matrix snapshot を比較して作る。git 履歴を直接解釈する処理はハーネス本体に持たせず、CI が比較対象ファイルを渡す。

## Risks / Trade-offs

- [Risk] annotation があるだけで test 品質までは保証できない → Mitigation: matrix は judgement と evidence の存在確認に限定し、レビューで test 内容を確認する。
- [Risk] ID 規約が曖昧だと false pass が起きる → Mitigation: SHALL 文と ID の対応が不明な場合は fail fast する。
- [Risk] manual evidence が濫用される → Mitigation: manual evidence は tasks.md に明示記録し、matrix と PR diff に表示してレビュー対象にする。
- [Risk] agent-runtime 側の CI 構成差異で展開が遅れる → Mitigation: ハーネス規約と CLI 入出力を repo 非依存にし、CI からは同じコマンドを呼ぶ。
