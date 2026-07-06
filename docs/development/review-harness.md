# Review Harness

`lethe-review-harness` は OpenSpec delta spec の SHALL 要件と evidence の対応を検証する CI 用 CLI です。検証対象は「各 SHALL requirement ID に evidence が存在すること」です。test の十分性や仕様解釈の妥当性はレビューで確認します。

## Requirement ID

Requirement ID は次の形式だけを有効とします。

```text
^[A-Z][A-Z0-9]+-[0-9]{2}$
```

例: `RVH-01`

SHALL 文は `### Requirement: RVH-01 ...` のように requirement 見出しで ID を宣言するか、SHALL 文の本文に同じ形式の ID を明示します。ID がない SHALL 文、または `RVH-1` や `rvh-01` のような不正 ID はエラーです。ファイル名や capability 名からの推測は行いません。

## Evidence Syntax

Automated evidence は test code のコメント行で宣言します。コメント行は `//` または `#` で始まる必要があります。

```rust
// covers: RVH-01
#[test]
fn extracts_requirement_ids() {}
```

Manual evidence は `tasks.md` に明示します。

```markdown
manual evidence: RVH-01
```

manual evidence は自動化できない受け入れ確認だけに使います。存在しない requirement ID を参照した evidence はエラーです。

## CLI

すべての root は明示指定します。デフォルト探索はありません。

```powershell
cargo run -p lethe-review-harness -- extract --spec-root openspec/changes/review-harness/specs
```

```powershell
cargo run -p lethe-review-harness -- verify --spec-root openspec/changes/review-harness/specs --evidence-root . --tasks-root openspec/changes/review-harness
```

```powershell
cargo run -p lethe-review-harness -- generate --spec-root openspec/changes/review-harness/specs --evidence-root . --tasks-root openspec/changes/review-harness
```

```powershell
cargo run -p lethe-review-harness -- diff --base base-coverage.json --head head-coverage.json
```

`verify` は未被覆 SHALL または unknown evidence を検出すると非 0 で終了します。`diff` は base/head の coverage matrix snapshot を比較し、新規 requirement、新規 evidence、失われた evidence を安定順で出力します。

## CI

LETHE の GitHub Actions は `public-release-guard.yml` で review-harness 自身の coverage を検証します。

```yaml
- name: Verify OpenSpec review harness coverage
  run: cargo run -p lethe-review-harness -- verify --spec-root openspec/changes/review-harness/specs --evidence-root . --tasks-root openspec/changes/review-harness
```

新しい change を review-harness 対象に加える場合は、対象 change の `specs` と `tasks.md` を明示して同じ command を追加します。

## Agent-Runtime Rollout

agent-runtime 側でも同じ ID 形式、`covers:` annotation、`manual evidence:` syntax を使います。CI からは LETHE と同じ CLI 入出力を呼び出します。

```powershell
cargo run -p lethe-review-harness -- verify --spec-root <agent-runtime-change>/specs --evidence-root <agent-runtime-repo> --tasks-root <agent-runtime-change>
```

agent-runtime 専用の alias、legacy annotation、silent fallback は追加しません。差分レポートが必要な PR では base/head それぞれで `generate` した JSON を保存し、`diff --base ... --head ...` に渡します。
