# Identity / person-page 準線形化 実装結果

実施日: 2026-07-15
対象branch: `fix/identity-dsu`

## 完了済み

- 候補C（affected component限定の局所再投影）を `e23e884` としてcommitした。
- topology、identifier owner、consent変化で全Observation rebuildへ遷移せず、old/new
  component閉包のObservationと既存message rowだけを読む。
- strict insert/update/deleteとmanifestを同一transactionで公開する。
- full rebuild oracleとのmanifest/row同値性、late bridge、consent opt-out、Slack batch
  partition不変性を回帰テストで確認した。
- 候補Bの基盤として、次の破壊的ID移行を実装した。
  - `person:resolved-N` を `person:component-{seed}` へ変更。
  - message/slideのperson内ordinal IDをObservation/append sequence/claim由来のstable
    fact IDへ変更。
  - non-corpus materialization formatを6へ更新し、旧derived materializationはaliasや
    互換layerを作らずcanonical Observationから再生成する。
  - late bridgeで吸収側messageのrow keyを維持し、ownerだけをstrict updateする回帰を
    追加した。

## 検証結果

- `cargo fmt --all -- --check`: 成功
- `cargo clippy --workspace --all-targets -- -D warnings`: 成功
- `cargo test --workspace`: 成功（失敗0、既存ignored 1）
- `python ./scripts/check_markdown_links.py`: 成功
- `git diff --check`: 成功

## 計算量

変更前はtopology/owner/consent変化ごとに累積全Observationを2 passし、固定batch `b`
ではObservation visitだけでも総量 `Theta(N^2 / b)` だった。

候補C後の当該経路は、affected component inputを `K`、old/new message rowを `M_K`
として概ね `O(K log K + projector(K) + M_K)` であり、無関係componentのObservation
visitは0である。stable component/fact IDにより無関係componentの再採番と、affected
component内のperson ordinal再採番も除去した。

未実装のnormalized IdentifierKey bucket、永続append-only DSU、identity nodeへのfact
間接所属、small-to-large component aggregate、keyed manifestまで実装した最終Bの目標は、
通常履歴全体でamortized `O((N+C) log C)` である。現段階は全candidate resolveとresident
manifest clone/serializeを残すため、この上界はまだ満たさない。

## 停止理由

安定ID段階の変更はindexへstage済みだが、この実行環境ではworktreeが参照する親
repositoryのGit管理領域
`D:/userdata/docs/projects/skcollege_database/.git/worktrees/wt-lethe-b` がwrite許可範囲外で
ある。`git commit` は `index.lock: Permission denied` で2回失敗した。残存lock fileはなく、
コード・テスト失敗ではなくGit metadataへの書込み権限が原因である。

制約の「詰まったらresult mdに記録し停止」に従い、DSU本体への追加着手を停止した。

## 再開の選択肢

1. 親repositoryの上記Git管理領域へwrite可能なセッションで、stage済み変更と本resultを
   `feat(identity): stabilize person and fact identifiers` としてcommitする。
2. worktreeのGit metadataをworkspace write対象内へ置いた作業環境を作り、同じbranchで
   commitする。
3. 安定IDcommit後に、normalized IdentifierKey bucketとappend-only DSUを別commitで
   実装し、full rebuild oracleとのprefix/property比較を継続する。
