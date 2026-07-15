# Identity / person-page 準線形化 実装結果

実施日: 2026-07-15
対象branch: `fix/identity-dsu`

## 最終状態

候補Cのcomponent局所再投影、候補Bの安定ID基盤、および候補B本体を実装した。
実装commitは次のとおり。

- `e23e884`: affected component限定の局所再投影
- `477559b`: `person:component-{seed}` とObservation由来stable fact ID
- `3af5361`: normalized `IdentifierKey` とappend-only `IdentityState`
- `91c8c7c`: identity-node fact、component aggregate、keyed manifestを用いる本体

non-corpus materializationの現行formatは `7` である。旧formatを読むalias、互換layer、
silent fallbackは設けず、canonical Observationから明示的に再構築する。

## 候補B本体

### 1. Normalized IdentifierKey bucket

`IdentifierKey` を `identifier_type + namespace + normalized_value` として一か所で構築する。
email/display nameはglobal namespaceでtrim/lowercaseし、source内IDはsource namespaceに閉じる。
blank source/valueとsource不一致はエラーにする。

High confidence emailはbucket代表とのunionだけを作り、全candidateのpairwise edge生成と
`IdentityProjector::resolve(all candidates)` を通常増分経路から除去した。Medium display
nameはunionせず、候補表示をbucket内のlinear starとして生成する。source内IDの競合は
last-winsせずinvariant errorにする。

### 2. 永続append-only DSU

`IdentityState` はdurable `IdentityNodeId`、parent、component weight、component aggregate、
IdentifierKey bucketを持つ。新規nodeとidentifier claimだけをappendし、High evidenceを
online unionする。物理rootはunion-by-weight、同weightはroot IDで決定する。公開person IDは
物理rootと分離し、component内最小durable nodeをseedとする。

component membership、identifiers、sources、display names、resolved_atはsmall-to-largeで
吸収する。永続projectionにはappend順のidentity replay eventを
`owner_key = __identity_events__` でkeyed rowとして保存し、restart/full rebuildは同じ
reducerへ順序どおりreplayする。欠損、重複、count/high-water不整合は明示エラーにする。

### 3. Identity nodeへのfact間接所属

message/slide rowのownerはperson IDではなく
`identity-node:{node_idを20桁zero-pad}` とした。row valueにもnode IDを保持する。query時は
person componentからmember nodeを得てowner rowsを読み、現在のperson IDへjoinする。

component mergeでは既存factのkey、body、ownerを書き換えない。公開person IDへ直接所属
していた過去factの一括更新を除去した。stable fact IDは次のまま維持する。

- message: `pm:{append_seqを20桁zero-pad}:{ObservationId}`
- slide: `ps:{append_seqを20桁zero-pad}:{ObservationId}:{claim}`

opt-outでmaterialized contentを物理削除する契約は維持し、対象componentのnode ownerだけを
列挙してstrict deleteする。

### 4. Component aggregate / keyed manifest

personごとに `PersonComponentAggregate` を
`owner_key = __person_components__` の独立rowとして保存する。aggregateはidentity、consent、
fact count、first/last activity、active channel、slide blob ref、およびfrontend profileの
deterministic rank/選択を持つ。merge時はcountの加算、min/max、setのsmall-to-large union、
`(richness, created_at, stable source_document_id)` のmaxで合成する。

通常deltaはaffected component row、新規fact、identity event、reply rowだけを同一transactionで
commitする。supplemental deltaも対象componentだけを更新する。residentのidentity/person-page
全体clone、全component clone、巨大manifest serializeは行わない。

format 7 manifestはwatermark/fingerprint/countと補助projectionだけを参照serializeする。
identity event、person component、message、slide、reply SLOはkeyed rowsに分離した。restartは
manifestのcountとkeyed row数・canonical内容を検証してstateを復元する。AppCoreには重複する
resident `snapshot.identity` / `snapshot.person_page` row群を保持しない。

## 正しさ

固定canonical high-waterと同一 `built_at` に対し、通常増分とfull rebuild oracleの次を比較する。

```text
incremental manifest == full rebuild manifest
incremental compact identity state == full rebuild compact identity state
incremental component aggregates == full rebuild component aggregates
incremental keyed row store == full rebuild keyed row store
```

回帰/propertyテストはlate email bridge、identifier追加によるconsent opt-out、Slack batch
partition `1/1/1`, `2/1`, `1/2`, `3`、freshness併用、永続stateのprefix/suffix replayを含む。
さらにunion深さの対数上界、medium bucketのlinear star、5,000件Slack履歴で全Observation
loadを呼ばずnode-owner factとkeyed row countが一致することを確認する。

検索v2、person API、timeline、filtering-before-exposure、blobのprojection参照制約、schema、
重複判定を含むworkspace全体の回帰テストも維持した。

## 計算量

変更前はtopology/owner/consent変化ごとに累積全Observationを2 passし、固定batch `b` では
Observation visitだけでも総量 `Theta(N^2 / b)` だった。さらに全candidate pair生成、
person×Observation、person×consent、resident manifest clone/serializeにも二次または全体走査が
あった。

変更後の通常append-only履歴では、IdentifierKey lookupとkeyed row操作が `O(log C)`、
DSU root lookupが `O(log C)` 以下、unionされるnode/set/aggregateの移動はsmall-to-largeにより
各要素高々 `O(log C)` 回である。factはidentity nodeに固定され、merge時に移動しない。
したがって通常履歴全体はamortized `O((N+C) log C)`、新規messageだけのdeltaは入力件数に
比例する。

restart時のidentity復元も全candidate resolveを使わず、append順eventを同じDSUへreplayする。
full rebuildは移行・破損復旧・正しさoracle用の別経路として残し、通常履歴のamortized上界には
含めない。明示的opt-outで対象componentのmaterialized factを物理削除する場合だけ、契約上
不可避な `Omega(K_component)` が加わる。

## 検証結果

- `cargo fmt --all -- --check`: 成功
- `cargo clippy --workspace --all-targets -- -D warnings`: 成功
- `cargo test --workspace -- --test-threads=1`: 成功（失敗0、既存ignored 1）
- `git diff --check`: 成功
