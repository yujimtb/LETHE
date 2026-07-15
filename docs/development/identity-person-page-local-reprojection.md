# Identity / person-page component-local re-projection

実装日: 2026-07-15
対象: 候補C（component局所再投影）と候補B（安定ID + DSU）

## 実装結果

候補C時点では、Slack 増分で identity topology、identifier owner、または consent が変化した場合も、
`proj:person-page` の全 Observation rebuild へ遷移しない。compact identity node は
元 Observation ID の append-only 参照を保持し、delta が触れた old/new component の
閉包だけを求める。該当参照は SQLite から読み、`append_seq` 順へ並べ、既存の
`PersonPageProjector` で再投影する。

局所結果の公開は次の契約に従う。

- old affected owner の message row を owner index で取得する。
- new affected component の完全な desired row set と比較し、strict
  insert/update/delete を作る。
- profile、slide、activity、identity、consent、manifest と row delta は既存の
  SQLite transaction で一度に commit する。
- Observation 参照の欠落、high-water 超過、append sequence 重複、owner 不整合は
  full rebuildへ迂回せず明示エラーにする。
- 無関係componentのObservationは読まない。materialization format 6以降の
  `person:component-{seed}` はcomponent-localなので、merge当事者以外をaffected
  componentへ含めない。

format 7の通常増分では、候補Cのreference再投影をonline `IdentityState` とcomponent
aggregateへ置換した。normalized `IdentifierKey` bucketからtouched node/componentを直接
得るため、全candidate resolveとaffected component Observation再読は行わない。候補Cの
full rebuild比較は正しさoracleとして維持する。

## Oracle とテスト契約

固定 canonical high-water と同一 `built_at` に対し、次を比較する。

```text
component incremental manifest == full rebuild manifest
component incremental row-store == full rebuild row-store
```

追加した回帰は次を含む。

- late Slack email bridge による二component merge
- identifier追加による既存consent decisionの適用と historical message削除
- 1/1/1、2/1、1/2、3件のSlack batch partition差を跨ぐ同値性
- late bridge時に無関係componentのObservation IDがlookupされないこと

full rebuildは正しさoracleおよびmaterialization移行・復旧用として残す。通常の
有効なSlack deltaが topology/owner/consent変化だけを理由にoracleへ遷移する経路は
削除した。

## Materialization migration

候補Cでnon-corpus materialization formatを `4` から `5` へ上げた。version 5では各
`CompactIdentityCandidate` が `observation_ids` を必須で持つ。version 4のderived
identity/person-page materializationはbootstrap時にcurrent扱いせず、canonical
Observationから一度だけversion 5をbuildして原子的にpublishする。

stable component person IDの導入時にformatを `6` へ上げた。version 5のderived
materializationも同様に破棄し、canonical Observationをappend順にreplayする。
同じmigrationでperson内ordinal row keyを廃止し、materialized fact IDを次へ変更した。

- message: `pm:{append_seqを20桁zero-pad}:{ObservationId}`
- slide: `ps:{append_seqを20桁zero-pad}:{ObservationId}:{claim}`

`claim` はimmutable Observation内の `owner`、`editor-{relation index}`、または
`analysis` であり、person IDを含まない。複数claimが同一componentへjoinする場合は
辞書順最小を選ぶ。この選択はcomponent mergeに対して閉じており、merge後のIDは
merge前に存在したfact IDのいずれかになる。owner再所属は同じrow keyのstrict update、
消えるdedup claimはstrict deleteとして公開する。message/slideの外部順序はIDの
`append_seq`、次いでfact IDで決定する。

旧manifestの読み替え、旧row ID alias、互換layerは設けない。canonical Lakeと検索v2
index契約は変更しない。

候補B本体でformatを `7` へ上げた。identity evidenceは
`owner_key = __identity_events__`、component aggregateは
`owner_key = __person_components__`、message/slide factは
`owner_key = identity-node:{node_id}` のkeyed rowへ分離した。manifestには全identity、
全person、全factを埋め込まず、watermark/fingerprint/countと補助projectionだけを置く。
version 6は互換読込みせずcanonical Observationからversion 7を再構築する。

## 計算量

変更前、Slack topology/owner/consent変化は累積全Observationを二回読むfull rebuild
となり、1 batchあたり少なくとも `Omega(N)`、固定batchを繰り返す総量は
`Theta(N^2 / b)` だった。

候補C後、該当経路のObservation I/Oとperson再投影は affected component inputを
`K`、old/new message rowを `M_K` として概ね
`O(K log K + projector(K) + M_K)` になる。`append_seq` sortは現在
`O(K log K)`、既存reference projectorはcomponent内personとObservationの積を含む。
無関係componentのObservation visitは0である。

format 7では全candidate resolve、fact rowのperson owner直接保持、resident manifest
clone/serializeも除去した。High confidence bucketは代表とのみunionし、物理DSU rootは
union-by-weight、component membershipとset aggregateはsmall-to-largeでmergeする。
message/slide factはidentity nodeに固定し、person query時にnode→componentをjoinする。

通常append-only履歴全体はamortized `O((N+C) log C)` である。新規messageだけならdelta
比例となる。opt-out時に対象componentのmaterialized contentを物理削除する契約には、
不可避な `Omega(K_component)` が加わる。
