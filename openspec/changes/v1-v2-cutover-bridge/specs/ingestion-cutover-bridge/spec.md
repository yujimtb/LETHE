## ADDED Requirements

### Requirement: CUT-01 再開可能な増分 identity bridge projection

system は canonical Observation ledger の `append_seq` を入力とし、v1 由来 Observation を現行 v2 formula (`source_instance_id:object_id:sha256(canonical_json)`) で解決する identity bridge projection を提供 SHALL する。projection は candidate または derivation gap と単調 watermark を bounded batch ごとに原子的に commit SHALL し、失敗時は watermark を進め SHALL NOT ない。初回の historical bootstrap は `append_seq` の先頭から再開可能な bounded batch として実行 SHALL し、startup ごとの全 scan、cutover ごとの全 scan、append-only 入力に対する全量 rebuild を要求 SHALL NOT する。canonical Observation を bridge のために更新・削除 SHALL NOT する。

#### Scenario: historical bootstrap を停止点から再開する
- **WHEN** historical bootstrap がある batch の candidate 書込み前または watermark commit 前に停止する
- **THEN** 再実行は最後に commit 済みの watermark より後ろから再開する
- **AND** 同じ batch の再試行は candidate/gap を二重化せず、canonical Observation を変更しない

#### Scenario: steady-state は新規 tail だけを処理する
- **WHEN** bridge watermark までの canonical Observation が処理済みで、その後に N 件が append される
- **THEN** 次回 apply は watermark より後ろの N 件だけを処理する
- **AND** 既処理 history の全 scan または全量 rebuild を行わない

#### Scenario: identity 原料欠落を gap として保持する
- **WHEN** 対象 Observation から non-blank `source_instance_id`、`object_id`、valid `canonical_json` のいずれかを決定論的に得られない
- **THEN** projection はその `append_seq` と理由を gap として記録する
- **AND** 推測した identity や silent fallback key を作らない

### Requirement: CUT-02 v2 による cross-version duplicate 解決と v1 不変性

v2 ingestion は新規 append の前に v2 identity を bridge candidate へ照合 SHALL する。同じ canonical JSON の candidate が存在する場合、最小 `append_seq` の既存 Observation ID を `outcome=duplicate` の `existing_id` として返し、新しい Observation を append SHALL NOT する。同じ identity key で canonical JSON の exact compare が一致しない場合は `canonical_collision` として quarantine SHALL する。v1 ingestion は bridge を参照 SHALL NOT し、凍結済みの response shape、request-level error semantics、`namespace_draft` identity 判定を変更 SHALL NOT する。

#### Scenario: v1 item の最初の v2 retry は既存 ID に収束する
- **WHEN** v1 で append 済みの Observation と同じ `source_instance_id`、`object_id`、canonical JSON を持つ draft が cutover 後に v2 へ送られる
- **THEN** v2 は v1 Observation の ID を `existing_id` とする `duplicate` を返す
- **AND** canonical ledger の Observation 件数は増えない

#### Scenario: 複数 legacy candidate の winner は決定論的である
- **WHEN** 同じ v2 identity に導出される既存 Observation が複数ある
- **THEN** resolver は最小 `append_seq` の Observation ID を ACK 対象にする
- **AND** candidate multiplicity を anomaly として保持し、既存 Observation を削除・統合しない

#### Scenario: v1 contract regression を許さない
- **WHEN** cutover 前の valid v1 request、duplicate request、validation failure request を凍結 contract test へ入力する
- **THEN** response shape、HTTP/error semantics、identity 判定は bridge 導入前と同一である
- **AND** v1 handler は bridge alias の有無で outcome を変えない

### Requirement: CUT-03 unit 単位の single-protocol cutover

system は v2 identity namespace と一致する安定した `source_instance_id` を cutover unit として扱い、unit ごとに `v1_active`、`draining`、`v2_active`、`v2_committed` の durable state と credential/admission generation を管理 SHALL する。同一 unit では一時点に v1 または v2 の片方だけを新規 admission SHALL し、system-wide switch や既知 client 名の列挙を要求 SHALL NOT する。`source_instance_id` は v1/v2 切替時に rename SHALL NOT する。

#### Scenario: client は独立したペースで移行する
- **WHEN** unit A が `v2_active` で unit B が `v1_active` である
- **THEN** A の valid v2 request と B の valid v1 request はそれぞれ受理される
- **AND** A の cutover は B の停止または同時移行を要求しない

#### Scenario: fence 後の stale v1 retry は handler 前で拒否される
- **WHEN** unit が `v2_active` または `v2_committed` になった後、失効した v1 credential/generation で retry される
- **THEN** admission layer は既存の authorization failure として v1 handler 到達前に拒否する
- **AND** v1 identity 判定または v1 wire error taxonomy に新しい意味を追加しない

#### Scenario: in-flight v1 と fence の race を閉じる
- **WHEN** unit を `v1_active` から `draining` へ移す時点で authorization 済みの v1 request が存在する
- **THEN** system は新規 v1 admission を閉じ、既存 request の transaction 完了後に `fence_append_seq` を記録する
- **AND** fence 記録後に当該 unit の v1 Observation が commit しない

#### Scenario: 同じ unit の dual admission を拒否する
- **WHEN** 同じ `source_instance_id` について v1 と v2 の producer が同時に送信を試みる
- **THEN** durable cutover state と credential generation に一致する片方だけを admission する
- **AND** 両 endpoint が同じ logical identity を別 key で append する期間を作らない

### Requirement: CUT-04 fail-closed activation gate

unit の v2 activation は、v1 admission の fence、`fence_append_seq` 以上の bridge watermark、fence 以下の対象 row に対する zero unresolved gap、zero canonical exact-compare error、client retry fixture の identity 不変性、および既知 v1 sample の bridge dry-run 成功を満たす場合にのみ実行 SHALL する。いずれかを証明できない場合は `draining` のまま fail closed SHALL し、未解決 item を v2 の新規 appendへ fallback SHALL NOT する。

#### Scenario: projection lag は activation を止める
- **WHEN** bridge watermark が unit の `fence_append_seq` より小さい
- **THEN** v2 credential/admission を有効化しない
- **AND** bridge が fence まで増分 catch-up した後に gate を再評価する

#### Scenario: legacy metadata gap は activation を止める
- **WHEN** fence 以下の対象 Observation に未解決 identity derivation gap がある
- **THEN** unit は v2 を activate しない
- **AND** source-native evidence に基づく決定論的な append-only mapping が gap を解消するまで block する

#### Scenario: dry-run は ledger を変更しない
- **WHEN** cutover verifier が既知 v1 sample の v2 identity resolution を dry-run する
- **THEN** verifier は期待する既存 Observation ID と照合結果を返す
- **AND** canonical ledger、registry、cutover state を変更しない

### Requirement: CUT-05 rollback safety boundary

system は unit の v2 `outcome=ingested` 件数を durable に追跡 SHALL する。v2 で新規 Observation が一件も append されていない unit は、v2 admission と in-flight request を閉じた後に `v1_active` へ戻してよい。最初の v2 `ingested` 後は `v2_committed` とし、v2 で ACK した object を v1 が再送しないことを一般的に証明できない限り自動 rollback を拒否 SHALL する。安全証明のない v1 再開や legacy identity への silent fallback を行い SHALL NOT する。

#### Scenario: v2 append 前は v1 へ戻せる
- **WHEN** unit が `draining` または `v2_active` であり、v2 result が `duplicate`、`rejected`、`quarantined` のみで `ingested=0` である
- **THEN** v2 admission と in-flight request を閉じた後、v1 credential を再発行して `v1_active` へ戻せる
- **AND** bridge candidate/gap/watermark は削除せず次回 cutover に再利用する

#### Scenario: v2 append 後の unsafe rollback を拒否する
- **WHEN** unit で一件以上の v2 `ingested` が記録された後に一般的な v1 rollback を要求する
- **THEN** system は rollback を拒否して `v2_committed` を維持する
- **AND** operator に forward-fix が必要であることを明示する

### Requirement: CUT-06 cutover verification と監査可能性

system は unit ごとに phase、credential generation、fence append sequence、first v2 append sequence、bridge watermark/lag、candidate/gap/multiplicity/collision count、bridge duplicate hit count、fence 後 stale v1 rejection count を machine-readable に公開 SHALL する。cutover state transition は authority と理由を含む append-only audit event として記録 SHALL する。

#### Scenario: cutover readiness を機械判定する
- **WHEN** operator または automation が unit の readiness を照会する
- **THEN** response は各 activation precondition の pass/fail と blocking append sequence/reason を返す
- **AND** 人手による DB 全 scan や client 名ごとの手書き判定を要求しない

#### Scenario: crash 後に state を再現する
- **WHEN** process が cutover transition または bridge batch の途中で停止して再起動する
- **THEN** append-only transition log と commit 済み watermark から同じ current phase と再開位置を再現する
- **AND** 未 commit transition/batch を成功扱いしない
