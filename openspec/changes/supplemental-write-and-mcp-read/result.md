# Supplemental projection complexity remediation result

**Date:** 2026-07-15

**Audit:** LETHE complexity audit High #1

**Branch:** `fix/supplemental-incremental`

## Outcome

supplemental write と Observation materialization から supplemental 全件の反復 list/sort/project を除去した。resident `SupplementalProjectionCache` が `(created_at, id)` 順の current record、cognition activity kind、frontend profile kind、CardQueue reducer、ReplySLO join index を保持する。write rollback は store だけでなく cache、fingerprint、record count、ClaimQueue dirty state も復元する。

- claim/transition/verification/decision の変更時だけ ClaimQueue を再投影する。
- ClaimQueue result は resume snapshot / plan state が共有し、同一 write 内の重複構築を行わない。
- ClaimQueue の supplemental ancestry は memoize、decision supersedes chain は path compression する。
- CardQueue は draft ごとの event state と時刻順 expiry index を持ち、変更 draft だけを replay する。
- ReplySLO は `draft_id -> observation_id` と `observation_id -> earliest sent_at` を増分維持する。
- fingerprint は record digest の可逆 256 bit accumulator とし、append/replacement を1件更新する。
- materialization format は v5 とし、旧 manifest は起動時の format migration rebuild で移行する。通常 write からの silent fallback はない。

## Complexity

S=current supplemental、C=cards、A=変更 draft の event 数、D=期限到来候補数とする。

| Operation | Before | After |
|---|---:|---:|
| fingerprint | O(S log S) / write | O(record bytes) / write |
| ClaimQueue construction | 最大4回 / write、chain/ancestry 最悪 O(S²) | 影響 kind で1回、O(S log S + edges) |
| CardQueue | O(S log S + C・S) / write | apply O(log S + A)、snapshot O(C + D) |
| cognition | supplemental 全件を複数回 replay / materialize | activity kind cache + 共有 ClaimQueue、O(S) 上限 |
| sequential S writes | CardQueue 分布で最悪 O(S³) | manifest serialization を含み O(S²)、projection hot path は各 write O(S) 上限 |

永続 manifest が projection snapshot 全体を JSON serialize する現契約のため、通常 write 全体には O(S) の下限が残る。今回の監査対象だった nested card×supplemental scan と write 内の重複 full replay は残らない。

## Correctness evidence

- CardQueue reducer は draft/approval/send の各 record 適用後に full replay と JSON 完全一致する。
- ReplySLO join index は draft/send の各 record 適用後に full replay と完全一致する。
- selfhost cache/fingerprint は summary、parking、claim、transition、decision、draft、approval、send の各 delta 後に full replay と完全一致する。
- supplemental delta の persisted manifest と full materialized build の既存完全一致 test を維持する。
- decision chain 40件と supersedes cycle で path-compressed result/audit を検証する。

## Verification

2026-07-15 に以下を実行し、すべて成功した。`cargo test --workspace` は failure 0、実 archive path を要求する既存の環境依存 test 1件だけが既定どおり ignored である。

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
