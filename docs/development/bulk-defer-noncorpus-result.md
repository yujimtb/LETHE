# Bulk non-corpus defer implementation result

- Date: 2026-07-15
- Branch: `fix/bulk-defer-noncorpus`
- Base: `a00e14a32dc031acf20213aa0291d6ab94c854c5`
- Scope: explicit bulk import sessions that defer all non-corpus projections

## Result

The implementation and verification are complete in this worktree.

- `POST /api/import/bulk-sessions/begin` persists a session and marks every
  non-corpus projection stale.
- Session-bound observation imports durably append canonical observations and
  incrementally catch the persistent corpus index up without invoking
  non-corpus materialization.
- `POST /api/import/bulk-sessions/{session_id}/end` fixes the final canonical
  high-water and publishes one non-corpus rebuild. Successful retries are
  idempotent and do not rebuild again.
- Non-corpus reads return `503 projection_stale` during the session; corpus
  reads remain available.
- Abandoned `deferred` or `catching_up` sessions are recovered during the next
  bootstrap. Corrupt persisted state fails explicitly.
- Source sync and supplemental writes conflict with an active bulk session so
  that they cannot publish a mixed non-corpus generation.

For cumulative request sizes `N_i`, the old topology-changing path could pay
`sum(T_full(N_i))`. The new path pays append plus incremental corpus work for
each request and one `T_full(N)` at end. This removes the request-level O(N^2)
term and is expected to be O(N) in total observations for the current ordinary
identity-bucket workload, subject to the documented internal cost of one full
projector run.

## Verification

The following commands completed successfully:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p lethe-selfhost bulk_import_session`

The regression test verifies zero non-corpus rebuilds across two session
append requests, corpus search availability, non-corpus stale reads, exactly
one end rebuild, idempotent end retry, equality with the normal sequential
materialization path, and bootstrap recovery after an abandoned session.

No production service, data, volume, WSL instance, Docker stack, or Windows
service was touched.

## Commit blocker

All implementation files were staged, but the sandbox denied both commit
attempts because the worktree's shared Git metadata is outside the writable
workspace roots:

```text
fatal: Unable to create
'D:/userdata/docs/projects/skcollege_database/.git/worktrees/wt-lethe-a/index.lock':
Permission denied
```

There is no stale `index.lock`. The worktree points to the shared Git directory
shown above, while the writable scope only includes this worktree and the
mission `codex` directory. Committing requires write authority for that shared
Git metadata path; no permission bypass was attempted.
