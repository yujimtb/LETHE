# Change: Bound coding-agent archive import requests

## Why

Claude Code and Codex archives can exceed the server's configured
`limits.max_import_drafts` request boundary. Sending an entire growing archive
as one request makes the daily importer fail even though every individual
Observation is valid.

## What Changes

- Require both coding-agent CLIs to receive an explicit positive
  `--batch-size`.
- Split drafts into ordered, bounded requests without repeatedly copying the
  unprocessed tail.
- Aggregate successful batch reports in request order and stop immediately
  after the first failed request.
- Document that operators must align the CLI value with the server's explicit
  `limits.max_import_drafts` setting.

## Impact

- `lethe-import-claude-code` and `lethe-import-codex` invocations must add
  `--batch-size=<count>`.
- No default, environment fallback, compatibility alias, or retry-from-another
  endpoint is introduced.
- Earlier successful batches remain durable if a later request fails; a rerun
  relies on the existing idempotency contract.
