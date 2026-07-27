# Design: Bounded coding-agent archive import requests

## Context

The archive parsers intentionally produce all valid drafts before making an
online request. The import API admits only a configured maximum number of drafts
per request. That limit belongs to deployment configuration and cannot be
inferred safely by the CLI.

## Goals

- Keep every HTTP request at or below an operator-supplied positive batch size.
- Preserve archive order and aggregate the existing `ImportReport` contract.
- Keep iteration linear in the number of drafts.
- Fail before network I/O when the batch size is absent or invalid.

## Non-goals

- Discovering server configuration through a fallback endpoint.
- Making a multi-request archive import globally atomic.
- Resuming after a failed batch inside the same process.
- Changing v1 or v2 wire contracts.

## Decisions

### The batch size is a required CLI value

Both CLIs require `--batch-size=<count>`. Zero, malformed, blank, and missing
values fail before connecting. The deployment must set it no higher than
`limits.max_import_drafts`; the client does not carry a duplicate default.

### Drafts are consumed by a single forward iterator

Each request collects at most `batch_size` drafts from one `IntoIter`. This
avoids repeated `Vec::split_off` copies of the remaining tail and keeps request
formation linear.

### The first failed request terminates the run

Reports from successful requests are aggregated in order. When one request
fails, no later draft is sent. Already committed requests are not rolled back;
the existing deterministic identities make a full rerun safe.
