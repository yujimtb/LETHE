# Coding-agent import batching

## ADDED Requirements

### Requirement: Coding-agent CLIs require an explicit request bound

`lethe-import-claude-code` and `lethe-import-codex` SHALL require
`--batch-size=<count>`. The value SHALL be a positive integer. A missing, blank,
zero, or malformed value SHALL fail before any import request is sent. The
client SHALL NOT provide a default or infer the server limit.

#### Scenario: Missing batch size is rejected

- **WHEN** an operator invokes either coding-agent importer without
  `--batch-size`
- **THEN** the process fails with an actionable missing-argument error
- **AND** it sends no import request

#### Scenario: Invalid batch size is rejected

- **WHEN** `--batch-size` is zero or is not an integer
- **THEN** the process fails before connecting to the import API

### Requirement: Archive drafts are sent as ordered bounded requests

The shared import client SHALL consume the draft vector in source order and
send requests containing no more than the explicit batch size. Request
formation SHALL be linear in the number of drafts and SHALL NOT repeatedly copy
the unprocessed tail.

#### Scenario: Final partial batch is preserved

- **WHEN** three drafts are imported with a batch size of two
- **THEN** the client sends request sizes two and one, in that order

### Requirement: Batch reports and failures are deterministic

The shared client SHALL add all count fields and append result entries in
request order. It SHALL stop after the first failed request and SHALL NOT send
later batches. Successful earlier requests remain durable and a rerun uses the
existing idempotency contract.

#### Scenario: Successful reports are aggregated

- **WHEN** all bounded requests succeed
- **THEN** the returned report contains the sum of every count
- **AND** its result entries preserve request order

#### Scenario: A failed request stops later work

- **WHEN** the second of three one-draft requests fails
- **THEN** the third request is not sent
- **AND** the client returns the second request's error
