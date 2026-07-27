## ADDED Requirements

### Requirement: Dedicated authenticated source export
LETHE SHALL expose canonical ask_bot source Observations only through
`GET /api/v3/export/source-observations` and SHALL require `read:source-observations`.

#### Scenario: Principal lacks the dedicated scope
- **WHEN** a caller has another read scope but not `read:source-observations`
- **THEN** LETHE returns an authorization error and no Observation content

#### Scenario: Caller tries an alternate API
- **WHEN** a consumer requests canonical source pages through search, v1, or v2
- **THEN** no alias or fallback source-export contract exists

### Requirement: Closed export shape
The request SHALL contain only required `after_append_seq` and `limit` fields plus optional
`watermark`, and the response SHALL contain only the pinned watermark, next append sequence,
completion flag, and ordered items consisting of append sequence and immutable outer Observation.

#### Scenario: Unknown or missing query field
- **WHEN** a request omits a required field or supplies an unknown field
- **THEN** LETHE rejects it before storage paging

#### Scenario: Unsupported Observation exists
- **WHEN** the pinned Lake range contains an Observation other than exact
  `schema:askbot-source-observation@1.0.0`
- **THEN** that Observation is not returned and its payload is not interpreted

### Requirement: Stable append watermark
The first page SHALL pin the current storage maximum append sequence and every continuation SHALL
remain bounded by the caller-supplied exact watermark.

#### Scenario: Concurrent source append
- **WHEN** new Observations commit above the watermark while pages are read
- **THEN** the export completes at the original watermark without returning the new rows

#### Scenario: Impossible watermark
- **WHEN** a caller supplies a watermark above the current durable maximum
- **THEN** LETHE returns a typed validation error and no page

### Requirement: Bounded deterministic paging
Each page SHALL honor the configured output limit and scan bound, preserve ascending append
sequence, return an explicit continuation position, and declare completion independently of visible
item count.

#### Scenario: Page crosses unrelated rows
- **WHEN** unrelated schemas occur between two ask_bot source Observations
- **THEN** LETHE scans across them and returns the source Observations in append order

#### Scenario: Scan bound is exhausted
- **WHEN** the configured scan bound is reached before a safe page boundary
- **THEN** LETHE returns a typed service error rather than a partial successful page

### Requirement: Backend and restart parity
SQLite and PostgreSQL SHALL return the same normalized export pages and replay result for the same
Lake contents and watermark.

#### Scenario: Consumer restarts during export
- **WHEN** a consumer resumes with the last acknowledged append sequence and the original watermark
- **THEN** it receives the same remaining ordered items without duplication or omission
