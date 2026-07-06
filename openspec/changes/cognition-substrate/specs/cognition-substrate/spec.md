## ADDED Requirements

### Requirement: ChatGPT Archive Import
The system SHALL import ChatGPT export JSON files from an archive working copy
under `chatgpt/`, map valid messages to `schema:chatgpt-message`, preserve
message timestamps as `published`, carry `backfill` metadata when requested, and
use identity keys shaped as `chatgpt:{conversation_id}:{message_id}:H(canonical)`.

#### Scenario: Fixture import is idempotent
- **GIVEN** a ChatGPT archive fixture with valid conversation messages
- **WHEN** the importer runs twice against the same online import API
- **THEN** the first run ingests the messages
- **AND** the second run reports the unchanged messages as duplicates

### Requirement: Supplemental Kind Anchor Policy
The system SHALL support per-kind `anchor_required` in the Supplemental Kind
Registry. Kinds with `anchor_required=true` SHALL reject empty anchors. Kinds
with `anchor_required=false` SHALL require `payload.origin` with actor,
occurred-at, and context identifier metadata.

#### Scenario: System event origin is required
- **GIVEN** an anchor-optional system-event supplemental kind
- **WHEN** a record omits anchors and includes complete `payload.origin`
- **THEN** registry validation accepts the record
- **WHEN** the same record omits `payload.origin`
- **THEN** registry validation rejects the record

### Requirement: Cognition Supplemental Kinds
The system SHALL register `reply-draft@1`, `reply-approval@1`,
`send-record@1`, `nudge-event@1`, `eos-state-transition@1`,
`mode-transition@1`, and `briefing-issue@1` with JSON Schema validation for
required fields and enum values.

#### Scenario: New kind schema rejects invalid payloads
- **GIVEN** each new cognition supplemental kind
- **WHEN** a payload is missing required fields or violates enum values
- **THEN** registry payload validation rejects the payload with field-level
  violations

### Requirement: MCP Supplemental Write
The MCP server SHALL expose exactly one generic `write_supplemental` tool for
supplemental writes. The tool SHALL require `write:supplemental`, SHALL describe
itself as post-processing for already-ingested lake observations, and SHALL use
the same registry and store validation path as the HTTP supplemental write API.

#### Scenario: MCP write validates scope and anchors
- **GIVEN** a read-only MCP token
- **WHEN** the client calls `write_supplemental`
- **THEN** the MCP response rejects the call for missing write scope
- **WHEN** a write-scoped token calls `write_supplemental` with an unresolved
  observation anchor
- **THEN** the MCP response rejects the call with supplemental validation

### Requirement: Freshness Projection
The system SHALL project source freshness from observations using configured
source thresholds and communication channel thresholds, returning source status
and missing or unobserved sources through `GET /projections/freshness`.

#### Scenario: Threshold miss is deterministic
- **GIVEN** observations for a configured source
- **WHEN** the latest observation age exceeds the configured threshold
- **THEN** the freshness projection marks that source missing
- **AND** replaying the same observations in a different order produces the same
  projection

### Requirement: Resume Snapshot Projection
The system SHALL project `session-summary@1`, `parking@1`, and open claims into
project-level resume cards with last activity time, latest session summary,
parking entries, and open claim entries.

#### Scenario: Latest project summary wins
- **GIVEN** multiple session summaries, parking entries, and open claims for one
  project
- **WHEN** the resume snapshot projection folds the records
- **THEN** it returns one project card with the latest summary
- **AND** replaying the same records in a different order produces the same
  projection

### Requirement: Plan State Projection
The system SHALL project open claims, parking counts and ages, and current
decisions after supersedes-chain resolution into project-level plan state.

#### Scenario: Superseded decisions are excluded
- **GIVEN** a project with a decision superseded by a later decision
- **WHEN** the plan-state projection folds the records
- **THEN** only the current decision appears
- **AND** open claim and parking ages are calculated from the configured
  projection time

### Requirement: Card Queue Projection
The system SHALL project `reply-draft@1`, `reply-approval@1`, and
`send-record@1` records into card states, use first-approval-wins across
interfaces, distinguish automatic sends, and expose the result through
`GET /projections/card-queue` with state, channel, automatic, limit, and cursor
filters.

#### Scenario: Approval and send chain reaches sent
- **GIVEN** one reply draft and multiple out-of-order approval records from
  different interfaces
- **WHEN** the card queue projection folds the records
- **THEN** the earliest approval determines the card state
- **AND** a later approved send record moves the card to sent

### Requirement: Claim Queue Backfill Filter
The claim queue projection SHALL preserve each claim's `backfill` flag and
SHALL allow reading groups with a backfill filter orthogonal to state filters.

#### Scenario: Backfill and live claims are separated
- **GIVEN** one open backfill claim and one open live claim
- **WHEN** claim queue groups are filtered with `backfill=true`
- **THEN** only the backfill claim group is returned
- **WHEN** groups are filtered with `backfill=false`
- **THEN** only the live claim group is returned

### Requirement: Public Search Evidence
The implementation SHALL keep corpus search on regex grep for this change and
record public MCP broad-query evidence after the current regex budget fix is
available. If the public query still exceeds budget, the implementation SHALL
track FTS materialization in a separate change.

#### Scenario: Public broad query is recorded
- **GIVEN** the public MCP endpoint and production corpus are available
- **WHEN** a broad one-word query is executed through public MCP
- **THEN** the result or the FTS follow-up decision is recorded in this change's
  task evidence
