## ADDED Requirements

### Requirement: CHRG-01 Channel records
The channel registry SHALL load channel records from ops configuration with an
identifier, kind, connection setting reference, default consent scope, reply SLO
value, channel-level and sender-level break-glass allowlists, and enabled flag.
Ingress from unregistered channels SHALL be quarantined.

#### Scenario: Configured channel is available
- **GIVEN** an ops configuration with an enabled channel record
- **WHEN** the channel registry is loaded
- **THEN** the channel record is available by identifier with its configured
  kind, connection reference, consent scope, SLO, break-glass declarations, and
  enabled state

#### Scenario: Unregistered channel is quarantined
- **GIVEN** an observation draft that references an unknown communication
  channel
- **WHEN** the ingestion gate applies channel context
- **THEN** the draft is quarantined instead of being stored as accepted

### Requirement: CHRG-02 Consent scope assignment
The system SHALL assign each inbound communication observation the channel
record's default consent scope in the ingestion gate. Example ops configuration
SHALL include the recommended defaults: organization spaces use
`org_federated`, and DMs, mentions, and self-authored communication use
`personal`.

#### Scenario: Channel default consent is applied
- **GIVEN** a registered channel with default consent scope `personal`
- **WHEN** a matching inbound communication observation is ingested
- **THEN** the stored observation carries that consent scope

#### Scenario: Filtering respects channel consent
- **GIVEN** communication observations stored with channel-derived consent
  scopes
- **WHEN** a read path applies filtering before exposure
- **THEN** observations outside the caller's consent scope are not exposed

### Requirement: CHRG-03 Break-glass declarations
The system SHALL expose break-glass allowlists through read-only projection data
for runtime mode decisions. LETHE SHALL NOT implement break-glass interruption
decision logic.

#### Scenario: Break-glass declarations are projected
- **GIVEN** channel-level and sender-level break-glass allowlists in ops
  configuration
- **WHEN** the break-glass projection is read
- **THEN** those declarations are returned for runtime consumption

#### Scenario: Decision logic remains outside LETHE
- **GIVEN** the LETHE runtime handles channel registry and projections
- **WHEN** break-glass implementation dependencies are inspected
- **THEN** LETHE exposes declarations without performing interruption or
  escalation decisions

### Requirement: CHRG-04 SLO material completeness
Each inbound communication observation SHALL include the message published time,
channel reference, sender, and thread context needed to recompute reply latency
from observations and send-record supplemental records.

#### Scenario: Reply latency can be folded from observations
- **GIVEN** an inbound communication observation and a matching send-record
  supplemental record
- **WHEN** the SLO fold is computed from stored records
- **THEN** reply latency is derived without calling an external channel API
