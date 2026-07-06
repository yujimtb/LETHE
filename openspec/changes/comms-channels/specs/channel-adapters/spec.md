## ADDED Requirements

### Requirement: CHAD-01 Slack adapter
The system SHALL map Slack DM, mention, and joined-channel ingress for the
personal lake into observations. The Slack identity key SHALL follow the
existing Slack adapter rule, and re-importing the same Slack ingress SHALL
deduplicate every unchanged observation.

#### Scenario: Slack ingress kinds are mapped
- **GIVEN** Slack fixtures for a DM, a mention, and a joined-channel message
- **WHEN** the Slack adapter maps them into observation drafts
- **THEN** each draft carries the expected communication metadata and identity
  key

#### Scenario: Slack re-import is idempotent
- **GIVEN** a Slack message already stored in the lake
- **WHEN** the same ingress is imported again
- **THEN** the import reports the unchanged observation as a duplicate

### Requirement: CHAD-02 Gmail adapter
The system SHALL map inbound Gmail messages into observations while preserving
Message-ID and References thread structure. Gmail identity keys SHALL be shaped
as `gmail:{message_id}:H(canonical)`, `published` SHALL use the email Date, and
`get_thread` SHALL reconstruct an email thread as a conversation.

#### Scenario: Gmail message Date is preserved
- **GIVEN** a Gmail fixture with Message-ID, References, and Date headers
- **WHEN** the Gmail adapter maps the fixture
- **THEN** the draft uses the Date header as `published`
- **AND** the thread headers are available for conversation reconstruction

#### Scenario: Gmail re-import is idempotent
- **GIVEN** a Gmail message already stored in the lake
- **WHEN** the same message is imported again
- **THEN** the import reports the unchanged observation as a duplicate

### Requirement: CHAD-03 Discord adapter
The system SHALL map Discord DM and joined-server message ingress into
observations. Discord identity keys SHALL be shaped as
`discord:{channel_id}:{message_id}:H(canonical)`.

#### Scenario: Discord message is mapped
- **GIVEN** a Discord DM fixture and a joined-server channel fixture
- **WHEN** the Discord adapter maps them into observation drafts
- **THEN** each draft carries the Discord channel id, message id, sender, and
  canonical identity key

#### Scenario: Discord re-import is idempotent
- **GIVEN** a Discord message already stored in the lake
- **WHEN** the same ingress is imported again
- **THEN** the import reports the unchanged observation as a duplicate

### Requirement: CHAD-04 Runtime-owned subscriptions
The system SHALL place persistent subscriptions such as Discord gateway and
Slack socket mode in the runtime supervisor and feed LETHE through an
authenticated HTTP import path. LETHE SHALL NOT hold outbound communication
tokens or call outbound send APIs.

#### Scenario: Persistent subscription enters through import
- **GIVEN** a runtime supervisor receives a persistent-channel event
- **WHEN** it submits the event to LETHE through the authenticated import API
- **THEN** LETHE ingests the resulting observation draft without owning the
  channel subscription

#### Scenario: LETHE has no send capability
- **GIVEN** the LETHE implementation dependencies are inspected
- **WHEN** outbound communication send APIs and token paths are searched
- **THEN** no LETHE implementation dependency provides external send capability

### Requirement: CHAD-05 Freshness projection inclusion
The system SHALL include Slack, Gmail, and Discord channel records as freshness
projection sources when thresholds are configured. Channel freshness thresholds
SHALL be configurable per channel record.

#### Scenario: Silent channel is reported
- **GIVEN** an enabled communication channel with a configured freshness
  threshold and no matching observations
- **WHEN** the freshness projection is read
- **THEN** the channel is reported as unobserved or missing according to the
  projection rules
