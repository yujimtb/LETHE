## ADDED Requirements

### Requirement: ASP-01 専用v3原子的page endpoint

LETHE SHALL expose `POST /api/v3/import/atomic-observation-pages` as a distinct authenticated endpoint. The request SHALL contain exactly a non-blank `source_instance_id` and a non-empty ordered `drafts` array, SHALL reject unknown fields, and SHALL require a positive `X-LETHE-Admission-Generation`. The endpoint SHALL NOT select v1/v2 or partial-success behavior through a flag, alias, or fallback.

#### Scenario: v3を明示しないrequest
- **WHEN** a client calls v1/v2 or omits the required v3 generation
- **THEN** LETHE does not apply atomic-page semantics and the v3 adapter treats the call as failed

#### Scenario: 不明field
- **WHEN** the v3 request contains an unknown field or an empty page
- **THEN** LETHE rejects the request before validation or append

### Requirement: ASP-02 append前の全item検証

LETHE SHALL validate request limits, unique non-blank client references, server-derived identity, Registry schema/source contracts, authority, policy, time, payload, and blob references for every draft before making any page item visible.

#### Scenario: page内の1件がschema違反
- **WHEN** one draft in an otherwise valid page violates its exact Registry contract
- **THEN** LETHE returns an `atomic_page_rejected` typed Problem and appends zero Observations and zero page audit events

#### Scenario: blob参照が欠落
- **WHEN** one draft references a missing or digest-mismatched blob
- **THEN** LETHE rejects the whole page and no other draft becomes visible

### Requirement: ASP-03 単一transactionとACK

After all drafts pass preparation, LETHE SHALL perform cutover admission, blob-reference admission, global canonical identity resolution, Observation append, and page audit append in one backend transaction. A successful response SHALL contain exactly one input-ordered result per draft and SHALL use only `ingested` or `duplicate`. The response is an ACK that every item is durable.

#### Scenario: 全件新規
- **WHEN** every prepared draft is new and the transaction commits
- **THEN** every result is `ingested` with its Observation ID in input order

#### Scenario: 同一page再送
- **WHEN** the exact committed page is retried with fixed identity inputs
- **THEN** every result is `duplicate` with the existing ID and no new Observation is appended

#### Scenario: storage failure
- **WHEN** the backend fails before commit
- **THEN** LETHE returns a transient typed Problem and neither Observations nor the page audit event are committed

### Requirement: ASP-04 collision時の全rollback

Canonical collision SHALL be a terminal page failure. If any prepared draft resolves to a canonical collision, LETHE SHALL rollback new Observations, audit records, identity metrics, and other page mutations from the same request.

#### Scenario: duplicateとcollisionと新規が混在
- **WHEN** one page contains a duplicate, a canonical collision, and a new valid draft
- **THEN** LETHE returns `atomic_page_rejected`, preserves the pre-request ledger exactly, and does not ACK any item

### Requirement: ASP-05 backend parity

SQLite and PostgreSQL implementations SHALL produce normalized-equivalent results and rollback state for successful, duplicate, collision, validation, missing-blob, stale-generation, and injected-storage-failure fixtures.

#### Scenario: backend比較
- **WHEN** the same ordered page fixture and failure injection are run against both backends
- **THEN** normalized results, ledger delta, audit delta, and rollback state are equal

### Requirement: ASP-06 ask_bot source envelope

The Registry SHALL define a closed versioned `schema:askbot-source-observation` outer payload and explicit source contracts for Slack, Drive, Docs, Sheets, Slides, Forms, and note observers. The envelope SHALL preserve the adapter-provided source schema IRI, typed native identifier, source position, revision, native payload, tombstone, blob digests, and payload digest without a normalized catalog copy.

#### Scenario: source-native record
- **WHEN** an ask_bot adapter submits a valid source envelope under its registered observer
- **THEN** LETHE stores the envelope and native payload without renaming provider fields or consulting a mapping table

#### Scenario: observerとsourceの不一致
- **WHEN** a source envelope uses an observer not registered for that source contract
- **THEN** LETHE rejects the whole page before append

### Requirement: ASP-07 空source unitのv3 bootstrap

LETHE SHALL expose `POST /api/v3/source-units/{source_instance_id}/bootstrap` as a distinct `admin:cutover` endpoint for a new v3-only source unit. The request SHALL contain exactly non-blank `authority` and `reason`. LETHE SHALL atomically verify that the unit has no cutover state and no canonical Observation carrying the source instance, append an audited initial `v2_active` transition, and issue v2 admission generation 1. The endpoint SHALL NOT create a v1 credential, run bridge readiness, accept a protocol selector, or infer emptiness from client input.

#### Scenario: 空unitをbootstrap
- **WHEN** the source unit has no state and no canonical Observation
- **THEN** LETHE returns `v2_active` generation 1 and only the matching v2 credential is active

#### Scenario: dataまたはstateが既に存在
- **WHEN** the source unit has any canonical Observation or a state other than the unchanged pre-append v3 bootstrap state
- **THEN** LETHE rejects bootstrap without changing state, credentials, metrics, or Observations
