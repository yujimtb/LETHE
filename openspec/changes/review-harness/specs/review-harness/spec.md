## ADDED Requirements

### Requirement: RVH-01 Spec delta SHALL extraction
The review harness SHALL extract every SHALL requirement ID from OpenSpec delta spec files. The harness SHALL fail when a SHALL statement has no explicit requirement ID or when a requirement ID does not match the canonical uppercase prefix plus two digit number format.

#### Scenario: Valid spec delta is parsed
- **WHEN** the harness reads a spec delta containing SHALL statements with valid requirement IDs
- **THEN** it outputs those requirement IDs with their source file and requirement text

#### Scenario: Invalid requirement ID fails
- **WHEN** the harness reads a SHALL statement with a missing or malformed requirement ID
- **THEN** it exits with an error that identifies the offending source file

### Requirement: RVH-02 Coverage matrix generation
The review harness SHALL detect automated coverage annotations in test code and manual evidence records in tasks.md. The harness SHALL generate a requirement ID by evidence coverage matrix. The harness SHALL fail when any extracted SHALL requirement has no evidence.

#### Scenario: Covered requirements pass verification
- **WHEN** every extracted SHALL requirement has automated coverage or manual evidence
- **THEN** the harness reports the coverage matrix and exits successfully

#### Scenario: Uncovered requirement fails verification
- **WHEN** at least one extracted SHALL requirement has no automated coverage and no manual evidence
- **THEN** the harness reports the uncovered requirement ID and exits with a failure

#### Scenario: Unknown evidence reference fails verification
- **WHEN** automated coverage or manual evidence references an unknown requirement ID
- **THEN** the harness reports the unknown evidence reference and exits with a failure

### Requirement: RVH-03 PR coverage diff reporting
The review harness SHALL compare two coverage matrix snapshots and report newly introduced requirements, newly introduced evidence, and lost evidence. The report SHALL be deterministic and suitable for CI output.

#### Scenario: Coverage diff is reported
- **WHEN** the harness compares a base matrix snapshot and a head matrix snapshot
- **THEN** it reports new requirements, new evidence, and lost evidence in stable sorted order

#### Scenario: No coverage diff is explicit
- **WHEN** the base matrix snapshot and head matrix snapshot contain the same requirements and evidence
- **THEN** it reports that no coverage diff exists
