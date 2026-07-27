## ADDED Requirements

### Requirement: SBA-01 認証済みcontent-addressed PUT

LETHE SHALL expose `PUT /api/v3/import/source-blobs/{sha256}` for the general Observation BlobStore. The endpoint SHALL require `write:observations`, a non-blank `X-LETHE-Source-Instance`, and a positive `X-LETHE-Admission-Generation`, and SHALL enforce cutover admission before accepting bytes.

#### Scenario: 未認証upload
- **WHEN** a request lacks the required scope, source instance, or valid generation
- **THEN** LETHE rejects it before reading bytes into the BlobStore

### Requirement: SBA-02 digestとsizeの厳格検証

The path digest SHALL be exactly 64 lowercase hexadecimal characters. The body SHALL be bounded by configured `limits.max_blob_bytes`; LETHE SHALL compute SHA-256 over the exact body and require it to equal the path digest before calling `BlobStore::put_blob`.

#### Scenario: digest不一致
- **WHEN** the body SHA-256 differs from the path
- **THEN** LETHE returns a validation Problem and does not store the body

#### Scenario: size超過
- **WHEN** the request body exceeds `limits.max_blob_bytes`
- **THEN** LETHE rejects the request with the configured maximum and stores no object

### Requirement: SBA-03 冪等receipt

Successful upload SHALL return a strict receipt containing the exact `blob:sha256:<digest>` reference and byte length. Reuploading identical bytes SHALL return the same reference without creating another logical blob. No operational-blob store or local-file fallback SHALL be attempted.

#### Scenario: 同一blob再送
- **WHEN** the same digest and bytes are uploaded twice
- **THEN** both responses contain the same blob reference and the general BlobStore contains one content-addressed object

### Requirement: SBA-04 Observation参照admission

A successful blob receipt alone SHALL NOT create an Observation. Atomic page append SHALL revalidate every referenced blob inside its transaction before making any Observation visible.

#### Scenario: upload後page失敗
- **WHEN** blob upload succeeds and the subsequent atomic page fails
- **THEN** no Observation references the blob and the object is only eligible for the existing audited orphan-GC process
