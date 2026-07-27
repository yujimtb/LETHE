## 1. Contract and service

- [x] 1.1 Define strict source-export query, item, and page types. [Owner: Implementer] [Spec: M03 Observation Lake, source-observation-export] Acceptance: serde rejects missing/unknown fields, zero/oversized limits, and impossible continuations.
- [x] 1.2 Implement AppService watermark validation and bounded ask_bot source-schema paging. [Owner: Implementer] [Spec: M03 Observation Lake] Acceptance: ordered pages skip unrelated schemas, stop at the fixed watermark, and never return partial success on scan exhaustion.
- [x] 1.3 Implement the exact v3 Axum endpoint and dedicated authorization scope. [Owner: Implementer] [Spec: M14 API Serving] Acceptance: route tests reject missing scope, invalid query, and alternate paths before source content is returned.

## 2. Backend and replay verification

- [x] 2.1 Add SQLite unit tests for watermark, gaps, unrelated rows, restart resume, and concurrent append. [Owner: Reviewer] [Spec: M03 Observation Lake] Acceptance: original-watermark replay has no duplicate, omission, or above-watermark row.
- [x] 2.2 Add normalized SQLite/PostgreSQL export parity to pre-NAS storage conformance. [Owner: Reviewer] [Spec: M03 Observation Lake] Acceptance: both backends return identical append sequence, outer Observation, continuation, and completion values.
- [x] 2.3 Add selfhost HTTP E2E tests for strict wire shape, authorization, and restart behavior. [Owner: Reviewer] [Spec: M14 API Serving] Acceptance: success and every negative case run without ignored required tests.

## 3. Documentation and release validation

- [x] 3.1 Document the exact endpoint, scope, watermark algorithm, limits, and no-fallback rule. [Owner: Implementer] [Spec: M03/M14] Acceptance: a Projection client can implement continuation without storage knowledge.
- [x] 3.2 Run format, clippy, dependency-layer checks, workspace tests, PostgreSQL/S3 conformance, and strict OpenSpec validation. [Owner: Reviewer] [Spec: all] Acceptance: all commands pass and no v1/v2 alias or direct-storage consumer contract was added.
