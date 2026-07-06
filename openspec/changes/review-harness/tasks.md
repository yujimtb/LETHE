## 1. Requirement Extraction

- [x] 1.1 Implement spec delta parser for `RVH-01` (Spec: `review-harness`; Owner: implementer; Acceptance: unit tests cover valid SHALL extraction and missing or malformed ID failures)
- [x] 1.2 Add CLI command path for requirement extraction output (Spec: `review-harness`; Owner: implementer; Acceptance: command emits stable JSON for parsed requirements)

## 2. Coverage Matrix

- [x] 2.1 Implement automated test coverage annotation detection for `RVH-02` (Spec: `review-harness`; Owner: implementer; Acceptance: unit tests detect `covers: REQ-ID` annotations from test files)
- [x] 2.2 Implement manual evidence detection and matrix verification for `RVH-02` (Spec: `review-harness`; Owner: implementer; Acceptance: uncovered and unknown-evidence fixtures fail fast with explicit errors)

## 3. Diff And CI Integration

- [x] 3.1 Implement coverage matrix diff reporting for `RVH-03` (Spec: `review-harness`; Owner: implementer; Acceptance: unit tests cover new requirements, new evidence, lost evidence, and no-diff output)
- [x] 3.2 Integrate review-harness into LETHE CI (Spec: `review-harness`; Owner: implementer; Acceptance: CI runs the harness on pull requests)

## 4. Documentation And Cross-Repo Rollout

- [x] 4.1 Document review-harness conventions and agent-runtime rollout guidance (Spec: `review-harness`; Owner: implementer; Acceptance: docs define requirement ID format, annotation syntax, manual evidence syntax, and reusable CI invocation)
