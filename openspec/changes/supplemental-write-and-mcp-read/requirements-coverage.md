# Requirements Coverage: supplemental-write-and-mcp-read

**Date:** 2026-07-06
**Scope:** Track I integration review after Tracks A-H

## Summary

| Area | Judgement | Evidence |
| --- | --- | --- |
| Supplemental write + kind registry | Pass | Unit and E2E tests cover registration, schema validation, authz, store invariants, conflicts, and HTTP write path. |
| Claim queue + decision projection | Pass | Unit tests cover fold semantics; E2E covers read API plus POST-to-projection integration. |
| Coding-agent archive/import/corpus | Pass for local implementation | Archive sync and importer tests cover append-only preservation, backbone-only mapping, idempotency, Codex measured schema, and corpus thread restoration. |
| MCP read port | Pass | E2E covers listener separation, OAuth JWT validation, metadata, tools, tool annotations, and projection-only reads. Live Tailscale Funnel exposes only the MCP port, Auth0 DCR/JWT smoke passed, and claude.ai, ChatGPT, Claude Code, and Codex all returned live `search_lake` data from the public MCP endpoint. |

## SHALL Matrix

| Requirement | Judgement | Evidence |
| --- | --- | --- |
| SKIND-01: Registry holds `SupplementalKindSchema` and resolves `kind@major`. | Pass | `crates/registry/src/registry/supplemental_kind.rs`, `crates/registry/src/registry/store.rs`; tests `supplemental_kind_register_and_get_by_kind_and_major_version`, version-rule tests. |
| SKIND-02: payload validation is a pure JSON Schema check with field violations. | Pass | `validate_supplemental_payload`; tests `payload_violation_fields_include_required_type_and_enum`, `payload_violation_fields_include_missing_required_field`. |
| SKIND-03: `supplemental.reject_unregistered_kinds` rejects unregistered kinds for personal lake. | Pass | `apps/selfhost/src/self_host/config.rs`, `deploy/personal-lake/config.toml`; test `unregistered_supplemental_kind_is_rejected`. |
| SKIND-04: initial six kind schemas are registered. | Pass | `base_supplemental_kind_schemas()`, `seed_registry()`; tests for `claim@1`, `parking@1`, transition claim-anchor validation. |
| SUPW-01: `POST /supplementals` returns 201 and persists records. | Pass | `apps/selfhost/src/self_host/app/supplemental_write.rs`; tests `supplemental_post_returns_201_and_persists_across_restart`, `supplemental_post_updates_claim_queue_projection_state`. |
| SUPW-02: write endpoint requires `write:supplemental`. | Pass | `create_supplemental` calls `authorize_headers`; test `supplemental_post_requires_write_scope_and_does_not_write_on_forbidden`. |
| SUPW-03: Store invariant failures map to 422 and AppendOnly conflicts map to 409. | Pass | `map_supplemental_store_error`; tests `supplemental_post_maps_store_invariants_to_422_details`, `supplemental_post_same_id_conflicts_but_same_content_different_uuid_is_allowed`. |
| SUPW-04: kind schema validation runs before Store insert. | Pass | `RegistryStore::validate_supplemental_record_kind` in write path; test `supplemental_post_rejects_claim_missing_verification_mode_before_write`. |
| SUPW-05: IDs are client UUIDs and content-based write dedup is not performed. | Pass | `validate_supplemental_id`; same-content different UUID E2E passes. |
| SUPW-06: `created_by` is stable actor; model goes to `model_version`. | Pass | README/spec docs and POST fixtures use `actor:extraction-pass` plus `model_version`. |
| CLQ-01: Consumers use projection, not raw supplemental, for action reads. | Pass | HTTP/MCP read surfaces call `claim_queue_response*` / `decision_search_response`; MCP tests assert projection metadata. |
| CLQ-02: claim dedup uses kind + derivedFrom set + payload hash, excluding `model_version`. | Pass | `ClaimDedupKey`; test `batch_rerun_claims_deduplicate_and_keep_absorbed_ids`. |
| CLQ-03: state fold is deterministic over transition and verification chains. | Pass | `fold_claim_states`; tests `replay_is_deterministic_for_different_input_orders`, `invalid_transition_is_skipped_and_audited`, `supplemental_post_updates_claim_queue_projection_state`. |
| CLQ-04: queue API returns same-origin groups. | Pass | `claim_groups`; tests `same_conversation_claims_are_returned_as_one_group`, `claim_queue_api_filters_pages_and_searches_decisions`. |
| CLQ-05: decision ledger search resolves supersedes chains. | Pass | `decision_views`; tests `decision_supersedes_chain_sets_superseded_by_on_old_decision`, `decision_post_anchored_to_imported_codex_observation_is_searchable`. |
| CLQ-06: read APIs expose claim queue and decisions with auth, filters, paging, metadata, and stale errors. | Pass | `apps/selfhost/src/self_host/app/projection_api.rs`, `server.rs`; E2E `claim_queue_api_filters_pages_and_searches_decisions`. |
| CAGT-01: source archive uses append-only daily sync and reserves `claude-code/`, `codex/`, `chatgpt/`. | Pass for configured host | `handoffs/track-f.md`; scheduled task `AgentSourceArchiveDaily`; manual sync commits recorded. |
| CAGT-02: importer writes backbone only and excludes tool results/argument bodies. | Pass | `crates/adapters/coding-agent`; tests `env_tool_result_content_never_enters_canonical`, `codex_fixture_excludes_tool_results_and_argument_body_from_canonical`. |
| CAGT-03: sidechain/subagent transcripts preserve parent metadata for thread restoration. | Pass | importer metadata plus corpus `ThreadResponse.structure`; tests `codex_subagent_metadata_is_preserved`, `coding_agent_get_thread_preserves_parent_child_sessions`. |
| CAGT-04: per-message identity and event timestamps are deterministic and idempotent. | Pass | importer identity tests, real archive subset evidence in tasks, and I2 decision-on-imported-observation E2E. |
| CAGT-05: Codex path/schema/sidechain measurements are recorded. | Pass | `specs/coding-agent-adapters/spec.md` Codex measurement section. |
| MCPR-01: MCP uses a dedicated listener; Funnel must expose only MCP port. | Pass | `main.rs`, `config.rs`, `compose.yaml`, README; test `mcp_and_internal_api_routes_are_separate`; live `tailscale funnel status --json` showed HTTPS 443 `/` proxying only to `http://127.0.0.1:8090`, and public `/health/deep` returned 404. |
| MCPR-02: Streamable HTTP transport is implemented. | Pass for live public endpoint | `apps/selfhost/src/self_host/mcp.rs`; test `five_mcp_tools_have_contracts_and_read_via_projection`; live public `POST https://yujiws.tail474356.ts.net/mcp` returned a valid JSON-RPC `tools/list` result with an Auth0-issued JWT. |
| MCPR-03: OAuth resource-server metadata and JWT validation; no token issuance/API key auth. | Pass | `mcp.rs`, `mcp_contract.rs`; tests `protected_resource_metadata_contract_is_public`, `mcp_jwt_validation_rejects_expired_and_wrong_audience_and_accepts_valid`; Auth0 tenant `lethe-mcp.jp.auth0.com` has API `LETHE MCP Read Port` with identifier/audience `https://yujiws.tail474356.ts.net/mcp`, DCR enabled, public JWKS loaded locally, tokenless public `/mcp` returned 401 + `WWW-Authenticate`, and Auth0-issued JWT returned 5 tools. |
| MCPR-04: exactly five read-only tools and all read projections only. | Pass | `mcp_contract.rs`; test `five_mcp_tools_have_contracts_and_read_via_projection` asserts names, descriptions, projection reads, and read-only annotations. |
| MCPR-05: personal lake corpus indexes all text-bearing personal observations. | Pass | `CorpusMode::PersonalAllText`; tests `personal_all_text_indexes_personal_lake_source_types`, `personal_corpus_grep_hits_all_text_source_types`. |
| MCPR-06: README/ops docs state PC/selfhost/Funnel uptime constraint. | Pass | `README.md`, `docs/development/personal-lake-ingestion.md`, `handoffs/track-h.md`. |

## Track I Evidence

- `supplemental_post_updates_claim_queue_projection_state`: claim POST appears as `open`, then `claim-transition@1` POST changes it to `parked`.
- `decision_post_anchored_to_imported_codex_observation_is_searchable`: Codex JSONL is imported through `CodexImporter`; a decision anchored to the persisted `sys:codex` observation is returned by `/projections/decisions`.
- Public Funnel/Auth0 evidence: `tailscale funnel status --json` exposed only `yujiws.tail474356.ts.net:443` -> `http://127.0.0.1:8090`; public metadata returned `authorization_servers = ["https://lethe-mcp.jp.auth0.com/"]` and `resource = "https://yujiws.tail474356.ts.net/mcp"`; public `/mcp` without token returned 401 + `WWW-Authenticate`; Auth0 DCR smoke client creation/deletion passed; Auth0-issued JWT for audience `https://yujiws.tail474356.ts.net/mcp` returned the five MCP tools.
- Live client evidence: claude.ai custom connector, ChatGPT custom app, Claude Code, and Codex each called `search_lake` on the public MCP endpoint with `query = "aquisition"`, `source_types = ["github-commit"]`, and `limit = 3`; each returned `result_count = 1` and `first_record_id = corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345`.
- Runtime repair evidence: selfhost bootstrap now rebuilds and materializes the projection snapshot from persisted observations/supplementals on startup; filtered grep now applies type filters before trigram indexing so large personal-lake searches do not time out before filtering.
- `cargo test -p lethe-e2e --test self_host_api -- --nocapture`: pass, 18 tests.
- `python scripts/personal_lake_pipeline_smoke.py --work-dir <temp>`: pass; synthetic claude/github imports produced 9 observations and duplicate re-runs were idempotent.
- `python scripts/personal_lake_w0_check.py --config <temp>/config.toml --db <temp>/lethe.sqlite3 --base-url http://127.0.0.1:18080 --api-token-env LETHE_API_SYNC_TOKEN`: pass against a locally started selfhost using the smoke DB.
