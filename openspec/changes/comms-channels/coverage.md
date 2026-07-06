# SHALL Coverage: comms-channels

**Verified:** 2026-07-07  
**Commands:** `cargo test --workspace`; `openspec validate comms-channels --strict`

## Notes

- Communication freshness is implemented in `crates/projections/cognition`, the
  projection crate introduced by `cognition-substrate`. `comms-channels` does
  not introduce a second freshness projection.
- Slack socket mode, Gmail polling, and Discord gateway subscriptions are
  runtime-supervisor responsibilities. LETHE owns the observation draft
  contract, ingestion validation, registry enrichment, read-only projections,
  and idempotent storage.
- Production personal config now enables Discord channel
  `chan:discord-primary:1507676023314059275` for `kana's server/#general` and
  uses `connection_ref = "discord-primary-tera"` to reuse the existing `tera`
  bot on the runtime-supervisor side. LETHE still stores no Discord bot token.
- LETHE keeps no outbound communication API capability. The dependency scan for
  `send_message`, `chat_postMessage`, Gmail send, Discord send, and outbound
  send API terms returned no LETHE implementation dependency.

## Registry

| Spec | Implementation | Evidence |
| --- | --- | --- |
| CHRG-01 channel records and unregistered quarantine | `crates/registry/src/registry/channel.rs`, `crates/registry/src/registry/store.rs`, `apps/selfhost/src/self_host/config.rs`, `crates/engine/src/lake/ingestion.rs` | `unregistered_communication_channel_is_quarantined`, config duplicate validation tests |
| CHRG-02 channel consent scope assignment | `IngestionGate::apply_channel_context` assigns `ConsentRef` from `ChannelRecord.default_consent_scope` | `valid_observation_ingested`, selfhost API corpus/filtering regressions |
| CHRG-03 break-glass declarations exposed | `BreakGlassProjection`, `/projections/break-glass`, projection catalog entry `proj:break-glass` | selfhost projection routing/catalog tests under workspace test run |
| CHRG-04 SLO material completeness | communication meta adds channel id, sender, thread ref, reply due time, freshness threshold, break-glass declarations | `reply_slo_matches_send_records_through_reply_draft_anchor`, ingestion communication metadata tests |

## Adapters

| Spec | Implementation | Evidence |
| --- | --- | --- |
| CHAD-01 Slack DM, mention, channel ingress and duplicate behavior | `crates/adapters/slack/src/slack/client.rs`, `crates/adapters/slack/src/slack/mapper.rs`, `apps/selfhost/src/self_host/slack.rs`, `classify_slack_ingress` | `map_dm_mention_and_channel_ingress_kinds`, `missing_ingress_kind_is_rejected`, `classify_slack_ingress_distinguishes_dm_mention_and_channel`, `slack_duplicate_resend_deduplicated` |
| CHAD-02 Gmail message mapping, Date as published, thread structure | `crates/adapters/gmail/src/gmail.rs`, corpus thread reconstruction support | `maps_gmail_message_with_date_as_published_and_thread_headers`, Gmail idempotency and invalid-date tests, corpus thread tests |
| CHAD-03 Discord DM/server message mapping and identity key | `crates/adapters/discord/src/discord.rs` | `maps_discord_dm_message`, Discord idempotency tests |
| CHAD-04 Runtime-owned subscriptions and no LETHE send capability | Gmail and Discord adapter `fetch_incremental`/`fetch_snapshot` return explicit supervisor-owned errors; Slack requires explicit channel context | adapter runtime-ownership tests and dependency scan |
| CHAD-05 Three channels included in freshness projection | `freshness_thresholds` merges configured source thresholds with enabled channel thresholds; `FreshnessProjector` prefers `communication_channel_id` | `freshness_prefers_communication_channel_id_for_channel_sources`, `freshness_marks_threshold_misses_deterministically` |

## Ops Documentation

| Requirement | Artifact |
| --- | --- |
| Example ops config for channel records, consent defaults, SLO, freshness, break-glass, and Slack mentions | `config.example.toml` |
| Personal lake operational docs for enabling communication channels | `docs/development/personal-lake-ingestion.md` |
| Personal configs enable the Discord `tera` channel record while leaving Slack/Gmail absent until live ingress is configured | `deploy/personal-lake/config.toml`, `deploy/personal-lake/config.host.toml` |
