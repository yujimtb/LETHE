use super::*;

pub(super) fn revisions_after_cursor(
    revisions: Vec<lethe_adapter_gslides::gslides::client::SlideRevision>,
    cursor: Option<&str>,
    reset: bool,
) -> Vec<lethe_adapter_gslides::gslides::client::SlideRevision> {
    if cursor.is_none() || reset {
        return revisions;
    }

    let cursor = cursor.unwrap();
    let mut found = false;
    revisions
        .into_iter()
        .filter(|revision| {
            if found {
                true
            } else if revision.revision_id == cursor {
                found = true;
                false
            } else {
                false
            }
        })
        .collect()
}

pub(super) fn latest_revision_to_capture(
    revisions: &[lethe_adapter_gslides::gslides::client::SlideRevision],
) -> Option<&lethe_adapter_gslides::gslides::client::SlideRevision> {
    // The Google APIs used here only let us fetch the current presentation state,
    // so capturing anything older than the newest unseen revision would falsely
    // attach latest content to historical revision IDs.
    revisions.last()
}

pub(super) fn thread_root_ts(
    message: &lethe_adapter_slack::slack::client::SlackMessage,
) -> Option<&str> {
    if let Some(thread_ts) = message.thread_ts.as_deref() {
        return Some(thread_ts);
    }
    (message.reply_count > 0).then_some(message.ts.as_str())
}

pub(super) const IDLE_THREAD_RECHECK_INTERVAL: u64 = 8;

pub(super) fn discovered_slack_threads(
    observations: &[StoredObservation],
) -> Result<Vec<DiscoveredSlackThread>, SelfHostError> {
    let mut threads = HashMap::<SlackThreadKey, u64>::new();
    for stored in observations {
        let observation = &stored.observation;
        if observation.schema.as_str() != "schema:slack-message" {
            continue;
        }
        let source_instance = required_slack_catalog_field(
            observation.meta.get("source_instance"),
            "meta.source_instance",
            &observation.id,
        )?;
        let channel_id = required_slack_catalog_field(
            observation.payload.get("channel_id"),
            "payload.channel_id",
            &observation.id,
        )?;
        let ts = required_slack_catalog_field(
            observation.payload.get("ts"),
            "payload.ts",
            &observation.id,
        )?;
        let thread_ts = observation
            .payload
            .get("thread_ts")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                (observation
                    .payload
                    .get("reply_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
                    > 0)
                .then_some(ts)
            });
        let Some(thread_ts) = thread_ts else {
            continue;
        };
        let key = SlackThreadKey {
            source_instance: source_instance.to_owned(),
            channel_id: channel_id.to_owned(),
            thread_ts: thread_ts.to_owned(),
        };
        threads
            .entry(key)
            .and_modify(|append_seq| *append_seq = (*append_seq).min(stored.append_seq))
            .or_insert(stored.append_seq);
    }
    Ok(threads
        .into_iter()
        .map(|(key, observation_append_seq)| DiscoveredSlackThread {
            key,
            observation_append_seq,
        })
        .collect())
}

fn required_slack_catalog_field<'a>(
    value: Option<&'a serde_json::Value>,
    field: &str,
    observation_id: &lethe_core::domain::ObservationId,
) -> Result<&'a str, SelfHostError> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "Slack observation {observation_id} requires non-blank {field} for thread catalog discovery"
            ))
        })
}

pub(super) fn non_empty_state(value: Option<String>) -> Option<String> {
    value.filter(|raw| !raw.trim().is_empty())
}

pub(super) fn restricted_fields() -> Vec<RestrictedFieldSpec> {
    [
        "identities",
        "DoB",
        "Birthplace",
        "dob",
        "birthplace",
        "email",
        "generated_email",
        "SNS",
    ]
    .into_iter()
    .map(|field_path| RestrictedFieldSpec {
        field_path: field_path.into(),
        level: AccessScope::Restricted,
        mask_strategy: MaskStrategy::Exclude,
    })
    .collect()
}

pub(super) fn slack_ts_value(value: &str) -> Result<(i64, u32), SelfHostError> {
    let (seconds, fractional) = value.split_once('.').ok_or_else(|| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    if fractional.len() != 6 || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        )));
    }
    let seconds = seconds.parse::<i64>().map_err(|_| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    let micros = fractional.parse::<u32>().map_err(|_| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    Ok((seconds, micros))
}
