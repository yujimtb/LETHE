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
    if message.reply_count == 0 {
        return None;
    }

    Some(message.thread_ts.as_deref().unwrap_or(message.ts.as_str()))
}

pub(super) fn thread_cursor_key(channel_id: &str, thread_ts: &str) -> String {
    format!("slack:{channel_id}:thread:{thread_ts}:oldest_ts")
}

pub(super) fn known_thread_roots_from_observations(
    observations: &[Observation],
    channel_id: &str,
) -> BTreeSet<String> {
    observations
        .iter()
        .filter_map(|observation| {
            if observation.schema.as_str() != "schema:slack-message" {
                return None;
            }

            if observation
                .payload
                .get("channel_id")
                .and_then(|value| value.as_str())
                != Some(channel_id)
            {
                return None;
            }

            let ts = observation
                .payload
                .get("ts")
                .and_then(|value| value.as_str())?;
            let thread_ts = observation
                .payload
                .get("thread_ts")
                .and_then(|value| value.as_str());
            let reply_count = observation
                .payload
                .get("reply_count")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);

            if thread_ts == Some(ts) || (thread_ts.is_none() && reply_count > 0) {
                return Some(ts.to_string());
            }

            None
        })
        .collect()
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

pub(super) fn slack_ts_value(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0)
}
