use crate::adapter::idempotency::CANONICAL_JSON_META_KEY;
use crate::adapter::traits::{Cursor, ObservationDraft, RawData, SourceAdapter};

pub fn source_adapter_contract<A: SourceAdapter>(
    adapter: &A,
    raw: &RawData,
    cursor: Option<&Cursor>,
) {
    let heartbeat = adapter.heartbeat();
    assert_eq!(heartbeat.observer, *adapter.observer_ref());
    assert_eq!(
        heartbeat.source_system.as_ref(),
        Some(adapter.source_system_ref())
    );

    for draft in adapter.to_observations(raw) {
        assert_eq!(draft.observer, *adapter.observer_ref());
        assert_eq!(
            draft.source_system.as_ref(),
            Some(adapter.source_system_ref())
        );
        assert!(!draft.idempotency_key.as_str().trim().is_empty());
    }

    match adapter.fetch_incremental(cursor) {
        crate::adapter::traits::FetchResult::Ok { next_cursor, .. } => {
            if let Some(next_cursor) = next_cursor {
                assert!(!next_cursor.value.trim().is_empty());
            }
        }
        crate::adapter::traits::FetchResult::Error(_) => {}
    }
}

pub fn canonical_identity_stable_under_side_state_change(
    before: &ObservationDraft,
    after: &ObservationDraft,
) {
    assert_eq!(before.idempotency_key, after.idempotency_key);
    assert_eq!(
        before.meta.get(CANONICAL_JSON_META_KEY),
        after.meta.get(CANONICAL_JSON_META_KEY)
    );
}

pub fn canonical_identity_changes_on_content_change(
    before: &ObservationDraft,
    after: &ObservationDraft,
) {
    assert_ne!(before.idempotency_key, after.idempotency_key);
    assert_ne!(
        before.meta.get(CANONICAL_JSON_META_KEY),
        after.meta.get(CANONICAL_JSON_META_KEY)
    );
}
