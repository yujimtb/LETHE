use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::governance::types::{
    AuditEvent, AuditEventKind, PrivacyAuditDecision, PrivacyAuditDetail,
};
use lethe_core::domain::values::ActorRef;

// ---------------------------------------------------------------------------
// AuditLog trait — audit event emission hook (M08 §9)
// ---------------------------------------------------------------------------

/// Trait for audit event sinks. Implementors can log to files, databases, etc.
pub trait AuditLog: Send + Sync {
    fn emit(&self, event: AuditEvent);
    fn events_since(&self, since: chrono::DateTime<chrono::Utc>) -> Vec<AuditEvent>;
    fn count(&self) -> usize;
}

// ---------------------------------------------------------------------------
// InMemoryAuditLog — bounded diagnostic mirror
// ---------------------------------------------------------------------------

const RECENT_AUDIT_EVENT_LIMIT: usize = 1024;

#[derive(Debug, Default)]
pub struct InMemoryAuditLog {
    events: Mutex<VecDeque<AuditEvent>>,
}

impl InMemoryAuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all_events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

impl AuditLog for InMemoryAuditLog {
    fn emit(&self, event: AuditEvent) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if events.len() == RECENT_AUDIT_EVENT_LIMIT {
            events.pop_front();
        }
        events.push_back(event);
    }

    fn events_since(&self, since: chrono::DateTime<chrono::Utc>) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|e| e.timestamp >= since)
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

// ---------------------------------------------------------------------------
// AuditEmitter — convenience builder for audit events
// ---------------------------------------------------------------------------

pub struct AuditEmitter {
    log: Arc<dyn AuditLog>,
    next_id: Mutex<u64>,
}

impl AuditEmitter {
    pub fn new(log: Arc<dyn AuditLog>) -> Self {
        Self {
            log,
            next_id: Mutex::new(1),
        }
    }

    pub fn emit(&self, actor: &ActorRef, kind: AuditEventKind, detail: serde_json::Value) {
        let mut counter = self
            .next_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = format!("audit:{}", *counter);
        *counter += 1;

        let event = AuditEvent {
            id,
            timestamp: chrono::Utc::now(),
            actor: actor.clone(),
            kind,
            detail,
        };
        self.log.emit(event);
    }

    pub fn emit_privacy_decision(
        &self,
        actor: &ActorRef,
        kind: AuditEventKind,
        subject: impl Into<String>,
        scope: impl Into<String>,
        decision: PrivacyAuditDecision,
        rule: impl Into<String>,
    ) {
        let detail = PrivacyAuditDetail {
            actor: actor.clone(),
            subject: subject.into(),
            scope: scope.into(),
            decision,
            rule: rule.into(),
            timestamp: chrono::Utc::now(),
        };
        self.emit(
            actor,
            kind,
            serde_json::to_value(detail).expect("privacy audit detail must serialize"),
        );
    }

    pub fn log(&self) -> &dyn AuditLog {
        self.log.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_emitter() -> (Arc<InMemoryAuditLog>, AuditEmitter) {
        let log = Arc::new(InMemoryAuditLog::new());
        let emitter = AuditEmitter::new(log.clone());
        (log, emitter)
    }

    #[test]
    fn emit_and_retrieve() {
        let (log, emitter) = make_emitter();
        let actor = ActorRef::new("actor:alice");
        emitter.emit(
            &actor,
            AuditEventKind::WriteExecution,
            serde_json::json!({"target": "obs:1"}),
        );
        emitter.emit(
            &actor,
            AuditEventKind::Export,
            serde_json::json!({"scope": "full"}),
        );

        assert_eq!(log.count(), 2);
        let all = log.all_events();
        assert_eq!(all[0].kind, AuditEventKind::WriteExecution);
        assert_eq!(all[1].kind, AuditEventKind::Export);
    }

    #[test]
    fn events_since_filters_by_time() {
        let (log, emitter) = make_emitter();
        let actor = ActorRef::new("actor:bob");
        let before = chrono::Utc::now();
        emitter.emit(&actor, AuditEventKind::PolicyDenial, serde_json::json!({}));

        let events = log.events_since(before);
        assert_eq!(events.len(), 1);

        // Future time returns empty
        let future = chrono::Utc::now() + chrono::Duration::hours(1);
        let empty = log.events_since(future);
        assert!(empty.is_empty());
    }

    #[test]
    fn ids_are_sequential() {
        let (_log, emitter) = make_emitter();
        let actor = ActorRef::new("actor:test");
        emitter.emit(&actor, AuditEventKind::Approval, serde_json::json!({}));
        emitter.emit(&actor, AuditEventKind::Rejection, serde_json::json!({}));

        let events = emitter
            .log()
            .events_since(chrono::DateTime::<chrono::Utc>::MIN_UTC);
        assert_eq!(events[0].id, "audit:1");
        assert_eq!(events[1].id, "audit:2");
    }

    #[test]
    fn in_memory_diagnostic_mirror_is_bounded() {
        let (log, emitter) = make_emitter();
        let actor = ActorRef::new("actor:test");
        for _ in 0..=RECENT_AUDIT_EVENT_LIMIT {
            emitter.emit(&actor, AuditEventKind::Approval, serde_json::json!({}));
        }

        let events = log.all_events();
        assert_eq!(events.len(), RECENT_AUDIT_EVENT_LIMIT);
        assert_eq!(events.first().unwrap().id, "audit:2");
        assert_eq!(events.last().unwrap().id, "audit:1025");
    }

    #[test]
    fn privacy_decision_audit_contains_required_content() {
        let (log, emitter) = make_emitter();
        let actor = ActorRef::new("actor:privacy-gate");
        emitter.emit_privacy_decision(
            &actor,
            AuditEventKind::ConsentGate,
            "person:1",
            "record:message:1",
            PrivacyAuditDecision::Deny,
            "latest consent decision is opted_out",
        );
        emitter.emit_privacy_decision(
            &actor,
            AuditEventKind::RetractionShield,
            "person:1",
            "record:message:1",
            PrivacyAuditDecision::Shield,
            "typed retraction target",
        );
        emitter.emit_privacy_decision(
            &actor,
            AuditEventKind::BlobAuthorization,
            "person:1",
            "record:message:1",
            PrivacyAuditDecision::Visible,
            "visible blob reference index",
        );
        let events = log.all_events();
        assert_eq!(events.len(), 3);
        for event in events {
            let detail: PrivacyAuditDetail = serde_json::from_value(event.detail).unwrap();
            assert_eq!(detail.actor, actor);
            assert_eq!(detail.subject, "person:1");
            assert_eq!(detail.scope, "record:message:1");
            assert!(!detail.rule.is_empty());
        }
    }

    #[test]
    fn empty_log() {
        let log = InMemoryAuditLog::new();
        assert_eq!(log.count(), 0);
        assert!(log.all_events().is_empty());
    }
}
