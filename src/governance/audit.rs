use std::sync::{Arc, Mutex};

use crate::domain::values::ActorRef;
use crate::governance::types::{AuditEvent, AuditEventKind};

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
// InMemoryAuditLog — MVP in-memory implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct InMemoryAuditLog {
    events: Mutex<Vec<AuditEvent>>,
}

impl InMemoryAuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all_events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl AuditLog for InMemoryAuditLog {
    fn emit(&self, event: AuditEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
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
    fn empty_log() {
        let log = InMemoryAuditLog::new();
        assert_eq!(log.count(), 0);
        assert!(log.all_events().is_empty());
    }
}
