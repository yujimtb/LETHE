use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::runtime::config::HealthConfig;
use lethe_core::domain::values::ObserverRef;

// ---------------------------------------------------------------------------
// HeartbeatPayload — what an observer sends (M15 §8.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub status: ObserverStatus,
    pub last_successful_capture_at: Option<DateTime<Utc>>,
    pub pending_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverStatus {
    Alive,
    Degraded,
    ShuttingDown,
}

// ---------------------------------------------------------------------------
// GapAlert — emitted when heartbeat silence is detected
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapAlert {
    pub observer: ObserverRef,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub gap_duration: Duration,
    pub threshold: Duration,
}

// ---------------------------------------------------------------------------
// HeartbeatMonitor — tracks observer heartbeats and detects gaps
// ---------------------------------------------------------------------------

pub struct HeartbeatMonitor {
    config: HealthConfig,
    last_heartbeats: HashMap<ObserverRef, HeartbeatRecord>,
}

#[derive(Debug, Clone)]
struct HeartbeatRecord {
    received_at: DateTime<Utc>,
    payload: HeartbeatPayload,
}

impl HeartbeatMonitor {
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            last_heartbeats: HashMap::new(),
        }
    }

    /// Record a heartbeat from an observer.
    pub fn receive_heartbeat(&mut self, observer: ObserverRef, payload: HeartbeatPayload) {
        self.last_heartbeats.insert(
            observer,
            HeartbeatRecord {
                received_at: Utc::now(),
                payload,
            },
        );
    }

    /// Record a heartbeat with explicit timestamp (for testing).
    pub fn receive_heartbeat_at(
        &mut self,
        observer: ObserverRef,
        payload: HeartbeatPayload,
        at: DateTime<Utc>,
    ) {
        self.last_heartbeats.insert(
            observer,
            HeartbeatRecord {
                received_at: at,
                payload,
            },
        );
    }

    /// Check all registered observers for gap alerts at the given time.
    pub fn detect_gaps_at(&self, now: DateTime<Utc>) -> Vec<GapAlert> {
        let mut alerts = Vec::new();
        for (observer, record) in &self.last_heartbeats {
            let threshold = self.max_gap_for(observer);
            let elapsed = now
                .signed_duration_since(record.received_at)
                .to_std()
                .unwrap_or(Duration::ZERO);
            if elapsed > threshold {
                alerts.push(GapAlert {
                    observer: observer.clone(),
                    last_heartbeat_at: Some(record.received_at),
                    gap_duration: elapsed,
                    threshold,
                });
            }
        }
        alerts
    }

    /// Check for gaps using current time.
    pub fn detect_gaps(&self) -> Vec<GapAlert> {
        self.detect_gaps_at(Utc::now())
    }

    /// Get the last heartbeat for an observer.
    pub fn last_heartbeat(&self, observer: &ObserverRef) -> Option<&HeartbeatPayload> {
        self.last_heartbeats.get(observer).map(|r| &r.payload)
    }

    /// Number of tracked observers.
    pub fn tracked_count(&self) -> usize {
        self.last_heartbeats.len()
    }

    fn max_gap_for(&self, observer: &ObserverRef) -> Duration {
        self.config
            .observer_overrides
            .get(observer.as_str())
            .map(|o| o.max_gap)
            .unwrap_or(self.config.default_max_gap)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_monitor() -> HeartbeatMonitor {
        HeartbeatMonitor::new(HealthConfig {
            default_heartbeat_interval: Duration::from_secs(60),
            default_max_gap: Duration::from_secs(300),
            observer_overrides: HashMap::new(),
        })
    }

    fn alive_payload() -> HeartbeatPayload {
        HeartbeatPayload {
            status: ObserverStatus::Alive,
            last_successful_capture_at: Some(Utc::now()),
            pending_count: 0,
        }
    }

    #[test]
    fn no_alerts_when_heartbeat_recent() {
        let mut monitor = default_monitor();
        let obs = ObserverRef::new("obs:slack-crawler");
        let now = Utc::now();
        monitor.receive_heartbeat_at(obs, alive_payload(), now);

        let alerts = monitor.detect_gaps_at(now + chrono::Duration::seconds(60));
        assert!(alerts.is_empty());
    }

    #[test]
    fn alert_when_heartbeat_silent_beyond_threshold() {
        let mut monitor = default_monitor();
        let obs = ObserverRef::new("obs:slack-crawler");
        let past = Utc::now() - chrono::Duration::seconds(600);
        monitor.receive_heartbeat_at(obs, alive_payload(), past);

        let alerts = monitor.detect_gaps_at(Utc::now());
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].observer, ObserverRef::new("obs:slack-crawler"));
        assert!(alerts[0].gap_duration > Duration::from_secs(300));
    }

    #[test]
    fn per_observer_override_threshold() {
        use crate::runtime::config::ObserverHealthThreshold;

        let mut config = HealthConfig {
            default_heartbeat_interval: Duration::from_secs(60),
            default_max_gap: Duration::from_secs(300),
            observer_overrides: HashMap::new(),
        };
        config.observer_overrides.insert(
            "obs:sensor".into(),
            ObserverHealthThreshold {
                heartbeat_interval: Duration::from_secs(10),
                max_gap: Duration::from_secs(30),
            },
        );

        let mut monitor = HeartbeatMonitor::new(config);
        let sensor = ObserverRef::new("obs:sensor");
        let past = Utc::now() - chrono::Duration::seconds(60);
        monitor.receive_heartbeat_at(sensor, alive_payload(), past);

        let alerts = monitor.detect_gaps_at(Utc::now());
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].threshold, Duration::from_secs(30));
    }

    #[test]
    fn tracked_count() {
        let mut monitor = default_monitor();
        assert_eq!(monitor.tracked_count(), 0);
        monitor.receive_heartbeat(ObserverRef::new("obs:a"), alive_payload());
        monitor.receive_heartbeat(ObserverRef::new("obs:b"), alive_payload());
        assert_eq!(monitor.tracked_count(), 2);
    }

    #[test]
    fn last_heartbeat_query() {
        let mut monitor = default_monitor();
        let obs = ObserverRef::new("obs:test");
        assert!(monitor.last_heartbeat(&obs).is_none());

        monitor.receive_heartbeat(obs.clone(), alive_payload());
        let hb = monitor.last_heartbeat(&obs).unwrap();
        assert_eq!(hb.status, ObserverStatus::Alive);
    }

    #[test]
    fn degraded_status_tracked() {
        let mut monitor = default_monitor();
        let obs = ObserverRef::new("obs:degraded");
        monitor.receive_heartbeat(
            obs.clone(),
            HeartbeatPayload {
                status: ObserverStatus::Degraded,
                last_successful_capture_at: None,
                pending_count: 42,
            },
        );
        let hb = monitor.last_heartbeat(&obs).unwrap();
        assert_eq!(hb.status, ObserverStatus::Degraded);
        assert_eq!(hb.pending_count, 42);
    }

    #[test]
    fn no_observers_no_alerts() {
        let monitor = default_monitor();
        assert!(monitor.detect_gaps().is_empty());
    }
}
