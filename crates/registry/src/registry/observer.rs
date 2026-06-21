//! M02 Registry — Observer & SourceSystem definitions

use serde::{Deserialize, Serialize};

use lethe_core::domain::{
    AuthorityModel, CaptureModel, ObserverRef, ObserverType, SchemaRef, SemVer, SourceClass,
    SourceSystemRef, TrustLevel,
};

/// A registered Observer that captures observations from a source system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observer {
    /// `obs:{name}` format.
    pub id: ObserverRef,
    pub name: String,
    pub observer_type: ObserverType,
    pub source_system: SourceSystemRef,
    pub adapter_version: SemVer,
    /// Schemas this observer is authorised to emit (`"*"` = any).
    #[serde(default)]
    pub schemas: Vec<SchemaRef>,
    pub authority_model: AuthorityModel,
    pub capture_model: CaptureModel,
    pub owner: String,
    pub trust_level: TrustLevel,
}

/// An external data source (e.g. Slack workspace, Google Drive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSystem {
    /// `sys:{name}` format.
    pub id: SourceSystemRef,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    pub source_class: SourceClass,
}
