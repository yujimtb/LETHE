//! M01 Domain Kernel — Command & EffectPlan

use serde::{Deserialize, Serialize};

use super::types::WriteMode;
use super::values::*;

/// A normalised write intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub command_id: CommandId,
    pub issued_by: ActorRef,
    pub write_mode: WriteMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<EntityRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<EntityRef>,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_revision: Option<String>,
    pub idempotency_key: IdempotencyKey,
}

/// The effect derived from a Command.  No side-effects live here; execution
/// is delegated to the imperative shell.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "effect")]
pub enum EffectPlan {
    AppendCanonical { drafts: Vec<serde_json::Value> },
    AppendSupplemental { drafts: Vec<serde_json::Value> },
    SubmitReview { review: serde_json::Value },
    InvokeSourceNative { requests: Vec<serde_json::Value> },
    Materialize { request: serde_json::Value },
    EmitAudit { events: Vec<serde_json::Value> },
}
