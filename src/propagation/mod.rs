//! M06: DAG Propagation
//!
//! Watermark management, incremental propagation scheduler,
//! topological order execution, and health status tracking.

pub mod idempotent;
pub mod scheduler;
pub mod watermark;

pub use idempotent::{IdempotentFold, assert_at_least_once_idempotent};
pub use scheduler::{LeafTail, PropagationScheduler};
pub use watermark::{LeafWatermarkState, WatermarkState, WatermarkStore};
