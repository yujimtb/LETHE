//! M06: DAG Propagation
//!
//! Watermark management, incremental propagation scheduler,
//! topological order execution, and health status tracking.

pub mod scheduler;
pub mod watermark;
pub mod idempotent;

pub use idempotent::{assert_at_least_once_idempotent, IdempotentFold};
pub use scheduler::{LeafTail, PropagationScheduler};
pub use watermark::{LeafWatermarkState, WatermarkState, WatermarkStore};
