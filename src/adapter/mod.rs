//! M09 Adapter Policy — Source adapter common patterns
//!
//! Provides:
//! - `SourceAdapter` trait (fetch/transform/heartbeat protocol)
//! - `AdapterConfig`, `FetchResult`, `Cursor`
//! - Idempotency key utilities
//! - Retry / backoff policy
//! - Heartbeat Observation generator

pub mod config;
pub mod conformance;
pub mod error;
pub mod heartbeat;
pub mod idempotency;
pub mod retry;
pub mod traits;

pub mod slack;
pub mod gslides;
pub mod writeback;

pub use config::*;
pub use conformance::*;
pub use error::*;
pub use heartbeat::*;
pub use idempotency::*;
pub use retry::*;
pub use traits::*;
