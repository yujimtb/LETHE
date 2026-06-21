//! M12: Identity Resolution
//!
//! 名寄せ Projection — cross-source person matching.
//! Results are Projection-level (NOT canonical truth).

pub mod projector;
pub mod types;

pub use projector::IdentityProjector;
pub use types::*;
