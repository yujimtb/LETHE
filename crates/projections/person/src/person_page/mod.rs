//! M13: Person Page
//!
//! Person page projector and API payload builder.
//! Depends on M12 identity resolution output.

pub mod projector;
pub mod types;

pub use projector::PersonPageProjector;
pub use types::*;
