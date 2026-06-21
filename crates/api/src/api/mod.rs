//! M14: API Serving
//!
//! Read mode resolution, response envelope with projection metadata,
//! filtering-before-exposure middleware, pagination, health endpoint.

pub mod envelope;
pub mod health;
pub mod pagination;
pub mod read_mode;

pub use envelope::{ProjectionMetadata, ResponseEnvelope};
pub use health::{HealthResponse, ProjectionHealthInfo};
pub use pagination::{PaginatedResponse, PaginationParams};
pub use read_mode::{ReadModeError, ReadModeResolver};
