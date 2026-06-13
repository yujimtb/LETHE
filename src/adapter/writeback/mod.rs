//! SaaS Write-back Adapter — generic trait for writing projection outputs
//! to external SaaS services.
//!
//! This module provides the `SaaSWriteAdapter` trait that any external
//! destination (Notion, Airtable, etc.) can implement to receive
//! projection results.

pub mod image_crop;
pub mod notion;
pub mod traits;

pub use traits::*;
