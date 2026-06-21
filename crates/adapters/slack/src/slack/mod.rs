//! M10 Slack Adapter — Slack message / channel / thread / edit / delete ingestion

pub mod client;
pub mod mapper;

pub use client::*;
pub use mapper::*;
